"""主入口：全局状态 + 主循环 + 定时任务注册。"""
import os
import re
import time
import logging
import logging.handlers
import threading

import schedule

from core.config import DEFAULT_CONFIG, get_runtime_dir

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("taobao_auto")

_config = dict(DEFAULT_CONFIG)
_tab = None
_stop_event = threading.Event()
_browser_lock = threading.Lock()
_task_lock = threading.Lock()
_qr_displayed = False


def init(cfg):
    """注入外部配置。"""
    global _config
    _config = dict(cfg)


def stop():
    """通知主循环优雅退出。"""
    _stop_event.set()
    schedule.clear()
    log.info("已发送停止信号")


def _setup_file_logging():
    """配置按天滚动的文件日志，只保留近三天。"""
    logger = logging.getLogger("taobao_auto")
    for h in logger.handlers:
        if isinstance(h, logging.handlers.TimedRotatingFileHandler):
            return
    log_dir = os.path.join(get_runtime_dir(), "logs")
    os.makedirs(log_dir, exist_ok=True)
    fh = logging.handlers.TimedRotatingFileHandler(
        os.path.join(log_dir, "taobao_auto.log"),
        when="midnight", backupCount=2, encoding="utf-8",
    )
    fh.suffix = "%Y%m%d"
    fh.extMatch = re.compile(r"^\d{8}(\.\w+)?$")
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(fh)


_task_running = {}


def _guarded(task_name, func):
    """防重入守卫：上一轮未完成时跳过本轮，异常不外泄。"""
    def wrapper():
        with _task_lock:
            if _task_running.get(task_name):
                log.debug("任务 %s 上一轮未完成，跳过", task_name)
                return
            _task_running[task_name] = True
        try:
            func()
        except Exception as e:
            log.error("定时任务 [%s] 执行异常: %s", task_name, e, exc_info=True)
        finally:
            with _task_lock:
                _task_running[task_name] = False
    return wrapper


def run_guarded_task(task_name, func):
    """供手动触发复用同一防重入逻辑。"""
    _guarded(task_name, func)()


def run():
    """脚本主流程，由面板子线程调用。"""
    from browser.driver import setup_browser
    from browser.login import (
        is_login_page, handle_login, poll_for_login,
        sync_cookies, check_and_solve_slider,
    )
    from core.notify import send_text_to_wechat
    from core import db as order_db

    _setup_file_logging()
    _stop_event.clear()
    schedule.clear()

    tab = setup_browser()
    tab.get(_config["target_url"])
    time.sleep(5)
    log.info("页面加载完成，当前 URL: %s", tab.url)

    if _stop_event.is_set():
        return

    if is_login_page(tab):
        log.info("当前在登录页，开始处理登录...")
        if not handle_login(tab):
            poll_for_login(tab)

    if _stop_event.is_set():
        return

    if is_login_page(tab):
        log.error("登录流程结束但仍在登录页，请检查账号状态")
        return

    global _qr_displayed
    log.info("已登录，开始同步 Cookie...")
    if _qr_displayed:
        send_text_to_wechat("淘宝扫码登录成功，开始同步数据。")
    else:
        log.info("本次登录通过缓存会话恢复，跳过扫码成功通知")
    _qr_displayed = False
    time.sleep(2)
    sync_cookies(tab)

    order_db.init_db()

    _register_scheduled_tasks()

    last_health_check = time.time()
    health_check_interval = 600

    while not _stop_event.is_set():
        try:
            schedule.run_pending()
        except Exception as e:
            log.error("定时任务调度异常: %s", e, exc_info=True)
        check_and_solve_slider()

        if time.time() - last_health_check >= health_check_interval:
            last_health_check = time.time()
            _check_login_health(tab)

        time.sleep(1)

    log.info("脚本已停止")


def _check_login_health(tab):
    """定期检查登录状态，检测到过期时自动触发重登录。"""
    from browser.login import is_login_page, trigger_relogin
    from browser.driver import get_tb_token

    try:
        if is_login_page(tab):
            log.warning("健康检查：当前处于登录页，触发重登录")
            with _browser_lock:
                trigger_relogin()
            return

        tb_token = get_tb_token(tab)
        if not tb_token:
            log.warning("健康检查：_tb_token_ 丢失，触发重登录")
            with _browser_lock:
                trigger_relogin()
    except Exception as e:
        log.warning("健康检查异常: %s", e)


def _register_scheduled_tasks():
    sched_time = _config["schedule_time"]
    from sync.campaigns import create_campaign
    schedule.every().day.at(sched_time).do(_guarded("create_campaign", create_campaign))
    log.info("已设置定时任务：每天 %s 创建活动", sched_time)

    from sync.campaigns import sync_campaigns
    schedule.every(1).hours.do(_guarded("sync_campaigns", sync_campaigns))
    log.info("已设置定时任务：每小时同步活动列表")

    from sync.products import (
        fetch_new_products,
        push_pending_products,
        sync_product_status_daily,
    )
    schedule.every(1).minutes.do(_guarded("fetch_products", fetch_new_products))
    schedule.every(20).seconds.do(_guarded("push_products", push_pending_products))
    product_sync_time = _config.get("product_full_sync_time", "04:00")
    schedule.every().day.at(product_sync_time).do(sync_product_status_daily)
    log.info(
        "已设置定时任务：每分钟增量拉取商品 + 每20秒推送队列，每天 %s 全量对账",
        product_sync_time,
    )

    from sync.risk_products import sync_risk_products
    interval_raw = _config.get("risk_product_sync_interval_seconds")
    if interval_raw is None:
        legacy_minutes = _config.get("risk_product_sync_interval_minutes")
        if legacy_minutes is not None:
            try:
                interval_raw = int(legacy_minutes) * 60
            except (TypeError, ValueError):
                interval_raw = 60
        else:
            interval_raw = 60
    try:
        interval = int(interval_raw)
    except (TypeError, ValueError):
        interval = 60
    interval = max(interval, 1)
    schedule.every(interval).seconds.do(_guarded("sync_risk_products", sync_risk_products))
    log.info("已设置定时任务：每 %d 秒同步风险商品（commissionPriority=4,5）", interval)

    from sync.orders import fetch_new_orders, push_pending_orders, sync_order_status_daily
    schedule.every(1).minutes.do(_guarded("fetch_orders", fetch_new_orders))
    schedule.every(20).seconds.do(_guarded("push_orders", push_pending_orders))
    order_sync_time = _config.get("order_status_sync_time", "03:00")
    schedule.every().day.at(order_sync_time).do(sync_order_status_daily)
    log.info(
        "已设置定时任务：每分钟增量拉取订单 + 每20秒推送队列，每天 %s 全量对账",
        order_sync_time,
    )

    from sync.tasks import process_tasks
    task_interval = _config.get("task_poll_interval", 10)
    schedule.every(task_interval).seconds.do(_guarded("process_tasks", process_tasks))
    log.info("已设置定时任务：每 %d 秒拉取并执行远程任务", task_interval)


def main():
    """独立运行入口（不通过面板时使用）。"""
    from core.config import load_config
    init(load_config())
    run()


if __name__ == "__main__":
    main()

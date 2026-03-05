"""订单同步模块：分钟级增量拉取 + 独立推送队列 + 每日状态对账。

依赖本地 SQLite（order_db）作为中间层，拉取与推送完全解耦。
"""
import json
import time
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import urlencode

import order_db
from sync_data import push_to_asyx

log = logging.getLogger("taobao_auto")

_MAX_RETRIES = 3
_CHECKPOINT_INTERVAL = 5  # 每5页更新一次水位线
_PAGE_INTERVAL = 2
_PER_PAGE_SIZE = 40
_PUSH_BATCH_SIZE = 500
_DEFAULT_OVERLAP_SECONDS = 30  # 默认重叠时间（秒）
_MIN_OVERLAP_SECONDS = 30  # 最小重叠时间
_MAX_OVERLAP_SECONDS = 120  # 最大重叠时间
_ORDER_QUEUE_HIGH_WATERMARK = 3000
_ORDER_QUEUE_MAX_WATERMARK = 12000

_ORDER_REFERER = (
    "https://fuwu.alimama.com/portal/v2/pages/report/"
    "publisher/order/index.htm"
)

_TIME_FMT = "%Y-%m-%d %H:%M:%S"


# ========== 底层：分页拉取 ==========

def _fetch_order_page(base_url, params, page_no):
    """拉取单页订单，失败时指数退避重试。"""
    from sync_data import _browser_get_json

    full_url = base_url + "?" + urlencode(params)

    for attempt in range(1, _MAX_RETRIES + 1):
        resp_text = _browser_get_json(full_url, _ORDER_REFERER)
        if not resp_text:
            if attempt < _MAX_RETRIES:
                wait = 2 ** attempt
                log.warning(
                    "订单第 %d 页无响应，%ds 后第 %d 次重试",
                    page_no, wait, attempt,
                )
                time.sleep(wait)
                continue
            log.error("订单第 %d 页拉取失败，已重试 %d 次", page_no, _MAX_RETRIES)
            return None

        try:
            result = json.loads(resp_text)
        except (json.JSONDecodeError, TypeError) as exc:
            if attempt < _MAX_RETRIES:
                wait = 2 ** attempt
                log.warning(
                    "订单第 %d 页 JSON 解析失败: %s，%ds 后重试",
                    page_no, exc, wait,
                )
                time.sleep(wait)
                continue
            log.error("订单第 %d 页 JSON 解析失败: %s", page_no, exc)
            return None

        if not result.get("success"):
            log.error(
                "订单接口返回失败 (第 %d 页): resultCode=%s",
                page_no, result.get("resultCode"),
            )
            return None
        return result.get("data", {})

    return None


def _iter_order_pages(start_time, end_time, keyword1=""):
    """生成器：逐页 yield 订单列表，支持游标深分页和优雅中断，动态调整请求间隔。"""
    import main

    if not main._tab:
        log.error("浏览器 tab 不可用")
        return

    tb_token = main.get_tb_token(main._tab)
    if not tb_token:
        log.error("未找到 _tb_token_，无法拉取订单")
        return

    base_url = main._config["order_list_api_url"]
    page_no = 1
    position_index = ""
    current_interval = _PAGE_INTERVAL
    consecutive_failures = 0

    while not main._stop_event.is_set():
        queue_depth = order_db.get_order_queue_count()
        if queue_depth >= _ORDER_QUEUE_MAX_WATERMARK:
            log.warning(
                "订单队列积压过高(%d)，提前停止本轮拉取，等待推送追平",
                queue_depth,
            )
            break

        params = {
            "t": str(int(time.time() * 1000)),
            "_tb_token_": tb_token,
            "toPage": str(page_no),
            "perPageSize": str(_PER_PAGE_SIZE),
            "startTime": start_time,
            "endTime": end_time,
            "textSearchType1": "ITEM_ID",
            "keyword1": keyword1,
            "textSearchType2": "CAMPAIGN_ID",
            "keyword2": "",
            "queryType": "1",
            "jumpType": "0" if page_no == 1 else "1",
            "positionIndex": position_index,
        }

        data = _fetch_order_page(base_url, params, page_no)
        if data is None:
            consecutive_failures += 1
            if consecutive_failures >= 3:
                current_interval = min(current_interval * 1.5, 10)
                log.warning("连续失败 %d 次，增加间隔至 %.1fs，终止本轮拉取", consecutive_failures, current_interval)
                break
            time.sleep(current_interval)
            continue

        consecutive_failures = 0
        if current_interval > _PAGE_INTERVAL:
            current_interval = max(current_interval * 0.8, _PAGE_INTERVAL)
            log.info("恢复间隔至 %.1fs", current_interval)

        if queue_depth >= _ORDER_QUEUE_HIGH_WATERMARK:
            current_interval = min(current_interval * 1.3, 8)
            log.info("订单队列 %d 条，拉取节流至 %.1fs/页", queue_depth, current_interval)

        has_next = data.get("hasNext", False)
        items = data.get("result", [])
        if items:
            yield items

        position_index = str(data.get("positionIndex", ""))
        if not has_next:
            break

        page_no += 1
        time.sleep(current_interval)


# ========== 批量入库处理 ==========

def _process_order_batch(page_items):
    """批量处理一页订单：批量 upsert + 入队。返回变化数。"""
    changed = order_db.upsert_and_enqueue_order_batch(page_items)
    return len(changed)


def _get_page_latest_create_time(page_items):
    """提取当前页最大 createTime，用于安全推进水位。"""
    latest = ""
    for item in page_items:
        create_time = item.get("createTime", "")
        if create_time and (not latest or create_time > latest):
            latest = create_time
    return latest


def _calc_lag_seconds(state_time):
    if not state_time:
        return -1
    try:
        return int((datetime.now() - datetime.strptime(state_time, _TIME_FMT)).total_seconds())
    except ValueError:
        return -1


# ========== 公开入口 1：增量拉取（每分钟）==========

def fetch_new_orders():
    """增量拉取新订单：从上次水位线到当前时间（秒级精度）。"""
    import main

    if not main._tab:
        log.error("浏览器 tab 不可用，增量订单同步终止")
        return

    browser_lock = main._browser_lock
    if not browser_lock.acquire(timeout=5):
        log.warning("浏览器锁被占用，跳过本轮增量拉取")
        return

    try:
        _do_fetch_new_orders()
    finally:
        browser_lock.release()


def _calculate_dynamic_overlap():
    """根据历史拉取耗时计算动态重叠时间。"""
    last_duration = order_db.get_sync_state("last_fetch_duration")
    if last_duration:
        try:
            duration_seconds = float(last_duration)
            overlap_seconds = min(
                max(duration_seconds * 1.5, _MIN_OVERLAP_SECONDS),
                _MAX_OVERLAP_SECONDS,
            )
            return int(overlap_seconds)
        except (ValueError, TypeError):
            pass
    return _DEFAULT_OVERLAP_SECONDS


def _do_fetch_new_orders():
    import main

    now = datetime.now()
    configured_start = (main._config.get("order_query_start_time") or "").strip()
    if configured_start:
        try:
            datetime.strptime(configured_start, _TIME_FMT)
            default_start = configured_start
        except ValueError:
            log.warning("订单查询起始时间格式错误: %s，使用今天作为默认值", configured_start)
            default_start = now.replace(hour=0, minute=0, second=0).strftime(_TIME_FMT)
    else:
        default_start = now.replace(hour=0, minute=0, second=0).strftime(_TIME_FMT)
    start_time = order_db.get_sync_state("last_incr_time", default_start)

    overlap_seconds = _calculate_dynamic_overlap()
    overlap = (
        datetime.strptime(start_time, _TIME_FMT) - timedelta(seconds=overlap_seconds)
    ).strftime(_TIME_FMT)
    end_time = now.strftime(_TIME_FMT)

    log.info("增量拉取订单: %s ~ %s (重叠 %ds)", overlap, end_time, overlap_seconds)

    fetch_start_time = time.time()

    total = 0
    new_count = 0
    page_count = 0
    last_confirmed_time = ""
    for page_items in _iter_order_pages(overlap, end_time):
        total += len(page_items)
        new_count += _process_order_batch(page_items)
        page_count += 1

        last_order_time = _get_page_latest_create_time(page_items)
        if last_order_time and last_order_time > last_confirmed_time:
            last_confirmed_time = last_order_time
            order_db.set_sync_state("last_incr_time", last_confirmed_time)
            if page_count % _CHECKPOINT_INTERVAL == 0:
                log.info("分段水位线已更新: %s (已处理 %d 页)", last_confirmed_time, page_count)

    if page_count == 0:
        # 本轮未成功处理任何分页，不推进水位，避免失败场景下跳过数据。
        log.warning("本轮未拉到可处理订单分页，保持上次订单水位不变")

    fetch_duration = time.time() - fetch_start_time
    order_db.set_sync_state("last_fetch_duration", str(round(fetch_duration, 1)))

    log.info("增量拉取完成，扫描 %d 条，新增/变化 %d 条，耗时 %.1fs", total, new_count, fetch_duration)
    queue_depth = order_db.get_order_queue_count()
    fetch_tpm = round(total / max(fetch_duration, 1) * 60, 1)
    lag_seconds = _calc_lag_seconds(order_db.get_sync_state("last_incr_time", ""))
    log.info(
        "METRIC orders_fetch throughput_fetch_per_min=%.1f queue_depth=%d fetch_lag_seconds=%d",
        fetch_tpm, queue_depth, lag_seconds,
    )


# ========== 公开入口 2：推送待发队列（每分钟）==========

def push_pending_orders():
    """循环消费订单推送队列直到清空，支持指数退避重试。"""
    import main

    total_pushed = 0
    total_retries = 0
    total_batches = 0
    failed_batches = 0
    start_ts = time.time()
    while True:
        batch = order_db.dequeue_orders(_PUSH_BATCH_SIZE)
        if not batch:
            break

        total_batches += 1
        ids = [row[0] for row in batch]
        orders = [row[1] for row in batch]

        pushed, used_attempts = _push_order_batch_with_retry(
            orders, main._config["order_save_api_url"], _PUSH_BATCH_SIZE,
        )
        total_retries += max(used_attempts - 1, 0)
        if pushed >= len(ids):
            order_db.delete_from_queue(ids)
            total_pushed += pushed
            log.info("订单推送成功 %d 条，已从队列删除", pushed)
        elif pushed > 0:
            # 仅删除已确认成功的前缀记录，避免部分成功场景误删未成功数据。
            success_ids = ids[:pushed]
            remain_ids = ids[pushed:]
            order_db.delete_from_queue(success_ids)
            order_db.release_order_queue(remain_ids)
            total_pushed += pushed
            log.warning(
                "订单批次部分成功：成功 %d / %d，仅删除成功记录，其余保留重试",
                pushed, len(ids),
            )
            break
        else:
            failed_batches += 1
            order_db.release_order_queue(ids)
            log.warning("订单推送失败，%d 条保留在队列等待重试", len(ids))
            break

    if total_pushed:
        log.info("订单推送队列消费完成，共推送 %d 条", total_pushed)
    elapsed = max(time.time() - start_ts, 1)
    push_tpm = round(total_pushed / elapsed * 60, 1)
    retry_rate = round(total_retries / max(total_batches, 1), 2)
    error_rate = round(failed_batches / max(total_batches, 1), 2)
    log.info(
        "METRIC orders_push throughput_push_per_min=%.1f retry_rate=%.2f error_rate=%.2f queue_depth=%d",
        push_tpm, retry_rate, error_rate, order_db.get_order_queue_count(),
    )


def _push_order_batch_with_retry(orders, api_url, batch_size):
    """推送订单批次，失败时指数退避重试。返回(成功数, 实际尝试次数)。"""
    for attempt in range(1, _MAX_RETRIES + 1):
        pushed = push_to_asyx(orders, api_url, batch_size)
        if pushed > 0:
            return pushed, attempt

        if attempt < _MAX_RETRIES:
            wait = 2 ** attempt
            log.warning(
                "订单推送失败，%ds 后第 %d 次重试",
                wait, attempt,
            )
            time.sleep(wait)

    return 0, _MAX_RETRIES


# ========== 公开入口 3：每日状态对账 ==========

def sync_order_status_daily():
    """每日全量对账：0~15 天全量 + 超龄订单按天分组查 + 清理非付款订单。"""
    import main

    if not main._tab:
        log.error("浏览器 tab 不可用，每日状态对账终止")
        return

    browser_lock = main._browser_lock
    if not browser_lock.acquire(timeout=10):
        log.warning("浏览器锁被占用，跳过本轮每日对账")
        return

    try:
        _do_daily_sync()
    finally:
        browser_lock.release()


def _do_daily_sync():
    import main

    log.info("===== 开始每日订单状态对账 =====")
    full_range = int(main._config.get("order_full_sync_range_days", 15))

    _phase_recent_orders(full_range)

    if not main._stop_event.is_set():
        _phase_stale_orders(full_range)

    cleaned = order_db.cleanup_non_paid()
    log.info(
        "清理非付款状态订单 %d 条，status 表剩余 %d 条",
        cleaned, order_db.get_order_status_count(),
    )
    log.info("===== 每日订单状态对账完成 =====")

    push_pending_orders()


def _phase_recent_orders(full_range):
    """阶段一：按天分段拉取 0~N 天的全量订单，批量更新状态。"""
    import main

    now = datetime.now()
    total = 0
    changed = 0

    for day_offset in range(full_range, -1, -1):
        if main._stop_event.is_set():
            log.info("收到停止信号，中断近期订单对账")
            break

        day = now - timedelta(days=day_offset)
        seg_start = day.replace(hour=0, minute=0, second=0).strftime(_TIME_FMT)
        seg_end = day.replace(hour=23, minute=59, second=59).strftime(_TIME_FMT)

        log.info("对账日期: %s", day.strftime("%Y-%m-%d"))
        for page_items in _iter_order_pages(seg_start, seg_end):
            total += len(page_items)
            changed += _process_order_batch(page_items)

        time.sleep(1)

    log.info("阶段一完成：扫描 %d 条，状态变化 %d 条", total, changed)


def _phase_stale_orders(full_range):
    """阶段二：超龄订单按 create_time 日期分组，同一天只拉取一次全量。"""
    import main

    stale = order_db.get_stale_orders(days=full_range)
    if not stale:
        log.info("阶段二：无超龄订单需要处理")
        return

    date_groups = defaultdict(set)
    for row in stale:
        create_time = row.get("create_time", "")
        if not create_time:
            continue
        date_groups[create_time[:10]].add(row["tb_trade_id"])

    log.info(
        "阶段二：%d 条超龄订单分布在 %d 个日期",
        len(stale), len(date_groups),
    )

    changed = 0
    for idx, (date_str, trade_ids) in enumerate(sorted(date_groups.items())):
        if main._stop_event.is_set():
            log.info("收到停止信号，中断超龄订单查询")
            break

        seg_start = date_str + " 00:00:00"
        seg_end = date_str + " 23:59:59"
        log.info("超龄对账日期: %s（待匹配 %d 条）", date_str, len(trade_ids))

        trade_ids_set = set(str(tid) for tid in trade_ids)
        for page_items in _iter_order_pages(seg_start, seg_end):
            matched = [
                o for o in page_items
                if str(o.get("tbTradeId", "")) in trade_ids_set
            ]
            if matched:
                changed += _process_order_batch(matched)

        if (idx + 1) % 5 == 0:
            log.info("超龄日期进度: %d/%d", idx + 1, len(date_groups))

        time.sleep(1)

    log.info("阶段二完成：涉及 %d 个日期，状态变化 %d 条", len(date_groups), changed)

"""登录流程 + 重登录 + Cookie 同步。"""
import time
import threading
import logging
from urllib.parse import urlparse

from browser.slider import js_click, js_exists, check_and_solve_slider as _solve_slider
from browser.driver import get_cookie_str, get_tb_token
from core.notify import send_screenshot_to_wechat, send_text_to_wechat
from core.http_client import asyx_authed_request

log = logging.getLogger("taobao_auto")

_relogin_lock = threading.Lock()
_last_relogin_time = 0
_RELOGIN_COOLDOWN = 60


def is_login_page(tab):
    """判断当前是否在登录页（仅根据 URL 判断）。"""
    parsed = urlparse(tab.url)
    if parsed.path.strip("/") == "":
        return True
    if "forward=" in (parsed.query or ""):
        return True
    return False


def get_login_frame(tab):
    """获取页面中唯一的登录 iframe。"""
    try:
        frame = tab.get_frame("tag:iframe")
        if frame:
            log.info("已切入登录 iframe")
            return frame
    except Exception as e:
        log.error("获取 iframe 失败: %s", e)
    return None


def check_and_solve_slider():
    """检测主页面和所有 iframe 中的滑块验证码并自动处理。"""
    import main

    if not main._tab:
        return False
    return _solve_slider(main._tab, main._config)


def _wait_qr_rendered(ctx, timeout=15):
    """等待二维码图片渲染完成，最多等 timeout 秒。返回是否成功。"""
    js_check = (
        'var container = document.querySelector("#qrcode-img");'
        'if(!container) return false;'
        'var img = container.querySelector("img");'
        'if(img && img.naturalWidth > 0 && img.complete) return true;'
        'var canvas = container.querySelector("canvas");'
        'if(canvas && canvas.width > 0) return true;'
        'return false;'
    )
    for i in range(timeout * 2):
        try:
            if ctx.run_js(js_check):
                log.info("二维码已渲染完成（等待 %.1fs）", (i + 1) * 0.5)
                return True
        except Exception:
            return False
        time.sleep(0.5)
    log.warning("等待二维码渲染超时（%ds），将使用当前页面截图", timeout)
    return False


def _try_click_ready(tab, frame):
    """在主页面和 iframe 中都尝试点击 Ready 按钮。"""
    import main

    selector = main._config["selector_ready_btn"]
    try:
        if js_click(tab, selector):
            return True
    except Exception:
        pass
    try:
        if frame and js_click(frame, selector):
            return True
    except Exception:
        pass
    return False


def _wait_url_leave_login(tab, timeout=30):
    """点击 Ready 按钮后，持续等待 URL 真正离开登录页，返回是否成功。"""
    import main

    for _ in range(timeout):
        if main._stop_event.is_set():
            return False
        if not is_login_page(tab):
            return True
        time.sleep(1)
    return False


def handle_login(tab):
    """处理登录流程：点击二维码切换、截图、检测刷新按钮。"""
    import main

    frame = get_login_frame(tab)
    search_ctx = frame or tab
    qr_toggle = main._config["selector_qr_toggle"]
    qr_refresh = main._config["selector_qr_refresh"]

    for _ in range(20):
        check_and_solve_slider()
        if js_exists(search_ctx, qr_toggle):
            break
        if _try_click_ready(tab, frame):
            log.info("检测到 Ready 按钮并点击，等待页面跳转...")
            if _wait_url_leave_login(tab):
                log.info("登录成功（URL 已跳转）")
                return True
            log.warning("点击 Ready 后页面未跳转，继续监测")
        time.sleep(0.5)

    if js_click(search_ctx, qr_toggle):
        log.info("已点击二维码切换按钮，等待二维码渲染...")
        main._qr_displayed = True
        time.sleep(1)
        _wait_qr_rendered(search_ctx)
        time.sleep(0.5)
        send_screenshot_to_wechat(tab)

        if js_click(search_ctx, qr_refresh):
            log.info("二维码已过期，已点击刷新，等待重新渲染...")
            time.sleep(1)
            _wait_qr_rendered(search_ctx)
            time.sleep(0.5)
            send_screenshot_to_wechat(tab)

    if _try_click_ready(tab, frame):
        log.info("检测到 Ready 按钮并点击，等待页面跳转...")
        if _wait_url_leave_login(tab):
            log.info("登录成功（URL 已跳转）")
            return True
        log.warning("点击 Ready 后页面未跳转，进入持续监测")
    return False


def poll_for_login(tab):
    """持续监测 Ready 按钮和登录状态，直到确认 URL 真正跳转。"""
    import main

    log.info("开始持续监测 Ready 按钮和登录状态...")
    frame = get_login_frame(tab)
    search_ctx = frame or tab
    qr_refresh = main._config["selector_qr_refresh"]

    while not main._stop_event.is_set():
        try:
            if not is_login_page(tab):
                log.info("登录成功（URL 已跳转）")
                return

            if _try_click_ready(tab, frame):
                log.info("监测到 Ready 按钮，已触发点击")
                if _wait_url_leave_login(tab, timeout=10):
                    log.info("登录成功（URL 已跳转）")
                    return
                log.warning("点击 Ready 后页面未跳转，继续监测...")
                continue

            if js_click(search_ctx, qr_refresh):
                main._qr_displayed = True
                log.info("轮询中发现二维码过期，已刷新")
                time.sleep(1)
                _wait_qr_rendered(search_ctx)
                time.sleep(0.5)
                send_screenshot_to_wechat(tab)

            time.sleep(1)
        except Exception as e:
            log.info("扫码后页面跳转（%s），等待确认...", type(e).__name__)
            time.sleep(3)
            if not is_login_page(tab):
                log.info("登录成功（URL 已跳转）")
                return


def sync_cookies(tab):
    """获取所有 cookie 并同步到远端 API。"""
    import main

    cookie_str = get_cookie_str(tab)
    payload = [{"label": "plugin.taobao.cookies", "value": cookie_str}]
    try:
        resp = asyx_authed_request(
            "PUT", main._config["cookie_api_url"], json=payload, timeout=30,
        )
        if not resp:
            log.error("无法获取 asyx token，跳过 Cookie 同步")
    except Exception as e:
        log.error("Cookie 同步失败: %s", e)


def trigger_relogin():
    """检测到会话过期时，主动跳转到阿里妈妈首页触发重新登录。
    内置冷却机制，60 秒内不会重复触发。调用方需自行持有 _browser_lock。
    """
    import main

    global _last_relogin_time
    with _relogin_lock:
        if time.time() - _last_relogin_time < _RELOGIN_COOLDOWN:
            log.info("重登录冷却中，跳过本次触发")
            return False
        _last_relogin_time = time.time()

    if not main._tab:
        return False

    log.info("检测到登录过期，正在跳转到登录页重新登录...")
    send_text_to_wechat("淘宝登录已过期，正在重新登录，请及时扫码。")
    main._tab.get("https://fuwu.alimama.com")
    time.sleep(5)

    if is_login_page(main._tab):
        if not handle_login(main._tab):
            poll_for_login(main._tab)
        if is_login_page(main._tab):
            log.error("重新登录失败，仍在登录页")
            return False

    log.info("重新登录成功")
    sync_cookies(main._tab)
    return True

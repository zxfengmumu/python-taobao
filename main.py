import os
import time
import json
import base64
import hashlib
import logging
import logging.handlers
import random
import threading
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlencode

import requests
import schedule
from DrissionPage import Chromium, ChromiumOptions

from config import DEFAULT_CONFIG, get_runtime_dir, get_bundle_dir

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
_CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/140.0.0.0 Safari/537.36"
)


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
    import re
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


def _save_http_detail(tag, method, url, content):
    """将超长的请求体/响应体保存到独立文件，返回文件路径。"""
    detail_dir = os.path.join(get_runtime_dir(), "logs", "http_details")
    os.makedirs(detail_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    safe_path = urlparse(url).path.replace("/", "_")[-60:]
    filename = f"{ts}_{method.upper()}_{tag}{safe_path}.txt"
    filepath = os.path.join(detail_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    return filepath


def _logged_request(method, url, **kwargs):
    """发起 HTTP 请求，详细记录请求路径、参数和响应。"""
    headers = kwargs.get("headers") or {}
    headers.setdefault("User-Agent", _CHROME_UA)
    kwargs["headers"] = headers

    max_inline = 2000
    parts = [f">>> HTTP {method.upper()} {url}"]
    if kwargs.get("params"):
        parts.append(f"  Params: {json.dumps(_mask_sensitive_data(kwargs['params']), ensure_ascii=False)}")
    body = kwargs.get("json") or kwargs.get("data")
    if body is not None and _config.get("debug_http_body", False):
        body_safe = _mask_sensitive_data(body)
        body_s = json.dumps(body_safe, ensure_ascii=False) if isinstance(body_safe, (dict, list)) else str(body_safe)
        if len(body_s) > max_inline:
            detail_path = _save_http_detail("req", method, url, body_s)
            parts.append(f"  Body: {body_s[:max_inline]}...(truncated, full: {detail_path})")
        else:
            parts.append(f"  Body: {body_s}")
    elif body is not None:
        parts.append("  Body: <hidden>")
    log.info("\n".join(parts))

    resp = requests.request(method, url, **kwargs)

    resp_text = resp.text
    safe_resp_text = _mask_sensitive_text(resp_text)
    if len(resp_text) > max_inline:
        detail_path = _save_http_detail("resp", method, url, safe_resp_text)
        log.info(
            "<<< HTTP %s %s | Status: %s\n  Response: %s...(truncated, full: %s)",
            method.upper(), url, resp.status_code, safe_resp_text[:max_inline], detail_path,
        )
    else:
        log.info(
            "<<< HTTP %s %s | Status: %s\n  Response: %s",
            method.upper(), url, resp.status_code, safe_resp_text,
        )
    return resp


def _mask_sensitive_data(data):
    """脱敏常见敏感字段，避免日志泄漏凭据。"""
    sensitive_keys = {
        "password", "asyx_password", "token", "authorization",
        "cookie", "cookies", "webhook", "key",
    }
    if isinstance(data, dict):
        masked = {}
        for k, v in data.items():
            key_l = str(k).lower()
            if any(s in key_l for s in sensitive_keys):
                masked[k] = "***"
            else:
                masked[k] = _mask_sensitive_data(v)
        return masked
    if isinstance(data, list):
        return [_mask_sensitive_data(v) for v in data]
    return data


def _mask_sensitive_text(text):
    """对纯文本响应进行粗粒度脱敏。"""
    if not isinstance(text, str):
        return text
    masked = text
    for key in ("password", "token", "authorization", "cookie", "set-cookie"):
        masked = masked.replace(f'"{key}":"', f'"{key}":"***')
        masked = masked.replace(f'"{key}": "', f'"{key}": "***')
    return masked


def setup_browser():
    """初始化浏览器，设置缓存路径和调试端口。"""
    global _tab
    port = int(_config["browser_port"])
    co = (
        ChromiumOptions()
        .set_local_port(port)
        .set_user_data_path(_config["browser_cache_path"])
        .set_argument("--disable-background-timer-throttling")
        .set_argument("--disable-backgrounding-occluded-windows")
        .set_argument("--disable-renderer-backgrounding")
    )
    browser = Chromium(co)
    _tab = browser.latest_tab
    log.info("浏览器已启动，端口 %s", port)
    return _tab


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


def _js_safe(css_selector):
    """将 CSS 选择器中的双引号替换为单引号，避免嵌入 JS 字符串时引号冲突。"""
    return css_selector.replace('"', "'")


def js_click(ctx, css_selector):
    """用 JS querySelector 查找并点击元素，成功返回 True。"""
    safe = _js_safe(css_selector)
    return bool(ctx.run_js(
        f'var el = document.querySelector("{safe}");'
        'if(el){el.click(); return true;} return false;'
    ))


def js_exists(ctx, css_selector):
    """用 JS querySelector 检测元素是否存在。"""
    safe = _js_safe(css_selector)
    return bool(ctx.run_js(
        f'return !!document.querySelector("{safe}")'
    ))


def js_visible(ctx, css_selector):
    """用 JS 检测元素是否存在且可见（有尺寸、未隐藏）。"""
    safe = _js_safe(css_selector)
    return bool(ctx.run_js(
        f'var el = document.querySelector("{safe}");'
        'if(!el) return false;'
        'var r = el.getBoundingClientRect();'
        'if(r.width === 0 || r.height === 0) return false;'
        'var s = getComputedStyle(el);'
        'return s.display !== "none" && s.visibility !== "hidden";'
    ))


def _detect_slider_fail(ctx):
    """检测滑块验证是否失败，查找并点击重试/刷新按钮。"""
    selector = _config.get("selector_slider_fail", "#nc_1_refresh1")
    if js_visible(ctx, selector) and js_click(ctx, selector):
        log.info("通过 JS 点击了滑块重试按钮: %s", selector)
        return True
    try:
        ele = ctx.ele(selector, timeout=0)
        if ele:
            ele.click()
            log.info("通过 DrissionPage 点击了滑块重试按钮: %s", selector)
            return True
    except Exception:
        pass
    return False


def check_and_solve_slider():
    """检测主页面和所有 iframe 中的滑块验证码并自动处理。

    检测策略（按优先级）：
    1. 主页面 DOM —— 风控弹窗可能直接渲染在页面中
    2. 所有 iframe —— 登录页等场景验证码在 iframe 内
    返回 True 表示检测到并处理了滑块。
    """
    if not _tab:
        return False

    try:
        if _try_solve_slider_in(_tab):
            return True
    except Exception as e:
        log.warning("主页面滑块检测异常: %s", e)

    for frame in _iter_all_frames(_tab):
        try:
            if _try_solve_slider_in(frame):
                return True
        except Exception as e:
            log.warning("iframe 滑块检测异常: %s", e)

    return False


def _iter_all_frames(tab):
    """遍历页面中所有 iframe，返回 frame 对象的生成器。"""
    try:
        iframe_eles = tab.eles("tag:iframe")
        if not iframe_eles:
            return
        for ele in iframe_eles:
            try:
                frame = tab.get_frame(ele)
                if frame:
                    yield frame
            except Exception:
                continue
    except Exception:
        try:
            frame = tab.get_frame("tag:iframe")
            if frame:
                yield frame
        except Exception:
            pass


def _try_solve_slider_in(ctx):
    """在指定上下文（主页面或 iframe）中检测并处理滑块，成功返回 True。"""
    sel_slider = _config.get("selector_slider", "#nc_1_n1z")
    sel_container = _config.get("selector_slider_container", "#nc_1__scale_text")

    if _detect_slider_fail(ctx):
        log.info("检测到滑块验证失败，点击刷新重试...")
        time.sleep(1)
        return True

    if not js_exists(ctx, sel_container):
        return False

    if not js_visible(ctx, sel_slider):
        return False

    slider = ctx.ele(sel_slider, timeout=0)
    if not slider:
        return False
    container = ctx.ele(sel_container, timeout=0)
    if not container:
        return False

    log.info("检测到滑块验证码弹窗，正在自动拖动...")
    container_w = container.rect.size[0]
    slider_w = slider.rect.size[0]
    distance = int(container_w - slider_w) + random.randint(3, 10)
    log.info("容器宽度=%d, 滑块宽度=%d, 拖动距离=%d", container_w, slider_w, distance)

    _human_drag(slider, distance)
    time.sleep(1.5)
    log.info("滑块验证码拖动完成")

    if _detect_slider_fail(ctx):
        log.info("拖动后检测到验证失败，点击刷新重试...")
        time.sleep(1)

    return True


def _human_drag(slider, distance):
    """模拟人类拖动滑块：ease-in-out 曲线产生加速→匀速→减速效果。"""
    actions = _tab.actions
    actions.move_to(slider)
    time.sleep(random.uniform(0.03, 0.08))
    actions.hold()
    time.sleep(random.uniform(0.02, 0.05))

    track = _build_slide_track(distance)
    for dx, dy, dt in track:
        actions.move(dx, dy)
        time.sleep(dt)

    time.sleep(random.uniform(0.02, 0.06))
    actions.release()


def _build_slide_track(distance):
    """构建 ease-in-out 滑动轨迹，返回 [(dx, dy, sleep_sec), ...]。

    使用三次贝塞尔缓动曲线：开始慢、中间快、结束慢，
    配合随机 Y 轴微小抖动和步间时间波动，模拟真人手感。
    """
    num_steps = random.randint(12, 18)
    total_time = random.uniform(0.3, 0.6)
    base_dt = total_time / num_steps

    positions = []
    for i in range(num_steps + 1):
        t = i / num_steps
        if t < 0.5:
            ease = 4 * t * t * t
        else:
            ease = 1 - pow(-2 * t + 2, 3) / 2
        positions.append(round(ease * distance))

    track = []
    for i in range(1, len(positions)):
        dx = positions[i] - positions[i - 1]
        if dx <= 0:
            continue
        dy = random.choice([-1, 0, 0, 0, 0, 0, 1])
        dt = base_dt * random.uniform(0.8, 1.2)
        track.append((dx, dy, max(dt, 0.005)))

    remaining = distance - positions[-1]
    if remaining > 0:
        track.append((remaining, 0, base_dt))

    return track


def _cookie_nv(c):
    """从 cookie 对象中提取 name 和 value。"""
    if isinstance(c, dict):
        return c["name"], c["value"]
    return c.name, c.value


def get_cookie_str(tab):
    """将浏览器所有 cookie 格式化为字符串。"""
    return "; ".join(
        f"{n}={v}" for n, v in (_cookie_nv(c) for c in tab.cookies())
    )


def get_tb_token(tab):
    """从 cookie 中提取 _tb_token_ 的值。"""
    for c in tab.cookies():
        name, value = _cookie_nv(c)
        if name == "_tb_token_":
            return value
    return None


def send_screenshot_to_wechat(tab):
    """全页截图并发送到企业微信机器人。"""
    try:
        img_bytes = tab.get_screenshot(as_bytes="png")
    except Exception as e:
        log.error("截图失败（页面可能已跳转）: %s", e)
        return
    payload = {
        "msgtype": "image",
        "image": {
            "base64": base64.b64encode(img_bytes).decode(),
            "md5": hashlib.md5(img_bytes).hexdigest(),
        },
    }
    try:
        _logged_request("POST", _config["wechat_webhook"], json=payload, timeout=30)
    except Exception as e:
        log.error("发送截图失败: %s", e)


def send_text_to_wechat(content):
    """发送文本消息到企业微信机器人。"""
    webhook = _config.get("wechat_webhook", "")
    if not webhook:
        log.warning("wechat_webhook 未配置，跳过文本消息发送")
        return
    payload = {
        "msgtype": "text",
        "text": {"content": content},
    }
    try:
        _logged_request("POST", webhook, json=payload, timeout=30)
    except Exception as e:
        log.error("发送文本消息失败: %s", e)


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
    selector = _config["selector_ready_btn"]
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
    for _ in range(timeout):
        if _stop_event.is_set():
            return False
        if not is_login_page(tab):
            return True
        time.sleep(1)
    return False


def handle_login(tab):
    """处理登录流程：点击二维码切换、截图、检测刷新按钮。"""
    frame = get_login_frame(tab)
    search_ctx = frame or tab
    qr_toggle = _config["selector_qr_toggle"]
    qr_refresh = _config["selector_qr_refresh"]

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

    global _qr_displayed
    if js_click(search_ctx, qr_toggle):
        log.info("已点击二维码切换按钮，等待二维码渲染...")
        _qr_displayed = True
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
    log.info("开始持续监测 Ready 按钮和登录状态...")
    frame = get_login_frame(tab)
    search_ctx = frame or tab
    qr_refresh = _config["selector_qr_refresh"]

    while not _stop_event.is_set():
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
                global _qr_displayed
                _qr_displayed = True
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


_asyx_token = None


def _get_asyx_token():
    """获取 asyx token，优先返回缓存，无缓存时调用登录接口。"""
    global _asyx_token
    if _asyx_token:
        return _asyx_token
    return _login_asyx()


def _login_asyx():
    """登录 asyx 平台获取 Bearer Token 并缓存。"""
    global _asyx_token
    body = {
        "username": _config["asyx_username"],
        "password": _config["asyx_password"],
    }
    try:
        resp = _logged_request(
            "POST", _config["asyx_login_url"], json=body,
            headers={"User-Agent": "Hutool"}, timeout=30,
        )
        data = resp.json()
        token = data.get("token", "")
        if token:
            _asyx_token = token
            log.info("ASYX 登录成功，token 已缓存")
            return token
        log.error("asyx 登录返回异常: %s", data)
    except Exception as e:
        log.error("asyx 登录失败: %s", e)
    return None


def _is_token_expired(resp):
    """检查响应是否表示 token 过期（HTTP 401 或 JSON code 401）。"""
    if resp.status_code == 401:
        return True
    try:
        if resp.json().get("code") == 401:
            return True
    except Exception:
        pass
    return False


def asyx_authed_request(method, url, **kwargs):
    """发送带 ASYX 认证的请求，遇 401 自动刷新 token 重试一次。"""
    global _asyx_token
    token = _get_asyx_token()
    if not token:
        return None

    headers = kwargs.pop("headers", None) or {}
    headers.setdefault("User-Agent", "Hutool")
    headers.setdefault("Content-Type", "application/json")
    headers["Authorization"] = f"Bearer {token}"
    kwargs["headers"] = headers

    resp = _logged_request(method, url, **kwargs)

    if _is_token_expired(resp):
        log.info("ASYX token 已过期，重新登录...")
        _asyx_token = None
        token = _get_asyx_token()
        if not token:
            return None
        headers["Authorization"] = f"Bearer {token}"
        resp = _logged_request(method, url, **kwargs)

    return resp


def sync_cookies(tab):
    """获取所有 cookie 并同步到远端 API。"""
    cookie_str = get_cookie_str(tab)
    payload = [{"label": "plugin.taobao.cookies", "value": cookie_str}]
    try:
        resp = asyx_authed_request(
            "PUT", _config["cookie_api_url"], json=payload, timeout=30,
        )
        if not resp:
            log.error("无法获取 asyx token，跳过 Cookie 同步")
    except Exception as e:
        log.error("Cookie 同步失败: %s", e)


def fetch_category_commissions(tab):
    """从模板配置接口动态获取类目佣金数据。"""
    tb_token = get_tb_token(tab)
    if not tb_token:
        log.error("未找到 _tb_token_，无法获取类目佣金")
        return None

    params = {
        "t": str(int(time.time() * 1000)),
        "_tb_token_": tb_token,
        "campaignTemplateId": "6",
        "cooperAgreementId": "",
        "invitationId": "",
    }
    headers = {
        "accept": "*/*",
        "cookie": get_cookie_str(tab),
        "referer": "https://fuwu.alimama.com/",
    }

    try:
        resp = _logged_request(
            "GET", _config["template_config_api_url"],
            params=params, headers=headers, timeout=30,
        )
        result = resp.json()
    except Exception as e:
        log.error("获取模板配置失败: %s", e)
        return None

    if not result.get("success"):
        log.error("模板配置接口返回失败: %s", result.get("resultCode"))
        return None

    return _parse_cat_commissions(result)


def _parse_cat_commissions(result):
    """从模板配置响应中解析类目佣金列表。"""
    for rule_inst in result["data"]["ruleInstanceList"]:
        if rule_inst.get("basicRuleCode") != "templateNormalCatCommissionRule":
            continue
        for rule in rule_inst.get("ruleList", []):
            if rule.get("ruleCode") != "templateNormalCatCommissionRule":
                continue
            fv = rule["featureValue"]
            root_cats = json.loads(fv["rootCats"])
            threshold = json.loads(fv["threshold"])
            cat_list = []
            for group in root_cats:
                for cat in group.get("subCats", []):
                    cat_id_str = str(cat["catId"])
                    rate = threshold.get(cat_id_str, {})
                    rate_val = rate.get("minCommissionRate", 0.5)
                    if isinstance(rate_val, float) and rate_val.is_integer():
                        rate_val = int(rate_val)
                    cat_list.append({
                        "rootCatId": cat["catId"],
                        "rootCatName": cat["catName"],
                        "minNormalCommissionRate": rate_val,
                    })
            log.info("动态获取到 %d 个类目佣金配置", len(cat_list))
            return cat_list

    log.error("未在模板配置中找到 templateNormalCatCommissionRule")
    return None


def _build_cat_commission_value(tab, template_data):
    """构建类目佣金规则值：优先从接口获取，失败则用模板兜底。"""
    cat_commissions = fetch_category_commissions(tab)
    for rule in template_data.get("campaignRuleInstanceList", []):
        if rule.get("ruleCode") != "campaignNormalCatCommissionRule":
            continue
        if cat_commissions:
            rule["featureValue"]["value"] = json.dumps(
                cat_commissions, ensure_ascii=False, separators=(",", ":")
            )
        else:
            log.warning("动态获取类目佣金失败，使用模板静态数据兜底")
            val = rule["featureValue"]["value"]
            if isinstance(val, list):
                rule["featureValue"]["value"] = json.dumps(
                    val, ensure_ascii=False, separators=(",", ":")
                )


def _browser_post_form(tab, url, form_data, referer):
    """通过浏览器 fetch 发送表单 POST，确保请求特征与真实浏览器一致。"""
    try:
        tab.set.activate()
    except Exception:
        pass

    body_encoded = urlencode(form_data)
    body_b64 = base64.b64encode(body_encoded.encode("ascii")).decode("ascii")

    tab.run_js(
        'window.__fs={done:false,result:""};'
        'window.__fb=atob("' + body_b64 + '");'
    )

    tab.run_js(
        'fetch("' + url + '",{'
        'method:"POST",'
        'headers:{"accept":"*/*","accept-language":"zh-CN,zh;q=0.9",'
        '"bx-v":"2.5.11","cache-control":"no-cache",'
        '"content-type":"application/x-www-form-urlencoded; charset=UTF-8",'
        '"pragma":"no-cache","x-requested-with":"XMLHttpRequest"},'
        'referrer:"' + referer + '",'
        'body:window.__fb,credentials:"include"'
        '}).then(function(r){return r.text();})'
        '.then(function(t){window.__fs={done:true,result:t};})'
        '.catch(function(e){'
        'window.__fs={done:true,result:JSON.stringify({error:e.message})};'
        '});'
    )

    for i in range(60):
        if tab.run_js("return window.__fs.done"):
            result = tab.run_js(
                "var r=window.__fs.result;"
                "delete window.__fs;delete window.__fb;"
                "return r;"
            )
            return result
        if (i + 1) % 20 == 0:
            check_and_solve_slider()
        time.sleep(0.5)

    log.error("浏览器 fetch 超时（30s）")
    tab.run_js("delete window.__fs;delete window.__fb;")
    return None


def create_campaign():
    """创建活动：动态生成日期和名称，通过浏览器发送请求。"""
    global _tab
    if not _tab:
        log.error("浏览器 tab 不可用，无法创建活动")
        return
    if not _browser_lock.acquire(timeout=10):
        log.warning("浏览器锁被占用，跳过本轮活动创建")
        return

    try:
        today = datetime.now()
        duration = int(_config["campaign_duration_days"])
        end_date = today + timedelta(days=duration - 1)
        today_str = today.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        prefix = _config["campaign_name_prefix"]
        campaign_name = f"{prefix}{today.strftime('%Y%m%d')}"

        template_path = os.path.join(get_bundle_dir(), "campaign_template.json")
        with open(template_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        data["campaignName"] = campaign_name
        data["publishStartTime"] = today_str
        data["publishEndTime"] = end_str
        data["participateStartTime"] = today_str
        data["participateEndTime"] = end_str

        _build_cat_commission_value(_tab, data)

        tb_token = get_tb_token(_tab)
        if not tb_token:
            log.error("未找到 _tb_token_，活动创建终止")
            return

        if "alimama.com" not in (_tab.url or ""):
            _tab.get("https://fuwu.alimama.com/")
            time.sleep(3)

        form_data = {
            "t": str(int(time.time() * 1000)),
            "_tb_token_": tb_token,
            "_data_": json.dumps(data, ensure_ascii=False, separators=(",", ":")),
        }
        api_url = _config["campaign_api_url"]
        referer = (
            "https://fuwu.alimama.com/portal/v2/pages/campaign/"
            "cpevent/form/index.htm?campaignTemplateId=6"
        )

        log.info(">>> Browser POST %s\n  Body: <hidden>", api_url)

        resp_text = _browser_post_form(_tab, api_url, form_data, referer)
        if not resp_text:
            log.error("活动创建请求无响应")
            return

        max_inline = 2000
        safe_resp_text = _mask_sensitive_text(resp_text)
        if len(safe_resp_text) > max_inline:
            detail_path = _save_http_detail("resp", "POST", api_url, safe_resp_text)
            log.info(
                "<<< Browser POST %s\n  Response: %s...(truncated, full: %s)",
                api_url, safe_resp_text[:max_inline], detail_path,
            )
        else:
            log.info("<<< Browser POST %s\n  Response: %s", api_url, safe_resp_text)

        result = json.loads(resp_text)
        if result.get("success") or result.get("data"):
            log.info("活动创建成功: %s", campaign_name)
        else:
            log.error("活动创建可能失败，响应: %s", _mask_sensitive_data(result))
    except Exception as e:
        log.error("活动创建失败: %s", e)
    finally:
        _browser_lock.release()


_task_running = {}


def _guarded(task_name, func):
    """防重入守卫：上一轮未完成时跳过本轮。"""
    def wrapper():
        with _task_lock:
            if _task_running.get(task_name):
                log.debug("任务 %s 上一轮未完成，跳过", task_name)
                return
            _task_running[task_name] = True
        try:
            func()
        finally:
            with _task_lock:
                _task_running[task_name] = False
    return wrapper


def run_guarded_task(task_name, func):
    """供手动触发复用同一防重入逻辑。"""
    _guarded(task_name, func)()


def run():
    """脚本主流程，由面板子线程调用。"""
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

    import order_db
    order_db.init_db()

    _register_scheduled_tasks()

    while not _stop_event.is_set():
        schedule.run_pending()
        check_and_solve_slider()
        time.sleep(1)

    log.info("脚本已停止")


def _register_scheduled_tasks():
    sched_time = _config["schedule_time"]
    schedule.every().day.at(sched_time).do(_guarded("create_campaign", create_campaign))
    log.info("已设置定时任务：每天 %s 创建活动", sched_time)

    from sync_data import sync_campaigns
    schedule.every(1).hours.do(_guarded("sync_campaigns", sync_campaigns))
    log.info("已设置定时任务：每小时同步活动列表")

    from sync_products import (
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

    from sync_orders import fetch_new_orders, push_pending_orders, sync_order_status_daily
    schedule.every(1).minutes.do(_guarded("fetch_orders", fetch_new_orders))
    schedule.every(20).seconds.do(_guarded("push_orders", push_pending_orders))
    order_sync_time = _config.get("order_status_sync_time", "03:00")
    schedule.every().day.at(order_sync_time).do(sync_order_status_daily)
    log.info(
        "已设置定时任务：每分钟增量拉取订单 + 每20秒推送队列，每天 %s 全量对账",
        order_sync_time,
    )

    from task_executor import process_tasks
    schedule.every(1).minutes.do(_guarded("process_tasks", process_tasks))
    log.info("已设置定时任务：每分钟拉取并执行远程任务")


def main():
    """独立运行入口（不通过面板时使用）。"""
    from config import load_config
    init(load_config())
    run()


if __name__ == "__main__":
    main()

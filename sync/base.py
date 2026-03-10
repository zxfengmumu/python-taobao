"""共享数据同步工具：分页抓取 + ASYX 推送 + 会话过期检测。"""
import json
import time
import logging
from urllib.parse import urlencode

from core import db as order_db

log = logging.getLogger("taobao_auto")

_PAGE_SIZE = 40
_PAGE_INTERVAL = 2
_PUSH_BATCH_SIZE = 200
_MAX_RETRIES = 3


def is_session_expired(result):
    """判定 API 返回是否表示会话过期（success=False 且 resultCode 为 None）。"""
    if result.get("success"):
        return False
    return result.get("resultCode") is None


def _fetch_single_page(full_url, referer, page_no):
    """通过浏览器 fetch 拉取单页数据，失败时指数退避重试，会话过期自动触发重登录。"""
    import main
    from browser.driver import browser_get_json

    relogin_attempted = False
    for attempt in range(1, _MAX_RETRIES + 1):
        resp_text = browser_get_json(full_url, referer)
        if not resp_text:
            if attempt < _MAX_RETRIES:
                wait = 2 ** attempt
                log.warning(
                    "拉取第 %d 页无响应，%ds 后第 %d 次重试",
                    page_no, wait, attempt,
                )
                time.sleep(wait)
                continue
            log.error("拉取第 %d 页失败，已重试 %d 次", page_no, _MAX_RETRIES)
            return None

        try:
            result = json.loads(resp_text)
        except (json.JSONDecodeError, TypeError) as e:
            if attempt < _MAX_RETRIES:
                wait = 2 ** attempt
                log.warning(
                    "拉取第 %d 页 JSON 解析失败: %s，%ds 后重试",
                    page_no, e, wait,
                )
                time.sleep(wait)
                continue
            log.error("拉取第 %d 页 JSON 解析失败: %s", page_no, e)
            return None

        if not result.get("success"):
            if is_session_expired(result) and not relogin_attempted:
                log.warning("接口返回 resultCode=None，判定为会话过期，触发重登录")
                relogin_attempted = True
                from browser.login import trigger_relogin
                from browser.driver import get_tb_token
                if trigger_relogin():
                    tb_token = get_tb_token(main._tab)
                    if tb_token:
                        from urllib.parse import parse_qs, urlparse, urlunparse
                        parsed = urlparse(full_url)
                        qs = parse_qs(parsed.query, keep_blank_values=True)
                        qs["_tb_token_"] = [tb_token]
                        qs["t"] = [str(int(time.time() * 1000))]
                        full_url = urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))
                    continue
                log.error("重登录失败，拉取第 %d 页终止", page_no)
                return None
            log.error(
                "接口返回失败 (第 %d 页): resultCode=%s",
                page_no, result.get("resultCode"),
            )
            return None

        return result.get("data", {})

    return None


def iter_pages(base_url, extra_params, referer, page_interval=_PAGE_INTERVAL):
    """通用分页生成器：逐页 yield (items, has_next)，支持动态页间隔节流。"""
    import main
    from browser.driver import get_tb_token

    tb_token = get_tb_token(main._tab)
    if not tb_token:
        log.error("未找到 _tb_token_，无法拉取数据")
        return

    page_no = 1
    while not main._stop_event.is_set():
        params = {
            "t": str(int(time.time() * 1000)),
            "_tb_token_": tb_token,
            "pageNo": str(page_no),
            "pageSize": str(_PAGE_SIZE),
            **extra_params,
        }
        full_url = base_url + "?" + urlencode(params)

        data = _fetch_single_page(full_url, referer, page_no)
        if data is None:
            break

        items = data.get("result", [])
        if not items:
            break

        has_next = bool(data.get("hasNext"))
        yield items, has_next

        if not has_next:
            break
        page_no += 1
        interval = page_interval(page_no) if callable(page_interval) else page_interval
        try:
            interval = float(interval)
        except (TypeError, ValueError):
            interval = _PAGE_INTERVAL
        time.sleep(max(interval, 0.2))


def push_to_asyx(data_list, api_url, batch_size=_PUSH_BATCH_SIZE):
    """分批推送数据到 ASYX 后端，返回成功推送总数。"""
    from core.http_client import asyx_authed_request

    if not data_list:
        return 0

    pushed = 0
    for i in range(0, len(data_list), batch_size):
        batch = data_list[i:i + batch_size]
        try:
            resp = asyx_authed_request(
                "POST", api_url, json=batch, timeout=60,
            )
            if not resp:
                log.error("无法获取 ASYX token，推送中止")
                break
            result = resp.json()
            if result.get("code") == 200:
                pushed += len(batch)
            else:
                log.error("ASYX 推送返回异常: %s", result)
                break
        except Exception as exc:
            log.error("ASYX 推送失败: %s", exc)
            break

    return pushed

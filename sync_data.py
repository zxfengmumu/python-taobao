"""活动同步 + 共享浏览器抓取工具。

提供：
- _browser_get_json / _fetch_single_page / iter_pages  共享分页抓取
- push_to_asyx  通用 ASYX 推送
- sync_campaigns  活动全量拉取 + 直推后端
"""
import json
import time
import logging
from urllib.parse import urlencode

import order_db

log = logging.getLogger("taobao_auto")

_PAGE_SIZE = 40
_PAGE_INTERVAL = 2
_PUSH_BATCH_SIZE = 200
_FETCH_TIMEOUT_MS = 25_000
_MAX_RETRIES = 3

_fetch_seq = 0


# ========== 共享：浏览器 fetch ==========

def _ensure_tab_on_alimama():
    """确保浏览器 tab 在 alimama.com 域名下。"""
    import main

    tab = main._tab
    if "alimama.com" not in (tab.url or ""):
        tab.get("https://fuwu.alimama.com/")
        time.sleep(3)


def _browser_get_json(url, referer):
    """通过浏览器 fetch 发送 GET 请求，带 AbortController 超时和唯一标识。"""
    import main
    global _fetch_seq

    tab = main._tab
    try:
        tab.set.activate()
    except Exception:
        pass

    _ensure_tab_on_alimama()

    _fetch_seq += 1
    key = f"__fs_{_fetch_seq}"

    tab.run_js(
        'window["' + key + '"]={done:false,result:""};'
        'var ac=new AbortController();'
        'var tid=setTimeout(function(){ac.abort();}, ' + str(_FETCH_TIMEOUT_MS) + ');'
        'fetch("' + url + '",{'
        'method:"GET",'
        'headers:{"accept":"*/*","accept-language":"zh-CN,zh;q=0.9",'
        '"bx-v":"2.5.11",'
        '"content-type":"application/x-www-form-urlencoded; charset=UTF-8",'
        '"x-requested-with":"XMLHttpRequest"},'
        'referrer:"' + referer + '",'
        'credentials:"include",'
        'signal:ac.signal'
        '}).then(function(r){return r.text();})'
        '.then(function(t){clearTimeout(tid);window["' + key + '"]={done:true,result:t};})'
        '.catch(function(e){'
        'clearTimeout(tid);window["' + key + '"]={done:true,result:JSON.stringify({error:e.message})};'
        '});'
    )

    for i in range(60):
        if tab.run_js('return window["' + key + '"].done'):
            result = tab.run_js(
                'var r=window["' + key + '"].result;'
                'delete window["' + key + '"];'
                'return r;'
            )
            return result
        if (i + 1) % 20 == 0:
            main.check_and_solve_slider()
        time.sleep(0.5)

    log.error("浏览器 fetch GET 超时（30s）, url=%s", url[:120])
    tab.run_js('delete window["' + key + '"];')
    return None


def _fetch_single_page(full_url, referer, page_no):
    """通过浏览器 fetch 拉取单页数据，失败时指数退避重试。"""
    for attempt in range(1, _MAX_RETRIES + 1):
        resp_text = _browser_get_json(full_url, referer)
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

    tb_token = main.get_tb_token(main._tab)
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


# ========== 共享：ASYX 推送 ==========

def push_to_asyx(data_list, api_url, batch_size=_PUSH_BATCH_SIZE):
    """分批推送数据到 ASYX 后端，返回成功推送总数。"""
    import main

    if not data_list:
        return 0

    pushed = 0
    for i in range(0, len(data_list), batch_size):
        batch = data_list[i:i + batch_size]
        try:
            resp = main.asyx_authed_request(
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


# ========== 活动同步 ==========

def _get_campaign_params():
    return {
        "phaseType": "31",
        "needEffect": "true",
        "keyword": "",
        "campaignTemplateId": "6",
    }


_CAMPAIGN_REFERER = (
    "https://fuwu.alimama.com/portal/v2/pages/campaign/"
    "cpevent/list/index.htm?pageNo=1&pageSize=40"
    "&showStatus=all&accessibleEmployeeId=all"
    "&keyword=&campaignTemplateId=6"
)


def sync_campaigns():
    """全量拉取活动列表并直推 ASYX 后端。"""
    import main

    if not main._tab:
        log.error("浏览器 tab 不可用，活动同步终止")
        return

    browser_lock = main._browser_lock
    if not browser_lock.acquire(timeout=10):
        log.warning("浏览器锁被占用，跳过本轮活动同步")
        return

    try:
        _do_sync_campaigns()
    finally:
        browser_lock.release()


def _do_sync_campaigns():
    import main

    log.info("===== 开始同步活动列表 =====")
    all_campaigns = []

    for items, _ in iter_pages(
        main._config["campaign_list_api_url"],
        _get_campaign_params(),
        _CAMPAIGN_REFERER,
    ):
        all_campaigns.extend(items)
        log.info("活动已拉取 %d 条", len(all_campaigns))

    if not all_campaigns:
        log.info("未拉取到活动数据，跳过推送")
        return

    pushed = push_to_asyx(
        all_campaigns, main._config["campaign_save_api_url"],
    )
    order_db.upsert_campaigns_batch(all_campaigns)
    log.info(
        "===== 活动同步完成：拉取 %d 条，推送 %d 条，已落本地库 =====",
        len(all_campaigns), pushed,
    )

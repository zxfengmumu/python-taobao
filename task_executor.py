"""任务调度执行模块：从 ASYX 拉取任务 → 分发到处理器 → 提交结果。

每分钟触发一次，循环拉取直到无任务为止。
处理器通过 (module, business, operation) 三元组路由。
无论执行成功与否，结果都必须提交回 ASYX。
"""
import json
import time
import logging
from urllib.parse import urlencode

from sync_data import _browser_get_json, push_to_asyx

log = logging.getLogger("taobao_auto")

_PRODUCT_AUDIT_URL = (
    "https://fuwu.alimama.com/openapi/param2/1/gateway.unionpub/"
    "mkt.campaign.sign.audit.json"
)
_PRODUCT_AUDIT_REFERER = (
    "https://fuwu.alimama.com/portal/v2/pages/campaign/"
    "goodsList/index.htm?showStatus=1"
)
_PRODUCT_LIST_REFERER = (
    "https://fuwu.alimama.com/portal/v2/pages/campaign/"
    "goodsList/index.htm?showStatus=0&pageNo=1&pageSize=40"
    "&campaignTemplateId=6&industryCateId=all&auditorId=all"
    "&concernLevel=all&signUpContactEmpId=all"
    "&textSearchType=ITEM_ID&keyword=&sortItem=signUpTime&sortType=desc"
)
_AUDIT_STATUS_SYNC_DELAY = 3


def _error_result(msg):
    """构造统一的错误结果对象。"""
    return {"error": True, "msg": msg}


def _query_product_by_item_id(item_id):
    """通过商品列表接口按 itemId 查询商品，返回匹配的商品列表。"""
    import main

    base_url = main._config.get("product_list_api_url", "")
    if not base_url:
        log.warning("product_list_api_url 未配置，无法查询商品状态")
        return []

    tb_token = main.get_tb_token(main._tab)
    if not tb_token:
        log.warning("未找到 _tb_token_，无法查询商品状态")
        return []

    params = {
        "t": str(int(time.time() * 1000)),
        "_tb_token_": tb_token,
        "pageNo": "1",
        "pageSize": "40",
        "showStatus": "0",
        "campaignTemplateId": "6",
        "textSearchType": "ITEM_ID",
        "keyword": str(item_id),
        "sortItem": "signUpTime",
        "sortType": "desc",
    }
    full_url = base_url + "?" + urlencode(params)

    resp_text = _browser_get_json(full_url, _PRODUCT_LIST_REFERER)
    if not resp_text:
        log.warning("查询商品状态无响应: itemId=%s", item_id)
        return []

    try:
        result = json.loads(resp_text)
    except (json.JSONDecodeError, TypeError):
        log.warning("查询商品状态响应解析失败: itemId=%s", item_id)
        return []

    if not result.get("success"):
        log.warning("查询商品状态接口返回失败: itemId=%s, resultCode=%s", item_id, result.get("resultCode"))
        return []

    return result.get("data", {}).get("result", [])


def _sync_product_status_after_audit(item_id):
    """审核后等待生效，查询商品最新状态并推送到 ASYX。"""
    import main

    log.info("等待 %ds 后查询商品最新状态: itemId=%s", _AUDIT_STATUS_SYNC_DELAY, item_id)
    time.sleep(_AUDIT_STATUS_SYNC_DELAY)

    products = _query_product_by_item_id(item_id)
    if not products:
        log.warning("审核后未查询到商品数据: itemId=%s", item_id)
        return

    api_url = main._config.get("product_save_api_url", "")
    if not api_url:
        log.warning("product_save_api_url 未配置，无法同步商品状态")
        return

    pushed = push_to_asyx(products, api_url)
    if pushed:
        log.info("审核后商品状态同步完成: itemId=%s, 推送 %d 条", item_id, pushed)
    else:
        log.warning("审核后商品状态推送失败: itemId=%s", item_id)


def _handle_product_audit(data, audit_type):
    """商品审核处理器：通过浏览器调用阿里妈妈审核接口。

    审核成功后等待一段时间，重新查询商品状态并同步到 ASYX。
    audit_type: 1=通过, 2=拒绝
    data: {"signUpRecordId": xxx, "itemId": xxx, ...}
    """
    import main

    tab = main._tab
    if not tab:
        return _error_result("浏览器 tab 不可用")

    tb_token = main.get_tb_token(tab)
    if not tb_token:
        return _error_result("未找到 _tb_token_")

    sign_up_record_id = data.get("signUpRecordId")
    if not sign_up_record_id:
        return _error_result("缺少 signUpRecordId")

    item_id = data.get("itemId")
    if not item_id:
        return _error_result("缺少 itemId")

    record_list = json.dumps(
        [{"signUpRecordId": sign_up_record_id, "concernLevel": 2}],
        separators=(",", ":"),
    )
    form_data = {
        "t": str(int(time.time() * 1000)),
        "_tb_token_": tb_token,
        "signUpRecordDTOList": record_list,
        "audit": str(audit_type),
        "phaseType": "41",
        "refuseReason": "",
    }

    if "alimama.com" not in (tab.url or ""):
        tab.get("https://fuwu.alimama.com/")
        time.sleep(3)

    resp_text = main._browser_post_form(
        tab, _PRODUCT_AUDIT_URL, form_data, _PRODUCT_AUDIT_REFERER,
    )
    if not resp_text:
        return _error_result("浏览器请求无响应")

    try:
        result = json.loads(resp_text)
    except (json.JSONDecodeError, TypeError):
        return _error_result(f"响应解析失败: {resp_text[:200]}")

    op_label = "通过" if audit_type == 1 else "拒绝"
    log.info("商品审核%s完成: signUpRecordId=%s, result=%s", op_label, sign_up_record_id, result)

    if not result.get("error"):
        _sync_product_status_after_audit(item_id)

    return result


_HANDLERS = {
    ("product", "audit", "pass"): lambda data: _handle_product_audit(data, audit_type=1),
    ("product", "audit", "reject"): lambda data: _handle_product_audit(data, audit_type=2),
}


def _fetch_task():
    """从 ASYX 拉取单个待执行任务，无任务时返回 None。"""
    import main

    url = main._config.get("task_fetch_url", "")
    if not url:
        log.error("task_fetch_url 未配置")
        return None

    try:
        resp = main.asyx_authed_request("GET", url, timeout=30)
        if not resp:
            return None
        body = resp.json()
        if body.get("code") != 200:
            log.error("任务拉取返回异常: code=%s, msg=%s", body.get("code"), body.get("msg"))
            return None
        task = body.get("data")
        if not task or not task.get("id"):
            return None
        required_fields = ("module", "business", "operation")
        if not all(task.get(f) for f in required_fields):
            log.error("任务数据缺少必要字段: %s", task)
            return None
        return task
    except Exception as e:
        log.error("任务拉取失败: %s", e)
        return None


def _submit_task_result(task_id, results):
    """将任务执行结果提交到 ASYX，无论成功失败都必须提交。"""
    import main

    url = main._config.get("task_submit_url", "")
    if not url:
        log.error("task_submit_url 未配置")
        return

    payload = {"id": task_id, "result": results}
    try:
        resp = main.asyx_authed_request("POST", url, json=payload, timeout=30)
        if not resp:
            log.error("任务结果提交失败: 无法获取 ASYX token, taskId=%s", task_id)
            return
        log.info("任务结果已提交: payload=%s", payload)
    except Exception as e:
        log.error("任务结果提交失败: taskId=%s, error=%s", task_id, e)


def _dispatch_task(task):
    """根据 module/business/operation 路由到处理器并执行。"""
    key = (task["module"], task["business"], task["operation"])
    handler = _HANDLERS.get(key)
    if not handler:
        log.warning("未知任务类型: %s", key)
        return _error_result(f"未知任务类型: {'/'.join(key)}")
    try:
        return handler(task.get("data") or {})
    except Exception as e:
        log.error("任务执行异常: type=%s, error=%s", key, e)
        return _error_result(str(e))


def process_tasks():
    """公开入口：循环拉取并执行任务，直到无任务为止。"""
    import main

    if not main._tab:
        log.error("浏览器 tab 不可用，任务执行终止")
        return

    if not main._browser_lock.acquire(timeout=5):
        log.warning("浏览器锁被占用，跳过本轮任务执行")
        return

    try:
        _do_process_tasks()
    finally:
        main._browser_lock.release()


def _do_process_tasks():
    """内部循环：拉取 → 执行 → 提交，直到拉取为空。"""
    import main

    executed = 0
    while not main._stop_event.is_set():
        task = _fetch_task()
        if not task:
            break

        task_id = task["id"]
        key_desc = f"{task['module']}/{task['business']}/{task['operation']}"
        log.info("开始执行任务: id=%s, type=%s", task_id, key_desc)

        results = _dispatch_task(task)
        _submit_task_result(task_id, results)
        executed += 1

    if executed:
        log.info("本轮任务执行完成，共处理 %d 个任务", executed)

"""活动同步（全量拉取 + 直推）+ 活动创建。"""
import os
import json
import time
import logging
from datetime import datetime, timedelta

from core import db as order_db
from core.config import get_bundle_dir
from sync.base import iter_pages, push_to_asyx

log = logging.getLogger("taobao_auto")

_CAMPAIGN_REFERER = (
    "https://fuwu.alimama.com/portal/v2/pages/campaign/"
    "cpevent/list/index.htm?pageNo=1&pageSize=40"
    "&showStatus=all&accessibleEmployeeId=all"
    "&keyword=&campaignTemplateId=6"
)


def _get_campaign_params():
    return {
        "phaseType": "31",
        "needEffect": "true",
        "keyword": "",
        "campaignTemplateId": "6",
    }


# ========== 活动同步 ==========

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


# ========== 活动创建 ==========

def fetch_category_commissions(tab):
    """从模板配置接口动态获取类目佣金数据。"""
    import main
    from browser.driver import get_tb_token, get_cookie_str
    from core.http_client import logged_request

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
        resp = logged_request(
            "GET", main._config["template_config_api_url"],
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


def create_campaign():
    """创建活动：动态生成日期和名称，通过浏览器发送请求。"""
    import main
    from browser.driver import get_tb_token, browser_post_form
    from core.http_client import mask_sensitive_data, mask_sensitive_text, _save_http_detail

    if not main._tab:
        log.error("浏览器 tab 不可用，无法创建活动")
        return
    if not main._browser_lock.acquire(timeout=10):
        log.warning("浏览器锁被占用，跳过本轮活动创建")
        return

    try:
        today = datetime.now()
        duration = int(main._config["campaign_duration_days"])
        end_date = today + timedelta(days=duration - 1)
        today_str = today.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        prefix = main._config["campaign_name_prefix"]
        campaign_name = f"{prefix}{today.strftime('%Y%m%d')}"

        template_path = os.path.join(get_bundle_dir(), "campaign_template.json")
        with open(template_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        data["campaignName"] = campaign_name
        data["publishStartTime"] = today_str
        data["publishEndTime"] = end_str
        data["participateStartTime"] = today_str
        data["participateEndTime"] = end_str

        _build_cat_commission_value(main._tab, data)

        tb_token = get_tb_token(main._tab)
        if not tb_token:
            log.error("未找到 _tb_token_，活动创建终止")
            return

        if "alimama.com" not in (main._tab.url or ""):
            main._tab.get("https://fuwu.alimama.com/")
            time.sleep(3)

        form_data = {
            "t": str(int(time.time() * 1000)),
            "_tb_token_": tb_token,
            "_data_": json.dumps(data, ensure_ascii=False, separators=(",", ":")),
        }
        api_url = main._config["campaign_api_url"]
        referer = (
            "https://fuwu.alimama.com/portal/v2/pages/campaign/"
            "cpevent/form/index.htm?campaignTemplateId=6"
        )

        log.info(">>> Browser POST %s\n  Body: <hidden>", api_url)

        resp_text = browser_post_form(main._tab, api_url, form_data, referer)
        if not resp_text:
            log.error("活动创建请求无响应")
            return

        max_inline = 2000
        safe_resp_text = mask_sensitive_text(resp_text)
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
            log.error("活动创建可能失败，响应: %s", mask_sensitive_data(result))
    except Exception as e:
        log.error("活动创建失败: %s", e)
    finally:
        main._browser_lock.release()

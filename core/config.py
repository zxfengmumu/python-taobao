import os
import sys
import json

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_runtime_dir() -> str:
    """返回运行时基础目录：打包后为 exe 所在目录，开发时为项目根目录。"""
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return _PROJECT_ROOT


def get_bundle_dir() -> str:
    """返回只读资源目录：打包后为 _MEIPASS 临时目录，开发时为项目根目录。"""
    if getattr(sys, "frozen", False):
        return sys._MEIPASS  # type: ignore[attr-defined]
    return _PROJECT_ROOT


_BASE_DIR = get_runtime_dir()
CONFIG_PATH = os.path.join(_BASE_DIR, "config.json")

DEFAULT_CONFIG = {
    # ── 浏览器 ──
    "browser_cache_path": r"C:\Users\Administrator\AppData\Local\Google\Chrome\data",
    "browser_port": 9223,
    "target_url": "https://fuwu.alimama.com/",
    "wechat_webhook": "",

    # ── 阿里妈妈网站接口 (fuwu.alimama.com) ──
    "campaign_api_url": (
        "https://fuwu.alimama.com/openapi/json2/1/gateway.unionpub/"
        "mkt.campaign.create.json"
    ),
    "template_config_api_url": (
        "https://fuwu.alimama.com/openapi/param2/1/gateway.unionpub/"
        "mkt.template.config.json"
    ),
    "campaign_list_api_url": (
        "https://fuwu.alimama.com/openapi/param2/1/gateway.unionpub/"
        "mkt.campaign.list.json"
    ),
    "product_list_api_url": (
        "https://fuwu.alimama.com/openapi/param2/1/gateway.unionpub/"
        "cross.campaign.signup.search.full.json"
    ),
    "order_list_api_url": (
        "https://fuwu.alimama.com/openapi/param2/1/gateway.unionpub/"
        "report.publisher.getCpPublisherOrder.json"
    ),

    # ── ASYX 平台接口 (asyx888.com) ──
    "asyx_login_url": "https://m.asyx888.com/prod-api/login",
    "asyx_username": "",
    "asyx_password": "",
    "cookie_api_url": "https://m.asyx888.com/prod-api/basis/integral/config/editBatch",
    "campaign_save_api_url": "https://m.asyx888.com/prod-api/taobao/campaign/saveBatch",
    "campaign_claim_switch_api_url": "https://m.asyx888.com/prod-api/sass/unified/colonel/activity/claim/switch",
    "product_save_api_url": "https://m.asyx888.com/prod-api/taobao/product/saveBatch",
    "order_save_api_url": "https://m.asyx888.com/prod-api/taobao/order/saveBatch",
    "task_fetch_url": "https://m.asyx888.com/prod-api/sass/tb/task/fetch",
    "task_submit_url": "https://m.asyx888.com/prod-api/sass/tb/task/submit",

    # ── 页面选择器 ──
    "selector_qr_toggle": "#login > div.corner-icon-view.view-type-qrcode > i",
    "selector_qr_refresh": "#qrcode-img > div > button",
    "selector_ready_btn": (
        "#mx_123 > div > div.Ready__Body-sc-th64it-3.idPBpi"
        " > div > a > button"
    ),
    "selector_slider": "#nc_1_n1z",
    "selector_slider_container": "#nc_1__scale_text",
    "selector_slider_fail": "#nc_1_refresh1",

    # ── 定时任务与业务参数 ──
    "schedule_time": "09:00",
    "campaign_name_prefix": "sn联盟招商",
    "campaign_duration_days": 35,
    "order_query_start_time": "",
    "order_full_sync_range_days": 15,
    "order_status_sync_time": "03:00",
    "product_full_sync_time": "04:00",
    "task_poll_interval": 10,
    "risk_product_sync_interval_seconds": 60,
    "debug_http_body": False,
}


def load_config():
    """从 config.json 加载配置，文件不存在或解析失败则返回默认值。"""
    if not os.path.isfile(CONFIG_PATH):
        return dict(DEFAULT_CONFIG)
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            saved = json.load(f)
        merged = dict(DEFAULT_CONFIG)
        merged.update(saved)
        return merged
    except (json.JSONDecodeError, OSError):
        return dict(DEFAULT_CONFIG)


def save_config(cfg):
    """将配置字典写入 config.json。"""
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

"""企业微信通知：截图 + 文本消息。"""
import base64
import hashlib
import logging

from core.http_client import logged_request

log = logging.getLogger("taobao_auto")


def send_screenshot_to_wechat(tab):
    """全页截图并发送到企业微信机器人。"""
    import main

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
        logged_request("POST", main._config["wechat_webhook"], json=payload, timeout=30)
    except Exception as e:
        log.error("发送截图失败: %s", e)


def send_text_to_wechat(content):
    """发送文本消息到企业微信机器人。"""
    import main

    webhook = main._config.get("wechat_webhook", "")
    if not webhook:
        log.warning("wechat_webhook 未配置，跳过文本消息发送")
        return
    payload = {
        "msgtype": "text",
        "text": {"content": content},
    }
    try:
        logged_request("POST", webhook, json=payload, timeout=30)
    except Exception as e:
        log.error("发送文本消息失败: %s", e)

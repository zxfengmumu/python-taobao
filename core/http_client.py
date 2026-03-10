"""HTTP 请求封装 + 日志脱敏 + ASYX 平台认证。"""
import json
import logging
import os
from datetime import datetime
from urllib.parse import urlparse

import requests

from core.config import get_runtime_dir

log = logging.getLogger("taobao_auto")

_CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/140.0.0.0 Safari/537.36"
)


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


def mask_sensitive_data(data):
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
                masked[k] = mask_sensitive_data(v)
        return masked
    if isinstance(data, list):
        return [mask_sensitive_data(v) for v in data]
    return data


def mask_sensitive_text(text):
    """对纯文本响应进行粗粒度脱敏。"""
    if not isinstance(text, str):
        return text
    masked = text
    for key in ("password", "token", "authorization", "cookie", "set-cookie"):
        masked = masked.replace(f'"{key}":"', f'"{key}":"***')
        masked = masked.replace(f'"{key}": "', f'"{key}": "***')
    return masked


def logged_request(method, url, **kwargs):
    """发起 HTTP 请求，详细记录请求路径、参数和响应。"""
    import main

    headers = kwargs.get("headers") or {}
    headers.setdefault("User-Agent", _CHROME_UA)
    kwargs["headers"] = headers

    max_inline = 2000
    parts = [f">>> HTTP {method.upper()} {url}"]
    if kwargs.get("params"):
        parts.append(f"  Params: {json.dumps(mask_sensitive_data(kwargs['params']), ensure_ascii=False)}")
    body = kwargs.get("json") or kwargs.get("data")
    if body is not None and main._config.get("debug_http_body", False):
        body_safe = mask_sensitive_data(body)
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
    safe_resp_text = mask_sensitive_text(resp_text)
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


# ── ASYX 平台认证 ──

_asyx_token = None


def _get_asyx_token():
    """获取 asyx token，优先返回缓存，无缓存时调用登录接口。"""
    global _asyx_token
    if _asyx_token:
        return _asyx_token
    return _login_asyx()


def _login_asyx():
    """登录 asyx 平台获取 Bearer Token 并缓存。"""
    import main

    global _asyx_token
    body = {
        "username": main._config["asyx_username"],
        "password": main._config["asyx_password"],
    }
    try:
        resp = logged_request(
            "POST", main._config["asyx_login_url"], json=body,
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

    resp = logged_request(method, url, **kwargs)

    if _is_token_expired(resp):
        log.info("ASYX token 已过期，重新登录...")
        _asyx_token = None
        token = _get_asyx_token()
        if not token:
            return None
        headers["Authorization"] = f"Bearer {token}"
        resp = logged_request(method, url, **kwargs)

    return resp

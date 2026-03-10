"""风险商品同步模块：按 commissionPriority=4,5 拉取并推送到 ASYX。"""
from __future__ import annotations

import logging
import time

from sync.base import iter_pages, push_to_asyx
from sync.products import _get_product_params, _PRODUCT_REFERER

log = logging.getLogger("taobao_auto")

_DEFAULT_LOCK_TIMEOUT_SECONDS = 5
_COMMISSION_PRIORITY_VALUE = "4,5"


def sync_risk_products():
    """同步风险商品：请求 commissionPriority=4,5 并推送到 ASYX。"""
    import main

    if not main._tab:
        log.error("浏览器 tab 不可用，风险商品同步终止")
        return

    api_url = main._config.get("product_save_api_url", "")
    if not api_url:
        log.error("product_save_api_url 未配置，风险商品同步终止")
        return

    if not main._browser_lock.acquire(timeout=_DEFAULT_LOCK_TIMEOUT_SECONDS):
        log.warning("浏览器锁被占用，跳过本轮风险商品同步")
        return

    try:
        _do_sync_risk_products(api_url)
    finally:
        main._browser_lock.release()


def _do_sync_risk_products(api_url: str):
    import main

    base_url = main._config.get("product_list_api_url", "")
    if not base_url:
        log.error("product_list_api_url 未配置，风险商品同步终止")
        return

    params = dict(_get_product_params())
    params["commissionPriority"] = _COMMISSION_PRIORITY_VALUE

    started = time.time()
    total_fetched = 0
    total_pushed = 0
    pages = 0

    log.info("开始同步风险商品：commissionPriority=%s", _COMMISSION_PRIORITY_VALUE)

    for items, _ in iter_pages(base_url, params, _PRODUCT_REFERER):
        pages += 1
        total_fetched += len(items)

        pushed = push_to_asyx(items, api_url)
        total_pushed += pushed

        if pushed < len(items):
            log.warning(
                "风险商品推送未完全成功：本页推送 %d/%d，提前结束本轮",
                pushed,
                len(items),
            )
            break

    elapsed = round(time.time() - started, 1)
    log.info(
        "风险商品同步完成：分页 %d，拉取 %d 条，推送 %d 条，耗时 %.1fs",
        pages,
        total_fetched,
        total_pushed,
        elapsed,
    )

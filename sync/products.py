"""商品同步模块：基于 signUpTime 水位截停的增量拉取 + 按活动维度的每日全量对账。

增量策略：
- API 按 signUpTime 倒序（最新报名的商品排在最前）
- 记录上轮最大 signUpTime 为水位线
- 遇到 signUpTime < (watermark - overlap) 时截停，复杂度 O(新增量)

每日对账策略：
- 从本地 campaigns 表获取活动列表
- 已结束活动：若配置了 _PRODUCT_STATUS_ENDED 则直接本地批量标记，否则仍走 API
- 进行中活动：按 campaignId 逐个拉取该活动下所有商品
- campaigns 表为空时兜底回退到全量翻页模式
"""
import logging
import time
from datetime import datetime, timedelta

from core import db as order_db
from sync.base import iter_pages, push_to_asyx

log = logging.getLogger("taobao_auto")

_PUSH_BATCH_SIZE = 500
_TIME_FMT = "%Y-%m-%d %H:%M:%S"
_PRODUCT_SIGNUP_WATERMARK_KEY = "last_product_signup_watermark"
_OVERLAP_SECONDS = 300

_PRODUCT_STATUS_ENDED: int | None = None

_PRODUCT_QUEUE_HIGH_WATERMARK = 3000
_PRODUCT_QUEUE_MAX_WATERMARK = 12000
_MAX_RETRIES = 3

_PRODUCT_REFERER = (
    "https://fuwu.alimama.com/portal/v2/pages/campaign/"
    "goodsList/index.htm?showStatus=0&pageNo=1&pageSize=40"
    "&campaignTemplateId=6&industryCateId=all&auditorId=all"
    "&concernLevel=all&signUpContactEmpId=all"
    "&textSearchType=ITEM_ID&keyword=&sortItem=signUpTime&sortType=desc"
)


def _get_product_params():
    return {
        "showStatus": "0",
        "campaignTemplateId": "6",
        "textSearchType": "ITEM_ID",
        "keyword": "",
        "sortItem": "signUpTime",
        "sortType": "desc",
    }


def _get_product_params_by_campaign(campaign_id):
    return {
        "showStatus": "0",
        "campaignTemplateId": "6",
        "textSearchType": "CAMPAIGN_ID",
        "keyword": str(campaign_id),
        "sortItem": "",
        "sortType": "",
    }


def _product_key(product):
    """提取商品唯一键 'itemId:campaignId'。"""
    item_id = str((product.get("advertisingUnit") or {}).get("itemId", ""))
    campaign_id = str(product.get("campaignId", ""))
    return f"{item_id}:{campaign_id}"


def _parse_signup_time(signup_time_str):
    """将 signUpTime 字符串解析为 datetime，支持常见两种格式，失败返回 None。"""
    if not signup_time_str:
        return None
    for fmt in (_TIME_FMT, "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(signup_time_str, fmt)
        except ValueError:
            continue
    return None


def _product_page_interval(_page_no):
    """队列高水位时拉取节流，优先让推送追平。"""
    if order_db.get_product_queue_count() >= _PRODUCT_QUEUE_HIGH_WATERMARK:
        return 4
    return 1.5


def _calc_lag_seconds(state_time):
    parsed = _parse_signup_time(state_time)
    if not parsed:
        return -1
    return int((datetime.now() - parsed).total_seconds())


# ========== 公开入口 1：增量拉取（每分钟）==========

def fetch_new_products():
    """增量拉取新商品：signUpTime 水位截停，复杂度 O(新增量)。"""
    import main

    if not main._tab:
        log.error("浏览器 tab 不可用，增量商品同步终止")
        return

    browser_lock = main._browser_lock
    if not browser_lock.acquire(timeout=5):
        log.warning("浏览器锁被占用，跳过本轮增量商品拉取")
        return

    try:
        _do_fetch_new_products()
    finally:
        browser_lock.release()


def _do_fetch_new_products():
    import main

    watermark_str = order_db.get_sync_state(_PRODUCT_SIGNUP_WATERMARK_KEY, "")
    watermark_dt = _parse_signup_time(watermark_str)
    cutoff_dt = (watermark_dt - timedelta(seconds=_OVERLAP_SECONDS)) if watermark_dt else None

    fetch_start = time.time()
    total = 0
    new_count = 0
    scanned_pages = 0
    max_signup_str = watermark_str
    hit_cutoff = False

    log.info(
        "增量商品拉取: watermark=%s, cutoff=%s",
        watermark_str or "（首次）",
        cutoff_dt.strftime(_TIME_FMT) if cutoff_dt else "（无）",
    )

    for items, _ in iter_pages(
        main._config["product_list_api_url"],
        _get_product_params(),
        _PRODUCT_REFERER,
        page_interval=_product_page_interval,
    ):
        if order_db.get_product_queue_count() >= _PRODUCT_QUEUE_MAX_WATERMARK:
            log.warning("商品队列积压过高，提前结束本轮拉取")
            break

        scanned_pages += 1
        page_batch, hit_cutoff, max_signup_str = _filter_page_by_cutoff(
            items, cutoff_dt, max_signup_str,
        )
        total += len(page_batch)
        if page_batch:
            changed = order_db.upsert_and_enqueue_product_batch(page_batch)
            new_count += len(changed)

        if hit_cutoff:
            log.info("命中 signUpTime 水位截停，已扫描 %d 页", scanned_pages)
            break

    floor_str = (datetime.now() - timedelta(seconds=_OVERLAP_SECONDS)).strftime(_TIME_FMT)
    if max_signup_str and max_signup_str > watermark_str:
        order_db.set_sync_state(_PRODUCT_SIGNUP_WATERMARK_KEY, max_signup_str)
    elif watermark_str and watermark_str < floor_str:
        order_db.set_sync_state(_PRODUCT_SIGNUP_WATERMARK_KEY, floor_str)
        log.info("无新增数据，水位线随时间推进至 %s", floor_str)

    fetch_duration = round(time.time() - fetch_start, 1)
    order_db.set_sync_state("last_product_fetch_duration", str(fetch_duration))

    lag_seconds = _calc_lag_seconds(order_db.get_sync_state(_PRODUCT_SIGNUP_WATERMARK_KEY, ""))
    log.info(
        "增量商品拉取完成，扫描 %d 条，新增/变化 %d 条，分页 %d，耗时 %.1fs",
        total, new_count, scanned_pages, fetch_duration,
    )
    log.info(
        "METRIC products_fetch queue_depth=%d fetch_lag_seconds=%d pages=%d",
        order_db.get_product_queue_count(), lag_seconds, scanned_pages,
    )


def _filter_page_by_cutoff(items, cutoff_dt, max_signup_str):
    """按截止时间过滤单页商品，返回 (page_batch, hit_cutoff, updated_max_signup_str)。"""
    page_batch = []
    hit_cutoff = False
    for product in items:
        signup_str = product.get("signUpTime", "")
        if signup_str and signup_str > max_signup_str:
            max_signup_str = signup_str
        if cutoff_dt and signup_str:
            signup_dt = _parse_signup_time(signup_str)
            if signup_dt and signup_dt < cutoff_dt:
                hit_cutoff = True
                break
        page_batch.append(product)
    return page_batch, hit_cutoff, max_signup_str


# ========== 公开入口 2：推送待发队列（每20秒）==========

def push_pending_products():
    """循环消费商品推送队列直到清空。"""
    import main

    total_pushed = 0
    total_retries = 0
    total_batches = 0
    failed_batches = 0
    start_ts = time.time()
    while True:
        batch = order_db.dequeue_products(_PUSH_BATCH_SIZE)
        if not batch:
            break

        total_batches += 1
        ids = [row[0] for row in batch]
        products = [row[1] for row in batch]

        pushed, used_attempts = _push_product_batch_with_retry(
            products, main._config["product_save_api_url"], _PUSH_BATCH_SIZE,
        )
        total_retries += max(used_attempts - 1, 0)
        if pushed >= len(ids):
            order_db.delete_products_from_queue(ids)
            total_pushed += pushed
            log.info("商品推送成功 %d 条，已从队列删除", pushed)
        elif pushed > 0:
            success_ids = ids[:pushed]
            remain_ids = ids[pushed:]
            order_db.delete_products_from_queue(success_ids)
            order_db.release_product_queue(remain_ids)
            total_pushed += pushed
            log.warning(
                "商品批次部分成功：成功 %d / %d，仅删除成功记录，其余保留重试",
                pushed, len(ids),
            )
            break
        else:
            failed_batches += 1
            order_db.release_product_queue(ids)
            log.warning("商品推送失败，%d 条保留在队列等待重试", len(ids))
            break

    if total_pushed:
        log.info("商品推送队列消费完成，共推送 %d 条", total_pushed)
    elapsed = max(time.time() - start_ts, 1)
    push_tpm = round(total_pushed / elapsed * 60, 1)
    retry_rate = round(total_retries / max(total_batches, 1), 2)
    error_rate = round(failed_batches / max(total_batches, 1), 2)
    log.info(
        "METRIC products_push throughput_push_per_min=%.1f retry_rate=%.2f error_rate=%.2f queue_depth=%d",
        push_tpm, retry_rate, error_rate, order_db.get_product_queue_count(),
    )


def _push_product_batch_with_retry(products, api_url, batch_size):
    for attempt in range(1, _MAX_RETRIES + 1):
        pushed = push_to_asyx(products, api_url, batch_size)
        if pushed > 0:
            return pushed, attempt
        if attempt < _MAX_RETRIES:
            time.sleep(2 ** attempt)
    return 0, _MAX_RETRIES


# ========== 公开入口 3：每日全量对账 ==========

def sync_product_status_daily():
    """每日全量对账：按活动维度逐个同步，已结束活动本地批量更新。"""
    import main

    if not main._tab:
        log.error("浏览器 tab 不可用，每日商品对账终止")
        return

    browser_lock = main._browser_lock
    if not browser_lock.acquire(timeout=10):
        log.warning("浏览器锁被占用，跳过本轮每日商品对账")
        return

    try:
        _do_daily_product_sync()
    finally:
        browser_lock.release()


def _do_daily_product_sync():
    import main

    log.info("===== 开始每日商品全量对账 =====")
    now_str = datetime.now().strftime(_TIME_FMT)
    ended_ids = order_db.get_ended_campaign_ids(now_str)
    active_ids = order_db.get_active_campaign_ids(now_str)

    if not ended_ids and not active_ids:
        log.warning("本地活动表为空，回退到全量翻页模式")
        _do_daily_product_sync_fallback()
        return

    log.info(
        "活动总计 %d 个（进行中 %d，已结束 %d）",
        len(ended_ids) + len(active_ids), len(active_ids), len(ended_ids),
    )
    total_changed = 0

    if ended_ids:
        total_changed += _bulk_end_campaign_products(ended_ids)

    for idx, campaign_id in enumerate(active_ids):
        if main._stop_event.is_set():
            log.info("收到停止信号，中断每日商品对账")
            break
        total_changed += _sync_one_campaign(campaign_id)
        if (idx + 1) % 10 == 0:
            log.info("活动对账进度: %d/%d", idx + 1, len(active_ids))
        time.sleep(1)

    log.info("===== 每日商品全量对账完成，状态变化 %d 条 =====", total_changed)


def _sync_one_campaign(campaign_id):
    """同步单个活动的所有商品，返回状态变化数。"""
    import main

    total = 0
    changed_count = 0
    for items, _ in iter_pages(
        main._config["product_list_api_url"],
        _get_product_params_by_campaign(campaign_id),
        _PRODUCT_REFERER,
    ):
        total += len(items)
        changed = order_db.upsert_and_enqueue_product_batch(items)
        changed_count += len(changed)

    if total > 0:
        log.info("活动 %s: 扫描 %d 条，变化 %d 条", campaign_id, total, changed_count)
    return changed_count


def _bulk_end_campaign_products(ended_ids):
    """处理已结束活动的商品。"""
    if _PRODUCT_STATUS_ENDED is not None:
        changed = order_db.mark_and_enqueue_campaign_products_ended(
            ended_ids, _PRODUCT_STATUS_ENDED,
        )
        log.info("已结束活动本地批量更新: %d 个活动，%d 条变化", len(ended_ids), changed)
        return changed

    total = 0
    for campaign_id in ended_ids:
        total += _sync_one_campaign(campaign_id)
        time.sleep(0.5)
    log.info("已结束活动 API 同步完成: %d 个活动，变化 %d 条", len(ended_ids), total)
    return total


def _do_daily_product_sync_fallback():
    """兜底全量翻页扫描（本地活动表为空时使用）。"""
    import main

    total = 0
    changed_count = 0
    for items, _ in iter_pages(
        main._config["product_list_api_url"],
        _get_product_params(),
        _PRODUCT_REFERER,
    ):
        total += len(items)
        changed = order_db.upsert_and_enqueue_product_batch(items)
        changed_count += len(changed)
        log.info("商品全量扫描进度：已扫描 %d 条", total)

    log.info("全量翻页扫描完成：扫描 %d 条，变化 %d 条", total, changed_count)

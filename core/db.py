"""本地 SQLite 数据库层。

提供五张表：
- order_update_queue：待推送到后端的订单队列
- order_status：订单状态跟踪（精简字段）
- product_status：商品报名状态跟踪
- product_update_queue：待推送到后端的商品队列
- sync_state：同步水位线 key-value 存储
"""
import json
import os
import sqlite3
import logging
import threading
from datetime import datetime, timedelta

from core.config import get_runtime_dir

log = logging.getLogger("taobao_auto")

_BASE_DIR = get_runtime_dir()
_DB_PATH = os.path.join(_BASE_DIR, "data", "orders.db")

_local = threading.local()

_CHUNK_SIZE = 500
_LEASE_SECONDS = 120

_DDL = """
    CREATE TABLE IF NOT EXISTS order_update_queue (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        tb_trade_id TEXT    NOT NULL UNIQUE,
        order_data  TEXT    NOT NULL,
        created_at  TEXT    DEFAULT (datetime('now','localtime')),
        inflight    INTEGER DEFAULT 0,
        lease_until TEXT,
        retry_count INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS order_status (
        tb_trade_id        TEXT PRIMARY KEY,
        tb_trade_parent_id TEXT,
        item_id            TEXT,
        create_time        TEXT,
        pay_status         INTEGER,
        updated_at         TEXT
    );

    CREATE TABLE IF NOT EXISTS product_status (
        item_id      TEXT NOT NULL,
        campaign_id  TEXT NOT NULL,
        status       INTEGER,
        updated_at   TEXT,
        PRIMARY KEY (item_id, campaign_id)
    );

    CREATE TABLE IF NOT EXISTS product_update_queue (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id      TEXT    NOT NULL,
        campaign_id  TEXT    NOT NULL,
        product_data TEXT    NOT NULL,
        created_at   TEXT    DEFAULT (datetime('now','localtime')),
        inflight     INTEGER DEFAULT 0,
        lease_until  TEXT,
        retry_count  INTEGER DEFAULT 0,
        UNIQUE(item_id, campaign_id)
    );

    CREATE TABLE IF NOT EXISTS sync_state (
        key   TEXT PRIMARY KEY,
        value TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_os_create_time
        ON order_status(create_time);
    CREATE INDEX IF NOT EXISTS idx_os_pay_status
        ON order_status(pay_status);
    CREATE INDEX IF NOT EXISTS idx_ps_updated_at
        ON product_status(updated_at);
    CREATE INDEX IF NOT EXISTS idx_ouq_created_at
        ON order_update_queue(created_at);
    CREATE INDEX IF NOT EXISTS idx_puq_created_at
        ON product_update_queue(created_at);

    CREATE TABLE IF NOT EXISTS campaigns (
        campaign_id      TEXT PRIMARY KEY,
        publish_end_time TEXT,
        synced_at        TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_campaigns_end_time ON campaigns(publish_end_time);
"""

_DDL_INFLIGHT_INDEXES = """
    CREATE INDEX IF NOT EXISTS idx_ouq_inflight_lease
        ON order_update_queue(inflight, lease_until);
    CREATE INDEX IF NOT EXISTS idx_puq_inflight_lease
        ON product_update_queue(inflight, lease_until);
"""


def _open_conn():
    """为当前线程创建并初始化一个新的 SQLite 连接。"""
    os.makedirs(os.path.dirname(_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.executescript(_DDL)
    _ensure_queue_columns(conn, "order_update_queue")
    _ensure_queue_columns(conn, "product_update_queue")
    conn.executescript(_DDL_INFLIGHT_INDEXES)
    conn.commit()
    return conn


def _get_conn() -> sqlite3.Connection:
    """返回当前线程专属的 SQLite 连接，不存在时自动创建。"""
    if not getattr(_local, "conn", None):
        _local.conn = _open_conn()
    return _local.conn


def init_db():
    """初始化数据库（在主线程调用一次即可）。"""
    _get_conn()
    log.info("数据库初始化完成: %s", _DB_PATH)


def close_db():
    """关闭当前线程的数据库连接。"""
    conn = getattr(_local, "conn", None)
    if conn is not None:
        conn.close()
        _local.conn = None


# ---------- sync_state ----------

def get_sync_state(key, default=None):
    row = _get_conn().execute(
        "SELECT value FROM sync_state WHERE key=?", (key,)
    ).fetchone()
    return row["value"] if row else default


def set_sync_state(key, value):
    conn = _get_conn()
    conn.execute(
        "INSERT INTO sync_state(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    conn.commit()


# ---------- order_status ----------

def get_order_status(tb_trade_id):
    """返回 dict 或 None。"""
    row = _get_conn().execute(
        "SELECT * FROM order_status WHERE tb_trade_id=?", (tb_trade_id,)
    ).fetchone()
    return dict(row) if row else None


def upsert_order_status_batch(orders):
    """批量 upsert 订单状态，返回有实质变化的订单原始 dict 列表。"""
    if not orders:
        return []
    conn = _get_conn()
    changed = upsert_order_status_batch_tx(conn, orders)
    conn.commit()
    return changed


def upsert_and_enqueue_order_batch(orders):
    """同事务完成订单 upsert + enqueue，避免崩溃窗口漏推。"""
    if not orders:
        return []
    conn = _get_conn()
    changed = upsert_order_status_batch_tx(conn, orders)
    _enqueue_order_batch_tx(conn, changed)
    conn.commit()
    return changed


def enqueue_order_batch(orders):
    """批量入队，同一 tb_trade_id 去重（覆盖旧数据）。"""
    conn = _get_conn()
    _enqueue_order_batch_tx(conn, orders)
    conn.commit()


def _enqueue_order_batch_tx(conn, orders):
    """事务内批量入队（不提交）。"""
    rows = []
    for order in orders:
        tb_trade_id = str(order.get("tbTradeId", ""))
        if not tb_trade_id:
            continue
        rows.append((tb_trade_id, json.dumps(order, ensure_ascii=False)))

    if rows:
        conn.executemany(
            "INSERT INTO order_update_queue(tb_trade_id,order_data) VALUES(?,?) "
            "ON CONFLICT(tb_trade_id) DO UPDATE SET "
            "order_data=excluded.order_data,"
            "created_at=datetime('now','localtime')",
            rows,
        )


def get_stale_orders(days=15):
    """获取 create_time 早于 N 天前的订单列表。"""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    rows = _get_conn().execute(
        "SELECT * FROM order_status WHERE create_time < ?", (cutoff,)
    ).fetchall()
    return [dict(r) for r in rows]


def cleanup_non_paid(days=3):
    """删除 N 天前创建且 pay_status != 12 的订单，返回删除行数。"""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    conn = _get_conn()
    cursor = conn.execute(
        "DELETE FROM order_status WHERE pay_status != 12 AND create_time < ?",
        (cutoff,),
    )
    conn.commit()
    return cursor.rowcount


def get_order_status_count():
    row = _get_conn().execute(
        "SELECT COUNT(*) AS cnt FROM order_status"
    ).fetchone()
    return row["cnt"]


# ---------- order_update_queue ----------

def dequeue_orders(limit=500):
    """原子领用最早 N 条待推送记录，返回 [(id, order_data_dict), ...]。"""
    return _claim_queue_batch("order_update_queue", "order_data", limit, _LEASE_SECONDS)


def _delete_by_ids(table, ids):
    """分批删除，每批不超过 _CHUNK_SIZE，防止超 SQLite 变量上限。"""
    if not ids:
        return
    conn = _get_conn()
    for i in range(0, len(ids), _CHUNK_SIZE):
        chunk = ids[i:i + _CHUNK_SIZE]
        placeholders = ",".join("?" * len(chunk))
        conn.execute(
            f"DELETE FROM {table} WHERE id IN ({placeholders})", chunk,
        )
    conn.commit()


def delete_from_queue(ids):
    _delete_by_ids("order_update_queue", ids)


def release_order_queue(ids):
    _release_by_ids("order_update_queue", ids)


def get_order_queue_count():
    row = _get_conn().execute(
        "SELECT COUNT(*) AS cnt FROM order_update_queue"
    ).fetchone()
    return row["cnt"]


# ---------- product_status ----------

def upsert_product_status_batch(products):
    """批量 upsert 商品状态，返回有实质变化的商品原始 dict 列表。"""
    if not products:
        return []
    conn = _get_conn()
    changed = upsert_product_status_batch_tx(conn, products)
    conn.commit()
    return changed


def upsert_and_enqueue_product_batch(products):
    """同事务完成商品 upsert + enqueue，避免崩溃窗口漏推。"""
    if not products:
        return []
    conn = _get_conn()
    changed = upsert_product_status_batch_tx(conn, products)
    _enqueue_product_batch_tx(conn, changed)
    conn.commit()
    return changed


def enqueue_product_batch(products):
    """批量入队，同一 (item_id, campaign_id) 去重。"""
    conn = _get_conn()
    _enqueue_product_batch_tx(conn, products)
    conn.commit()


def _enqueue_product_batch_tx(conn, products):
    """事务内批量入队（不提交）。"""
    rows = []
    for product in products:
        item_id = str((product.get("advertisingUnit") or {}).get("itemId", ""))
        campaign_id = str(product.get("campaignId", ""))
        if not item_id or not campaign_id:
            continue
        rows.append((item_id, campaign_id, json.dumps(product, ensure_ascii=False)))

    if rows:
        conn.executemany(
            "INSERT INTO product_update_queue"
            "(item_id,campaign_id,product_data) VALUES(?,?,?) "
            "ON CONFLICT(item_id,campaign_id) DO UPDATE SET "
            "product_data=excluded.product_data,"
            "created_at=datetime('now','localtime')",
            rows,
        )


def dequeue_products(limit=500):
    """原子领用最早 N 条待推送商品，返回 [(id, product_data_dict), ...]。"""
    return _claim_queue_batch("product_update_queue", "product_data", limit, _LEASE_SECONDS)


def delete_products_from_queue(ids):
    _delete_by_ids("product_update_queue", ids)


def release_product_queue(ids):
    _release_by_ids("product_update_queue", ids)


def get_product_queue_count():
    row = _get_conn().execute(
        "SELECT COUNT(*) AS cnt FROM product_update_queue"
    ).fetchone()
    return row["cnt"]


def get_product_status_count():
    row = _get_conn().execute(
        "SELECT COUNT(*) AS cnt FROM product_status"
    ).fetchone()
    return row["cnt"]


def _column_exists(conn, table, column):
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == column for r in rows)


def _ensure_queue_columns(conn, table):
    """兼容旧库：补齐 inflight/lease/retry_count 列。"""
    if not _column_exists(conn, table, "inflight"):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN inflight INTEGER DEFAULT 0")
    if not _column_exists(conn, table, "lease_until"):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN lease_until TEXT")
    if not _column_exists(conn, table, "retry_count"):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN retry_count INTEGER DEFAULT 0")


def _release_by_ids(table, ids):
    if not ids:
        return
    conn = _get_conn()
    for i in range(0, len(ids), _CHUNK_SIZE):
        chunk = ids[i:i + _CHUNK_SIZE]
        placeholders = ",".join("?" * len(chunk))
        conn.execute(
            f"UPDATE {table} SET inflight=0, lease_until=NULL WHERE id IN ({placeholders})",
            chunk,
        )
    conn.commit()


def _claim_queue_batch(table, data_column, limit, lease_seconds):
    """事务内领取待推送记录并加租约，避免重复消费。"""
    conn = _get_conn()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lease_until = (datetime.now() + timedelta(seconds=lease_seconds)).strftime("%Y-%m-%d %H:%M:%S")
    try:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            f"SELECT id, {data_column} AS payload FROM {table} "
            "WHERE inflight=0 OR (lease_until IS NOT NULL AND lease_until < ?) "
            "ORDER BY id LIMIT ?",
            (now_str, limit),
        ).fetchall()
        if not rows:
            conn.commit()
            return []

        ids = [r["id"] for r in rows]
        placeholders = ",".join("?" * len(ids))
        conn.execute(
            f"UPDATE {table} SET inflight=1, lease_until=?, retry_count=retry_count+1 "
            f"WHERE id IN ({placeholders})",
            [lease_until, *ids],
        )
        conn.commit()
    except sqlite3.Error:
        conn.rollback()
        raise

    result = []
    for r in rows:
        try:
            data = json.loads(r["payload"])
        except (json.JSONDecodeError, TypeError):
            data = {}
        result.append((r["id"], data))
    return result


def upsert_order_status_batch_tx(conn, orders):
    """事务版订单 upsert（不提交）。"""
    if not orders:
        return []
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    changed = []

    trade_ids = []
    parsed_orders = []
    for order in orders:
        tb_trade_id = str(order.get("tbTradeId", ""))
        if not tb_trade_id:
            continue
        trade_ids.append(tb_trade_id)
        parsed_orders.append({
            "tb_trade_id": tb_trade_id,
            "tb_trade_parent_id": str(order.get("tbTradeParentId", "")),
            "item_id": str(order.get("mktItemId", "")),
            "create_time": order.get("createTime", ""),
            "pay_status": order.get("payStatus"),
            "original": order,
        })
    if not trade_ids:
        return []

    existing_map: dict[str, int | None] = {}
    for i in range(0, len(trade_ids), _CHUNK_SIZE):
        chunk = trade_ids[i:i + _CHUNK_SIZE]
        placeholders = ",".join("?" * len(chunk))
        rows = conn.execute(
            f"SELECT tb_trade_id, pay_status FROM order_status WHERE tb_trade_id IN ({placeholders})",
            chunk,
        ).fetchall()
        for row in rows:
            existing_map[row["tb_trade_id"]] = row["pay_status"]

    to_insert = []
    to_update = []
    for parsed in parsed_orders:
        tb_trade_id = parsed["tb_trade_id"]
        pay_status = parsed["pay_status"]
        if tb_trade_id not in existing_map:
            to_insert.append((
                tb_trade_id, parsed["tb_trade_parent_id"], parsed["item_id"],
                parsed["create_time"], pay_status, now_str,
            ))
            changed.append(parsed["original"])
        elif existing_map[tb_trade_id] != pay_status:
            to_update.append((pay_status, now_str, tb_trade_id))
            changed.append(parsed["original"])

    if to_insert:
        conn.executemany(
            "INSERT INTO order_status"
            "(tb_trade_id,tb_trade_parent_id,item_id,"
            "create_time,pay_status,updated_at) VALUES(?,?,?,?,?,?)",
            to_insert,
        )
    if to_update:
        conn.executemany(
            "UPDATE order_status SET pay_status=?,updated_at=? WHERE tb_trade_id=?",
            to_update,
        )
    return changed


def upsert_product_status_batch_tx(conn, products):
    """事务版商品 upsert（不提交）。"""
    if not products:
        return []
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    changed = []

    parsed_products = []
    key_pairs = []
    for product in products:
        item_id = str((product.get("advertisingUnit") or {}).get("itemId", ""))
        campaign_id = str(product.get("campaignId", ""))
        if not item_id or not campaign_id:
            continue
        key_pairs.append((item_id, campaign_id))
        parsed_products.append({
            "item_id": item_id,
            "campaign_id": campaign_id,
            "status": product.get("status"),
            "original": product,
        })
    if not key_pairs:
        return []

    existing_map: dict[tuple[str, str], int | None] = {}
    for i in range(0, len(key_pairs), _CHUNK_SIZE):
        chunk = key_pairs[i:i + _CHUNK_SIZE]
        composite_keys = [f"{item_id}\x00{campaign_id}" for item_id, campaign_id in chunk]
        placeholders = ",".join("?" * len(composite_keys))
        rows = conn.execute(
            "SELECT item_id, campaign_id, status FROM product_status "
            f"WHERE item_id || char(0) || campaign_id IN ({placeholders})",
            composite_keys,
        ).fetchall()
        for row in rows:
            existing_map[(row["item_id"], row["campaign_id"])] = row["status"]

    to_insert = []
    to_update = []
    seen_keys: set[tuple[str, str]] = set()
    changed_map: dict[tuple[str, str], dict] = {}
    for parsed in parsed_products:
        key = (parsed["item_id"], parsed["campaign_id"])
        status = parsed["status"]
        if key in seen_keys:
            to_update.append((status, now_str, parsed["item_id"], parsed["campaign_id"]))
            changed_map[key] = parsed["original"]
        elif key not in existing_map:
            to_insert.append((parsed["item_id"], parsed["campaign_id"], status, now_str))
            changed_map[key] = parsed["original"]
            seen_keys.add(key)
        elif existing_map[key] != status:
            to_update.append((status, now_str, parsed["item_id"], parsed["campaign_id"]))
            changed_map[key] = parsed["original"]
            seen_keys.add(key)
    changed = list(changed_map.values())

    if to_insert:
        conn.executemany(
            "INSERT INTO product_status"
            "(item_id,campaign_id,status,updated_at) VALUES(?,?,?,?)",
            to_insert,
        )
    if to_update:
        conn.executemany(
            "UPDATE product_status SET status=?,updated_at=? "
            "WHERE item_id=? AND campaign_id=?",
            to_update,
        )
    return changed


# ---------- campaigns ----------

def upsert_campaigns_batch(campaigns):
    """批量 upsert 活动，提取 campaignId 和 publishEndTime 落本地库。"""
    if not campaigns:
        return
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for c in campaigns:
        campaign = c.get("campaign") or {}
        campaign_id = str(campaign.get("campaignId", ""))
        if not campaign_id:
            continue
        rows.append((campaign_id, str(campaign.get("publishEndTime", "")), now_str))
    if not rows:
        return
    conn = _get_conn()
    conn.executemany(
        "INSERT INTO campaigns(campaign_id,publish_end_time,synced_at) VALUES(?,?,?) "
        "ON CONFLICT(campaign_id) DO UPDATE SET "
        "publish_end_time=excluded.publish_end_time,"
        "synced_at=excluded.synced_at",
        rows,
    )
    conn.commit()


def get_active_campaign_ids(now_str):
    """返回 publish_end_time >= now_str 的活动 ID 列表（报名进行中）。"""
    rows = _get_conn().execute(
        "SELECT campaign_id FROM campaigns WHERE publish_end_time >= ?",
        (now_str,),
    ).fetchall()
    return [r["campaign_id"] for r in rows]


def get_ended_campaign_ids(now_str):
    """返回 publish_end_time < now_str 的活动 ID 列表（已结束）。"""
    rows = _get_conn().execute(
        "SELECT campaign_id FROM campaigns WHERE publish_end_time < ?",
        (now_str,),
    ).fetchall()
    return [r["campaign_id"] for r in rows]


def get_campaign_count():
    row = _get_conn().execute(
        "SELECT COUNT(*) AS cnt FROM campaigns"
    ).fetchone()
    return row["cnt"]


def mark_and_enqueue_campaign_products_ended(campaign_ids, ended_status):
    """将指定活动下状态未为 ended_status 的商品批量更新并入队，返回变化数。"""
    if not campaign_ids or ended_status is None:
        return 0
    conn = _get_conn()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_changed = 0
    for i in range(0, len(campaign_ids), _CHUNK_SIZE):
        chunk = campaign_ids[i:i + _CHUNK_SIZE]
        placeholders = ",".join("?" * len(chunk))
        rows = conn.execute(
            f"SELECT item_id, campaign_id FROM product_status "
            f"WHERE campaign_id IN ({placeholders}) AND (status IS NULL OR status != ?)",
            [*chunk, ended_status],
        ).fetchall()
        if not rows:
            continue
        conn.executemany(
            "UPDATE product_status SET status=?,updated_at=? WHERE item_id=? AND campaign_id=?",
            [(ended_status, now_str, r["item_id"], r["campaign_id"]) for r in rows],
        )
        queue_rows = [
            (
                r["item_id"],
                r["campaign_id"],
                json.dumps(
                    {"advertisingUnit": {"itemId": r["item_id"]},
                     "campaignId": r["campaign_id"],
                     "status": ended_status},
                    ensure_ascii=False,
                ),
            )
            for r in rows
        ]
        conn.executemany(
            "INSERT INTO product_update_queue(item_id,campaign_id,product_data) VALUES(?,?,?) "
            "ON CONFLICT(item_id,campaign_id) DO UPDATE SET "
            "product_data=excluded.product_data,"
            "created_at=datetime('now','localtime')",
            queue_rows,
        )
        total_changed += len(rows)
    conn.commit()
    return total_changed

# mcp_server.py (patched tool defs)
import os, sqlite3
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from fastmcp import FastMCP
from pydantic import BaseModel, Field
import json

load_dotenv(".env")
DB_PATH = os.getenv("DB_PATH", "katana_index.sqlite")

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

db = get_db()
mcp = FastMCP("katana-index-mcp", version="0.1.0")

def row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {k: row[k] for k in row.keys()}

# --------- Pydantic input models ----------
class LimitIn(BaseModel):
    limit: int = Field(10, ge=1, le=500)

class BlockQueryIn(BaseModel):
    number: int
    with_txs: bool = False
    tx_sample: int = Field(20, ge=1, le=500)

class TxQueryIn(BaseModel):
    hash: str

# ----------------- Tools ------------------

# --- tool defs in mcp_server.py ---

def _get(obj, key, default):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return default

@mcp.tool()
def latest_blocks(value: dict | int = 10):
    """Return the latest N blocks (descending by number)."""
    limit = int(value if isinstance(value, int) else _get(value, "limit", 10))
    limit = max(1, min(limit, 500))
    rows = db.execute("""
        SELECT number, hash, parent_hash, timestamp, gas_limit, gas_used,
               base_fee_per_gas_wei, miner, tx_count
        FROM blocks ORDER BY number DESC LIMIT ?
    """, (limit,)).fetchall()
    return [row_to_dict(r) for r in rows]

@mcp.tool()
def latest_txs(value: dict | int = 10):
    """Return the latest N transactions (descending by block_number, tx_index)."""
    limit = int(value if isinstance(value, int) else _get(value, "limit", 10))
    limit = max(1, min(limit, 500))
    rows = db.execute("""
        SELECT hash, block_number, tx_index, "from", "to", value_wei, nonce, gas,
               gas_price, max_fee_per_gas_wei, max_priority_fee_per_gas_wei,
               status, gas_used, effective_gas_price_wei, contract_address
        FROM txs ORDER BY block_number DESC, tx_index DESC LIMIT ?
    """, (limit,)).fetchall()
    return [row_to_dict(r) for r in rows]

@mcp.tool()
def block_by_number(value: dict):
    """Return a block by number (optionally with some txs)."""
    number = int(_get(value, "number", -1))
    with_txs = bool(_get(value, "with_txs", False))
    tx_sample = int(_get(value, "tx_sample", 20))
    if number < 0:
        return {"error": "number is required"}
    blk = db.execute("""
        SELECT number, hash, parent_hash, timestamp, gas_limit, gas_used,
               base_fee_per_gas_wei, miner, tx_count
        FROM blocks WHERE number=?
    """, (number,)).fetchone()
    if not blk:
        return {"error": f"block {number} not found"}
    out = row_to_dict(blk)
    if with_txs:
        tx_sample = max(1, min(tx_sample, 500))
        txs = db.execute("""
            SELECT hash, tx_index, "from", "to", value_wei, status
            FROM txs WHERE block_number=? ORDER BY tx_index ASC LIMIT ?
        """, (number, tx_sample)).fetchall()
        out["txs"] = [row_to_dict(r) for r in txs]
    return out

@mcp.tool()
def tx_by_hash(value: dict | str):
    """Return a transaction by its hash."""
    h = value if isinstance(value, str) else _get(value, "hash", "")
    if not h:
        return {"error": "hash is required"}
    row = db.execute("""
        SELECT hash, block_number, tx_index, "from", "to", value_wei, nonce, gas,
               gas_price, max_fee_per_gas_wei, max_priority_fee_per_gas_wei,
               status, gas_used, effective_gas_price_wei, contract_address
        FROM txs WHERE lower(hash)=lower(?)
    """, (h,)).fetchone()
    if not row:
        return {"error": f"tx {h} not found"}
    return row_to_dict(row)

@mcp.tool()
def health(value: dict | None = None):
    """
    Return indexer + metrics health. Reads the latest row from metrics_snapshot (id=1)
    and basic counts from blocks/txs.
    """
    # basic counts
    blk = db.execute("SELECT COUNT(*) AS n, MAX(number) AS maxn FROM blocks").fetchone()
    txs = db.execute("SELECT COUNT(*) AS n, MAX(block_number) AS maxt FROM txs").fetchone()

    # metrics snapshot (single row with id=1)
    ms = db.execute("SELECT * FROM metrics_snapshot WHERE id=1").fetchone()
    out: Dict[str, Any] = {
        "db_path": DB_PATH,
        "blocks": {"count": blk["n"], "max_number": blk["maxn"]},
        "txs": {"count": txs["n"], "max_block_number": txs["maxt"]},
        "metrics": None,
        "ready": False,
    }

    if ms:
        ms_dict = {k: ms[k] for k in ms.keys()}
        # expand JSON fields if present
        if ms_dict.get("top_senders_json"):
            ms_dict["top_senders"] = json.loads(ms_dict.pop("top_senders_json"))
        if ms_dict.get("top_receivers_json"):
            ms_dict["top_receivers"] = json.loads(ms_dict.pop("top_receivers_json"))
        out["metrics"] = ms_dict
        out["ready"] = True

    return out

@mcp.tool()
def top_senders(value: dict | int = 10):
    """
    Return top sender addresses by tx count.
    Prefers metrics_snapshot.top_senders_json; falls back to live query over the
    same window if snapshot is missing.
    """
    limit = int(value if isinstance(value, int) else _get(value, "limit", 10))
    limit = max(1, min(limit, 500))

    ms = db.execute("SELECT window_n_blocks, to_block, top_senders_json FROM metrics_snapshot WHERE id=1").fetchone()
    if ms and ms["top_senders_json"]:
        arr = json.loads(ms["top_senders_json"])
        return arr[:limit]

    if ms:
        to_block = ms["to_block"]
        from_block = max(0, to_block - int(ms["window_n_blocks"]) + 1)
        rows = db.execute("""
            SELECT "from" AS address, COUNT(*) AS count
            FROM txs
            WHERE block_number BETWEEN ? AND ?
            GROUP BY "from"
            ORDER BY count DESC
            LIMIT ?
        """, (from_block, to_block, limit)).fetchall()
        return [row_to_dict(r) for r in rows]

    rows = db.execute("""
        SELECT "from" AS address, COUNT(*) AS count
        FROM txs
        GROUP BY "from"
        ORDER BY count DESC
        LIMIT ?
    """, (limit,)).fetchall()
    return [row_to_dict(r) for r in rows]

@mcp.tool()
def top_receivers(value: dict | int = 10):
    """
    Return top receiver addresses by tx count.
    Prefers metrics_snapshot.top_receivers_json; falls back to live query over the
    same window if snapshot is missing.
    """
    limit = int(value if isinstance(value, int) else _get(value, "limit", 10))
    limit = max(1, min(limit, 500))

    ms = db.execute("SELECT window_n_blocks, to_block, top_receivers_json FROM metrics_snapshot WHERE id=1").fetchone()
    if ms and ms["top_receivers_json"]:
        arr = json.loads(ms["top_receivers_json"])
        return arr[:limit]

    if ms:
        to_block = ms["to_block"]
        from_block = max(0, to_block - int(ms["window_n_blocks"]) + 1)
        rows = db.execute("""
            SELECT "to" AS address, COUNT(*) AS count
            FROM txs
            WHERE block_number BETWEEN ? AND ? AND "to" IS NOT NULL
            GROUP BY "to"
            ORDER BY count DESC
            LIMIT ?
        """, (from_block, to_block, limit)).fetchall()
        return [row_to_dict(r) for r in rows]

    rows = db.execute("""
        SELECT "to" AS address, COUNT(*) AS count
        FROM txs
        WHERE "to" IS NOT NULL
        GROUP BY "to"
        ORDER BY count DESC
        LIMIT ?
    """, (limit,)).fetchall()
    return [row_to_dict(r) for r in rows]

@mcp.tool()
def recent_nft_transfers(value: dict | int = 20):
    """
    Return the most recent NFT transfers (ERC-721 + ERC-1155 Single).
    Optional: { "limit": 20, "collection": "0x..." }
    """
    limit = int(value if isinstance(value, int) else _get(value, "limit", 20))
    limit = max(1, min(limit, 500))

    coll = None if isinstance(value, int) else _get(value, "collection", None)
    if coll:
        rows = db.execute("""
            SELECT block_number, tx_hash, log_index, collection, token_id, qty, "from", "to", ts
            FROM nft_transfers
            WHERE lower(collection)=lower(?)
            ORDER BY block_number DESC, log_index ASC
            LIMIT ?
        """, (coll, limit)).fetchall()
    else:
        rows = db.execute("""
            SELECT block_number, tx_hash, log_index, collection, token_id, qty, "from", "to", ts
            FROM nft_transfers
            ORDER BY block_number DESC, log_index ASC
            LIMIT ?
        """, (limit,)).fetchall()

    return [row_to_dict(r) for r in rows]

@mcp.tool()
def top_collections(value: dict | int = 10):
    """
    Top NFT collections by transfer count in the last window.
    Args: { "limit": 10, "window_hours": 24 }
    """
    limit = int(value if isinstance(value, int) else _get(value, "limit", 10))
    limit = max(1, min(limit, 200))
    window_hours = 24 if isinstance(value, int) else int(_get(value, "window_hours", 24))
    window_secs = max(1, window_hours) * 3600

    rows = db.execute("""
        WITH now(cur_ts) AS (SELECT strftime('%s','now'))
        SELECT nt.collection,
               COUNT(*) AS transfers,
               SUM(nt.qty) AS units,
               COUNT(DISTINCT nt."from") + COUNT(DISTINCT nt."to") AS participants
        FROM nft_transfers AS nt, now
        WHERE nt.ts >= now.cur_ts - ?
        GROUP BY nt.collection
        ORDER BY transfers DESC
        LIMIT ?
    """, (window_secs, limit)).fetchall()

    return [row_to_dict(r) for r in rows]

@mcp.tool()
def nft_trending(value: dict | int = 20):
    """
    Trending NFTs by activity in the last window.
    Args: { "limit": 20, "window_hours": 6 }
    Returns: collection, token_id, tx_count, actor_count
    """
    limit = int(value if isinstance(value, int) else _get(value, "limit", 20))
    limit = max(1, min(limit, 200))
    window_hours = 6 if isinstance(value, int) else int(_get(value, "window_hours", 6))
    window_secs = max(1, window_hours) * 3600

    rows = db.execute("""
        WITH now(cur_ts) AS (SELECT strftime('%s','now'))
        SELECT nt.collection,
               nt.token_id,
               COUNT(*) AS tx_count,
               (COUNT(DISTINCT nt."from") + COUNT(DISTINCT nt."to")) AS actor_count
        FROM nft_transfers AS nt, now
        WHERE nt.ts >= now.cur_ts - ?
        GROUP BY nt.collection, nt.token_id
        ORDER BY tx_count DESC, actor_count DESC
        LIMIT ?
    """, (window_secs, limit)).fetchall()

    return [row_to_dict(r) for r in rows]

@mcp.tool()
def unique_holders(value: dict | str):
    """
    Unique holders for a collection.
    Args: { "collection": "0x..." }  OR just the collection string
    """
    coll = value if isinstance(value, str) else _get(value, "collection", "")
    if not coll:
        return {"error": "collection is required"}

    row = db.execute("""
        SELECT COUNT(*) AS unique_holders
        FROM nft_owners
        WHERE lower(collection)=lower(?)
    """, (coll,)).fetchone()

    return {"collection": coll, "unique_holders": row["unique_holders"] if row else 0}

@mcp.tool()
def holders_top5_share(value: dict | str):
    """
    Top-5 owner concentration (share of tokens) for a collection.
    Args: { "collection": "0x..." } OR string
    Note: uses simplified owner map (one owner per token_id).
    """
    coll = value if isinstance(value, str) else _get(value, "collection", "")
    if not coll:
        return {"error": "collection is required"}

    owners = db.execute("""
        SELECT owner, COUNT(*) AS tokens
        FROM nft_owners
        WHERE lower(collection)=lower(?)
        GROUP BY owner
        ORDER BY tokens DESC
    """, (coll,)).fetchall()

    if not owners:
        return {"collection": coll, "top5_share": None, "owners": []}

    total = sum(r["tokens"] for r in owners)
    top5  = sum(r["tokens"] for r in owners[:5])
    share = (top5 / total) if total else None

    return {
        "collection": coll,
        "top5_share": share,
        "owners": [row_to_dict(r) for r in owners[:5]]
    }

@mcp.tool()
def top_owners(value: dict | str):
    """
    Top owners by token count for a collection.
    Args: { "collection": "0x...", "limit": 10 } OR string
    """
    if isinstance(value, str):
        coll = value
        limit = 10
    else:
        coll = _get(value, "collection", "")
        limit = int(_get(value, "limit", 10))
    if not coll:
        return {"error": "collection is required"}
    limit = max(1, min(limit, 200))

    rows = db.execute("""
        SELECT owner, COUNT(*) AS tokens
        FROM nft_owners
        WHERE lower(collection)=lower(?)
        GROUP BY owner
        ORDER BY tokens DESC
        LIMIT ?
    """, (coll, limit)).fetchall()

    return [row_to_dict(r) for r in rows]

@mcp.tool()
def token_flow(value: dict | str):
    """
    Windowed ERC-20 flow stats for a token.
    Args: { "token": "0x...", "window_hours": 24 } OR just token string
    Returns: transfers, unique_senders, unique_receivers, mints, burns, total_value_wei
    """
    if isinstance(value, str):
        token = value
        window_hours = 24
    else:
        token = _get(value, "token", "")
        window_hours = int(_get(value, "window_hours", 24))
    if not token:
        return {"error": "token is required"}
    window_secs = max(1, window_hours) * 3600

    row = db.execute("""
        WITH now(cur_ts) AS (SELECT strftime('%s','now'))
        SELECT
          COUNT(*)                                                        AS transfers,
          COUNT(DISTINCT "from")                                         AS unique_senders,
          COUNT(DISTINCT "to")                                           AS unique_receivers,
          SUM(CASE WHEN lower("from")='0x0000000000000000000000000000000000000000' THEN 1 ELSE 0 END) AS mints,
          SUM(CASE WHEN lower("to")  ='0x0000000000000000000000000000000000000000' THEN 1 ELSE 0 END) AS burns,
          COALESCE(SUM(CAST(value_wei AS INTEGER)), 0)                   AS total_value_wei
        FROM erc20_transfers, now
        WHERE lower(token)=lower(?) AND ts >= now.cur_ts - ?
    """, (token, window_secs)).fetchone()

    meta = db.execute("SELECT name, symbol, decimals, total_supply_wei FROM erc20_meta WHERE lower(token)=lower(?)", (token,)).fetchone()
    out = row_to_dict(row)
    out["token"] = token
    if meta:
        out["meta"] = row_to_dict(meta)
    return out

@mcp.tool()
def token_top_holders(value: dict | str):
    """
    Approximate top holders by aggregating ERC-20 transfers since your index start.
    Args: { "token": "0x...", "limit": 10 } OR just token string
    NOTE: This is a naive net flow (in - out). It ignores initial pre-index balances.
    """
    if isinstance(value, str):
        token = value
        limit = 10
    else:
        token = _get(value, "token", "")
        limit = int(_get(value, "limit", 10))
    if not token:
        return {"error": "token is required"}
    limit = max(1, min(limit, 200))

    rows = db.execute("""
        WITH credits AS (
          SELECT "to" AS addr, SUM(CAST(value_wei AS INTEGER)) AS val
          FROM erc20_transfers
          WHERE lower(token)=lower(?) AND lower("to")<>'0x0000000000000000000000000000000000000000'
          GROUP BY "to"
        ),
        debits AS (
          SELECT "from" AS addr, SUM(CAST(value_wei AS INTEGER)) AS val
          FROM erc20_transfers
          WHERE lower(token)=lower(?) AND lower("from")<>'0x0000000000000000000000000000000000000000'
          GROUP BY "from"
        )
        SELECT address, SUM(val) AS net_wei FROM (
          SELECT addr AS address, val       FROM credits
          UNION ALL
          SELECT addr AS address, -val      FROM debits
        )
        GROUP BY address
        ORDER BY net_wei DESC
        LIMIT ?
    """, (token, token, limit)).fetchall()

    return [row_to_dict(r) for r in rows]

# ========== NEW: strongly-typed inputs for wrappers ==========
class BlockIn(BaseModel):
    number: int
    with_txs: bool = False
    tx_sample: int = Field(20, ge=1, le=500)

class HashIn(BaseModel):
    hash: str

class WindowIn(BaseModel):
    window_hours: int = Field(24, ge=1, le=24*30)
    limit: int = Field(10, ge=1, le=500)

class CollectionIn(BaseModel):
    collection: str
    limit: int = Field(10, ge=1, le=500)

class TokenIn(BaseModel):
    token: str
    window_hours: int = Field(24, ge=1, le=24*30)
    limit: int = Field(10, ge=1, le=500)

# ========== NEW: typed wrapper tools (call existing tools) ==========
@mcp.tool(name="blocks_latest")
def blocks_latest_t(args: LimitIn):
    """Latest N blocks."""
    return latest_blocks(args.limit)

@mcp.tool(name="txs_latest")
def txs_latest_t(args: LimitIn):
    """Latest N transactions."""
    return latest_txs(args.limit)

@mcp.tool(name="block_get")
def block_get_t(args: BlockIn):
    """Block by number (optional tx sample)."""
    return block_by_number({"number": args.number, "with_txs": args.with_txs, "tx_sample": args.tx_sample})

@mcp.tool(name="tx_get")
def tx_get_t(args: HashIn):
    """Transaction by hash."""
    return tx_by_hash({"hash": args.hash})

@mcp.tool(name="network_health")
def network_health_t() -> dict:
    """Indexer + metrics health."""
    return health({})

@mcp.tool(name="participants_top_senders")
def participants_top_senders_t(args: LimitIn):
    """Top senders by tx count over current window."""
    return top_senders({"limit": args.limit})

@mcp.tool(name="participants_top_receivers")
def participants_top_receivers_t(args: LimitIn):
    """Top receivers by tx count over current window."""
    return top_receivers({"limit": args.limit})

@mcp.tool(name="nft_top_collections")
def nft_top_collections_t(args: WindowIn):
    """Top NFT collections in a time window."""
    return top_collections({"limit": args.limit, "window_hours": args.window_hours})

@mcp.tool(name="nft_trending_tokens")
def nft_trending_tokens_t(args: WindowIn):
    """Trending NFTs (collection, token_id) in a window."""
    return nft_trending({"limit": args.limit, "window_hours": args.window_hours})

@mcp.tool(name="nft_recent")
def nft_recent_t(args: LimitIn):
    """Recent NFT transfers."""
    return recent_nft_transfers({"limit": args.limit})

@mcp.tool(name="nft_collection_holders")
def nft_collection_holders_t(args: CollectionIn):
    """Unique holders, top owners, top-5 share for a collection."""
    uh = unique_holders({"collection": args.collection})
    top = top_owners({"collection": args.collection, "limit": args.limit})
    conc = holders_top5_share({"collection": args.collection})
    return {
        "collection": args.collection,
        "unique_holders": uh.get("unique_holders") if isinstance(uh, dict) else uh,
        "top_owners": top,
        "top5_share": conc.get("top5_share") if isinstance(conc, dict) else None,
        "owners_sample": conc.get("owners") if isinstance(conc, dict) else []
    }

@mcp.tool(name="token_flow_window")
def token_flow_window_t(args: TokenIn):
    """ERC-20 flow stats + top holders for a token in a window."""
    flow = token_flow({"token": args.token, "window_hours": args.window_hours})
    holders = token_top_holders({"token": args.token, "limit": args.limit})
    return {"flow": flow, "top_holders": holders}

if __name__ == "__main__":
    # HTTP mode for n8n
    # Use whichever your FastMCP version supports:
    try:
        mcp.run(host="0.0.0.0", port=8000, transport="http")
    except TypeError:
        from fastmcp import Transport
        mcp.run(transport=Transport.HTTP, host="0.0.0.0", port=8000)

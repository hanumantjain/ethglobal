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

    # fallback: compute from txs over the last window if we have block bounds
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

    # final fallback: all-time top senders
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

    # fallback: compute from txs over the last window if we have block bounds
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

    # final fallback: all-time top receivers
    rows = db.execute("""
        SELECT "to" AS address, COUNT(*) AS count
        FROM txs
        WHERE "to" IS NOT NULL
        GROUP BY "to"
        ORDER BY count DESC
        LIMIT ?
    """, (limit,)).fetchall()
    return [row_to_dict(r) for r in rows]


if __name__ == "__main__":
    # HTTP mode for n8n
    # Use whichever your FastMCP version supports:
    try:
        mcp.run(host="0.0.0.0", port=8000, transport="http")
    except TypeError:
        from fastmcp import Transport
        mcp.run(transport=Transport.HTTP, host="0.0.0.0", port=8000)

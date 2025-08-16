# mcp_server.py
import os
import sqlite3
from typing import List, Dict, Any

from dotenv import load_dotenv
from fastmcp import FastMCP  # ToolSpec and mcp_tool not needed in latest versions

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

# ----------------- Tools -----------------

@mcp.tool()
def latest_blocks(limit: int = 10) -> List[Dict[str, Any]]:
    """Return the latest N blocks (descending by number)."""
    limit = max(1, min(int(limit), 500))
    rows = db.execute(
        """
        SELECT number, hash, parent_hash, timestamp, gas_limit, gas_used,
               base_fee_per_gas_wei, miner, tx_count
        FROM blocks
        ORDER BY number DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [row_to_dict(r) for r in rows]

@mcp.tool()
def latest_txs(limit: int = 10) -> List[Dict[str, Any]]:
    """Return the latest N transactions (descending by block_number, tx_index)."""
    limit = max(1, min(int(limit), 500))
    rows = db.execute(
        """
        SELECT hash, block_number, tx_index, "from", "to", value_wei, nonce, gas,
               gas_price, max_fee_per_gas_wei, max_priority_fee_per_gas_wei,
               status, gas_used, effective_gas_price_wei, contract_address
        FROM txs
        ORDER BY block_number DESC, tx_index DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [row_to_dict(r) for r in rows]

@mcp.tool()
def block_by_number(number: int, with_txs: bool = False, tx_sample: int = 20) -> Dict[str, Any]:
    """Return a block by number (optionally with some txs)."""
    blk = db.execute(
        """
        SELECT number, hash, parent_hash, timestamp, gas_limit, gas_used,
               base_fee_per_gas_wei, miner, tx_count
        FROM blocks WHERE number=?
        """,
        (number,),
    ).fetchone()
    if not blk:
        return {"error": f"block {number} not found"}

    out = row_to_dict(blk)

    if with_txs:
        tx_sample = max(1, min(int(tx_sample), 500))
        txs = db.execute(
            """
            SELECT hash, tx_index, "from", "to", value_wei, status
            FROM txs
            WHERE block_number=?
            ORDER BY tx_index ASC
            LIMIT ?
            """,
            (number, tx_sample),
        ).fetchall()
        out["txs"] = [row_to_dict(r) for r in txs]
    return out

@mcp.tool()
def tx_by_hash(hash: str) -> Dict[str, Any]:
    """Return a transaction by its hash."""
    h = hash.lower()
    row = db.execute(
        """
        SELECT hash, block_number, tx_index, "from", "to", value_wei, nonce, gas,
               gas_price, max_fee_per_gas_wei, max_priority_fee_per_gas_wei,
               status, gas_used, effective_gas_price_wei, contract_address
        FROM txs WHERE lower(hash)=?
        """,
        (h,),
    ).fetchone()
    if not row:
        return {"error": f"tx {hash} not found"}
    return row_to_dict(row)

@mcp.tool()
def health() -> Dict[str, Any]:
    """Return simple MCP + DB health stats."""
    blk = db.execute("SELECT MAX(number) AS max_block, COUNT(*) AS nblocks FROM blocks").fetchone()
    tx = db.execute("SELECT COUNT(*) AS ntx, MAX(block_number) AS max_tx_block FROM txs").fetchone()
    return {
        "db_path": DB_PATH,
        "blocks": {"count": blk["nblocks"], "max_number": blk["max_block"]},
        "txs": {"count": tx["ntx"], "max_block_number": tx["max_tx_block"]},
    }

if __name__ == "__main__":
    mcp.run(host="0.0.0.0", port=8000, transport="http")


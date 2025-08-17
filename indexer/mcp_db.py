# mcp_db.py
import sqlite3
from typing import Dict, Any
from mcp_config import DB_PATH

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

db = get_db()

def row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {k: row[k] for k in row.keys()}

def _get(obj, key, default):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return default

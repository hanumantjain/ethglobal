import sqlite3, time
from typing import Dict, Any
import config

def db():
    conn = sqlite3.connect(config.DB_PATH, isolation_level=None, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn

def ensure_schema(conn: sqlite3.Connection):
    # read SQL from external file
    with open("schema.sql", "r", encoding="utf-8") as f:
        conn.executescript(f.read())
    conn.execute("INSERT OR IGNORE INTO meta(k, v) VALUES('last_safe_block','-1');")

def get_meta(conn, key, default):
    row = conn.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone()
    return row[0] if row else default

def set_meta(conn, key, value):
    conn.execute("INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v;", (key, value))

def upsert_metrics_snapshot(conn, payload: Dict[str, Any]):
    cols = ",".join(payload.keys())
    qmarks = ",".join(["?"] * len(payload))
    conn.execute(f"""
        INSERT INTO metrics_snapshot (id,{cols})
        VALUES (1,{qmarks})
        ON CONFLICT(id) DO UPDATE SET
        {", ".join([f"{k}=excluded.{k}" for k in payload.keys()])}
    """, tuple(payload.values()))

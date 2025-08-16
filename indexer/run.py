import asyncio
import sqlite3
import os
import uvloop
from dotenv import load_dotenv
from web3 import AsyncWeb3
from web3.providers.async_rpc import AsyncHTTPProvider
from web3.types import HexBytes

# ----------------------- load .env -----------------------
load_dotenv(".env")  # always load from local file

RPC_URL         = os.getenv("RPC_URL")
CONFIRMS        = int(os.getenv("CONFIRMS", "6"))
BACKFILL_BLOCKS = int(os.getenv("BACKFILL_BLOCKS", "5000"))
RECEIPT_CONC    = int(os.getenv("RECEIPT_CONC", "20"))
DB_PATH         = os.getenv("DB_PATH", "katana_index.sqlite")

if not RPC_URL:
    raise SystemExit("Missing RPC_URL in .env")

# ----------------------- DB schema helpers -----------------------
def db():
    conn = sqlite3.connect(DB_PATH, isolation_level=None, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn

def ensure_schema(conn: sqlite3.Connection):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS meta (
        k TEXT PRIMARY KEY,
        v TEXT
    );

    CREATE TABLE IF NOT EXISTS blocks (
        number                INTEGER PRIMARY KEY,
        hash                  TEXT NOT NULL,
        parent_hash           TEXT NOT NULL,
        timestamp             INTEGER NOT NULL,
        gas_limit             TEXT,
        gas_used              TEXT,
        base_fee_per_gas_wei  TEXT,
        miner                 TEXT,
        tx_count              INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS txs (
        hash                      TEXT PRIMARY KEY,
        block_number              INTEGER NOT NULL,
        tx_index                  INTEGER NOT NULL,
        "from"                    TEXT NOT NULL,
        "to"                      TEXT,
        value_wei                 TEXT NOT NULL,
        nonce                     INTEGER NOT NULL,
        gas                       INTEGER NOT NULL,
        gas_price                 TEXT,
        max_fee_per_gas_wei       TEXT,
        max_priority_fee_per_gas_wei TEXT,

        status                    INTEGER,
        gas_used                  INTEGER,
        effective_gas_price_wei   TEXT,
        contract_address          TEXT,

        FOREIGN KEY(block_number) REFERENCES blocks(number) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_txs_block ON txs(block_number);
    CREATE INDEX IF NOT EXISTS idx_txs_from ON txs("from");
    CREATE INDEX IF NOT EXISTS idx_txs_to   ON txs("to");
    """)
    conn.execute("INSERT OR IGNORE INTO meta(k, v) VALUES('last_safe_block','-1');")

def get_meta(conn, key, default):
    row = conn.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone()
    return row[0] if row else default

def set_meta(conn, key, value):
    conn.execute("INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v;", (key, value))

# ----------------------- helpers -----------------------
def to_hex(x):
    if x is None: return None
    if isinstance(x, HexBytes): return x.hex()
    if isinstance(x, int): return hex(x)
    return str(x)

def to_addr(x):
    if x is None: return None
    return AsyncWeb3.to_checksum_address(x)

# ----------------------- core indexer -----------------------
async def fetch_block(w3, num):
    return await w3.eth.get_block(block_identifier=num, full_transactions=True)

async def fetch_receipt(w3, tx_hash):
    return await w3.eth.get_transaction_receipt(tx_hash)

async def index_range(conn, w3, start, end):
    for n in range(start, end + 1):
        b = await fetch_block(w3, n)

        # insert block
        conn.execute("""
        INSERT OR IGNORE INTO blocks(number, hash, parent_hash, timestamp, gas_limit, gas_used, base_fee_per_gas_wei, miner, tx_count)
        VALUES(?,?,?,?,?,?,?,?,?)
        """, (
            b["number"],
            b["hash"].hex(),
            b["parentHash"].hex(),
            int(b["timestamp"]),
            to_hex(b.get("gasLimit")),
            to_hex(b.get("gasUsed")),
            to_hex(b.get("baseFeePerGas")),
            to_addr(b.get("miner") or b.get("coinbase")),
            len(b["transactions"])
        ))

        # receipts in parallel
        sem = asyncio.Semaphore(RECEIPT_CONC)
        async def rec_task(tx):
            async with sem:
                try:
                    return tx, await fetch_receipt(w3, tx["hash"])
                except:
                    return tx, None

        rec_pairs = await asyncio.gather(*[rec_task(tx) for tx in b["transactions"]])

        # insert txs
        for tx, rec in rec_pairs:
            conn.execute("""
            INSERT OR IGNORE INTO txs(
                hash, block_number, tx_index, "from","to", value_wei, nonce, gas,
                gas_price, max_fee_per_gas_wei, max_priority_fee_per_gas_wei,
                status, gas_used, effective_gas_price_wei, contract_address
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                tx["hash"].hex(),
                tx["blockNumber"],
                tx["transactionIndex"],
                to_addr(tx["from"]),
                to_addr(tx.get("to")),
                str(tx["value"]),
                tx["nonce"],
                tx["gas"],
                to_hex(tx.get("gasPrice")),
                to_hex(tx.get("maxFeePerGas")),
                to_hex(tx.get("maxPriorityFeePerGas")),
                (rec and rec.get("status")),
                (rec and rec.get("gasUsed")),
                (to_hex(rec.get("effectiveGasPrice")) if rec else None),
                (to_addr(rec.get("contractAddress")) if rec else None)
            ))

        set_meta(conn, "last_safe_block", str(n))
        print(f"Indexed block {n} (txs {len(b['transactions'])})")

async def main():
    w3 = AsyncWeb3(AsyncHTTPProvider(RPC_URL))
    latest = await w3.eth.block_number
    print(f"Connected to Katana, head={latest}")

    conn = db()
    ensure_schema(conn)

    last_safe = int(get_meta(conn, "last_safe_block", "-1"))
    if last_safe < 0:
        start_from = max(0, latest - BACKFILL_BLOCKS)
    else:
        start_from = last_safe + 1

    while True:
        head = await w3.eth.block_number
        safe = max(0, head - CONFIRMS)

        if start_from <= safe:
            await index_range(conn, w3, start_from, safe)
            start_from = safe + 1
        else:
            await asyncio.sleep(1.0)

if __name__ == "__main__":
    uvloop.run(main())

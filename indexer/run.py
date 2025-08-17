import asyncio
import sqlite3
import os
import uvloop
from dotenv import load_dotenv
from web3 import AsyncWeb3, Web3
from web3.providers.async_rpc import AsyncHTTPProvider
from web3.types import HexBytes
import pathlib
import json, time
from typing import List, Dict, Any

# ----------------------- load .env -----------------------
load_dotenv(".env")  # always load from local file

RPC_URL              = os.getenv("RPC_URL")
CONFIRMS             = int(os.getenv("CONFIRMS", "6"))
BACKFILL_BLOCKS      = int(os.getenv("BACKFILL_BLOCKS", "5000"))
RECEIPT_CONC         = int(os.getenv("RECEIPT_CONC", "20"))
DB_PATH              = os.getenv("DB_PATH", "katana_index.sqlite")
METRIC_WINDOW        = int(os.getenv("METRIC_WINDOW", "200"))
TOP_K                = int(os.getenv("TOP_K", "10"))
METRICS_EVERY_BLOCKS = int(os.getenv("METRICS_EVERY_BLOCKS", "1000"))


# --- NFT event topics (keccak256) ---
ERC721_TRANSFER_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ERC1155_TRANSFER_SINGLE = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
# (optional) batch: ERC1155_TRANSFER_BATCH = "0x4a39dc06d4c0dbc64b70...2d0d"
ZERO_ADDR = "0x0000000000000000000000000000000000000000"

if not RPC_URL:
    raise SystemExit("Missing RPC_URL in .env")

# Import smart contract watchlist
CONTRACTS_PATH = os.getenv("CONTRACTS_PATH", "contracts.json")
CONTRACTS = []
p = pathlib.Path(CONTRACTS_PATH)
if p.exists():
    try:
        CONTRACTS = json.loads(p.read_text())
    except Exception as e:
        print(f"[contracts] failed to parse {CONTRACTS_PATH}: {e}")
else:
    print(f"[contracts] {CONTRACTS_PATH} not found; continuing without contract watchlist")

# normalize & cache the addresses we will watch
WATCH_ERC20 = []
for c in CONTRACTS:
    if (c.get("type") or "").lower() == "erc20":
        try:
            WATCH_ERC20.append(Web3.to_checksum_address(c["address"]))
        except Exception:
            pass
WATCH_ERC20_LOWER = {a.lower() for a in WATCH_ERC20}

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
                       
        -- known contracts (optional metadata cache)
    CREATE TABLE IF NOT EXISTS contracts (
      address TEXT PRIMARY KEY,
      name    TEXT,
      type    TEXT
    );

    -- ERC20 transfers (fast path)
    CREATE TABLE IF NOT EXISTS erc20_transfers (
      block_number INTEGER,
      tx_hash      TEXT,
      log_index    INTEGER,
      token        TEXT,            -- contract address
      "from"       TEXT,
      "to"         TEXT,
      value_wei    TEXT,            -- as decimal string
      ts           INTEGER,
      PRIMARY KEY (tx_hash, log_index)
    );
    CREATE INDEX IF NOT EXISTS idx_erc20_token_ts ON erc20_transfers(token, ts);
    CREATE INDEX IF NOT EXISTS idx_erc20_from ON erc20_transfers("from");
    CREATE INDEX IF NOT EXISTS idx_erc20_to   ON erc20_transfers("to");

    -- lightweight token meta cache (filled via eth_call on demand)
    CREATE TABLE IF NOT EXISTS erc20_meta (
      token           TEXT PRIMARY KEY,
      name            TEXT,
      symbol          TEXT,
      decimals        INTEGER,
      total_supply_wei TEXT,
      updated_at      INTEGER
    );
                       

    -- raw logs you decide to keep (optional but handy for debugging)
    CREATE TABLE IF NOT EXISTS logs (
    block_number INTEGER,
    tx_hash TEXT,
    log_index INTEGER,
    address TEXT,             -- emitter (contract)
    topic0 TEXT, topic1 TEXT, topic2 TEXT, topic3 TEXT,
    data TEXT,
    PRIMARY KEY (tx_hash, log_index)
    );

    -- normalized NFT transfers (ERC-721 + 1155 TransferSingle)
    CREATE TABLE IF NOT EXISTS nft_transfers (
    block_number INTEGER,
    tx_hash TEXT,
    log_index INTEGER,
    collection TEXT,          -- contract address (checksum)
    token_id TEXT,            -- 721: uint256 ; 1155: id
    qty INTEGER DEFAULT 1,    -- 721=1 ; 1155=value
    "from" TEXT,
    "to"   TEXT,
    ts INTEGER,               -- block.timestamp (denormalize for speed)
    PRIMARY KEY (tx_hash, log_index)
    );
    CREATE INDEX IF NOT EXISTS idx_nft_coll ON nft_transfers(collection);
    CREATE INDEX IF NOT EXISTS idx_nft_token ON nft_transfers(collection, token_id);

    -- optional: current owner map (fast holder/unique-holder queries)
    CREATE TABLE IF NOT EXISTS nft_owners (
    collection TEXT,
    token_id TEXT,
    owner TEXT,
    PRIMARY KEY (collection, token_id)
    );
    CREATE INDEX IF NOT EXISTS idx_nft_owner ON nft_owners(owner);

    CREATE TABLE IF NOT EXISTS metrics_snapshot (
        id INTEGER PRIMARY KEY CHECK (id=1),
        computed_at          INTEGER NOT NULL,
        window_n_blocks      INTEGER NOT NULL,
        from_block           INTEGER NOT NULL,
        to_block             INTEGER NOT NULL,
        avg_block_time_s     REAL,
        tps                  REAL,
        gas_utilization_avg  REAL,
        base_fee_wei_avg     TEXT,
        base_fee_wei_p50     TEXT,
        tx_success           INTEGER,
        tx_failed            INTEGER,
        top_senders_json     TEXT,
        top_receivers_json   TEXT
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

def hex_to_int(x):
    if x is None: return None
    if isinstance(x, int): return x
    if isinstance(x, bytes): return int.from_bytes(x, "big")
    s = str(x)
    return int(s, 16) if s.startswith("0x") else int(s)

def upsert_metrics_snapshot(conn, payload: Dict[str, Any]):
    cols = ",".join(payload.keys())
    qmarks = ",".join(["?"] * len(payload))
    conn.execute(f"""
        INSERT INTO metrics_snapshot (id,{cols})
        VALUES (1,{qmarks})
        ON CONFLICT(id) DO UPDATE SET
        {", ".join([f"{k}=excluded.{k}" for k in payload.keys()])}
    """, tuple(payload.values()))

def topic_to_addr(topic_hex: str) -> str:
    # topics are 32-byte values; address is the last 20 bytes
    return AsyncWeb3.to_checksum_address("0x" + topic_hex[-40:])

def topic_to_u256(topic_hex: str) -> int:
    return int(topic_hex, 16)

def decode_1155_data(data_hex: str) -> tuple[int, int]:
    """
    ERC1155 TransferSingle data = abi.encode(id (uint256), value (uint256))
    """
    h = data_hex[2:] if data_hex.startswith("0x") else data_hex
    if len(h) < 64*2:
        raise ValueError("bad 1155 data length")
    id_hex  = h[:64]
    val_hex = h[64:128]
    return int(id_hex, 16), int(val_hex, 16)

# ----------------------- core indexer -----------------------
async def fetch_block(w3, num):
    return await w3.eth.get_block(block_identifier=num, full_transactions=True)

async def fetch_receipt(w3, tx_hash):
    return await w3.eth.get_transaction_receipt(tx_hash)

# --- ERC-20 topics ---
ERC20_TRANSFER_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ERC20_APPROVAL_TOPIC0 = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"

def topic_to_address_from_32b(t: str) -> str:
    return AsyncWeb3.to_checksum_address("0x" + t[-40:])

async def process_erc20_logs_for_block(conn, w3, block_number: int, block_ts: int):
    if not WATCH_ERC20:
        return

    # Pull logs for this block scoped to watched addresses
    logs = await w3.eth.get_logs({
        "fromBlock": block_number,
        "toBlock": block_number,
        "address": WATCH_ERC20
    })

    for lg in logs:
        addr      = AsyncWeb3.to_checksum_address(lg["address"])
        txh       = lg["transactionHash"].hex() if hasattr(lg["transactionHash"], "hex") else str(lg["transactionHash"])
        log_index = int(lg["logIndex"])
        topics    = [t.hex() if hasattr(t, "hex") else str(t) for t in lg["topics"]]
        if not topics:
            continue
        topic0 = topics[0].lower()
        data_hex = lg["data"] if isinstance(lg["data"], str) else lg["data"].hex()

        # Decode known ERC-20 events
        if topic0 == ERC20_TRANSFER_TOPIC0 and len(topics) >= 3:
            from_addr = topic_to_address_from_32b(topics[1])
            to_addr   = topic_to_address_from_32b(topics[2])
            # data is uint256 value
            h = data_hex[2:] if data_hex.startswith("0x") else data_hex
            value_int = int(h or "0", 16)
            conn.execute("""
                INSERT OR IGNORE INTO erc20_transfers
                (block_number, tx_hash, log_index, token, "from", "to", value_wei, ts)
                VALUES (?,?,?,?,?,?,?,?)
            """, (block_number, txh, log_index, addr, from_addr, to_addr, str(value_int), int(block_ts)))

        # You can store approvals as needed, but not necessary for first insights
        # elif topic0 == ERC20_APPROVAL_TOPIC0 and len(topics) >= 3:
        #     owner = topic_to_address_from_32b(topics[1])
        #     spender = topic_to_address_from_32b(topics[2])
        #     value = int((data_hex[2:] if data_hex.startswith("0x") else data_hex) or "0", 16)
        #     # (optional) store in a table if you care about allowances


async def process_nft_logs_for_block(conn, w3, block_number: int, block_ts: int):
    # fetch only the NFT transfer topics for this block
    logs = await w3.eth.get_logs({
        "fromBlock": block_number,
        "toBlock": block_number,
        "topics": [[ERC721_TRANSFER_TOPIC0, ERC1155_TRANSFER_SINGLE]]
    })

    for lg in logs:
        # normalize fields per log
        addr       = AsyncWeb3.to_checksum_address(lg["address"])
        txh        = lg["transactionHash"].hex() if hasattr(lg["transactionHash"], "hex") else str(lg["transactionHash"])
        log_index  = int(lg["logIndex"])
        topics     = [t.hex() if hasattr(t, "hex") else str(t) for t in lg["topics"]]
        topic0     = topics[0].lower()
        data_hex   = lg["data"] if isinstance(lg["data"], str) else lg["data"].hex()

        # (optional) persist raw log for debugging
        try:
            conn.execute("""
                INSERT OR IGNORE INTO logs
                (block_number, tx_hash, log_index, address, topic0, topic1, topic2, topic3, data)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                block_number, txh, log_index, addr,
                topics[0] if len(topics) > 0 else None,
                topics[1] if len(topics) > 1 else None,
                topics[2] if len(topics) > 2 else None,
                topics[3] if len(topics) > 3 else None,
                data_hex
            ))
        except Exception:
            # don't let a debug insert break indexing
            pass

        if topic0 == ERC721_TRANSFER_TOPIC0:
            # ERC-721: topics[1]=from, [2]=to, [3]=tokenId (indexed)
            if len(topics) < 4:
                continue
            from_addr = topic_to_addr(topics[1])
            to_addr_  = topic_to_addr(topics[2])
            token_id  = str(topic_to_u256(topics[3]))
            qty       = 1

            conn.execute("""
                INSERT OR IGNORE INTO nft_transfers
                (block_number, tx_hash, log_index, collection, token_id, qty, "from","to", ts)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (block_number, txh, log_index, addr, token_id, qty, from_addr, to_addr_, int(block_ts)))

            # owners map (burn/mint/transfer)
            if to_addr_.lower() == ZERO_ADDR:
                conn.execute("DELETE FROM nft_owners WHERE collection=? AND token_id=?", (addr, token_id))
            else:
                conn.execute("""
                    INSERT INTO nft_owners(collection, token_id, owner)
                    VALUES(?,?,?)
                    ON CONFLICT(collection, token_id) DO UPDATE SET owner=excluded.owner
                """, (addr, token_id, to_addr_))

        elif topic0 == ERC1155_TRANSFER_SINGLE:
            # ERC-1155 TransferSingle: topics[1]=operator, [2]=from, [3]=to ; data encodes (id,value)
            if len(topics) < 4:
                continue
            from_addr = topic_to_addr(topics[2])
            to_addr_  = topic_to_addr(topics[3])
            token_id_u256, qty_u256 = decode_1155_data(data_hex)
            token_id = str(token_id_u256)
            qty      = int(qty_u256)

            conn.execute("""
                INSERT OR IGNORE INTO nft_transfers
                (block_number, tx_hash, log_index, collection, token_id, qty, "from","to", ts)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (block_number, txh, log_index, addr, token_id, qty, from_addr, to_addr_, int(block_ts)))

            # simplified owner map for hackathon
            if to_addr_.lower() == ZERO_ADDR:
                conn.execute("DELETE FROM nft_owners WHERE collection=? AND token_id=?", (addr, token_id))
            else:
                conn.execute("""
                    INSERT INTO nft_owners(collection, token_id, owner)
                    VALUES(?,?,?)
                    ON CONFLICT(collection, token_id) DO UPDATE SET owner=excluded.owner
                """, (addr, token_id, to_addr_))
        # else: ignore other topics



async def index_range(conn, w3, start, end):
    """
    Index blocks [start, end] inclusive.
    Also computes rolling network metrics every `METRICS_EVERY_BLOCKS` blocks,
    so you'll see metrics populate during long backfills.
    """
    blocks_since_metrics = 0

    for n in range(start, end + 1):
        # ---- fetch block (with full txs) ----
        b = await w3.eth.get_block(block_identifier=n, full_transactions=True)

        # ---- insert block ----
        conn.execute("""
        INSERT OR IGNORE INTO blocks(
            number, hash, parent_hash, timestamp, gas_limit, gas_used,
            base_fee_per_gas_wei, miner, tx_count
        ) VALUES(?,?,?,?,?,?,?,?,?)
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

        await process_nft_logs_for_block(conn, w3, n, int(b["timestamp"]))
        await process_nft_logs_for_block(conn, w3, n, int(b["timestamp"]))
        await process_erc20_logs_for_block(conn, w3, n, int(b["timestamp"]))
        # ---- fetch receipts in parallel (bounded) ----
        sem = asyncio.Semaphore(RECEIPT_CONC)

        async def rec_task(tx):
            async with sem:
                try:
                    rec = await w3.eth.get_transaction_receipt(tx["hash"])
                except Exception:
                    rec = None  # receipts can lag; we'll backfill on future passes
                return tx, rec

        rec_pairs = await asyncio.gather(
            *[rec_task(tx) for tx in b["transactions"]]
        )

        # ---- insert txs ----
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

        # ---- update progress + meta ----
        set_meta(conn, "last_safe_block", str(n))
        blocks_since_metrics += 1

        # ---- periodic metrics snapshot ----
        if blocks_since_metrics >= METRICS_EVERY_BLOCKS:
            try:
                compute_metrics(conn, METRIC_WINDOW, TOP_K)
                print(f"[metrics] snapshot @ block {n} (window={METRIC_WINDOW})")
            except Exception as e:
                print(f"[metrics] compute failed @ block {n}: {e}")
            finally:
                blocks_since_metrics = 0

        # optional: log every block (or throttle if too noisy)
        print(f"Indexed block {n} (txs {len(b['transactions'])})")

    # ---- final snapshot for the tail of this range ----
    if blocks_since_metrics > 0:
        try:
            compute_metrics(conn, METRIC_WINDOW, TOP_K)
            print(f"[metrics] final snapshot @ block {end} (window={METRIC_WINDOW})")
        except Exception as e:
            print(f"[metrics] final compute failed @ block {end}: {e}")


def compute_metrics(conn, window_blocks: int = 200, top_k: int = 10):
    # find range
    to_block = conn.execute("SELECT MAX(number) FROM blocks").fetchone()[0]
    if to_block is None:
        return
    from_block = max(0, to_block - window_blocks + 1)

    # fetch recent blocks (ordered)
    rows = conn.execute("""
        SELECT number, timestamp, gas_limit, gas_used, base_fee_per_gas_wei, tx_count
        FROM blocks
        WHERE number BETWEEN ? AND ?
        ORDER BY number ASC
    """, (from_block, to_block)).fetchall()

    if len(rows) < 2:
        return  # need at least 2 to compute deltas

    # block times & aggregates
    deltas = []
    tot_txs = 0
    gas_util_vals = []
    base_fees = []

    prev_ts = rows[0][1]
    for (num, ts, gas_limit, gas_used, base_fee, tx_count) in rows:
        # delta to previous
        dt = max(0, ts - prev_ts)
        if dt > 0:
            deltas.append(dt)
        prev_ts = ts

        tot_txs += int(tx_count or 0)

        gl = hex_to_int(gas_limit) if gas_limit is not None else None
        gu = hex_to_int(gas_used) if gas_used is not None else None
        if gl and gu is not None and gl > 0:
            gas_util_vals.append(gu / float(gl))

        if base_fee is not None:
            base_fees.append(hex_to_int(base_fee))

    avg_block_time = (sum(deltas) / len(deltas)) if deltas else None
    tps = (tot_txs / sum(deltas)) if deltas and sum(deltas) > 0 else None
    gas_util_avg = (sum(gas_util_vals) / len(gas_util_vals)) if gas_util_vals else None

    base_fee_avg = str(int(sum(base_fees) / len(base_fees))) if base_fees else None
    base_fee_p50 = None
    if base_fees:
        bfs = sorted(base_fees)
        base_fee_p50 = str(bfs[len(bfs)//2])

    # tx status (success/failed) in the same window
    tx_ok = conn.execute("""
        SELECT COUNT(*) FROM txs
        WHERE block_number BETWEEN ? AND ? AND status=1
    """, (from_block, to_block)).fetchone()[0]
    tx_fail = conn.execute("""
        SELECT COUNT(*) FROM txs
        WHERE block_number BETWEEN ? AND ? AND status=0
    """, (from_block, to_block)).fetchone()[0]

    # top senders / receivers by tx count
    top_senders = conn.execute(f"""
        SELECT "from" AS addr, COUNT(*) AS cnt
        FROM txs
        WHERE block_number BETWEEN ? AND ?
        GROUP BY "from"
        ORDER BY cnt DESC
        LIMIT ?
    """, (from_block, to_block, top_k)).fetchall()
    top_receivers = conn.execute(f"""
        SELECT "to" AS addr, COUNT(*) AS cnt
        FROM txs
        WHERE block_number BETWEEN ? AND ? AND "to" IS NOT NULL
        GROUP BY "to"
        ORDER BY cnt DESC
        LIMIT ?
    """, (from_block, to_block, top_k)).fetchall()

    payload = {
        "computed_at": int(time.time()),
        "window_n_blocks": int(window_blocks),
        "from_block": int(from_block),
        "to_block": int(to_block),
        "avg_block_time_s": float(avg_block_time) if avg_block_time is not None else None,
        "tps": float(tps) if tps is not None else None,
        "gas_utilization_avg": float(gas_util_avg) if gas_util_avg is not None else None,
        "base_fee_wei_avg": base_fee_avg,
        "base_fee_wei_p50": base_fee_p50,
        "tx_success": int(tx_ok or 0),
        "tx_failed": int(tx_fail or 0),
        "top_senders_json": json.dumps([{"address": r[0], "count": r[1]} for r in top_senders]),
        "top_receivers_json": json.dumps([{"address": r[0], "count": r[1]} for r in top_receivers]),
    }
    upsert_metrics_snapshot(conn, payload)


def seed_contracts(conn):
    for c in CONTRACTS:
        try:
            addr = Web3.to_checksum_address(c["address"])
        except Exception:
            continue
        conn.execute("""
            INSERT INTO contracts(address, name, type)
            VALUES(?,?,?)
            ON CONFLICT(address) DO UPDATE SET name=excluded.name, type=excluded.type
        """, (addr, c.get("name") or addr, c.get("type") or "unknown"))


async def main():
    w3 = AsyncWeb3(AsyncHTTPProvider(RPC_URL))
    latest = await w3.eth.block_number
    print(f"Connected to Katana, head={latest}")

    conn = db()
    ensure_schema(conn)
    seed_contracts(conn)

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

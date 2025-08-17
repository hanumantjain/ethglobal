import asyncio, time, json
from typing import Dict, Any
from web3 import AsyncWeb3
from web3.providers.async_rpc import AsyncHTTPProvider

import config
from helpers import (
    to_hex, to_addr, hex_to_int,
    topic_to_addr, topic_to_u256, decode_1155_data,
    topic_to_address_from_32b
)
from db import upsert_metrics_snapshot, set_meta

# ---------- light wrappers ----------
async def fetch_block(w3, num):
    return await w3.eth.get_block(block_identifier=num, full_transactions=True)

async def fetch_receipt(w3, tx_hash):
    return await w3.eth.get_transaction_receipt(tx_hash)

# ---------- ERC-20 ----------
async def process_erc20_logs_for_block(conn, w3, block_number: int, block_ts: int):
    if not config.WATCH_ERC20:
        return
    logs = await w3.eth.get_logs({
        "fromBlock": block_number,
        "toBlock": block_number,
        "address": config.WATCH_ERC20
    })
    for lg in logs:
        addr      = to_addr(lg["address"])
        txh       = lg["transactionHash"].hex() if hasattr(lg["transactionHash"], "hex") else str(lg["transactionHash"])
        log_index = int(lg["logIndex"])
        topics    = [t.hex() if hasattr(t, "hex") else str(t) for t in lg["topics"]]
        if not topics:
            continue
        topic0   = topics[0].lower()
        data_hex = lg["data"] if isinstance(lg["data"], str) else lg["data"].hex()

        if topic0 == config.ERC20_TRANSFER_TOPIC0 and len(topics) >= 3:
            from_addr = topic_to_address_from_32b(topics[1])
            to_addr_  = topic_to_address_from_32b(topics[2])
            h = data_hex[2:] if data_hex.startswith("0x") else data_hex
            value_int = int(h or "0", 16)
            conn.execute("""
                INSERT OR IGNORE INTO erc20_transfers
                (block_number, tx_hash, log_index, token, "from", "to", value_wei, ts)
                VALUES (?,?,?,?,?,?,?,?)
            """, (block_number, txh, log_index, addr, from_addr, to_addr_, str(value_int), int(block_ts)))

# ---------- NFTs ----------
async def process_nft_logs_for_block(conn, w3, block_number: int, block_ts: int):
    logs = await w3.eth.get_logs({
        "fromBlock": block_number,
        "toBlock": block_number,
        "topics": [[config.ERC721_TRANSFER_TOPIC0, config.ERC1155_TRANSFER_SINGLE]]
    })
    for lg in logs:
        addr       = to_addr(lg["address"])
        txh        = lg["transactionHash"].hex() if hasattr(lg["transactionHash"], "hex") else str(lg["transactionHash"])
        log_index  = int(lg["logIndex"])
        topics     = [t.hex() if hasattr(t, "hex") else str(t) for t in lg["topics"]]
        topic0     = topics[0].lower()
        data_hex   = lg["data"] if isinstance(lg["data"], str) else lg["data"].hex()

        # optional raw log
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
            pass

        if topic0 == config.ERC721_TRANSFER_TOPIC0:
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
            if to_addr_.lower() == config.ZERO_ADDR:
                conn.execute("DELETE FROM nft_owners WHERE collection=? AND token_id=?", (addr, token_id))
            else:
                conn.execute("""
                    INSERT INTO nft_owners(collection, token_id, owner)
                    VALUES(?,?,?)
                    ON CONFLICT(collection, token_id) DO UPDATE SET owner=excluded.owner
                """, (addr, token_id, to_addr_))

        elif topic0 == config.ERC1155_TRANSFER_SINGLE:
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
            if to_addr_.lower() == config.ZERO_ADDR:
                conn.execute("DELETE FROM nft_owners WHERE collection=? AND token_id=?", (addr, token_id))
            else:
                conn.execute("""
                    INSERT INTO nft_owners(collection, token_id, owner)
                    VALUES(?,?,?)
                    ON CONFLICT(collection, token_id) DO UPDATE SET owner=excluded.owner
                """, (addr, token_id, to_addr_))

# ---------- metrics + range ----------
def compute_metrics(conn, window_blocks: int = 200, top_k: int = 10):
    to_block = conn.execute("SELECT MAX(number) FROM blocks").fetchone()[0]
    if to_block is None:
        return
    from_block = max(0, to_block - window_blocks + 1)

    rows = conn.execute("""
        SELECT number, timestamp, gas_limit, gas_used, base_fee_per_gas_wei, tx_count
        FROM blocks
        WHERE number BETWEEN ? AND ?
        ORDER BY number ASC
    """, (from_block, to_block)).fetchall()
    if len(rows) < 2:
        return

    deltas, tot_txs, gas_util_vals, base_fees = [], 0, [], []
    prev_ts = rows[0][1]
    for (_, ts, gas_limit, gas_used, base_fee, tx_count) in rows:
        dt = max(0, ts - prev_ts)
        if dt > 0: deltas.append(dt)
        prev_ts = ts
        tot_txs += int(tx_count or 0)
        gl = hex_to_int(gas_limit) if gas_limit is not None else None
        gu = hex_to_int(gas_used) if gas_used is not None else None
        if gl and gu is not None and gl > 0:
            gas_util_vals.append(gu / float(gl))
        if base_fee is not None:
            base_fees.append(hex_to_int(base_fee))

    avg_block_time = (sum(deltas) / len(deltas)) if deltas else None
    tps            = (tot_txs / sum(deltas)) if deltas and sum(deltas) > 0 else None
    gas_util_avg   = (sum(gas_util_vals) / len(gas_util_vals)) if gas_util_vals else None
    base_fee_avg   = str(int(sum(base_fees) / len(base_fees))) if base_fees else None
    base_fee_p50   = None
    if base_fees:
        bfs = sorted(base_fees)
        base_fee_p50 = str(bfs[len(bfs)//2])

    tx_ok = conn.execute("""
        SELECT COUNT(*) FROM txs
        WHERE block_number BETWEEN ? AND ? AND status=1
    """, (from_block, to_block)).fetchone()[0]
    tx_fail = conn.execute("""
        SELECT COUNT(*) FROM txs
        WHERE block_number BETWEEN ? AND ? AND status=0
    """, (from_block, to_block)).fetchone()[0]

    top_senders = conn.execute("""
        SELECT "from" AS addr, COUNT(*) AS cnt
        FROM txs
        WHERE block_number BETWEEN ? AND ?
        GROUP BY "from"
        ORDER BY cnt DESC
        LIMIT ?
    """, (from_block, to_block, top_k)).fetchall()
    top_receivers = conn.execute("""
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

async def index_range(conn, w3, start, end):
    blocks_since_metrics = 0
    for n in range(start, end + 1):
        b = await w3.eth.get_block(block_identifier=n, full_transactions=True)

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
        await process_nft_logs_for_block(conn, w3, n, int(b["timestamp"]))  # (as-is)
        await process_erc20_logs_for_block(conn, w3, n, int(b["timestamp"]))

        sem = asyncio.Semaphore(config.RECEIPT_CONC)
        async def rec_task(tx):
            async with sem:
                try:
                    rec = await w3.eth.get_transaction_receipt(tx["hash"])
                except Exception:
                    rec = None
                return tx, rec

        rec_pairs = await asyncio.gather(*[rec_task(tx) for tx in b["transactions"]])

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
        blocks_since_metrics += 1

        if blocks_since_metrics >= config.METRICS_EVERY_BLOCKS:
            try:
                compute_metrics(conn, config.METRIC_WINDOW, config.TOP_K)
                print(f"[metrics] snapshot @ block {n} (window={config.METRIC_WINDOW})")
            except Exception as e:
                print(f"[metrics] compute failed @ block {n}: {e}")
            finally:
                blocks_since_metrics = 0

        print(f"Indexed block {n} (txs {len(b['transactions'])})")

    if blocks_since_metrics > 0:
        try:
            compute_metrics(conn, config.METRIC_WINDOW, config.TOP_K)
            print(f"[metrics] final snapshot @ block {end} (window={config.METRIC_WINDOW})")
        except Exception as e:
            print(f"[metrics] final compute failed @ block {end}: {e}")

def seed_contracts(conn):
    from web3 import Web3
    for c in config.CONTRACTS:
        try:
            addr = Web3.to_checksum_address(c["address"])
        except Exception:
            continue
        conn.execute("""
            INSERT INTO contracts(address, name, type)
            VALUES(?,?,?)
            ON CONFLICT(address) DO UPDATE SET name=excluded.name, type=excluded.type
        """, (addr, c.get("name") or addr, c.get("type") or "unknown"))

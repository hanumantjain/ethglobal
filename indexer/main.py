import asyncio, uvloop
from web3 import AsyncWeb3
from web3.providers.async_rpc import AsyncHTTPProvider

import config
from db import db, ensure_schema, get_meta
from indexer import index_range, seed_contracts

async def main():
    w3 = AsyncWeb3(AsyncHTTPProvider(config.RPC_URL))
    latest = await w3.eth.block_number
    print(f"Connected to Katana, head={latest}")

    conn = db()
    ensure_schema(conn)
    seed_contracts(conn)

    last_safe = int(get_meta(conn, "last_safe_block", "-1"))
    if last_safe < 0:
        start_from = max(0, latest - config.BACKFILL_BLOCKS)
    else:
        start_from = last_safe + 1

    while True:
        head = await w3.eth.block_number
        safe = max(0, head - config.CONFIRMS)

        if start_from <= safe:
            await index_range(conn, w3, start_from, safe)
            start_from = safe + 1
        else:
            await asyncio.sleep(1.0)

if __name__ == "__main__":
    uvloop.run(main())

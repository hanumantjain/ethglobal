import os, json, pathlib
from dotenv import load_dotenv
from web3 import Web3

# always load from local file
load_dotenv(".env")

# -------- env / config --------
RPC_URL              = os.getenv("RPC_URL")
CONFIRMS             = int(os.getenv("CONFIRMS", "6"))
BACKFILL_BLOCKS      = int(os.getenv("BACKFILL_BLOCKS", "5000"))
RECEIPT_CONC         = int(os.getenv("RECEIPT_CONC", "20"))
DB_PATH              = os.getenv("DB_PATH", "katana_index.sqlite")
METRIC_WINDOW        = int(os.getenv("METRIC_WINDOW", "200"))
TOP_K                = int(os.getenv("TOP_K", "10"))
METRICS_EVERY_BLOCKS = int(os.getenv("METRICS_EVERY_BLOCKS", "1000"))

if not RPC_URL:
    raise SystemExit("Missing RPC_URL in .env")

# --- NFT event topics (keccak256) ---
ERC721_TRANSFER_TOPIC0    = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ERC1155_TRANSFER_SINGLE   = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
ZERO_ADDR                 = "0x0000000000000000000000000000000000000000"

# --- ERC-20 topics ---
ERC20_TRANSFER_TOPIC0     = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ERC20_APPROVAL_TOPIC0     = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"

# contracts watchlist (optional)
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

WATCH_ERC20 = []
for c in CONTRACTS:
    if (c.get("type") or "").lower() == "erc20":
        try:
            WATCH_ERC20.append(Web3.to_checksum_address(c["address"]))
        except Exception:
            pass
WATCH_ERC20_LOWER = {a.lower() for a in WATCH_ERC20}

# mcp_tools_wrappers.py
from pydantic import BaseModel, Field
from mcp_app import mcp
from mcp_tools_core import (
    latest_blocks, latest_txs, block_by_number, tx_by_hash, health,
    top_senders, top_receivers, recent_nft_transfers, top_collections,
    nft_trending, unique_holders, holders_top5_share, top_owners,
    token_flow, token_top_holders
)

# ---------- Typed input models ----------
class LimitIn(BaseModel):
    limit: int = Field(10, ge=1, le=500)

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

# ---------- Typed wrapper tools ----------
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

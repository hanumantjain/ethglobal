from web3 import AsyncWeb3
from web3.types import HexBytes

# ---------------- helpers ----------------
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

def topic_to_address_from_32b(t: str) -> str:
    return AsyncWeb3.to_checksum_address("0x" + t[-40:])

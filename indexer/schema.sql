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
  token        TEXT,
  "from"       TEXT,
  "to"         TEXT,
  value_wei    TEXT,
  ts           INTEGER,
  PRIMARY KEY (tx_hash, log_index)
);
CREATE INDEX IF NOT EXISTS idx_erc20_token_ts ON erc20_transfers(token, ts);
CREATE INDEX IF NOT EXISTS idx_erc20_from ON erc20_transfers("from");
CREATE INDEX IF NOT EXISTS idx_erc20_to   ON erc20_transfers("to");

-- lightweight token meta cache (filled via eth_call on demand)
CREATE TABLE IF NOT EXISTS erc20_meta (
  token            TEXT PRIMARY KEY,
  name             TEXT,
  symbol           TEXT,
  decimals         INTEGER,
  total_supply_wei TEXT,
  updated_at       INTEGER
);

-- raw logs (optional)
CREATE TABLE IF NOT EXISTS logs (
  block_number INTEGER,
  tx_hash TEXT,
  log_index INTEGER,
  address TEXT,
  topic0 TEXT, topic1 TEXT, topic2 TEXT, topic3 TEXT,
  data TEXT,
  PRIMARY KEY (tx_hash, log_index)
);

-- normalized NFT transfers (ERC-721 + 1155 TransferSingle)
CREATE TABLE IF NOT EXISTS nft_transfers (
  block_number INTEGER,
  tx_hash TEXT,
  log_index INTEGER,
  collection TEXT,
  token_id TEXT,
  qty INTEGER DEFAULT 1,
  "from" TEXT,
  "to"   TEXT,
  ts INTEGER,
  PRIMARY KEY (tx_hash, log_index)
);
CREATE INDEX IF NOT EXISTS idx_nft_coll ON nft_transfers(collection);
CREATE INDEX IF NOT EXISTS idx_nft_token ON nft_transfers(collection, token_id);

-- current owner map
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

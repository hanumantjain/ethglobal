# mcp_config.py
import os
from dotenv import load_dotenv

# Load .env from project root (same as indexer)
load_dotenv(".env")

DB_PATH = os.getenv("DB_PATH", "katana_index.sqlite")

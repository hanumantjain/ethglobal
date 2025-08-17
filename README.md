## Architecture diagram
```
                            ┌─────────────────────────────┐
                            │        Public/Users         │
                            │  Browser / Frontend         │
                            │  (e.g., Vite/React app)     │
                            └──────────────┬──────────────┘
                                           │
                                           │ HTTP/WebSocket
                                           ▼
                 ┌──────────────────────────────────────────────────────┐
                 │                    n8n (self-hosted)                 │
                 │                                                      │
                 │                                                      │
                 │  ┌──────────────┐     ┌───────────────────────────┐  │
Frontend ─────▶ │  │ Webhook Node │───▶ │  AI Agent                 │  │
requests         │  │  /webhook... │     │  - Calls MCP tools        │  │
                 │  └──────┬───────┘     │  - analysis & insights    │  │
                 │         │              └───────────┬──────────────┘  │
                 │         │                          │                 │
                 │         │                  MCP Client Calls          │
                 │         │       (JSON-RPC over HTTP, tool invocations) 
                 │         │                          │                 │
                 │         │          ┌───────────────┴───────────────┐ │
                 │         │          │  MCPs: Katana & OpenSea       │ │
                 │         │          │  Tools:                       │ │
                 │         │          │   - blocks/txs queries        │ │
                 │         │          │   - health / metrics          │ │
                 │         │          │   - NFT  analytics            │ │
                 │         │          │   - Transactions              │ │
                 │         │          └───────┬───────────────────────┘ │
                 │         │                  │  SQLite reads           │
                 │         │                  ▼                         │
                 │         │          ┌──────────────────────┐          │
                 │         │          │  SQLite DB           │          │
                 │         │          │  katana_index.sqlite │          │
                 │         │          └─────────▲────────────┘          │
                 │         │                    │ writes                │
                 │         │                    │                       │
                 │         │          ┌─────────┴───────────┐           │
                 │         │          │  Indexer            │           │
                 │         │          │  - pulls blocks/txs │           │
                 │         │          │  - NFT & ERC-20 logs│           │
                 │         │          │  - metrics snapshot │           │
                 │         │          └─────────┬───────────┘           │
                 │         │                    │ RPC                   │
                 │         │                    ▼                       │
                 │         │          ┌──────────────────────┐          │
                 │         └────────▶ │  Chain RPC (Katana)  │ ◀───────┘
                 │                    │  (AsyncHTTPProvider) │          |
                 │                    └──────────────────────┘          |
                 └──────────────────────────────────────────────────────┘


```

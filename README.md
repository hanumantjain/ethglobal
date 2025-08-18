# KataBot
AI-powered MCP server for NFT, wallets, transfers, and analytics
## Description
This project is an AI-powered MCP server that allows users to manage cryptocurrency and NFT assets securely. Through a natural language chatbot interface, users can query wallet balances, transaction histories, and transfer tokens without exposing private keys, Dynamic's secure auth flow. It integrates with OpenSea's MCP data sources to provide NFT data, making it easy for collectors and traders to access ownership records, listings, and transfer details instantly through AI-driven interactions.
Additionally, a custom MCP server built for the Katana protocol provides in-depth analytics and insights for smart contracts and NFT activity. The system tracks top senders and receivers, analyzes transaction patterns, and exposes key contract information, all through a single AI agent. By combining MCP’s privacy-preserving computations with AI’s intuitive query handling, the platform delivers a secure, intelligent, and user-friendly interface for blockchain asset management and analytics across multiple NFT ecosystems.

## How Its Made
This project is an AI-powered MCP server that allows users to manage cryptocurrency and NFT assets securely. Through a natural language chatbot interface, users can query wallet balances, transaction histories, and transfer tokens without exposing private keys, thanks to multi-party computation (MCP). It integrates with OpenSea to provide NFT data, making it easy for collectors and traders to access ownership records, listings, and transfer details instantly through AI-driven interactions.

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

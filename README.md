# Nocturne: AI Trading Agent on Bitget

This project implements an AI-powered trading agent that leverages LLM models to analyse real-time market data from TAAPI, make informed trading decisions, and execute trades on the **Bitget** USDT-M perpetual futures exchange. The agent runs in a continuous loop, monitoring specified cryptocurrency assets at configurable intervals, using technical indicators to decide on buy/sell/hold actions, and manages positions with take-profit and stop-loss orders.

> **Branch note:** This branch (`bitget-integration`) replaces the original Hyperliquid integration with the Bitget V2 REST API. The public interface of the exchange client is intentionally identical so that no other agent logic needed to change.

## Table of Contents

- [Disclaimer](#disclaimer)
- [Architecture](#architecture)
- [Structure](#structure)
- [Env Configuration](#env-configuration)
- [Usage](#usage)
- [Tool Calling](#tool-calling)
- [Deployment to EigenCloud](#deployment-to-eigencloud)

## Disclaimer

There is no guarantee of any returns. This code has not been audited. Please use at your own risk.

## Architecture

See the full [Architecture Documentation](docs/ARCHITECTURE.md) for subsystems, data flow, and design principles.

![Architecture Diagram](docs/architecture.png)

## Structure
- `src/main.py`: Entry point, handles user input and main trading loop.
- `src/agent/decision_maker.py`: LLM logic for trade decisions (OpenRouter with tool calling for TAAPI indicators).
- `src/indicators/taapi_client.py`: Fetches indicators from TAAPI.
- `src/trading/bitget_api.py`: Executes trades on Bitget USDT-M perpetual futures (Bitget V2 REST API).
- `src/config_loader.py`: Centralized config loaded from `.env`.

## Env Configuration
Populate `.env` (use `.env.example` as reference):

| Variable | Required | Description |
|---|---|---|
| `TAAPI_API_KEY` | ✓ | Technical analysis API key from [taapi.io](https://taapi.io) |
| `BITGET_API_KEY` | ✓ | API key from [Bitget](https://www.bitget.com/account/newapi) |
| `BITGET_API_SECRET` | ✓ | API secret from Bitget |
| `BITGET_PASSPHRASE` | ✓ | Passphrase set when creating the Bitget API key |
| `OPENROUTER_API_KEY` | ✓ | LLM gateway key from [openrouter.ai](https://openrouter.ai) |
| `ASSETS` | ✓ | Space- or comma-separated asset list, e.g. `"BTC ETH SOL"` |
| `INTERVAL` | ✓ | Decision interval, e.g. `5m` or `1h` |
| `LLM_MODEL` | | OpenRouter model name (default `x-ai/grok-4`) |
| `BITGET_BASE_URL` | | Override Bitget API base URL (default `https://api.bitget.com`) |
| `OPENROUTER_BASE_URL` | | Override OpenRouter base URL |
| `OPENROUTER_REFERER` | | HTTP-Referer header forwarded to OpenRouter |
| `OPENROUTER_APP_TITLE` | | X-Title header forwarded to OpenRouter |

### Obtaining API Keys
- **TAAPI_API_KEY**: Sign up at [TAAPI.io](https://taapi.io/) and generate an API key from your dashboard.
- **BITGET_API_KEY / BITGET_API_SECRET / BITGET_PASSPHRASE**: Log in to [Bitget](https://www.bitget.com), go to **Account → API Management**, create a new API key, enable *Futures trading* permissions, and note down all three values. Make sure to IP-whitelist your server address for security.
- **OPENROUTER_API_KEY**: Create an account at [OpenRouter.ai](https://openrouter.ai/), then generate an API key in your account settings.
- **LLM_MODEL**: No key needed; specify a model name like `"x-ai/grok-4"` (see OpenRouter models list).

## Usage
```bash
poetry run python src/main.py --assets BTC ETH --interval 1h
```

### Local API Endpoints
When the agent runs, it also serves a minimal API:
- `GET /diary?limit=200` — returns recent JSONL diary entries as JSON.
- `GET /logs?path=llm_requests.log&limit=2000` — tails the specified log file.

Configure bind host/port via env:
- `API_HOST` (default `0.0.0.0`)
- `API_PORT` or `APP_PORT` (default `3000`)

Docker:
```bash
docker build --platform linux/amd64 -t trading-agent .
docker run --rm -p 3000:3000 --env-file .env trading-agent
# Now: curl http://localhost:3000/diary
```

## Tool Calling
The agent can dynamically fetch any TAAPI indicator (e.g., EMA, RSI) via tool calls. See [TAAPI Indicators](https://taapi.io/indicators/) for details.

## Deployment to EigenCloud

EigenCloud (via EigenX CLI) allows deploying this trading agent in a Trusted Execution Environment (TEE) with secure key management.

### Prerequisites
- Allowlisted Ethereum account (Sepolia for testnet). Request onboarding at [EigenCloud Onboarding](https://onboarding.eigencloud.xyz).
- Docker installed.
- Sepolia ETH for deployments.

### Installation
#### macOS/Linux
```bash
curl -fsSL https://eigenx-scripts.s3.us-east-1.amazonaws.com/install-eigenx.sh | bash
```

#### Windows
```bash
curl -fsSL https://eigenx-scripts.s3.us-east-1.amazonaws.com/install-eigenx.ps1 | powershell -
```

### Initial Setup
```bash
docker login
eigenx auth login
```

### Deploy the Agent
From the project directory:
```bash
cp .env.example .env
# Edit .env: set ASSETS, INTERVAL, and all API keys
eigenx app deploy
```

### Monitoring
```bash
eigenx app info --watch
eigenx app logs --watch
```

### Updates
Edit code or .env, then:
```bash
eigenx app upgrade <app-name>
```

For full CLI reference, see the [EigenX Documentation](https://github.com/Layr-Labs/eigenx-cli).

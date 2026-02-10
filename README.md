# Kalshi Trading Bot

Async Kalshi trading bot with config-driven strategy, safety limits, and optional Discord/Supabase.

## Setup

1. Copy `.env.example` to `.env` and fill in your Kalshi API credentials.
2. Install: `pip install -r requirements.txt`
3. Run: `python main.py`

## Strategies

- **fair_value** (default): Trades target tickers when market price deviates from fair value (mid) by a threshold. Set `KALSHI_TICKERS` (comma-separated).
- **risk_neutral**: Hedges existing positions when they are up 50%—sells enough contracts to recover initial capital. Uses positions from your Kalshi portfolio; no tickers required. Optional Supabase update and Discord alerts.

## Config (env or `.env`)

| Variable | Required | Description |
|----------|----------|-------------|
| `KALSHI_API_KEY_ID` | Yes | Kalshi API key ID |
| `KALSHI_PRIVATE_KEY_PATH` or `KALSHI_PRIVATE_KEY_PEM` | Yes | Private key file path or PEM string |
| `KALSHI_MAX_EXPOSURE` | Yes | Max total exposure in dollars |
| `KALSHI_STRATEGY` | No | `fair_value` or `risk_neutral` (default: fair_value) |
| `KALSHI_TICKERS` | For fair_value | Comma-separated tickers to trade |
| `KALSHI_DISCORD_WEBHOOK_URL` | No | Discord webhook for order/error alerts |
| `KALSHI_SUPABASE_URL`, `KALSHI_SUPABASE_KEY` | No | For risk_neutral: update positions table after hedge |
| Tuning | No | `KALSHI_HEARTBEAT_INTERVAL_SECONDS`, `KALSHI_LOOP_INTERVAL_SECONDS`, `KALSHI_ORDER_SIZE`, `KALSHI_THRESHOLD_CENTS`, `KALSHI_MIN_INVESTMENT_TO_HEDGE` (see `.env.example`) |

See `.env.example` for all options. The bot uses Pydantic config (no hardcoded values), loguru for all logging, retries with backoff on API 429/5xx, and SafetyEngine on every order.

## Legacy bot

`bot.py` is the original sync risk-neutralization bot (kalshi-python SDK, Supabase, Discord). Use `main.py` for the async bot with both strategies and safety engine.

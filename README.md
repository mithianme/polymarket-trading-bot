# Polymarket Arbitrage Bot

A real-time arbitrage bot that exploits pricing lag between CEX spot markets (Binance) and Polymarket's 5m/15m crypto up/down prediction markets. It detects when Polymarket contracts are mispriced relative to live price momentum, then enters positions and holds to near-resolution for profit.

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)
![Status](https://img.shields.io/badge/Status-Active-brightgreen)

---

## How It Works

The bot continuously streams real-time prices from Binance via WebSocket for BTC, ETH, and XRP. It compares the CEX-implied probability of price movement against Polymarket's live order book prices for the corresponding up/down contracts. When a statistically significant edge is detected (after fees), the bot enters a position and sells near candle resolution.

```
Binance Live Price ──► CEX Implied Probability
                                │
                          ┌─────▼─────┐
                          │  Edge =   │
                          │ CEX - Poly│
                          └─────┬─────┘
                                │
Polymarket Order Book ──► Poly Mid Price
                                │
                     Edge > threshold?
                        ▼              ▼
                      YES             NO
                   EXECUTE          SKIP
```

## Features

- **Dual Timeframe Strategy** — Monitors both 5m and 15m Polymarket candle markets simultaneously
- **Trend Continuation Signals** — Uses candle direction, momentum acceleration, and short-term momentum as a blended signal
- **Multi-Timeframe Confirmation** — 5m trades require 15m trend agreement
- **Kalshi Confluence** — Optional cross-platform validation using Kalshi market data
- **Dynamic Market Discovery** — Auto-detects the current live Polymarket slug every interval (markets rotate every 5/15 min)
- **Order Book Analysis** — Bid/ask imbalance as a confirming signal; liquidity and spread guards
- **Volatility Regime Filter** — Only trades during adequate volatility periods
- **Signal Persistence** — Requires 3 consecutive positive edge readings before firing
- **Half-Kelly Sizing** — Position sizes derived from Kelly Criterion at configurable fraction
- **Paper & Live Trading** — Full paper trading mode with instant fills; live mode uses the official `py-clob-client` SDK with signed orders
- **Persistent Close Retry** — 5-attempt emergency close with progressively aggressive pricing; stuck position detector at 2× TTL
- **Live Rich Dashboard** — Full-screen terminal UI with equity, P&L sparkline, open positions, market scanner, trade history, and error log
- **Built-in Backtester** — Replay historical CSV data through the same strategy logic
- **Kill Switch** — Auto-halts trading on configurable daily drawdown threshold
- **Proxy Support** — Optional HTTP proxy for all outbound connections

## Quick Start

### Prerequisites

- Python 3.10+
- A Polymarket account with API credentials (for live trading)

### Installation

```bash
git clone https://github.com/mithianme/polymarket-trading-bot.git
cd polymarket-trading-bot
pip install -r requirements.txt
```

#### Dependencies

```
requests
websockets
python-dotenv
rich
py-clob-client
```

### Configuration

Copy the example environment file and fill in your settings:

```bash
cp _env .env
```

Key settings in `.env`:

| Variable | Default | Description |
|---|---|---|
| `PAPER_TRADING` | `true` | Set to `false` for live trading |
| `ENABLE_LIVE_TRADING` | `false` | Must be `true` alongside three other flags for live mode |
| `STARTING_PORTFOLIO` | `50.0` | Starting equity in USD |
| `MIN_EDGE_TO_EXECUTE` | `0.05` | Minimum net edge (after fees) to enter a trade |
| `FEE_BUFFER_PCT` | `0.020` | Fee buffer subtracted from raw edge |
| `KELLY_FRACTION` | `0.25` | Fraction of Kelly Criterion for sizing |
| `DAILY_DRAWDOWN_LIMIT` | `0.20` | Kill switch threshold |
| `STOP_LOSS_PCT` | `0.045` | Trailing stop loss percentage from high-water mark |
| `MAX_SIMULTANEOUS_POS` | `3` | Maximum concurrent open positions |
| `KALSHI_CONFLUENCE_REQUIRED` | `true` | Require Kalshi agreement for 15m trades |

#### Live Trading Safeguard

Live trading requires **all four** flags set to `true`:

```env
PAPER_TRADING=false
ENABLE_LIVE_TRADING=true
ALLOW_REAL_ORDERS=true
CONFIRM_LIVE_TRADING=true
```

You must also provide `POLY_PRIVATE_KEY` and optionally `POLY_API_KEY`, `POLY_API_SECRET`, `POLY_PASSPHRASE`, and `POLY_FUNDER`.

### Running

```bash
# Paper trading (default)
python polymarket_trading_bot.py

# Backtesting
python polymarket_trading_bot.py --backtest backtest_data.csv
```

## Dashboard

The bot launches a full-screen Rich terminal dashboard that displays:

- Live BTC/ETH prices with directional arrows
- Equity, total P&L, win rate, and drawdown KPIs
- Drawdown risk and position utilisation bars
- P&L sparkline chart
- Open positions with entry/current price, unrealized P&L, edge, confidence, age, and trailing stop levels
- Live market scanner showing all monitored contracts with Poly price, CEX implied probability, net edge, and signal status
- Recent trade history with fill status and P&L
- Error log and system configuration summary

## Backtest CSV Format

```csv
timestamp,asset,price
1700000000,BTC,37500.00
1700000005,ETH,2050.00
1700000010,XRP,0.62
```

## Architecture

| Component | Role |
|---|---|
| `binance_ws_listener` | Streams live CEX prices via WebSocket |
| `refresh_markets` | Discovers current Polymarket event slugs every 30s |
| `refresh_books` | Polls order books for all tracked contracts every 5s |
| `refresh_kalshi` | Fetches Kalshi market data for confluence |
| `strategy_loop` | Evaluates signals and executes trades every 1s |
| `manage_positions` | Monitors open positions, trailing stops, and pending orders every 1s |
| `dashboard_thread` | Renders the Rich terminal dashboard |
| `BacktestEngine` | Offline CSV replay through strategy logic |

All async tasks are wrapped in a `supervised_task` runner that auto-restarts on crash with configurable backoff.

## Risk Disclaimer

This bot trades real money in live mode. Prediction markets carry significant risk. Use paper trading mode to evaluate strategy performance before committing capital. The authors are not responsible for any financial losses.

Contributors: mithian, Socket, stanlee

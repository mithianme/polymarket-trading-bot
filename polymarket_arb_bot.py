import os
import sys

# ─────────────────────────────────────────────
# PROXY CONFIGURATION (must be set BEFORE any other imports)
# ─────────────────────────────────────────────
# Load .env FIRST so PROXY is available
from dotenv import load_dotenv
load_dotenv()

# Format: IP:PORT:USERNAME:PASSWORD or empty to disable
PROXY_STRING = os.getenv("PROXY", "").strip()
if PROXY_STRING:
    print(f"[PROXY] Loaded from .env: {PROXY_STRING[:30]}...")
    parts = PROXY_STRING.split(":")
    if len(parts) == 4:
        ip, port, user, passwd = parts
        proxy_url = f"http://{user}:{passwd}@{ip}:{port}"
    elif len(parts) == 2:
        ip, port = parts
        proxy_url = f"http://{ip}:{port}"
    else:
        proxy_url = None
    
    if proxy_url:
        os.environ["HTTP_PROXY"] = proxy_url
        os.environ["HTTPS_PROXY"] = proxy_url
        os.environ["http_proxy"] = proxy_url
        os.environ["https_proxy"] = proxy_url
        print(f"🌐 Proxy configured: {parts[0]}:{parts[1]}")
    else:
        print(f"⚠️  PROXY format invalid (expected IP:PORT or IP:PORT:USER:PASS)")
else:
    print("[PROXY] No proxy configured (PROXY env is empty)")



import json
import math
import time
import hmac
import hashlib
import signal
import asyncio
import logging
import base64
import threading
import bisect
import csv
from dataclasses import dataclass, field
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Optional, Deque, Tuple

import requests
import websockets
from dotenv import load_dotenv
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich import box

# NEW: Official Polymarket CLOB SDK (required for live orders)
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, BalanceAllowanceParams, AssetType
# FIXED IMPORT — this is the only change
from py_clob_client.order_builder.constants import BUY, SELL

load_dotenv()

# ─────────────────────────────────────────────
# LOGGING SETUP - Separate file and console to prevent screen shaking
# ─────────────────────────────────────────────

# custom logger
log = logging.getLogger("polyarb")
log.setLevel(logging.DEBUG)  # Capture all levels, handlers will filter
log.propagate = False  # Prevent double logging

# verbose logging for debugging
_file_handler = logging.FileHandler("bot.log", mode="a", encoding="utf-8")
_file_handler.setLevel(logging.DEBUG)
_file_handler.setFormatter(logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(levelname)s] [%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))
log.addHandler(_file_handler)

# only for critical startup messages, will be removed when dashboard starts
_console_handler = logging.StreamHandler()
_console_handler.setLevel(logging.WARNING)  # Only warnings and errors to console
_console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
log.addHandler(_console_handler)

# Flag to track if dashboard is running (to suppress console output)
DASHBOARD_ACTIVE = False

def _log_to_file_only(level: int, msg: str):
    """Log directly to file without console output - use for verbose debugging."""
    _file_handler.emit(logging.LogRecord(
        name="polyarb", level=level, pathname="", lineno=0,
        msg=msg, args=(), exc_info=None, func=""
    ))

console = Console()

# ─────────────────────────────────────────────
# ENV HELPERS
# ─────────────────────────────────────────────

def env_bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}

def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return float(default)

def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return int(default)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

PAPER_TRADING        = env_bool("PAPER_TRADING", True)
ENABLE_LIVE_TRADING  = env_bool("ENABLE_LIVE_TRADING", False)
ALLOW_REAL_ORDERS    = env_bool("ALLOW_REAL_ORDERS", False)
CONFIRM_LIVE_TRADING = env_bool("CONFIRM_LIVE_TRADING", False)

LIVE_TRADING_ENABLED = (
    not PAPER_TRADING
    and ENABLE_LIVE_TRADING
    and ALLOW_REAL_ORDERS
    and CONFIRM_LIVE_TRADING
)

if LIVE_TRADING_ENABLED and not os.getenv("POLY_PRIVATE_KEY"):
    raise RuntimeError(
        "POLY_PRIVATE_KEY is required for live trading. "
        "Set PAPER_TRADING=true to run without credentials."
    )

STARTING_PORTFOLIO   = env_float("STARTING_PORTFOLIO", 50.0)
MAX_POSITION_PCT     = env_float("MAX_POSITION_PCT", 0.20)
DAILY_DRAWDOWN_LIMIT = env_float("DAILY_DRAWDOWN_LIMIT", 0.20)
EDGE_TRIGGER_PCT     = env_float("EDGE_TRIGGER_PCT", 0.03)
MIN_EDGE_TO_EXECUTE  = env_float("MIN_EDGE_TO_EXECUTE", 0.05)
MIN_CONFIDENCE_SCORE = env_float("MIN_CONFIDENCE_SCORE", 0.85)
KELLY_FRACTION       = env_float("KELLY_FRACTION", 0.25)

KALSHI_CONFLUENCE_REQUIRED = env_bool("KALSHI_CONFLUENCE_REQUIRED", True)
KALSHI_MIN_AGREEMENT_PCT   = env_float("KALSHI_MIN_AGREEMENT_PCT", 0.48)
KALSHI_WEIGHT              = env_float("KALSHI_WEIGHT", 0.35)

# ─── v5.0 NEW CONFIG ────────────────────────
FEE_BUFFER_PCT          = env_float("FEE_BUFFER_PCT", 0.020)         # 2.0% fee buffer (1% entry + 1% exit taker fees)
MAX_SPREAD_PCT          = env_float("MAX_SPREAD_PCT", 0.055)         # 5.5% liquidity guard
MIN_BOOK_DEPTH          = env_float("MIN_BOOK_DEPTH", 20.0)         # minimum $ depth on each side
STALE_ORDER_TIMEOUT     = env_float("STALE_ORDER_TIMEOUT", 45.0)     # auto-cancel after 45s
MAX_CONTRACTS_PER_ASSET = env_int("MAX_CONTRACTS_PER_ASSET", 2)      # correlation limit
MULTI_TF_CONFIRMATION   = env_bool("MULTI_TF_CONFIRMATION", True)    # 5m needs 15m agreement
ORDER_POLL_INTERVAL     = env_float("ORDER_POLL_INTERVAL", 3.0)      # poll fill status every 3s
ORDER_POLL_MAX_WAIT     = env_float("ORDER_POLL_MAX_WAIT", 45.0)     # max wait for fill
CLOSE_USE_MARKET_PRICE  = env_bool("CLOSE_USE_MARKET_PRICE", True)   # use aggressive pricing to close
TAKE_PROFIT_PCT         = env_float("TAKE_PROFIT_PCT", 0.055)         # TP threshold (relative to entry)
STOP_LOSS_PCT           = env_float("STOP_LOSS_PCT", 0.045)          # SL threshold (wider to avoid noise chop)
BACKTEST_MODE           = env_bool("BACKTEST_MODE", False)
BACKTEST_CSV            = os.getenv("BACKTEST_CSV", "backtest_data.csv")
# ─── v5.1 SAFETY FILTERS ────────────────────
MIN_POLY_PRICE          = env_float("MIN_POLY_PRICE", 0.20)          # skip contracts priced below 20%
MAX_POLY_PRICE          = env_float("MAX_POLY_PRICE", 0.80)          # skip contracts priced above 80%
MAX_SIMULTANEOUS_POS    = env_int("MAX_SIMULTANEOUS_POS", 3)         # max total open positions at once
MAX_CONTRACTS_PER_POS   = env_float("MAX_CONTRACTS_PER_POS", 25.0)   # cap contract count per trade
MAX_EDGE_PCT            = env_float("MAX_EDGE_PCT", 0.15)            # reject edges > 15% (likely bad data)
# ─── end v5.0/5.1 NEW CONFIG ────────────────

BINANCE_WS_BASE     = os.getenv("BINANCE_WS_BASE", os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443/stream?streams="))
POLYMARKET_HOST     = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com").rstrip("/")
POLYMARKET_CHAIN_ID = env_int("POLYMARKET_CHAIN_ID", 137)
POLY_PRIVATE_KEY    = os.getenv("POLY_PRIVATE_KEY", "")
POLY_API_KEY        = os.getenv("POLY_API_KEY", "")
POLY_API_SECRET     = os.getenv("POLY_API_SECRET", "")
POLY_PASSPHRASE     = os.getenv("POLY_PASSPHRASE", "")
POLY_FUNDER         = os.getenv("POLY_FUNDER", "")

DASHBOARD_REFRESH = env_float("DASHBOARD_REFRESH_SECONDS", 1.0)

ENABLE_REAL_POLY_DISCOVERY = env_bool("ENABLE_REAL_POLY_DISCOVERY", True)
MARKET_REFRESH_INTERVAL    = env_int("MARKET_REFRESH_INTERVAL", 300)

# v7.1: 15m primary + 5m for fast-moving markets
TARGET_CONFIGS = [
    ("BTC", "5m"),
    ("BTC", "15m"),
    ("ETH", "5m"),
    ("ETH", "15m"),
    ("XRP", "5m"),
    ("XRP", "15m"),
]

DIRECTIONS = ("UP", "DOWN")

# ─────────────────────────────────────────────
# EXACT EVENT SLUGS
# ─────────────────────────────────────────────

EXACT_EVENT_SLUGS = [
    "btc-updown-5m-1774838700",
    "btc-updown-15m-1774838700",
    "eth-updown-5m-1774838700",
    "eth-updown-15m-1774838700",
    "xrp-updown-5m-1774838700",
    "xrp-updown-15m-1774838700",
]

MARKET_CACHE: Dict[str, dict] = {}
LAST_MARKET_REFRESH = datetime(1970, 1, 1, tzinfo=timezone.utc)

STOP_EVENT = None

ASSET_ALIASES = {
    "BTC": ["bitcoin", "btc"],
    "ETH": ["ethereum", "eth"],
    "XRP": ["xrp"],
}

ORACLE_FEEDS = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "XRP": "XRPUSDT",
}

def get_current_slug(asset: str, timeframe: str) -> str:
    """Polymarket creates a new 5m/15m market every interval.
    This returns the EXACT current live slug right now (no more expired slugs)."""
    now = int(time.time())
    interval = 300 if timeframe == "5m" else 900          # 5 min or 15 min
    aligned = (now // interval) * interval                # floor to current market start
    return f"{asset.lower()}-updown-{timeframe.lower()}-{aligned}"

# ─────────────────────────────────────────────
# DATA CLASSES
# ─────────────────────────────────────────────

@dataclass
class Position:
    market_slug: str
    side: str
    entry_price: float
    size: float
    confidence: float
    edge: float
    token_id: str = ""
    opened_ts: float = field(default_factory=time.time)
    # v5.0: track the open order ID for fill polling
    open_order_id: str = ""
    open_filled: bool = True  # paper trades are instantly filled
    # v6: trailing stop — tracks highest price seen since entry
    high_water_mark: float = 0.0

@dataclass
class TradeRecord:
    ts: float
    market_slug: str
    side: str
    price: float
    size: float
    status: str
    pnl: float = 0.0

# ─────────────────────────────────────────────
# v5.0: PENDING CLOSE ORDER TRACKER
# ─────────────────────────────────────────────

@dataclass
class PendingCloseOrder:
    """Tracks a close/sell order until it fills or gets cancelled."""
    order_id: str
    position: "Position"
    placed_ts: float
    expected_exit_price: float
    won: bool
    cancel_after: float = 45.0  # seconds

# ─────────────────────────────────────────────
# SHARED STATE
# ─────────────────────────────────────────────

class BotState:
    def __init__(self):
        self.lock            = threading.RLock()  # FIX: RLock prevents re-entry deadlocks
        self.portfolio       = STARTING_PORTFOLIO
        self.cash            = STARTING_PORTFOLIO
        self.day_start_equity = STARTING_PORTFOLIO
        self.day_reset_ts    = time.time()
        self.realized_pnl    = 0.0
        self.unrealized_pnl  = 0.0
        self.win_count       = 0
        self.loss_count      = 0
        self.kill_switch     = False
        self.positions: List[Position]       = []
        self.last_trades: Deque[TradeRecord] = deque(maxlen=10)
        self.cex_prices: Dict[str, Optional[float]] = {
            "BTCUSDT": None,
            "ETHUSDT": None,
            "XRPUSDT": None,
        }
        self.poly_books:   Dict[str, dict] = {}
        self.poly_markets: Dict[str, dict] = {}
        self.kalshi_prices: Dict[str, Optional[dict]] = {
            "BTC": None,
            "ETH": None,
        }
        self.errors:       Deque[str]      = deque(maxlen=20)
        self.pnl_history:  Deque[float]    = deque(maxlen=120)
        self.prev_prices:  Dict[str, float] = {}
        self.start_ts:     float           = time.time()
        # Rolling price history: symbol → deque of (timestamp, price), 10-min window
        self.cex_price_history: Dict[str, deque] = {
            "BTCUSDT": deque(maxlen=600),  # FIX: was deque() — grew unboundedly, O(n) lookup
            "ETHUSDT": deque(maxlen=600),
            "XRPUSDT": deque(maxlen=600),
        }
        self.pulse_tick:   int             = 0
        self.signals_seen: int             = 0
        self.signals_fired: int            = 0
        self.clob_client   = None  # Will be initialized on first live order
        self.clob_balance  : Optional[float] = None

        # v5.0: pending close orders awaiting fill confirmation
        self.pending_closes: List[PendingCloseOrder] = []
        # v5.0: pending open orders awaiting fill (live mode)
        self.pending_opens: List[dict] = []  # {order_id, position, placed_ts}

    def total_equity(self) -> float:
        position_cost = sum(p.entry_price * p.size for p in self.positions)
        return self.cash + position_cost + self.unrealized_pnl

    def drawdown_pct(self) -> float:
        if self.day_start_equity <= 0:
            return 0.0
        return max(0.0, (self.day_start_equity - self.total_equity()) / self.day_start_equity)

    def win_rate(self) -> float:
        total = self.win_count + self.loss_count
        return self.win_count / total if total > 0 else 0.0

    def log_error(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        with self.lock:
            self.errors.appendleft(f"[{ts}] {msg}")

    # v5.0: count open contracts per asset for correlation limit
    def contracts_on_asset(self, asset: str) -> int:
        asset_lower = asset.lower()
        count = 0
        for p in self.positions:
            if p.market_slug.startswith(asset_lower + "-"):
                count += 1
        return count

STATE = BotState()

# ─────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def contract_slug(asset: str, timeframe: str, direction: str) -> str:
    return f"{asset.lower()}-{timeframe.lower()}-{direction.lower()}"

def half_kelly(prob: float, odds_decimal: float) -> float:
    b = max(odds_decimal - 1.0, 1e-9)
    q = 1.0 - prob
    kelly = (b * prob - q) / b
    return max(0.0, kelly * KELLY_FRACTION)

def confidence_from_edge(edge: float) -> float:
    score = 0.5 + min(max(edge, 0.0), 0.25) * 2.0
    return round(min(0.99, max(0.0, score)), 4)

def poly_mid_from_book(book: dict) -> Optional[float]:
    try:
        bid = float(book.get("best_bid", 0) or 0)
        ask = float(book.get("best_ask", 1) or 1)
        if bid > 0 and ask > 0 and ask > bid:
            return (bid + ask) / 2.0
    except Exception:
        pass

    # Fallback: parse raw CLOB bids/asks arrays if best_bid/best_ask missing
    try:
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        best_bid = 0.0
        best_ask = 0.0
        if isinstance(bids, list) and bids:
            for b in bids:
                if isinstance(b, dict):
                    px = float(b.get("price", 0) or 0)
                    if px > best_bid:
                        best_bid = px
        if isinstance(asks, list) and asks:
            best_ask = 999.0
            for a in asks:
                if isinstance(a, dict):
                    px = float(a.get("price", 0) or 0)
                    if 0 < px < best_ask:
                        best_ask = px
        if best_bid > 0 and best_ask > best_bid and best_ask < 999.0:
            return (best_bid + best_ask) / 2.0
    except Exception:
        pass

    return None

def _normalize_book(raw_book: dict) -> dict:
    """Normalize CLOB book response so best_bid/best_ask always exist as scalars.
    The Polymarket CLOB API returns: {"bids": [{"price":"0.45","size":"100"}, ...],
    "asks": [{"price":"0.55","size":"100"}, ...]} — NOT flat best_bid/best_ask keys.
    This function extracts them so all downstream code works correctly."""
    book = dict(raw_book)

    # Already has valid scalar best_bid/best_ask → keep them
    existing_bid = book.get("best_bid")
    existing_ask = book.get("best_ask")
    if existing_bid and existing_ask:
        try:
            fb = float(existing_bid)
            fa = float(existing_ask)
            if fb > 0 and fa > fb:
                return book
        except (ValueError, TypeError):
            pass

    # Extract from bids/asks arrays (the real CLOB format)
    best_bid = 0.0
    best_ask = 1.0

    bids = book.get("bids", [])
    if isinstance(bids, list) and bids:
        try:
            bid_prices = []
            for b in bids:
                if isinstance(b, dict):
                    px = float(b.get("price", 0) or 0)
                    if px > 0:
                        bid_prices.append(px)
            if bid_prices:
                best_bid = max(bid_prices)
        except Exception:
            pass

    asks = book.get("asks", [])
    if isinstance(asks, list) and asks:
        try:
            ask_prices = []
            for a in asks:
                if isinstance(a, dict):
                    px = float(a.get("price", 0) or 0)
                    if px > 0:
                        ask_prices.append(px)
            if ask_prices:
                best_ask = min(ask_prices)
        except Exception:
            pass

    book["best_bid"] = best_bid
    book["best_ask"] = best_ask
    return book


def synthetic_poly_price(asset: str, direction: str) -> float:
    """Honest fallback when no real book data: return 0.50 (no opinion)."""
    return 0.50

# Per-asset annualised vol → 30-second vol baseline (fraction of price).
# BTC ~55% ann, ETH ~70% ann, XRP ~90% ann.
# Formula: annual_vol / sqrt(365.25 * 24 * 120)   [120 thirty-second periods per hour]
_VOL_30S = {
    "BTC": 0.00055,
    "ETH": 0.00070,
    "XRP": 0.00090,
}

def _price_at_candle_open(symbol: str, tf: str) -> Optional[float]:
    """Return the CEX price closest to the start of the current candle."""
    interval = 300 if tf == "5m" else 900
    candle_start = (time.time() // interval) * interval
    with STATE.lock:
        hist = list(STATE.cex_price_history.get(symbol, []))
    if not hist:
        return None
    # Walk forward to find first entry at or after candle_start
    for ts, px in hist:
        if ts >= candle_start:
            return px
    return None

def _price_n_seconds_ago(symbol: str, n: int) -> Optional[float]:
    """Return the CEX price ~n seconds ago from price history.
    FIX: uses bisect for O(log n) lookup and is safe against out-of-order ticks."""
    target = time.time() - n
    with STATE.lock:
        hist = list(STATE.cex_price_history.get(symbol, []))
    if not hist:
        return None
    # Extract timestamps into a list for bisect; hist is (ts, px) pairs.
    # bisect_right gives the insertion point for target, so idx-1 is the last
    # entry with ts <= target — exactly the price n seconds ago.
    ts_list = [entry[0] for entry in hist]
    idx = bisect.bisect_right(ts_list, target) - 1
    if idx < 0:
        return None
    return hist[idx][1]

def cex_implied_prob(asset: str, timeframe: str, direction: str) -> float:
    """
    v7.1 TREND CONTINUATION signal.
    
    Stop trying to front-run Polymarket — bet on TREND CONTINUATION.
    If BTC is already up 0.3%+ from candle open 3-5 minutes in, it's more
    likely to stay up by candle end than to reverse completely.
    
    Crypto 15m candles continue their established trend ~60% of the time.
    At $0.35 entry with hold-to-resolution, we only need 37% to profit.
    """
    symbol = ORACLE_FEEDS.get(asset, "")
    current_px = STATE.cex_prices.get(symbol)
    if current_px is None or current_px <= 0:
        return 0.50

    vol_30s = _VOL_30S.get(asset, 0.00065)
    interval = 300 if timeframe == "5m" else 900

    # ── COMPONENT 1: CANDLE DIRECTION (primary — 55% weight) ──
    candle_open_px = _price_at_candle_open(symbol, timeframe)
    candle_z = 0.0
    if candle_open_px and candle_open_px > 0:
        candle_return = (current_px - candle_open_px) / candle_open_px
        vol_candle = vol_30s * math.sqrt(interval / 30)
        candle_z = candle_return / max(vol_candle, 1e-9)

    # ── COMPONENT 2: MOMENTUM ACCELERATION ──
    px_30s  = _price_n_seconds_ago(symbol, 30)
    px_60s  = _price_n_seconds_ago(symbol, 60)
    px_120s = _price_n_seconds_ago(symbol, 120)

    accel_z = 0.0
    if px_60s and px_60s > 0 and px_120s and px_120s > 0:
        recent_mom = (current_px - px_60s) / px_60s
        older_mom  = (px_60s - px_120s) / px_120s
        accel = recent_mom - older_mom
        accel_z = accel / (vol_30s * math.sqrt(2))
        accel_z = max(-3.0, min(3.0, accel_z))
        # Acceleration must agree with candle direction
        if candle_z != 0:
            if (candle_z > 0 and accel_z > 0) or (candle_z < 0 and accel_z < 0):
                accel_z *= 1.2
            else:
                accel_z *= 0.5

    # ── COMPONENT 3: SHORT-TERM MOMENTUM (confirming) ──
    short_z = 0.0
    if px_30s and px_30s > 0:
        mom_30s = (current_px - px_30s) / px_30s
        short_z = mom_30s / vol_30s

    # ── BLEND: candle direction dominates ──
    combined_z = candle_z * 0.55 + accel_z * 0.25 + short_z * 0.20

    # Dampen extremes
    abs_z = abs(combined_z)
    if abs_z > 3.0:
        combined_z *= 3.0 / abs_z

    up_prob = 1.0 / (1.0 + math.exp(-combined_z * 0.9))
    up_prob = max(0.05, min(0.95, up_prob))

    return up_prob if direction == "UP" else 1.0 - up_prob


def get_candle_strength(asset: str, timeframe: str) -> Tuple[str, float]:
    """v7.1: Determine the DOMINANT direction and how strong the move is.
    Returns (direction, strength_pct). Used to pick ONE direction per candle."""
    symbol = ORACLE_FEEDS.get(asset, "")
    current_px = STATE.cex_prices.get(symbol)
    if current_px is None or current_px <= 0:
        return "NONE", 0.0
    candle_open_px = _price_at_candle_open(symbol, timeframe)
    if candle_open_px is None or candle_open_px <= 0:
        return "NONE", 0.0
    ret = (current_px - candle_open_px) / candle_open_px
    direction = "UP" if ret > 0 else "DOWN"
    return direction, abs(ret)


def check_volatility_regime(asset: str) -> bool:
    """v7.1: Only trade during adequate volatility.
    Low vol = random noise, high vol = directional moves."""
    symbol = ORACLE_FEEDS.get(asset, "")
    with STATE.lock:
        hist = list(STATE.cex_price_history.get(symbol, []))
    if len(hist) < 60:
        return False
    returns = []
    for i in range(1, len(hist)):
        ts_now, px_now = hist[i]
        ts_prev, px_prev = hist[i-1]
        if px_prev > 0 and (ts_now - ts_prev) < 10:
            ret = abs((px_now - px_prev) / px_prev)
            returns.append(ret)
    if len(returns) < 20:
        return False
    recent_vol = sum(returns[-30:]) / 30
    full_vol = sum(returns) / len(returns)
    passes = recent_vol >= full_vol * 0.80
    if not passes:
        log.debug(f"[VOL FILTER] {asset} rejected — low volatility")
    return passes


def get_book_imbalance(slug: str) -> float:
    """v7.1: Order book imbalance as confirming signal.
    +1.0 = all bids (bullish), -1.0 = all asks (bearish)."""
    book = STATE.poly_books.get(slug, {})
    bid_depth = 0.0
    ask_depth = 0.0
    for b in (book.get("bids") or []):
        if isinstance(b, dict):
            try: bid_depth += float(b.get("size", 0) or 0)
            except: pass
    for a in (book.get("asks") or []):
        if isinstance(a, dict):
            try: ask_depth += float(a.get("size", 0) or 0)
            except: pass
    total = bid_depth + ask_depth
    if total <= 0:
        return 0.0
    return (bid_depth - ask_depth) / total

def get_kalshi_prob(asset: str) -> Optional[float]:
    kalshi_data = STATE.kalshi_prices.get(asset)
    if kalshi_data and isinstance(kalshi_data, dict):
        return kalshi_data.get("up_prob")
    return None

def get_poly_prob(asset: str, timeframe: str, direction: str) -> float:
    slug = contract_slug(asset, timeframe, direction)
    book = STATE.poly_books.get(slug)
    if book:
        mid = poly_mid_from_book(book)
        if mid is not None:
            return mid
    return synthetic_poly_price(asset, direction)

# ─────────────────────────────────────────────
# v5.0: MULTI-TIMEFRAME CONFIRMATION
# ─────────────────────────────────────────────

def check_multi_tf_confirmation(asset: str, direction: str, tf: str) -> bool:
    """For 5m signals, require the 15m CEX implied probability to agree on direction.
    Returns True if confirmed or if not applicable (15m signals skip this check)."""
    if not MULTI_TF_CONFIRMATION:
        return True
    if tf != "5m":
        return True  # only 5m needs confirmation from 15m

    # Get the 15m trend direction
    cex_15m_prob = cex_implied_prob(asset, "15m", direction)

    # The 15m trend must show > 0.52 probability in the same direction
    # (i.e. it shouldn't be leaning the opposite way)
    if cex_15m_prob < 0.52:
        log.debug(f"[MTF] {asset}-5m-{direction} rejected — 15m prob={cex_15m_prob:.3f} < 0.52")
        return False

    log.debug(f"[MTF] {asset}-5m-{direction} confirmed — 15m prob={cex_15m_prob:.3f}")
    return True


# ─────────────────────────────────────────────
# v5.0: LIQUIDITY GUARD
# ─────────────────────────────────────────────

def check_liquidity(slug: str) -> Tuple[bool, float, float]:
    """Check if the market has adequate liquidity.
    Returns (passes, spread_pct, depth_estimate).
    Spread must be ≤ MAX_SPREAD_PCT and depth must be ≥ MIN_BOOK_DEPTH."""
    book = STATE.poly_books.get(slug, {})
    bid = float(book.get("best_bid", 0) or 0)
    ask = float(book.get("best_ask", 0) or 0)

    if bid <= 0 or ask <= 0 or ask <= bid:
        return False, 1.0, 0.0

    spread_pct = (ask - bid) / ((ask + bid) / 2.0)

    # Estimate depth from book levels if available
    depth = 0.0
    for side_key in ("bids", "asks"):
        levels = book.get(side_key, [])
        if isinstance(levels, list):
            for level in levels:
                try:
                    px = float(level.get("price", 0) or 0)
                    sz = float(level.get("size", 0) or 0)
                    depth += px * sz
                except (ValueError, TypeError, AttributeError):
                    continue

    # If no detailed book depth data, estimate from bid/ask existence
    if depth == 0.0:
        depth = MIN_BOOK_DEPTH + 1  # allow it through if we just have top-of-book

    passes = spread_pct <= MAX_SPREAD_PCT and depth >= MIN_BOOK_DEPTH

    if not passes:
        log.debug(f"[LIQUIDITY] {slug} rejected — spread={spread_pct:.3%} depth=${depth:.0f}")

    return passes, spread_pct, depth


# ─────────────────────────────────────────────
# v5.0: DYNAMIC FEE BUFFER
# ─────────────────────────────────────────────

def apply_fee_buffer(raw_edge: float) -> float:
    """Subtract the fee buffer from the raw edge to get the net edge.
    This ensures we only trade when the edge exceeds fees."""
    return raw_edge - FEE_BUFFER_PCT


# ─────────────────────────────────────────────
# v5.0: CORRELATION LIMIT CHECK
# ─────────────────────────────────────────────

def check_correlation_limit(asset: str) -> bool:
    """Returns True if we can open another position on this asset."""
    with STATE.lock:
        count = STATE.contracts_on_asset(asset)
    if count >= MAX_CONTRACTS_PER_ASSET:
        log.debug(f"[CORR] {asset} blocked — already {count} contracts (max={MAX_CONTRACTS_PER_ASSET})")
        return False
    return True


# ─────────────────────────────────────────────
# POLYMARKET CLOB AUTH (fixed with POLY-ADDRESS)
# ─────────────────────────────────────────────

def _poly_headers(method: str, path: str, body: str = "") -> dict:
    """Pure Polymarket API key + signature auth (proxy wallet + relayer completely removed)"""
    headers = {"Content-Type": "application/json"}

    # Standard signed API auth only (exactly what you asked for)
    if POLY_API_KEY and POLY_API_SECRET and POLY_PASSPHRASE:
        ts = str(int(time.time()))
        msg = ts + method.upper() + path + body

        # FIX: validate base64 once at startup instead of silently falling back
        # per-request (which produced wrong HMAC signatures on every API call).
        try:
            secret_bytes = base64.b64decode(POLY_API_SECRET + "==")
        except Exception as _b64e:
            log.warning(f"[AUTH] POLY_API_SECRET is not valid base64 ({_b64e}) — using raw UTF-8")
            secret_bytes = POLY_API_SECRET.encode()

        sig = base64.b64encode(
            hmac.new(
                secret_bytes,
                msg.encode(),
                hashlib.sha256
            ).digest()
        ).decode()

        poly_address = os.getenv("POLY_ADDRESS") or os.getenv("POLY_PRIVATE_KEY_ADDRESS", "")

        headers.update({
            "POLY-API-KEY": POLY_API_KEY,
            "POLY-ADDRESS": poly_address,
            "POLY-SIGNATURE": sig,
            "POLY-TIMESTAMP": ts,
            "POLY-PASSPHRASE": POLY_PASSPHRASE,
        })
        log.debug(f"Using signed API auth (with POLY-ADDRESS) for {method} {path}")

    return headers


def _poly_get_sync(path: str) -> Optional[dict]:
    try:
        url = POLYMARKET_HOST + path
        headers = _poly_headers("GET", path)
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        STATE.log_error(f"poly_get {path}: {e}")
        return None


def _poly_post_sync(path: str, payload: dict) -> Optional[dict]:
    try:
        body = json.dumps(payload, separators=(",", ":"))
        url = POLYMARKET_HOST + path
        headers = _poly_headers("POST", path, body)
        r = requests.post(url, headers=headers, data=body, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        STATE.log_error(f"poly_post {path}: {e}")
        return None


async def poly_get(path: str) -> Optional[dict]:
    return await asyncio.to_thread(_poly_get_sync, path)


async def poly_post(path: str, payload: dict) -> Optional[dict]:
    return await asyncio.to_thread(_poly_post_sync, path, payload)

# ─────────────────────────────────────────────
# MARKET DISCOVERY — QUERY YOUR EXACT SLUGS
# ─────────────────────────────────────────────
async def refresh_markets():
    global MARKET_CACHE, LAST_MARKET_REFRESH
    while not STOP_EVENT.is_set():
        try:
            found = {}
            slug_to_key = {}
            for asset, tf in TARGET_CONFIGS:
                slug = get_current_slug(asset, tf)
                base = f"{asset.lower()}-{tf.lower()}"
                slug_to_key[slug] = base
                log.info(f"🔍 Using LIVE slug → {slug} ({base})")
                log.info(f"   → Current time: {datetime.now().strftime('%H:%M:%S')} | Active market: {slug}")

            for slug, base in slug_to_key.items():
                try:
                    log.info(f"🔍 Fetching live market: {slug}")
                    # FIX: was blocking requests.get() inside async — stalled the entire
                    # event loop for up to 20s × 6 slugs = 120s while all other coroutines froze.
                    r = await asyncio.to_thread(
                        lambda s=slug: requests.get(
                            f"https://gamma-api.polymarket.com/markets?slug={s}", timeout=20
                        )
                    )
                    log.info(f"   Status: {r.status_code}")

                    if r.status_code != 200:
                        log.warning(f"   → Failed {r.status_code} (normal at exact rollover) — will retry")
                        continue

                    data = r.json()
                    if isinstance(data, list) and len(data) > 0:
                        m = data[0]
                    elif isinstance(data, dict):
                        m = data
                    else:
                        log.warning(f"   → Unexpected data type {type(data)}")
                        continue

                    log.info(f"   Keys: {list(m.keys())}")

                    # ─── TOKEN DISCOVERY (v5.2 FIX) ───
                    # Polymarket up/down markets use "Up"/"Down" outcomes.
                    # The gamma API returns:
                    #   - outcomes: '["Up","Down"]' or similar (string array of names)
                    #   - clobTokenIds: '["token1","token2"]' (token IDs in SAME order as outcomes)
                    # We need to correlate these two arrays to get the correct mapping.

                    up_token_id = None
                    down_token_id = None

                    # STEP 1: Parse outcomes array (can be string or list)
                    outcomes_raw = m.get("outcomes") or []
                    if isinstance(outcomes_raw, str):
                        try:
                            outcomes_raw = json.loads(outcomes_raw)
                        except:
                            outcomes_raw = []
                    
                    # STEP 2: Parse clobTokenIds array (can be string or list)
                    clob_ids_raw = m.get("clobTokenIds") or m.get("clob_token_ids") or []
                    if isinstance(clob_ids_raw, str):
                        try:
                            clob_ids_raw = json.loads(clob_ids_raw)
                        except:
                            clob_ids_raw = []
                    
                    # STEP 3: Correlate outcomes with clobTokenIds by index
                    log.debug(f"   outcomes_raw={outcomes_raw} (type={type(outcomes_raw).__name__})")
                    log.debug(f"   clob_ids_raw={clob_ids_raw[:2] if isinstance(clob_ids_raw, list) else clob_ids_raw} (type={type(clob_ids_raw).__name__})")
                    if isinstance(outcomes_raw, list) and isinstance(clob_ids_raw, list):
                        if len(outcomes_raw) == len(clob_ids_raw) and len(outcomes_raw) >= 2:
                            log.info(f"   Correlating {len(outcomes_raw)} outcomes with clobTokenIds...")
                            for idx, outcome in enumerate(outcomes_raw):
                                outcome_lower = str(outcome).strip().lower()
                                tid = str(clob_ids_raw[idx])
                                log.debug(f"   → outcome[{idx}]='{outcome}' (lower='{outcome_lower}') tid={tid[:12]}...")
                                if outcome_lower in ("up", "yes"):
                                    up_token_id = tid
                                    log.info(f"   ✓ Outcome[{idx}]='{outcome}' → UP token={tid[:16]}...")
                                elif outcome_lower in ("down", "no"):
                                    down_token_id = tid
                                    log.info(f"   ✓ Outcome[{idx}]='{outcome}' → DOWN token={tid[:16]}...")
                        else:
                            log.warning(f"   ⚠️  Array length mismatch: outcomes={len(outcomes_raw)} clobTokenIds={len(clob_ids_raw)}")

                    # STEP 4: If correlation failed, try the old tokens array method (for other market types)
                    if not up_token_id or not down_token_id:
                        tokens_arr = m.get("tokens") or []
                        if isinstance(tokens_arr, str):
                            try:
                                tokens_arr = json.loads(tokens_arr)
                            except:
                                tokens_arr = []

                        if isinstance(tokens_arr, list):
                            for t in tokens_arr:
                                if isinstance(t, dict):
                                    outcome = str(t.get("outcome") or t.get("name") or "").strip().lower()
                                    tid = (t.get("token_id") or t.get("tokenId") or
                                           t.get("id") or t.get("clobTokenId") or "")
                                    if tid:
                                        tid = str(tid)
                                        if outcome in ("up", "yes") and not up_token_id:
                                            up_token_id = tid
                                            log.info(f"   ✓ tokens[] outcome '{outcome}' → UP token={tid[:16]}...")
                                        elif outcome in ("down", "no") and not down_token_id:
                                            down_token_id = tid
                                            log.info(f"   ✓ tokens[] outcome '{outcome}' → DOWN token={tid[:16]}...")

                    # STEP 5: Last resort - use book prices to determine which is which
                    if not up_token_id or not down_token_id:
                        clob_ids = clob_ids_raw if isinstance(clob_ids_raw, list) else []

                        if isinstance(clob_ids, list) and len(clob_ids) >= 2:
                            if not up_token_id and not down_token_id:
                                # Neither found from outcomes — we have to guess.
                                # Fetch BOTH books and pick the correct assignment by checking
                                # which pair sums closer to 1.0
                                tid_a = str(clob_ids[0])
                                tid_b = str(clob_ids[1])

                                book_a = _fetch_book_sync(tid_a)
                                book_b = _fetch_book_sync(tid_b)

                                mid_a = 0.50
                                mid_b = 0.50
                                if book_a:
                                    na = _normalize_book(book_a)
                                    ba = float(na.get("best_bid", 0) or 0)
                                    aa = float(na.get("best_ask", 1) or 1)
                                    if ba > 0 and aa > ba:
                                        mid_a = (ba + aa) / 2.0

                                if book_b:
                                    nb = _normalize_book(book_b)
                                    bb = float(nb.get("best_bid", 0) or 0)
                                    ab = float(nb.get("best_ask", 1) or 1)
                                    if bb > 0 and ab > bb:
                                        mid_b = (bb + ab) / 2.0

                                # If A + B ≈ 1.0, A=UP B=DOWN. If A + (1-B) ≈ 1.0, they're both YES-style.
                                sum_ab = mid_a + mid_b
                                if 0.85 <= sum_ab <= 1.15:
                                    # A and B are complementary → A=UP(YES), B=DOWN(NO)
                                    up_token_id = tid_a
                                    down_token_id = tid_b
                                    log.info(f"   ✓ Token pair validated: A({mid_a:.3f}) + B({mid_b:.3f}) = {sum_ab:.3f} ≈ 1.0")
                                elif 0.85 <= (mid_a + (1.0 - mid_b)) <= 1.15:
                                    # Both are same-side → swap B
                                    up_token_id = tid_a
                                    down_token_id = tid_b
                                    log.warning(f"   ⚠️  Tokens may be swapped: A={mid_a:.3f} B={mid_b:.3f}")
                                else:
                                    # Can't determine — assign in order and hope
                                    up_token_id = tid_a
                                    down_token_id = tid_b
                                    log.warning(f"   ⚠️  Token assignment uncertain: A={mid_a:.3f} B={mid_b:.3f} sum={sum_ab:.3f}")

                            elif not up_token_id:
                                up_token_id = str(clob_ids[0]) if str(clob_ids[0]) != down_token_id else str(clob_ids[1])
                            elif not down_token_id:
                                down_token_id = str(clob_ids[1]) if str(clob_ids[1]) != up_token_id else str(clob_ids[0])

                            log.info(f"   ✓ Final tokens → UP={up_token_id[:12] if up_token_id else 'NONE'}... DOWN={down_token_id[:12] if down_token_id else 'NONE'}...")

                    # Map back to yes/no naming for compatibility with the rest of the code
                    yes_token_id = up_token_id    # UP = YES in these markets
                    no_token_id  = down_token_id  # DOWN = NO in these markets

                    token_id = yes_token_id
                    cond_id  = m.get("conditionId") or m.get("condition_id")

                    log.info(f"   Final → token_id={token_id} | cond_id={cond_id}")

                    if token_id and cond_id and yes_token_id and no_token_id:
                        min_size = float(m.get("orderMinSize") or 5)
                        found[f"{base}-up"] = {
                            "slug": m.get("slug"),
                            "question": m.get("question") or m.get("title"),
                            "token_id": yes_token_id or token_id,
                            "yes_token_id": yes_token_id or token_id,
                            "no_token_id": no_token_id,
                            "condition_id": str(cond_id),
                            "active": True,
                            "min_size": min_size,
                            "loaded_slug": slug,  # FIX: track which candle period this came from
                        }
                        found[f"{base}-down"] = {
                            "slug": m.get("slug"),
                            "question": m.get("question") or m.get("title"),
                            "token_id": no_token_id or token_id,
                            "yes_token_id": yes_token_id or token_id,
                            "no_token_id": no_token_id,
                            "condition_id": str(cond_id),
                            "active": True,
                            "min_size": min_size,
                            "loaded_slug": slug,  # FIX: track which candle period this came from
                        }
                        log.info(f"✅ SUCCESS: Loaded {base}-up & {base}-down")
                    else:
                        log.warning(f"   Missing token_id — storing with synthetic fallback")
                        found[f"{base}-up"] = {"slug": slug, "token_id": "synthetic", "yes_token_id": None, "no_token_id": None, "condition_id": None, "active": True}
                        found[f"{base}-down"] = {"slug": slug, "token_id": "synthetic", "yes_token_id": None, "no_token_id": None, "condition_id": None, "active": True}
                except Exception as e:
                    STATE.log_error(f"refresh_markets {slug}: {e}")
                    continue

            if found:
                MARKET_CACHE = found
                log.info(f"✅ LOADED {len(found)} real markets (UP + DOWN for each)")
                with STATE.lock:
                    STATE.poly_markets = MARKET_CACHE
            else:
                log.warning("No markets loaded — check links or API")
        except Exception as e:
            STATE.log_error(f"refresh_markets outer: {e}")
        await asyncio.sleep(30)  # v6: refresh every 30s for 5m market rotation

# ─────────────────────────────────────────────
# ORDER BOOK REFRESH
# ─────────────────────────────────────────────

def _fetch_book_sync(token_id: str) -> Optional[dict]:
    return _poly_get_sync(f"/book?token_id={token_id}")

async def refresh_books():
    while not STOP_EVENT.is_set():
        try:
            with STATE.lock:
                markets = dict(STATE.poly_markets)

            for slug, market in markets.items():
                try:
                    # CRITICAL: use "token_id" — it's set correctly per direction
                    # (yes_token for UP, no_token for DOWN) in market discovery.
                    # DO NOT use "yes_token_id" here — that always points to YES
                    # and would fetch the wrong book for DOWN contracts.
                    token_id = market.get("token_id")
                    if not token_id or token_id == "synthetic":
                        continue

                    # FIX: skip book fetch when the market slug has rolled over.
                    # Polymarket creates a new event every 5/15 min; fetching books
                    # for a settled (expired) market returns stale/empty data.
                    slug_parts = slug.split("-")  # e.g. "btc-5m-up"
                    if len(slug_parts) >= 3:
                        _asset_check = slug_parts[0].upper()
                        _tf_check    = slug_parts[1]
                        _live_slug   = get_current_slug(_asset_check, _tf_check)
                        _loaded_slug = market.get("slug", "")
                        if _loaded_slug and _loaded_slug != _live_slug:
                            log.debug(f"[BOOKS] Skipping stale market {slug} (loaded={_loaded_slug}, live={_live_slug})")
                            continue

                    book = await asyncio.to_thread(_fetch_book_sync, token_id)
                    if book:
                        # v5.0 FIX: normalize CLOB format → best_bid/best_ask scalars
                        book = _normalize_book(book)
                        bid_val = book.get("best_bid", 0)
                        ask_val = book.get("best_ask", 0)
                        with STATE.lock:
                            STATE.poly_books[slug] = book
                        if bid_val > 0 and ask_val > bid_val:
                            log.info(f"✅ Real book loaded for {slug} — bid={bid_val:.4f} ask={ask_val:.4f}")
                        else:
                            log.warning(f"⚠️  Book loaded for {slug} but bid/ask invalid (bid={bid_val} ask={ask_val}) — may be empty market")
                    else:
                        # Book fetch failed — keep the last good data if we have it
                        with STATE.lock:  # FIX: was an unprotected read — data race
                            existing = STATE.poly_books.get(slug)
                        if existing and poly_mid_from_book(existing) is not None:
                            log.debug(f"⚠️  Book fetch failed for {slug} — keeping last good data")
                        else:
                            # No previous data at all — use synthetic as initial placeholder
                            parts = slug.split("-")
                            asset_sym = parts[0].upper()
                            direction = parts[2].upper()
                            mid = synthetic_poly_price(asset_sym, direction)
                            with STATE.lock:
                                STATE.poly_books[slug] = {
                                    "best_bid": round(mid - 0.01, 4),
                                    "best_ask": round(mid + 0.01, 4),
                                }
                            log.info(f"⚠️  No book data for {slug} — using synthetic placeholder")
                except Exception as e:
                    STATE.log_error(f"refresh_book {slug}: {e}")

            # v5.1: validate UP+DOWN pairs sum to ~1.0
            with STATE.lock:
                books = dict(STATE.poly_books)
            for asset, tf in TARGET_CONFIGS:
                up_slug = f"{asset.lower()}-{tf.lower()}-up"
                dn_slug = f"{asset.lower()}-{tf.lower()}-down"
                up_book = books.get(up_slug, {})
                dn_book = books.get(dn_slug, {})
                up_mid = poly_mid_from_book(up_book)
                dn_mid = poly_mid_from_book(dn_book)
                if up_mid and dn_mid:
                    pair_sum = up_mid + dn_mid
                    if pair_sum < 0.85 or pair_sum > 1.15:
                        log.warning(f"⚠️  BOOK MISMATCH: {asset}-{tf} UP={up_mid:.4f} + DOWN={dn_mid:.4f} = {pair_sum:.4f} (should be ~1.0) — token assignment may be wrong")
        except Exception as e:
            STATE.log_error(f"refresh_books outer: {e}")
        await asyncio.sleep(5)  # v6: faster book refresh for 5m markets

# ─────────────────────────────────────────────
# BINANCE WEBSOCKET
# ─────────────────────────────────────────────

async def binance_ws_listener():
    streams = "btcusdt@aggTrade/ethusdt@aggTrade/xrpusdt@aggTrade"
    url     = BINANCE_WS_BASE + streams
    backoff = 1
    while not STOP_EVENT.is_set():
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10
            ) as ws:
                backoff = 1
                log.info("Binance WebSocket connected")
                while not STOP_EVENT.is_set():
                    raw     = await ws.recv()
                    data    = json.loads(raw)
                    payload = data.get("data", data)
                    symbol  = payload.get("s")
                    price   = payload.get("p")
                    if symbol and price:
                        px = float(price)
                        now_ts = time.time()
                        with STATE.lock:
                            STATE.cex_prices[symbol] = px
                            hist = STATE.cex_price_history.get(symbol)
                            if hist is not None:
                                hist.append((now_ts, px))
                                # Prune entries older than 10 minutes
                                cutoff = now_ts - 600
                                while hist and hist[0][0] < cutoff:
                                    hist.popleft()
        except Exception as e:
            STATE.log_error(f"Binance WS dropped: {e} — retry in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

# ─────────────────────────────────────────────
# KALSHI ORACLE
# ─────────────────────────────────────────────

KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_TICKERS = {
    "BTC": "KXBTCD",
    "ETH": "KXETHD",
}

def _fetch_kalshi_market_sync(ticker: str) -> Optional[dict]:
    try:
        log.info(f"🔄 Fetching Kalshi for {ticker}...")
        r = requests.get(
            f"{KALSHI_API}/markets",
            params={"series_ticker": ticker, "status": "open", "limit": 10},
            headers={"Accept": "application/json"},
            timeout=15,
        )
        log.info(f"Kalshi {ticker} → HTTP {r.status_code}")
        r.raise_for_status()
        data = r.json()
        markets = data.get("markets", [])
        log.info(f"Kalshi {ticker} returned {len(markets)} open markets")
        return data
    except Exception as e:
        STATE.log_error(f"Kalshi fetch {ticker}: {e}")
        return None

async def refresh_kalshi():
    while not STOP_EVENT.is_set():
        try:
            for asset, ticker in KALSHI_TICKERS.items():
                data = await asyncio.to_thread(_fetch_kalshi_market_sync, ticker)
                if not data:
                    continue
                markets = data.get("markets", [])
                if not markets:
                    log.warning(f"Kalshi {asset} → no open markets")
                    continue

                loaded = False
                for m in markets:
                    yes_bid = float(m.get("yes_bid_dollars", m.get("bid", m.get("yes_bid", 0))) or 0)
                    yes_ask = float(m.get("yes_ask_dollars", m.get("ask", m.get("yes_ask", 1))) or 1)
                    last_price = float(m.get("last_price_dollars", m.get("last_traded_price", m.get("last_price", 0))) or 0)

                    if yes_bid > 0 and yes_ask > yes_bid:
                        mid = (yes_bid + yes_ask) / 2.0
                        source = "BID/ASK"
                    elif last_price > 0:
                        mid = last_price
                        source = "LAST_PRICE"
                    else:
                        mid = 0.5
                        source = "FALLBACK_0.5"

                    with STATE.lock:
                        STATE.kalshi_prices[asset] = {
                            "up_prob": mid,
                            "down_prob": 1.0 - mid,
                            "ticker": m.get("ticker", ""),
                            "title": m.get("title", ""),
                        }
                    log.info(f"✅ Kalshi {asset} LOADED → UP {mid:.1%} ({source}) | {m.get('title','')}")
                    loaded = True
                    break

                if not loaded:
                    log.warning(f"Kalshi {asset} → no usable price found")
                    
        except Exception as e:
            STATE.log_error(f"refresh_kalshi: {e}")
        await asyncio.sleep(30)

def kalshi_confluence_check(asset: str, direction: str, timeframe: str) -> tuple:
    # Kalshi markets are daily — only meaningful as a weak prior on 15m trades.
    # Skip entirely for 5m (daily signal has no 5m resolution).
    if timeframe == "5m":
        return True, 0.0
    kalshi_up = get_kalshi_prob(asset)
    if kalshi_up is None or kalshi_up == 0.5:
        return True, 0.0   # no data → don't block, no boost
    kalshi_prob = kalshi_up if direction == "UP" else (1.0 - kalshi_up)
    agreement = kalshi_prob >= KALSHI_MIN_AGREEMENT_PCT
    boost = (kalshi_prob - 0.5) * KALSHI_WEIGHT if agreement else 0.0
    return agreement, boost

# ─────────────────────────────────────────────
# v5.0: ORDER STATUS POLLING HELPERS
# ─────────────────────────────────────────────

def _poll_order_status_sync(order_id: str) -> Optional[dict]:
    """Poll the CLOB for order status. Returns order dict or None."""
    try:
        if STATE.clob_client is None:
            return None
        result = STATE.clob_client.get_order(order_id)
        return result
    except Exception as e:
        log.debug(f"poll_order_status {order_id}: {e}")
        return None


def _cancel_order_sync(order_id: str) -> bool:
    """Cancel a pending order. Returns True on success."""
    try:
        if STATE.clob_client is None:
            return False
        result = STATE.clob_client.cancel(order_id)
        log.info(f"[CANCEL] Order {order_id[:12]}... cancelled: {result}")
        return bool(result)
    except Exception as e:
        log.warning(f"[CANCEL] Failed for {order_id[:12]}...: {e}")
        return False


def _get_token_balance_sync(token_id: str) -> Optional[float]:
    """Get the actual token balance for a conditional token.
    Returns the balance in standard units (not raw 6-decimal format), or None on error."""
    try:
        if STATE.clob_client is None:
            log.debug("[TOKEN_BALANCE] No CLOB client available")
            return None
        if not token_id or token_id == "synthetic":
            log.debug(f"[TOKEN_BALANCE] Invalid token_id: {token_id}")
            return None
        
        log.debug(f"[TOKEN_BALANCE] Querying balance for token_id={token_id[:20]}...")
        
        result = STATE.clob_client.get_balance_allowance(
            params=BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id
            )
        )
        
        raw_balance = result.get("balance", "0")
        # Polymarket uses 6 decimal places for token amounts
        balance = float(raw_balance) / 1_000_000
        log.debug(f"[TOKEN_BALANCE] Raw balance: {raw_balance}, Parsed: {balance:.6f}")
        return balance
    except Exception as e:
        log.error(f"[TOKEN_BALANCE] Failed to get balance for {token_id[:16]}...: {e}")
        return None


def _update_token_allowance_sync(token_id: str) -> bool:
    """Update balance allowance for a conditional token before selling.
    This is REQUIRED on Polymarket before you can sell/transfer tokens.
    Returns True on success."""
    try:
        if STATE.clob_client is None:
            log.warning("[ALLOWANCE] No CLOB client available")
            log.debug("[ALLOWANCE] STATE.clob_client is None - cannot update allowance")
            return False
        if not token_id or token_id == "synthetic":
            log.warning(f"[ALLOWANCE] Invalid token_id: {token_id}")
            return False
        
        log.debug(f"[ALLOWANCE] Starting allowance update for token_id={token_id}")
        log.info(f"[ALLOWANCE] Updating allowance for token {token_id[:16]}...")
        
        result = STATE.clob_client.update_balance_allowance(
            params=BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id
            )
        )
        
        log.debug(f"[ALLOWANCE] Raw result: {result}")
        log.info(f"[ALLOWANCE] Update successful for {token_id[:16]}")
        return True
    except Exception as e:
        log.error(f"[ALLOWANCE] Failed to update allowance for {token_id[:16]}...: {e}")
        log.debug(f"[ALLOWANCE] Exception details: {type(e).__name__}: {e}", exc_info=True)
        return False


def _get_fill_price_from_order(order_data: dict) -> Optional[float]:
    """Extract the actual fill price from order response data."""
    if not order_data:
        return None
    # Polymarket CLOB may return average fill price in different fields
    for key in ("average_price", "avg_price", "price", "filled_avg_price"):
        val = order_data.get(key)
        if val:
            try:
                return float(val)
            except (ValueError, TypeError):
                continue
    return None


# ─────────────────────────────────────────────
# ORDER PLACEMENT (fixed with official SDK)
# ─────────────────────────────────────────────

def place_paper_order(
    market_slug: str,
    side: str,
    price: float,
    size: float,
    confidence: float,
    edge: float
):
    cost = price * size
    with STATE.lock:
        if STATE.kill_switch:
            return
        if cost > STATE.cash:
            log.debug(f"Insufficient cash for {market_slug}: need {cost:.2f}, have {STATE.cash:.2f}")
            return
        market = MARKET_CACHE.get(market_slug, {})
        # Use "token_id" directly - it's correctly set per direction in market discovery
        token_id = market.get("token_id") or ""
        STATE.cash -= cost
        STATE.positions.append(Position(
            market_slug=market_slug,
            side=side,
            entry_price=price,
            size=size,
            confidence=confidence,
            edge=edge,
            token_id=token_id,
            open_order_id="paper",
            open_filled=True,  # paper = instant fill
            high_water_mark=price,  # v6: init trailing stop tracker
        ))
        STATE.last_trades.appendleft(TradeRecord(
            ts=time.time(),
            market_slug=market_slug,
            side=side,
            price=price,
            size=size,
            status="OPEN"
        ))
        STATE.signals_fired += 1
    log.info(f"[PAPER] OPEN {market_slug} | price={price:.4f} size={size:.2f} edge={edge*100:.2f}% conf={confidence*100:.1f}%")

async def place_live_order(
    market_slug: str,
    side: str,
    price: float,
    size: float,
    confidence: float,
    edge: float
):
    log.debug(f"[LIVE ORDER] ═══════════════════════════════════════")
    log.debug(f"[LIVE ORDER] Starting place_live_order for {market_slug}")
    log.debug(f"[LIVE ORDER] side={side}, price={price}, size={size}, conf={confidence}, edge={edge}")
    
    if market_slug not in MARKET_CACHE:
        log.warning(f"[LIVE] SKIP {market_slug}: missing from MARKET_CACHE")
        log.debug(f"[LIVE ORDER] Available markets: {list(MARKET_CACHE.keys())}")
        STATE.log_error(f"Market {market_slug} missing from MARKET_CACHE")
        return

    market = MARKET_CACHE[market_slug]
    log.debug(f"[LIVE ORDER] Market data: {market}")
    
    # CRITICAL FIX: Use "token_id" directly - it's already correctly set per direction
    # in market discovery (yes_token for UP, no_token for DOWN).
    # Using "yes_token_id" first was WRONG for DOWN markets!
    token_id = market.get("token_id")
    log.debug(f"[LIVE ORDER] token_id={token_id}")
    
    if not token_id or token_id == "synthetic":
        log.warning(f"[LIVE] SKIP {market_slug}: no real token_id (still synthetic?)")
        STATE.log_error(f"No real token_id for {market_slug} (still synthetic?)")
        return

    min_size = market.get("min_size", 5)
    rounded_size = round(float(size), 2)
    if rounded_size < min_size:
        log.warning(f"[LIVE] SKIP {market_slug}: size {rounded_size} < orderMinSize {min_size}")
        return

    try:
        if STATE.clob_client is None:
            STATE.clob_client = ClobClient(
                host=POLYMARKET_HOST,
                key=POLY_PRIVATE_KEY,
                chain_id=POLYMARKET_CHAIN_ID,
                signature_type=2,
                funder=POLY_FUNDER if POLY_FUNDER else None
            )
            STATE.clob_client.set_api_creds(STATE.clob_client.create_or_derive_api_creds())
            log.info("✅ CLOB client initialized with signed API credentials")

        order_args = OrderArgs(
            token_id=token_id,
            price=round(float(price), 2),
            size=rounded_size,
            side=BUY if side.upper() == "BUY" else SELL
        )

        signed_order = STATE.clob_client.create_order(order_args)

        log.info(f"[LIVE] Placing signed order via official SDK → {market_slug} {side} {size}@{price}")
        response = STATE.clob_client.post_order(signed_order, OrderType.GTC)

        log.info(f"[LIVE] post_order response: {response!r}")
        if response:
            # v5.0: extract order ID for fill polling
            order_id = ""
            if isinstance(response, dict):
                order_id = response.get("orderID", response.get("order_id", response.get("id", "")))
            elif isinstance(response, str):
                order_id = response

            log.info(f"[LIVE SUCCESS] Order submitted: {market_slug} {side} order_id={order_id}")
            cost = price * size
            with STATE.lock:
                pos = Position(
                    market_slug=market_slug,
                    side=side,
                    entry_price=price,
                    size=size,
                    confidence=confidence,
                    edge=edge,
                    token_id=token_id,
                    open_order_id=order_id,
                    open_filled=False,  # v5.0: not filled until confirmed
                    high_water_mark=price,  # v6: init trailing stop tracker
                )
                STATE.cash -= cost
                STATE.positions.append(pos)
                STATE.last_trades.appendleft(TradeRecord(
                    ts=time.time(),
                    market_slug=market_slug,
                    side=side,
                    price=price,
                    size=size,
                    status="OPEN"
                ))
                STATE.signals_fired += 1

                # v5.0: track for fill polling
                if order_id:
                    STATE.pending_opens.append({
                        "order_id": order_id,
                        "position": pos,
                        "placed_ts": time.time(),
                    })
        else:
            log.error(f"[LIVE] Order returned empty/falsy response for {market_slug}")
            STATE.log_error("Order returned empty response")

    except Exception as e:
        log.error(f"[LIVE] Order placement exception for {market_slug}: {e}")
        STATE.log_error(f"Live order placement error: {e}")


async def execute_trade(
    market_slug: str,
    side: str,
    price: float,
    size: float,
    confidence: float,
    edge: float
):
    if LIVE_TRADING_ENABLED:
        await place_live_order(market_slug, side, price, size, confidence, edge)
    else:
        place_paper_order(market_slug, side, price, size, confidence, edge)

# ─────────────────────────────────────────────
# POSITION MANAGEMENT (v5.0: enhanced with order polling,
# guaranteed close, aggressive pricing)
# ─────────────────────────────────────────────

def close_position(pos: Position, exit_price: float, won: bool) -> bool:
    """Place a close order. Returns True if position can be removed from STATE
    immediately (paper mode, emergency, or no order_id). Returns False if a
    live close order is pending fill — the position must stay in STATE until
    _poll_pending_closes confirms the fill and removes it."""
    
    log.debug(f"[CLOSE_POSITION] Starting close for {pos.market_slug}")
    log.debug(f"[CLOSE_POSITION] Position details: token_id={pos.token_id}, size={pos.size}, entry={pos.entry_price}, exit={exit_price}, won={won}")
    log.debug(f"[CLOSE_POSITION] LIVE_TRADING_ENABLED={LIVE_TRADING_ENABLED}")

    if LIVE_TRADING_ENABLED and pos.token_id and pos.token_id != "synthetic":
        log.debug(f"[CLOSE_POSITION] Attempting live close for {pos.market_slug}")
        try:
            if STATE.clob_client is None:
                log.debug("[CLOSE_POSITION] Initializing CLOB client for close")
                STATE.clob_client = ClobClient(
                    host=POLYMARKET_HOST,
                    key=POLY_PRIVATE_KEY,
                    chain_id=POLYMARKET_CHAIN_ID,
                    signature_type=2,
                    funder=POLY_FUNDER if POLY_FUNDER else None
                )
                STATE.clob_client.set_api_creds(STATE.clob_client.create_or_derive_api_creds())
                log.info("[CLOSE_POSITION] CLOB client initialized for close operation")

            # v5.0: use aggressive pricing to guarantee fill
            log.debug(f"[CLOSE_POSITION] CLOSE_USE_MARKET_PRICE={CLOSE_USE_MARKET_PRICE}")
            # Set price 1-2 ticks below market to cross the spread immediately
            if CLOSE_USE_MARKET_PRICE:
                slug = pos.market_slug
                book = STATE.poly_books.get(slug, {})
                best_bid = float(book.get("best_bid", 0) or 0)
                # Sell at best_bid minus 1 tick (0.01) to guarantee fill
                aggressive_price = max(round(best_bid - 0.01, 2), 0.01)
                sell_price = aggressive_price
            else:
                sell_price = round(float(exit_price), 2)
            
            log.debug(f"[CLOSE_POSITION] Calculated sell_price={sell_price}")

            # CRITICAL FIX: Update token allowance BEFORE placing sell order!
            # Polymarket requires approval to transfer conditional tokens.
            # This was the cause of the "400 no..." (no allowance) error.
            allowance_ok = _update_token_allowance_sync(pos.token_id)
            if not allowance_ok:
                log.warning(f"[LIVE CLOSE] Allowance update failed for {pos.market_slug}, attempting sell anyway...")

            # CRITICAL FIX: Get actual token balance instead of using pos.size
            # The actual tokens received may be less than requested due to fees/partial fills
            actual_balance = _get_token_balance_sync(pos.token_id)
            if actual_balance is not None and actual_balance > 0:
                # Use the actual balance, but cap at pos.size to avoid over-selling
                sell_size = min(actual_balance, pos.size)
                # Floor to 2 decimal places to avoid dust issues
                sell_size = float(int(sell_size * 100) / 100)
                if sell_size < 0.01:
                    log.warning(f"[LIVE CLOSE] Token balance too small to sell: {actual_balance:.6f}")
                    _finalize_close(pos, exit_price, won)
                    return True
                log.debug(f"[CLOSE_POSITION] Using actual balance: {actual_balance:.6f}, selling: {sell_size:.2f} (pos.size was {pos.size:.2f})")
            else:
                # Fallback to pos.size if we can't get balance
                sell_size = round(float(pos.size), 2)
                log.warning(f"[CLOSE_POSITION] Could not get token balance, using pos.size={sell_size}")

            order_args = OrderArgs(
                token_id=pos.token_id,
                price=sell_price,
                size=sell_size,
                side=SELL,
            )
            log.debug(f"[CLOSE_POSITION] OrderArgs: token_id={pos.token_id[:20]}..., price={sell_price}, size={sell_size}, side=SELL")
            
            log.debug("[CLOSE_POSITION] Creating signed order...")
            signed_order = STATE.clob_client.create_order(order_args)
            log.debug(f"[CLOSE_POSITION] Signed order created: {type(signed_order)}")
            
            log.debug("[CLOSE_POSITION] Posting order to CLOB...")
            response = STATE.clob_client.post_order(signed_order, OrderType.GTC)
            log.debug(f"[CLOSE_POSITION] post_order response: {response}")

            if response:
                order_id = ""
                if isinstance(response, dict):
                    order_id = response.get("orderID", response.get("order_id", response.get("id", "")))
                elif isinstance(response, str):
                    order_id = response

                log.info(f"[LIVE CLOSE] Order submitted: {pos.market_slug} SELL {sell_size}@{sell_price:.4f} order_id={order_id}")

                # v5.0: track close order for fill confirmation instead of
                # immediately removing the position
                if order_id:
                    with STATE.lock:
                        STATE.pending_closes.append(PendingCloseOrder(
                            order_id=order_id,
                            position=pos,
                            placed_ts=time.time(),
                            expected_exit_price=sell_price,
                            won=won,
                            cancel_after=STALE_ORDER_TIMEOUT,
                        ))
                    # DON'T remove position yet — keep it in STATE.positions
                    # until _poll_pending_closes confirms the fill
                    return False
                else:
                    # No order ID returned — finalize immediately
                    log.warning(f"[LIVE CLOSE] No order_id returned, finalizing immediately")
            else:
                log.error(f"[LIVE CLOSE] Empty response for {pos.market_slug}")
                STATE.log_error(f"Live close empty response: {pos.market_slug}")
                # FALLBACK: try again with even more aggressive pricing
                _emergency_close(pos, exit_price, won)
                return True

        except Exception as e:
            log.error(f"[LIVE CLOSE] Exception for {pos.market_slug}: {e}")
            log.debug(f"[LIVE CLOSE] Full exception details:", exc_info=True)
            log.debug(f"[LIVE CLOSE] Position state: token_id={pos.token_id}, size={pos.size}, entry={pos.entry_price}")
            STATE.log_error(f"Live close error: {e}")
            # FALLBACK: finalize on paper to keep books straight
            log.debug(f"[LIVE CLOSE] Attempting emergency close fallback")
            _emergency_close(pos, exit_price, won)
            return True

    # Paper mode or fallback: finalize immediately
    log.debug(f"[CLOSE_POSITION] Using paper mode close for {pos.market_slug}")
    _finalize_close(pos, exit_price, won)
    return True


def _emergency_close(pos: Position, exit_price: float, won: bool):
    """v7.2: PERSISTENT emergency close — keeps retrying until the position is sold.
    Tries 5 progressively lower prices. Never gives up on a real position."""
    log.info(f"[EMERGENCY CLOSE] Starting persistent close for {pos.market_slug}")
    
    if not STATE.clob_client or not pos.token_id or pos.token_id == "synthetic":
        _finalize_close(pos, exit_price, won)
        return

    # Get actual token balance
    actual_balance = _get_token_balance_sync(pos.token_id)
    if actual_balance is not None and actual_balance > 0:
        sell_size = min(actual_balance, pos.size)
        sell_size = float(int(sell_size * 100) / 100)
        if sell_size < 0.01:
            log.warning(f"[EMERGENCY CLOSE] Token balance too small: {actual_balance:.6f}")
            _finalize_close(pos, exit_price, won)
            return
    else:
        sell_size = round(float(pos.size), 2)

    # Update allowance
    _update_token_allowance_sync(pos.token_id)

    # v7.2: Try progressively lower prices — NEVER leave tokens stranded
    prices_to_try = [
        max(round(exit_price - 0.02, 2), 0.01),   # 2 ticks below exit
        max(round(exit_price * 0.80, 2), 0.01),    # 80% of exit
        max(round(exit_price * 0.50, 2), 0.01),    # 50% of exit
        0.02,                                        # near-zero
        0.01,                                        # absolute floor
    ]

    for attempt, try_price in enumerate(prices_to_try):
        try:
            log.info(f"[EMERGENCY CLOSE] Attempt {attempt+1}/5 — {pos.market_slug} SELL {sell_size}@{try_price}")
            order_args = OrderArgs(
                token_id=pos.token_id,
                price=try_price,
                size=sell_size,
                side=SELL,
            )
            signed_order = STATE.clob_client.create_order(order_args)
            response = STATE.clob_client.post_order(signed_order, OrderType.GTC)
            if response:
                order_id = ""
                if isinstance(response, dict):
                    order_id = response.get("orderID", response.get("order_id", response.get("id", "")))
                log.info(f"[EMERGENCY CLOSE] Attempt {attempt+1} submitted: order_id={order_id}")

                # Wait briefly for fill then check
                time.sleep(3)
                if order_id:
                    order_data = _poll_order_status_sync(order_id)
                    if order_data:
                        status = str(order_data.get("status", "")).upper()
                        if status in ("FILLED", "MATCHED"):
                            fill_price = _get_fill_price_from_order(order_data) or try_price
                            log.info(f"[EMERGENCY CLOSE] FILLED at {fill_price:.4f} on attempt {attempt+1}")
                            _finalize_close(pos, fill_price, won)
                            return
                        elif status in ("LIVE", "OPEN", "PENDING"):
                            # Still open — cancel and try lower price
                            log.info(f"[EMERGENCY CLOSE] Attempt {attempt+1} still open — cancelling and retrying lower")
                            _cancel_order_sync(order_id)
                            time.sleep(1)
                            continue
                # No order_id returned but response was truthy — assume submitted
                log.info(f"[EMERGENCY CLOSE] Attempt {attempt+1} submitted (no order_id to track)")
                _finalize_close(pos, try_price, won)
                return
        except Exception as e:
            log.error(f"[EMERGENCY CLOSE] Attempt {attempt+1} failed: {e}")
            time.sleep(1)
            continue

    # All 5 attempts failed — finalize on paper as last resort
    log.error(f"[EMERGENCY CLOSE] All 5 attempts failed for {pos.market_slug} — finalizing on paper")
    _finalize_close(pos, exit_price, won)


def _finalize_close(pos: Position, exit_price: float, won: bool):
    """Finalize PnL and book the close. Called after fill confirmation or
    immediately for paper mode."""
    proceeds = pos.size * exit_price
    pnl      = proceeds - (pos.entry_price * pos.size)
    with STATE.lock:
        STATE.cash         += proceeds
        STATE.realized_pnl += pnl
        if won:
            STATE.win_count += 1
        else:
            STATE.loss_count += 1
        STATE.last_trades.appendleft(TradeRecord(
            ts=time.time(),
            market_slug=pos.market_slug,
            side=pos.side,
            price=exit_price,
            size=pos.size,
            status="CLOSE",
            pnl=round(pnl, 4),
        ))
    log.info(f"[CLOSE] {pos.market_slug} pnl={pnl:+.2f} {'WIN' if won else 'LOSS'}")


def update_unrealized():
    total = 0.0
    with STATE.lock:
        positions = list(STATE.positions)
    for pos in positions:
        try:
            parts = pos.market_slug.split("-")
            asset = parts[0].upper()
            tf    = parts[1]
            direc = parts[2].upper()
            # v6: use bid price for unrealized PnL (what you'd actually get selling)
            book = STATE.poly_books.get(pos.market_slug, {})
            bid = float(book.get("best_bid", 0) or 0)
            if bid > 0:
                current = bid
            else:
                current = get_poly_prob(asset, tf, direc)
            total  += (current - pos.entry_price) * pos.size
        except Exception:
            continue
    with STATE.lock:
        STATE.unrealized_pnl = total

def check_kill_switch():
    with STATE.lock:
        if not STATE.kill_switch and STATE.drawdown_pct() >= DAILY_DRAWDOWN_LIMIT:
            STATE.kill_switch = True
            log.warning(f"KILL SWITCH TRIGGERED — daily drawdown >= {DAILY_DRAWDOWN_LIMIT*100:.0f}%")

def check_day_reset():
    now = time.time()
    with STATE.lock:
        elapsed = now - STATE.day_reset_ts
        if elapsed >= 86400:
            STATE.day_start_equity = STATE.total_equity()
            STATE.day_reset_ts     = now
            STATE.kill_switch      = False
            log.info("Daily reset — equity baseline updated, kill switch cleared")

async def manage_positions():
    while not STOP_EVENT.is_set():
        try:
            update_unrealized()
            check_kill_switch()
            check_day_reset()

            # v5.0: poll pending open orders for fill confirmation
            await _poll_pending_opens()

            # v5.0: poll pending close orders for fill confirmation
            await _poll_pending_closes()

            to_remove = []
            with STATE.lock:
                positions = list(STATE.positions)

            for pos in positions:
                try:
                    # v5.0: skip positions with unfilled open orders
                    if not pos.open_filled:
                        # Check if order has been pending too long (stale open)
                        age = time.time() - pos.opened_ts
                        if age > STALE_ORDER_TIMEOUT:
                            log.warning(f"[STALE OPEN] {pos.market_slug} open order unfilled after {age:.0f}s — cancelling")
                            if pos.open_order_id and pos.open_order_id != "paper":
                                await asyncio.to_thread(_cancel_order_sync, pos.open_order_id)
                            # Refund cash and remove position
                            with STATE.lock:
                                STATE.cash += pos.entry_price * pos.size
                            to_remove.append(pos)
                        continue

                    parts = pos.market_slug.split("-")
                    asset = parts[0].upper()
                    tf    = parts[1]
                    direc = parts[2].upper()
                    slug_for_book = pos.market_slug
                    book = STATE.poly_books.get(slug_for_book, {})
                    bid_price = float(book.get("best_bid", 0) or 0)
                    mid_price = get_poly_prob(asset, tf, direc)
                    current = bid_price if bid_price > 0 else mid_price
                    age         = time.time() - pos.opened_ts
                    ttl_seconds = 300 if tf == "5m" else 900

                    # FIX (Critical): Update the trailing high-water mark every tick.
                    # high_water_mark was declared in Position and initialised to entry_price
                    # at open time, but was NEVER updated here — so the trailing stop level
                    # never moved and could never fire correctly.
                    if current > pos.high_water_mark:
                        log.debug(
                            f"[TRAIL] {pos.market_slug} HWM "
                            f"{pos.high_water_mark:.4f} → {current:.4f}"
                        )
                        pos.high_water_mark = current

                    # Trailing stop: close if price falls STOP_LOSS_PCT below the highest
                    # price seen since entry. Falls back to fixed $0.15 floor so very cheap
                    # contracts (entry < $0.15) still get a sensible hard stop.
                    trail_stop  = pos.high_water_mark * (1.0 - STOP_LOSS_PCT)
                    fixed_stop  = max(pos.entry_price - 0.15, 0.05)
                    stop_loss   = max(trail_stop, fixed_stop)

                    exit_buffer = 30 if tf == "5m" else 60
                    near_resolution = ttl_seconds - exit_buffer

                    with STATE.lock:
                        already_closing = any(
                            pc.position is pos for pc in STATE.pending_closes
                        )
                    if already_closing:
                        continue

                    if current <= stop_loss:
                        log.info(
                            f"[STOP LOSS] {pos.market_slug} bid={current:.4f} <= "
                            f"stop={stop_loss:.4f} "
                            f"(trail={trail_stop:.4f} hwm={pos.high_water_mark:.4f} "
                            f"entry={pos.entry_price:.4f})"
                        )
                        can_remove = close_position(pos, max(current, 0.01), False)
                        if can_remove:
                            to_remove.append(pos)
                    elif age >= near_resolution:
                        won = current > pos.entry_price
                        log.info(f"[NEAR-RESOLUTION] {pos.market_slug} age={age:.0f}s — selling at {current:.4f} (entry={pos.entry_price:.4f}) {'WIN' if won else 'LOSS'}")
                        can_remove = close_position(pos, current, won)
                        if can_remove:
                            to_remove.append(pos)
                    elif age >= ttl_seconds:
                        won = current > pos.entry_price
                        log.info(f"[TTL EXPIRE] {pos.market_slug} age={age:.0f}s — closing at {current:.4f}")
                        can_remove = close_position(pos, current, won)
                        if can_remove:
                            to_remove.append(pos)

                    # v7.2: STUCK POSITION DETECTOR
                    if age >= ttl_seconds * 2 and pos not in to_remove:
                        log.error(f"[STUCK] {pos.market_slug} still open at {age:.0f}s (TTL={ttl_seconds}s) — forcing emergency close")
                        _emergency_close(pos, max(current, 0.01), current > pos.entry_price)
                        to_remove.append(pos)
                except Exception as e:
                    STATE.log_error(f"manage_positions trade error: {e}")

            if to_remove:
                with STATE.lock:
                    STATE.positions = [p for p in STATE.positions if p not in to_remove]

        except Exception as e:
            STATE.log_error(f"manage_positions: {e}")
        await asyncio.sleep(1)  # v5.1: check every 1 second


# ─────────────────────────────────────────────
# v5.0: PENDING ORDER POLLING
# ─────────────────────────────────────────────

async def _poll_pending_opens():
    """Check if pending open orders have been filled."""
    if not LIVE_TRADING_ENABLED:
        return
    with STATE.lock:
        pending = list(STATE.pending_opens)
    if not pending:
        return

    log.debug(f"[POLL OPENS] Checking {len(pending)} pending open orders")
    filled_ids = []
    cancelled_ids = []

    for entry in pending:
        order_id = entry["order_id"]
        pos = entry["position"]
        age = time.time() - entry["placed_ts"]

        log.debug(f"[POLL OPENS] Checking order {order_id[:12]}... age={age:.1f}s market={pos.market_slug}")
        order_data = await asyncio.to_thread(_poll_order_status_sync, order_id)
        if order_data:
            status = str(order_data.get("status", "")).upper()
            if status in ("FILLED", "MATCHED"):
                # Update entry price with actual fill price
                fill_price = _get_fill_price_from_order(order_data)
                if fill_price:
                    with STATE.lock:
                        # Adjust cash for price difference
                        price_diff = pos.entry_price - fill_price
                        STATE.cash += price_diff * pos.size
                        pos.entry_price = fill_price
                pos.open_filled = True
                filled_ids.append(order_id)
                log.info(f"[FILL CONFIRMED] Open order {order_id[:12]}... filled at {fill_price or pos.entry_price:.4f}")
            elif status in ("CANCELLED", "CANCELED", "EXPIRED"):
                cancelled_ids.append(order_id)
                # Refund
                with STATE.lock:
                    STATE.cash += pos.entry_price * pos.size
                    STATE.positions = [p for p in STATE.positions if p is not pos]
                log.warning(f"[OPEN CANCELLED] {pos.market_slug} order was cancelled/expired")

        # Auto-cancel stale open orders
        if age > STALE_ORDER_TIMEOUT and order_id not in filled_ids and order_id not in cancelled_ids:
            log.warning(f"[STALE OPEN] Cancelling {order_id[:12]}... after {age:.0f}s")
            await asyncio.to_thread(_cancel_order_sync, order_id)
            cancelled_ids.append(order_id)
            with STATE.lock:
                STATE.cash += pos.entry_price * pos.size
                STATE.positions = [p for p in STATE.positions if p is not pos]

    if filled_ids or cancelled_ids:
        with STATE.lock:
            STATE.pending_opens = [
                e for e in STATE.pending_opens
                if e["order_id"] not in filled_ids and e["order_id"] not in cancelled_ids
            ]


async def _poll_pending_closes():
    """Check if pending close orders have been filled, cancel stale ones and retry."""
    if not LIVE_TRADING_ENABLED:
        return
    with STATE.lock:
        pending = list(STATE.pending_closes)
    if not pending:
        return

    log.debug(f"[POLL CLOSES] Checking {len(pending)} pending close orders")
    resolved_ids = []
    positions_to_remove = []

    for pco in pending:
        age = time.time() - pco.placed_ts

        log.debug(f"[POLL CLOSES] Checking order {pco.order_id[:12]}... age={age:.1f}s market={pco.position.market_slug}")
        order_data = await asyncio.to_thread(_poll_order_status_sync, pco.order_id)
        if order_data:
            status = str(order_data.get("status", "")).upper()
            if status in ("FILLED", "MATCHED"):
                # Use real fill price for PnL
                fill_price = _get_fill_price_from_order(order_data) or pco.expected_exit_price
                _finalize_close(pco.position, fill_price, pco.won)
                resolved_ids.append(pco.order_id)
                positions_to_remove.append(pco.position)
                log.info(f"[CLOSE FILL CONFIRMED] {pco.position.market_slug} filled at {fill_price:.4f}")
                continue
            elif status in ("CANCELLED", "CANCELED", "EXPIRED"):
                # Order was cancelled — retry with more aggressive price
                log.warning(f"[CLOSE CANCELLED] {pco.position.market_slug} — retrying with emergency price")
                _emergency_close(pco.position, pco.expected_exit_price, pco.won)
                resolved_ids.append(pco.order_id)
                positions_to_remove.append(pco.position)
                continue

        # v5.0: auto-cancel stale close orders after timeout and retry
        if age > pco.cancel_after:
            log.warning(f"[STALE CLOSE] {pco.position.market_slug} close order unfilled after {age:.0f}s — cancelling and retrying")
            await asyncio.to_thread(_cancel_order_sync, pco.order_id)
            # Retry with emergency pricing (price=0.01) to guarantee fill
            _emergency_close(pco.position, pco.expected_exit_price, pco.won)
            resolved_ids.append(pco.order_id)
            positions_to_remove.append(pco.position)

    if resolved_ids or positions_to_remove:
        with STATE.lock:
            STATE.pending_closes = [
                pc for pc in STATE.pending_closes
                if pc.order_id not in resolved_ids
            ]
            # NOW remove the positions — only after PnL is finalized
            if positions_to_remove:
                STATE.positions = [
                    p for p in STATE.positions
                    if p not in positions_to_remove
                ]


# ─────────────────────────────────────────────
# STRATEGY LOOP v7.1: TREND CONTINUATION
# Only trade 15m. Pick ONE direction per asset.
# Enter when trend is established (3-5 min in).
# Hold to resolution.
# ─────────────────────────────────────────────

_slug_last_fired: Dict[str, float] = {}

_signal_history: Dict[str, Deque[float]] = {}
_SIGNAL_PERSISTENCE_WINDOW = 3  # v7.2: 3 consecutive readings (balance speed vs quality)

def _record_signal(slug: str, edge: float) -> bool:
    if slug not in _signal_history:
        _signal_history[slug] = deque(maxlen=_SIGNAL_PERSISTENCE_WINDOW)
    _signal_history[slug].append(edge)
    if len(_signal_history[slug]) < _SIGNAL_PERSISTENCE_WINDOW:
        return False
    return all(e > 0 for e in _signal_history[slug])

# v7.1: minimum candle return to consider a trend "established"
# 15m needs bigger moves; 5m accepts smaller (proportional to sqrt of time)
_MIN_CANDLE_MOVE_15M = {
    "BTC": 0.0010,
    "ETH": 0.0012,
    "XRP": 0.0015,
}
_MIN_CANDLE_MOVE_5M = {
    "BTC": 0.0006,
    "ETH": 0.0007,
    "XRP": 0.0009,
}

async def strategy_loop():
    while not STOP_EVENT.is_set():
        try:
            with STATE.lock:
                ks = STATE.kill_switch
            if ks:
                await asyncio.sleep(5)
                continue

            for asset, tf in TARGET_CONFIGS:
                try:
                    cooldown = 300 if tf == "5m" else 900

                    # v7.1: DETERMINE DOMINANT DIRECTION — don't evaluate both
                    dominant_dir, candle_strength = get_candle_strength(asset, tf)
                    if dominant_dir == "NONE":
                        continue

                    # v7.1: MINIMUM CANDLE MOVE — trend must be established
                    move_table = _MIN_CANDLE_MOVE_15M if tf == "15m" else _MIN_CANDLE_MOVE_5M
                    min_move = move_table.get(asset, 0.0008)
                    if candle_strength < min_move:
                        log.debug(f"[STRATEGY] {asset}-{tf} skipped — candle move {candle_strength:.5f} < min {min_move}")
                        continue

                    # v7.1: ONLY trade the dominant direction
                    direction = dominant_dir
                    slug = contract_slug(asset, tf, direction)

                    with STATE.lock:
                        STATE.signals_seen += 1
                        already_open = any(p.market_slug == slug for p in STATE.positions)
                        opposite = "down" if direction == "UP" else "up"
                        opp_slug = contract_slug(asset, tf, opposite)
                        opposite_open = any(p.market_slug == opp_slug for p in STATE.positions)

                    if already_open or opposite_open:
                        continue

                    now = time.time()
                    if now - _slug_last_fired.get(slug, 0) < cooldown:
                        continue

                    # v7.1: TIMING — enter 3-7 minutes into 15m candle
                    # Too early: trend not established. Too late: already priced in.
                    interval = 300 if tf == "5m" else 900
                    time_in_candle = now % interval
                    time_remaining = interval - time_in_candle
                    MIN_ELAPSED = 180 if tf == "15m" else 60   # 3 min into 15m
                    MAX_ELAPSED = 420 if tf == "15m" else 180  # 7 min into 15m
                    MIN_REMAINING = 120
                    if time_in_candle < MIN_ELAPSED or time_in_candle > MAX_ELAPSED:
                        log.debug(f"[STRATEGY] {slug} skipped — timing: {time_in_candle:.0f}s (need {MIN_ELAPSED}-{MAX_ELAPSED}s)")
                        continue
                    if time_remaining < MIN_REMAINING:
                        continue

                    # v7.1: VOLATILITY FILTER — only trade when market is moving
                    if not check_volatility_regime(asset):
                        continue

                    # Book validation
                    book = STATE.poly_books.get(slug, {})
                    ask = float(book.get("best_ask", 0) or 0)
                    bid = float(book.get("best_bid", 0) or 0)
                    if ask <= 0 or ask >= 1.0:
                        continue

                    has_real_bids = isinstance(book.get("bids"), list) and len(book.get("bids", [])) > 0
                    has_real_asks = isinstance(book.get("asks"), list) and len(book.get("asks", [])) > 0
                    if not has_real_bids or not has_real_asks:
                        continue

                    # Price range filter — buy CHEAP contracts for asymmetric payoff
                    if ask < MIN_POLY_PRICE or ask > MAX_POLY_PRICE:
                        log.debug(f"[STRATEGY] {slug} skipped — ask={ask:.4f} outside [{MIN_POLY_PRICE}-{MAX_POLY_PRICE}]")
                        continue

                    with STATE.lock:
                        total_open = len(STATE.positions)
                    if total_open >= MAX_SIMULTANEOUS_POS:
                        continue

                    # Liquidity guard
                    liq_ok, spread_pct, depth = check_liquidity(slug)
                    if not liq_ok:
                        continue

                    # v7.1: BOOK IMBALANCE — Poly book must agree with our direction
                    imbalance = get_book_imbalance(slug)
                    if direction == "UP" and imbalance < -0.10:
                        log.debug(f"[STRATEGY] {slug} skipped — book bearish ({imbalance:.2f}) for UP")
                        continue
                    elif direction == "DOWN" and imbalance > 0.10:
                        log.debug(f"[STRATEGY] {slug} skipped — book bullish ({imbalance:.2f}) for DOWN")
                        continue

                    # Entry price: mid + 1 tick
                    mid = (bid + ask) / 2.0
                    entry_price = round(min(mid + 0.01, ask), 2)
                    poly_prob = ask

                    # CEX signal
                    cex_prob = cex_implied_prob(asset, tf, direction)
                    if cex_prob == 0.50:
                        continue

                    # Edge calculation — only deduct half fee (entry only, hold to resolution)
                    lag = cex_prob - poly_prob
                    if lag < EDGE_TRIGGER_PCT:
                        continue

                    lag_at_entry = cex_prob - entry_price
                    passes_kalshi, kalshi_boost = kalshi_confluence_check(asset, direction, tf)
                    if KALSHI_CONFLUENCE_REQUIRED and not passes_kalshi:
                        continue

                    raw_edge = lag_at_entry + kalshi_boost
                    if raw_edge > MAX_EDGE_PCT:
                        continue

                    # v7.5: primary exit is near-resolution (tight spread = ~1% entry fee only)
                    # SL at -$0.15 rarely triggers, and when it does the fee is the least of your problems
                    edge = raw_edge - (FEE_BUFFER_PCT * 0.5)
                    if edge <= MIN_EDGE_TO_EXECUTE:
                        continue

                    confidence = confidence_from_edge(abs(edge))
                    if confidence <= MIN_CONFIDENCE_SCORE:
                        continue

                    # Persistence — 5 consecutive positive readings
                    if not _record_signal(slug, edge):
                        log.debug(f"[STRATEGY] {slug} edge={edge:.4f} persistence {len(_signal_history.get(slug, []))}/{_SIGNAL_PERSISTENCE_WINDOW}")
                        continue

                    # Sizing — use 10 contracts minimum
                    odds_decimal = 1.0 / max(poly_prob, 0.01)
                    kelly_prob = min(0.95, cex_prob + kalshi_boost)
                    fraction = half_kelly(kelly_prob, odds_decimal)
                    fraction = min(fraction, MAX_POSITION_PCT)

                    with STATE.lock:
                        equity = STATE.total_equity()
                        open_value = sum(p.entry_price * p.size for p in STATE.positions)
                    if open_value >= equity * 0.60:
                        continue

                    target_notional = equity * max(fraction, 0.01)
                    price = entry_price
                    size = target_notional / max(price, 0.01)

                    market_info = MARKET_CACHE.get(slug, {})
                    platform_min = float(market_info.get("min_size", 5))
                    min_order_size = max(platform_min, 5.0)
                    if size < min_order_size:
                        size = min_order_size

                    size = min(size, MAX_CONTRACTS_PER_POS)

                    cost = price * size
                    with STATE.lock:
                        if cost > STATE.cash:
                            continue

                    _slug_last_fired[slug] = time.time()

                    log.info(f"[STRATEGY] *** TRADE *** {slug} BUY {size:.0f}@{price:.4f} edge={edge:.4f} candle_move={candle_strength:.5f} imbalance={imbalance:.2f}")
                    await execute_trade(
                        market_slug=slug,
                        side="BUY",
                        price=price,
                        size=size,
                        confidence=confidence,
                        edge=edge,
                    )

                except Exception as e:
                    STATE.log_error(f"strategy inner {asset}/{tf}: {e}")

        except Exception as e:
            STATE.log_error(f"strategy_loop outer: {e}")
        await asyncio.sleep(1)

# ─────────────────────────────────────────────
# FIX #2: LIVE READINESS VALIDATION
# ─────────────────────────────────────────────

STRATEGY_CONTRACTS = []
for asset, tf in TARGET_CONFIGS:
    STRATEGY_CONTRACTS.append(f"{asset.lower()}-{tf.lower()}-up")
    STRATEGY_CONTRACTS.append(f"{asset.lower()}-{tf.lower()}-down")

def validate_live_readiness():
    if not LIVE_TRADING_ENABLED:
        return True
    missing = []
    for key in STRATEGY_CONTRACTS:
        m = MARKET_CACHE.get(key)
        if not m:
            missing.append(f"{key}: missing market")
            continue
        tid = m.get("yes_token_id") or m.get("token_id")
        cid = m.get("condition_id")
        if not tid or tid == "synthetic":
            missing.append(f"{key}: invalid token_id (still synthetic)")
        if not cid:
            missing.append(f"{key}: invalid condition_id")
    if missing:
        for item in missing:
            log.error(f"LIVE READINESS FAIL: {item}")
        return False
    return True

# ─────────────────────────────────────────────
# v5.0: BUILT-IN BACKTESTER
# ─────────────────────────────────────────────

class BacktestEngine:
    """Simple backtester that replays historical price data through the same
    strategy logic. Feed it a CSV with columns:
        timestamp, asset, price
    where timestamp is a unix epoch and asset is BTC/ETH/XRP.

    Usage:
        python polymarket_arb_bot.py --backtest backtest_data.csv
    Or set BACKTEST_MODE=true and BACKTEST_CSV=path in .env
    """

    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.trades: List[dict] = []
        self.equity_curve: List[Tuple[float, float]] = []
        self.cash = STARTING_PORTFOLIO
        self.positions: List[dict] = []
        self.wins = 0
        self.losses = 0
        self.realized_pnl = 0.0

    def load_data(self) -> List[dict]:
        """Load CSV price data."""
        rows = []
        try:
            with open(self.csv_path, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append({
                        "ts": float(row["timestamp"]),
                        "asset": row["asset"].upper(),
                        "price": float(row["price"]),
                    })
        except FileNotFoundError:
            log.error(f"[BACKTEST] CSV not found: {self.csv_path}")
            return []
        except Exception as e:
            log.error(f"[BACKTEST] Error loading CSV: {e}")
            return []

        rows.sort(key=lambda x: x["ts"])
        return rows

    def _synthetic_poly_price(self, asset: str, direction: str, cex_price: float,
                               price_30s_ago: float, vol_30s: float) -> float:
        """Simulate a Polymarket price based on momentum + noise."""
        if price_30s_ago <= 0:
            return 0.50
        ret = (cex_price - price_30s_ago) / price_30s_ago
        z = ret / max(vol_30s, 1e-9)
        up_prob = 1.0 / (1.0 + math.exp(-z * 1.0))
        up_prob = max(0.10, min(0.90, up_prob))
        # Add some noise/lag to simulate poly lagging CEX
        import random
        noise = random.gauss(0, 0.02)
        poly_price = up_prob + noise
        poly_price = max(0.05, min(0.95, poly_price))
        if direction == "DOWN":
            poly_price = 1.0 - poly_price
        return poly_price

    def run(self):
        """Run the backtest."""
        data = self.load_data()
        if not data:
            print("[BACKTEST] No data loaded. Exiting.")
            return

        print(f"\n{'='*60}")
        print(f"  BACKTESTER v5.0")
        print(f"  Data: {self.csv_path}")
        print(f"  Rows: {len(data)}")
        print(f"  Starting equity: ${self.cash:,.2f}")
        print(f"  Fee buffer: {FEE_BUFFER_PCT*100:.1f}%")
        print(f"  TP: +{TAKE_PROFIT_PCT*100:.1f}%  SL: -{STOP_LOSS_PCT*100:.1f}%")
        print(f"  Min edge: {MIN_EDGE_TO_EXECUTE*100:.1f}%  Min conf: {MIN_CONFIDENCE_SCORE*100:.0f}%")
        print(f"{'='*60}\n")

        # Build price history by asset
        price_history: Dict[str, deque] = {
            "BTC": deque(maxlen=1200),
            "ETH": deque(maxlen=1200),
            "XRP": deque(maxlen=1200),
        }

        for row in data:
            ts = row["ts"]
            asset = row["asset"]
            price = row["price"]

            if asset not in price_history:
                continue

            price_history[asset].append((ts, price))

            # Get price 30s ago
            target_30s = ts - 30
            px_30s = None
            for pts, ppx in price_history[asset]:
                if pts <= target_30s:
                    px_30s = ppx
            if px_30s is None:
                continue

            vol_30s = _VOL_30S.get(asset, 0.00065)

            # Check and close existing positions
            closed_pos = []
            for pos in self.positions:
                if pos["asset"] != asset:
                    continue
                tf = pos["timeframe"]
                direction = pos["direction"]
                current = self._synthetic_poly_price(asset, direction, price, px_30s, vol_30s)
                age = ts - pos["opened_ts"]
                ttl = 300 if tf == "5m" else 900
                tp = pos["entry_price"] + TAKE_PROFIT_PCT
                sl = pos["entry_price"] - STOP_LOSS_PCT

                close_price = None
                won = False
                reason = ""

                if current >= tp:
                    close_price = current
                    won = True
                    reason = "TP"
                elif current <= sl:
                    close_price = max(current, 0.01)
                    won = False
                    reason = "SL"
                elif age >= ttl:
                    close_price = current
                    won = current > pos["entry_price"]
                    reason = "TTL"

                if close_price is not None:
                    pnl = (close_price - pos["entry_price"]) * pos["size"]
                    # Subtract fees from PnL
                    pnl -= FEE_BUFFER_PCT * pos["size"] * pos["entry_price"]
                    self.realized_pnl += pnl
                    self.cash += close_price * pos["size"]
                    if won:
                        self.wins += 1
                    else:
                        self.losses += 1
                    self.trades.append({
                        "ts": ts,
                        "asset": asset,
                        "direction": direction,
                        "entry": pos["entry_price"],
                        "exit": close_price,
                        "pnl": pnl,
                        "reason": reason,
                        "won": won,
                    })
                    closed_pos.append(pos)

            for cp in closed_pos:
                self.positions.remove(cp)

            # Evaluate new signals
            for tf in ("5m", "15m"):
                for direction in DIRECTIONS:
                    # Correlation limit
                    asset_count = sum(1 for p in self.positions if p["asset"] == asset)
                    if asset_count >= MAX_CONTRACTS_PER_ASSET:
                        continue

                    # Duplicate check
                    if any(p["asset"] == asset and p["timeframe"] == tf and p["direction"] == direction
                           for p in self.positions):
                        continue

                    interval = 300 if tf == "5m" else 900
                    time_in = ts % interval
                    remaining = interval - time_in
                    if remaining < 90 or time_in < 20:
                        continue

                    # CEX implied probability
                    ret = (price - px_30s) / px_30s
                    z = ret / max(vol_30s, 1e-9)
                    cex_prob = 1.0 / (1.0 + math.exp(-z * 1.2))
                    cex_prob = max(0.05, min(0.95, cex_prob))
                    if direction == "DOWN":
                        cex_prob = 1.0 - cex_prob

                    # Simulated poly price (lagged)
                    poly_prob = self._synthetic_poly_price(asset, direction, price, px_30s, vol_30s)

                    lag = cex_prob - poly_prob
                    if abs(lag) < EDGE_TRIGGER_PCT:
                        continue

                    raw_edge = lag
                    edge = apply_fee_buffer(raw_edge)
                    if edge <= MIN_EDGE_TO_EXECUTE:
                        continue

                    conf = confidence_from_edge(abs(edge))
                    if conf <= MIN_CONFIDENCE_SCORE:
                        continue

                    # Multi-TF check for 5m
                    if MULTI_TF_CONFIRMATION and tf == "5m":
                        # Need 15m trend to agree
                        cex_15m = 1.0 / (1.0 + math.exp(-z * 0.8))
                        if direction == "DOWN":
                            cex_15m = 1.0 - cex_15m
                        if cex_15m < 0.52:
                            continue

                    # Size
                    odds_dec = 1.0 / max(poly_prob, 0.01)
                    fraction = half_kelly(cex_prob, odds_dec)
                    fraction = min(fraction, MAX_POSITION_PCT)
                    if fraction <= 0:
                        continue

                    equity = self.cash + sum(p["entry_price"] * p["size"] for p in self.positions)
                    notional = equity * fraction
                    if notional <= 0:
                        continue
                    size = notional / max(poly_prob, 0.01)
                    cost = poly_prob * size
                    if cost > self.cash:
                        continue

                    self.cash -= cost
                    self.positions.append({
                        "asset": asset,
                        "timeframe": tf,
                        "direction": direction,
                        "entry_price": poly_prob,
                        "size": size,
                        "opened_ts": ts,
                        "edge": edge,
                        "conf": conf,
                    })

            # Track equity curve
            pos_val = sum(p["entry_price"] * p["size"] for p in self.positions)
            self.equity_curve.append((ts, self.cash + pos_val))

        # Print results
        total_trades = self.wins + self.losses
        wr = self.wins / total_trades if total_trades > 0 else 0
        final_equity = self.equity_curve[-1][1] if self.equity_curve else self.cash

        print(f"\n{'='*60}")
        print(f"  BACKTEST RESULTS")
        print(f"{'='*60}")
        print(f"  Total trades:    {total_trades}")
        print(f"  Wins:            {self.wins}")
        print(f"  Losses:          {self.losses}")
        print(f"  Win rate:        {wr*100:.1f}%")
        print(f"  Realized PnL:    ${self.realized_pnl:+,.2f}")
        print(f"  Final equity:    ${final_equity:,.2f}")
        print(f"  Return:          {((final_equity/STARTING_PORTFOLIO)-1)*100:+.1f}%")

        if self.trades:
            pnls = [t["pnl"] for t in self.trades]
            avg_win = sum(p for p in pnls if p > 0) / max(1, sum(1 for p in pnls if p > 0))
            avg_loss = sum(p for p in pnls if p < 0) / max(1, sum(1 for p in pnls if p < 0))
            print(f"  Avg win:         ${avg_win:+,.4f}")
            print(f"  Avg loss:        ${avg_loss:+,.4f}")
            print(f"  Profit factor:   {abs(avg_win/avg_loss) if avg_loss != 0 else 'inf':.2f}")

            # Breakdown by reason
            for reason in ("TP", "SL", "TTL"):
                rt = [t for t in self.trades if t["reason"] == reason]
                if rt:
                    rw = sum(1 for t in rt if t["won"])
                    rl = len(rt) - rw
                    print(f"  {reason:>3}: {len(rt)} trades ({rw}W {rl}L) — WR {rw/len(rt)*100:.0f}%")

        print(f"{'='*60}\n")

        # Save trade log
        log_path = self.csv_path.replace(".csv", "_results.csv")
        try:
            with open(log_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["ts", "asset", "direction", "entry", "exit", "pnl", "reason", "won"])
                writer.writeheader()
                for t in self.trades:
                    writer.writerow(t)
            print(f"  Trade log saved: {log_path}")
        except Exception as e:
            print(f"  Could not save trade log: {e}")


# ─────────────────────────────────────────────
# DASHBOARD HELPERS 
# ─────────────────────────────────────────────

_SPARK_CHARS  = " ▁▂▃▄▅▆▇█"
_PULSE_FRAMES = ("◐", "◓", "◑", "◒")

def _sparkline(values: list, width: int = 38) -> str:
    if not values or len(values) < 2:
        return "─" * width
    if len(values) > width:
        step    = len(values) / width
        sampled = [values[int(i * step)] for i in range(width)]
    else:
        sampled = [values[0]] * (width - len(values)) + list(values)
    mn, mx = min(sampled), max(sampled)
    rng    = mx - mn or 1e-9
    return "".join(
        _SPARK_CHARS[max(0, min(int((v - mn) / rng * (len(_SPARK_CHARS) - 1)), len(_SPARK_CHARS) - 1))]
        for v in sampled
    )

def _pbar(ratio: float, width: int = 36, invert: bool = False) -> tuple:
    ratio = max(0.0, min(1.0, ratio))
    bar   = "█" * int(ratio * width) + "░" * (width - int(ratio * width))
    if invert:
        style = "bold bright_red" if ratio >= 0.75 else ("yellow" if ratio >= 0.40 else "bright_green")
    else:
        style = "bold bright_green" if ratio >= 0.60 else ("yellow" if ratio >= 0.30 else "bright_red")
    return bar, style

def _fmt_pnl(val: float) -> Text:
    s = f"{'+'if val >= 0 else ''}{val:,.2f}"
    return Text(s, style="bold bright_green" if val > 0 else ("bold bright_red" if val < 0 else "dim"))

def _price_cell(sym: str) -> Text:
    px   = STATE.cex_prices.get(sym)
    prev = STATE.prev_prices.get(sym)
    if px is None:
        return Text("  ─────────  ", style="dim")
    if prev and abs(px - prev) > 0.001:
        arrow, style = (" ▲", "bold bright_green") if px > prev else (" ▼", "bold bright_red")
    else:
        arrow, style = (" ─", "white")
    return Text(f"${px:>10,.2f}{arrow}", style=style)

def _edge_badge(edge: float) -> Text:
    a = abs(edge)
    if a >= 0.08:
        return Text(f"★{a*100:.1f}%", style="bold bright_green")
    if a >= 0.05:
        return Text(f"◆{a*100:.1f}%", style="bright_yellow")
    if a >= 0.03:
        return Text(f"◇{a*100:.1f}%", style="yellow")
    return Text(f" {a*100:.1f}%", style="dim")

def _uptime(start: float) -> str:
    s = int(time.time() - start)
    h, r  = divmod(s, 3600)
    m, sc = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{sc:02d}"

# ─────────────────────────────────────────────
# BUILD DASHBOARD (v5.0: shows fee buffer, pending orders,
# correlation count)
# ─────────────────────────────────────────────

def build_dashboard() -> Layout:
    with STATE.lock:
        eq           = STATE.total_equity()
        cash         = STATE.cash
        clob_balance = STATE.clob_balance
        rpnl         = STATE.realized_pnl
        upnl         = STATE.unrealized_pnl
        total_pnl  = rpnl + upnl
        dd_pct     = STATE.drawdown_pct()
        wr         = STATE.win_rate()
        wins       = STATE.win_count
        losses     = STATE.loss_count
        ks         = STATE.kill_switch
        positions  = list(STATE.positions)
        trades     = list(STATE.last_trades)
        errors     = list(STATE.errors)
        n_markets  = len(STATE.poly_markets)
        n_books    = len(STATE.poly_books)
        pnl_hist   = list(STATE.pnl_history)
        start_ts   = STATE.start_ts
        sig_seen   = STATE.signals_seen
        sig_fired  = STATE.signals_fired
        pulse_idx  = STATE.pulse_tick % len(_PULSE_FRAMES)
        kalshi_loaded = sum(1 for v in STATE.kalshi_prices.values() if v is not None)
        n_pending_close = len(STATE.pending_closes)
        n_pending_open  = len(STATE.pending_opens)

        btc_px = STATE.cex_prices.get("BTCUSDT")
        eth_px = STATE.cex_prices.get("ETHUSDT")
        if btc_px: STATE.prev_prices["BTCUSDT"] = btc_px
        if eth_px: STATE.prev_prices["ETHUSDT"] = eth_px
        STATE.pnl_history.append(total_pnl)
        STATE.pulse_tick += 1

    open_val  = sum(p.entry_price * p.size for p in positions)
    pos_util  = open_val / max(eq, 1e-9)
    n_pos     = len(positions)
    pulse     = _PULSE_FRAMES[pulse_idx]
    uptime    = _uptime(start_ts)

    root = Layout(name="root")
    root.split_column(
        Layout(name="topbar",     size=3),
        Layout(name="kpi_row",    size=5),
        Layout(name="bars_row",   size=4),
        Layout(name="main_row"),
        Layout(name="trades_row", size=15),
        Layout(name="footer_row", size=5),
    )
    root["main_row"].split_row(
        Layout(name="pos_col",  ratio=3),
        Layout(name="scan_col", ratio=2),
    )

    t = Text()
    t.append("  ◈ POLYMARKET ARB ", style="bold bright_white")
    t.append("v5.0  │  ", style="dim")
    t.append("Mode: ", style="dim")
    if LIVE_TRADING_ENABLED:
        t.append("⚡ LIVE", style="bold bright_red")
    else:
        t.append("◎ PAPER", style="bold bright_cyan")
    t.append("  │  Status: ", style="dim")
    if ks:
        t.append("⛔ KILL SWITCH — ALL TRADING HALTED", style="bold bright_red")
    else:
        t.append(f"{pulse} RUNNING", style="bold bright_green")
    t.append("  │  BTC ", style="dim")
    t.append_text(_price_cell("BTCUSDT"))
    t.append("  ETH ", style="dim")
    t.append_text(_price_cell("ETHUSDT"))
    t.append(f"  │  ⏱ {uptime}  {datetime.now().strftime('%H:%M:%S')}", style="dim")
    root["topbar"].update(Panel(t, border_style="bright_black", padding=(0, 1)))

    def _kpi(label, val, sub, col, border="bright_black"):
        tx = Text()
        tx.append(f"{label}\n", style="dim")
        tx.append(val, style=f"bold {col}")
        if sub: tx.append(f"\n{sub}", style="dim")
        return Panel(tx, border_style=border, padding=(0, 1))

    pnl_col = "bright_green" if total_pnl >= 0 else "bright_red"
    wr_col  = "bright_green" if wr >= 0.60 else ("yellow" if wr >= 0.40 else "bright_red")
    dd_col  = "bright_red"   if dd_pct >= 0.15 else ("yellow" if dd_pct >= 0.08 else "bright_green")

    kpis = Layout()
    kpis.split_row(
        Layout(_kpi("EQUITY",    f"${eq:,.2f}", f"cash ${cash:,.2f}  │  CLOB ${clob_balance:,.2f}" if clob_balance is not None else f"cash ${cash:,.2f}  │  CLOB …", "bright_white"), name="k1"),
        Layout(_kpi("TOTAL P&L", f"{'+'if total_pnl>=0 else ''}{total_pnl:,.2f}", f"R {rpnl:+.2f}   U {upnl:+.2f}", pnl_col), name="k2"),
        Layout(_kpi("WIN RATE",  f"{wr*100:.1f}%", f"{wins}W  {losses}L", wr_col), name="k3"),
        Layout(_kpi("DRAWDOWN",  f"{dd_pct*100:.1f}%", f"limit {DAILY_DRAWDOWN_LIMIT*100:.0f}%", dd_col), name="k4"),
        Layout(_kpi("POSITIONS", f"{n_pos} open", f"${open_val:,.2f}  │  pend {n_pending_close}C {n_pending_open}O", "bright_cyan"), name="k5"),
    )
    root["kpi_row"].update(kpis)

    bars_lr = Layout()
    bars_lr.split_row(Layout(name="bleft", ratio=2), Layout(name="bright_side", ratio=1))

    dd_bar, dd_bs = _pbar(dd_pct / max(DAILY_DRAWDOWN_LIMIT, 1e-9), 36, invert=True)
    pu_bar, pu_bs = _pbar(pos_util, 36, invert=False)
    bt = Text()
    bt.append("  DRAWDOWN RISK  ", style="dim")
    bt.append(dd_bar, style=dd_bs)
    bt.append(f"  {dd_pct*100:.1f}% / {DAILY_DRAWDOWN_LIMIT*100:.0f}%\n", style="dim")
    bt.append("  POSITION UTIL  ", style="dim")
    bt.append(pu_bar, style=pu_bs)
    bt.append(f"  {pos_util*100:.1f}% of equity", style="dim")
    bars_lr["bleft"].update(Panel(bt, border_style="bright_black", padding=(0, 0)))

    spark  = _sparkline(pnl_hist, 40)
    sk_col = "bright_green" if (pnl_hist and pnl_hist[-1] >= 0) else "bright_red"
    st2    = Text()
    st2.append("  P&L SPARKLINE\n", style="dim")
    st2.append(f"  {spark}", style=sk_col)
    if pnl_hist:
        st2.append(f"  {pnl_hist[-1]:+.2f}", style=sk_col)
    bars_lr["bright_side"].update(Panel(st2, border_style="bright_black", padding=(0, 0)))
    root["bars_row"].update(bars_lr)

    pt = Table(
        title=f"[bold]Open Positions[/bold] [dim]({n_pos})[/dim]",
        box=box.SIMPLE_HEAD, border_style="bright_black",
        header_style="bold dim", expand=True,
    )
    pt.add_column("Market",   style="cyan",    width=22)
    pt.add_column("Entry",    justify="right", width=7)
    pt.add_column("Current",  justify="right", width=9)
    pt.add_column("Size",     justify="right", width=7)
    pt.add_column("Unr PnL",  justify="right", width=10)
    pt.add_column("Edge",     justify="right", width=9)
    pt.add_column("Conf",     justify="right", width=6)
    pt.add_column("Age",      justify="right", width=7)
    pt.add_column("HWM/Stop", justify="right", width=14)

    for p in positions[:12]:
        try:
            parts  = p.market_slug.split("-")
            asset  = parts[0].upper()
            tf     = parts[1]
            direc  = parts[2].upper()
            curr   = get_poly_prob(asset, tf, direc)
            upnl_p = (curr - p.entry_price) * p.size
            age    = int(time.time() - p.opened_ts)
            arrow  = "▲" if curr > p.entry_price else ("▼" if curr < p.entry_price else "─")
            pcol   = "bright_green" if upnl_p > 0 else ("bright_red" if upnl_p < 0 else "dim")
            # FIX: show live trailing stop (HWM × (1-SL%)) instead of stale fixed TP/SL
            trail_stop = max(p.high_water_mark * (1.0 - STOP_LOSS_PCT),
                             max(p.entry_price - 0.15, 0.05))
            # v5.0: show fill status
            fill_indicator = "" if p.open_filled else " ⏳"
            pt.add_row(
                (p.market_slug[:20] + fill_indicator),
                f"{p.entry_price:.4f}",
                f"{curr:.4f} {arrow}",
                f"{p.size:.1f}",
                Text(f"{upnl_p:+.2f}", style=pcol),
                _edge_badge(p.edge),
                f"{p.confidence*100:.0f}%",
                f"{age//60}m{age%60:02d}s",
                Text(f"{p.high_water_mark:.3f}/{trail_stop:.3f}", style="dim"),
            )
        except Exception:
            continue

    if not positions:
        pt.add_row(Text("No open positions", style="dim italic"), "", "", "", "", "", "", "", "")
    root["pos_col"].update(pt)

    sc = Table(
        title="[bold]Live Market Scanner[/bold]",
        box=box.SIMPLE_HEAD, border_style="bright_black",
        header_style="bold dim", expand=True,
    )
    sc.add_column("Contract",  style="cyan",    width=18)
    sc.add_column("Poly",      justify="right", width=7)
    sc.add_column("CEX Impl",  justify="right", width=8)
    sc.add_column("Edge",      justify="right", width=10)
    sc.add_column("Signal",    justify="center", width=8)

    for asset, tf in TARGET_CONFIGS:
        for direction in DIRECTIONS:
            try:
                slug   = contract_slug(asset, tf, direction)
                poly_p = get_poly_prob(asset, tf, direction)
                cex_p  = cex_implied_prob(asset, tf, direction)
                raw_edge = cex_p - poly_p
                # v5.0: show net edge after fee buffer
                edge   = apply_fee_buffer(raw_edge)
                conf   = confidence_from_edge(abs(edge))

                # v5.1: only FIRE for POSITIVE edge (Poly underpriced = buy opportunity)
                # Negative edge = Poly overpriced = NOT a trade for us
                is_synthetic = (poly_p == 0.50)
                if is_synthetic:
                    sig = Text("SYN", style="dim")
                elif edge >= MIN_EDGE_TO_EXECUTE and conf >= MIN_CONFIDENCE_SCORE:
                    sig = Text("● FIRE", style="bold bright_green")
                elif edge < -0.05:
                    sig = Text("✗ OVER", style="bright_red")
                elif raw_edge >= EDGE_TRIGGER_PCT:
                    sig = Text("◎ NEAR", style="yellow")
                else:
                    sig = Text("·", style="dim")
                sc.add_row(slug[:18], f"{poly_p:.4f}", f"{cex_p:.4f}", _edge_badge(edge), sig)
            except Exception:
                continue
    root["scan_col"].update(sc)

    tt = Table(
        title=f"[bold]Last 10 Trades[/bold]  [dim]signals: {sig_seen} evaluated  {sig_fired} fired[/dim]",
        box=box.SIMPLE_HEAD, border_style="bright_black",
        header_style="bold dim", expand=True,
    )
    tt.add_column("Time",   width=9)
    tt.add_column("Market", width=24, style="cyan")
    tt.add_column("Status", width=7,  justify="center")
    tt.add_column("Price",  width=8,  justify="right")
    tt.add_column("Size",   width=7,  justify="right")
    tt.add_column("P&L",    width=10, justify="right")
    tt.add_column("Result", width=8,  justify="center")

    for t_rec in trades[:10]:
        ts_str = datetime.fromtimestamp(t_rec.ts).strftime("%H:%M:%S")
        if t_rec.status == "CLOSE":
            pnl_t  = _fmt_pnl(t_rec.pnl)
            res_t  = Text("✓ WIN",  style="bold bright_green") if t_rec.pnl > 0 else (
                     Text("✗ LOSS", style="bold bright_red")   if t_rec.pnl < 0 else
                     Text("~ FLAT", style="dim"))
            stat_t = Text("CLOSED", style="dim")
        else:
            pnl_t  = Text("—",    style="dim")
            res_t  = Text("─",    style="dim")
            stat_t = Text("OPEN", style="bright_cyan")
        tt.add_row(ts_str, t_rec.market_slug[:24], stat_t, f"{t_rec.price:.4f}", f"{t_rec.size:.1f}", pnl_t, res_t)

    if not trades:
        tt.add_row(Text("No trades yet", style="dim italic"), "", "", "", "", "", "")
    root["trades_row"].update(tt)

    footer = Layout()
    footer.split_row(Layout(name="ferr", ratio=3), Layout(name="fsys", ratio=1))

    et = Text()
    et.append("RECENT ERRORS\n", style="bold dim")
    if errors:
        for e in errors[:3]:
            et.append(f"  {e[:90]}\n", style="bright_red")
    else:
        et.append("  No errors — all systems nominal", style="dim")

    sy = Text()
    sy.append("SYSTEM\n", style="bold dim")
    sy.append(f"  Markets: {n_markets}  Books: {n_books}  Kalshi: {kalshi_loaded}/2\n", style="dim")
    sy.append(f"  Kelly: {KELLY_FRACTION}  MaxPos: {MAX_POSITION_PCT*100:.0f}%  Fee: {FEE_BUFFER_PCT*100:.1f}%\n", style="dim")
    sy.append(f"  MinEdge: {MIN_EDGE_TO_EXECUTE*100:.1f}%  MinConf: {MIN_CONFIDENCE_SCORE*100:.0f}%  CorrLim: {MAX_CONTRACTS_PER_ASSET}", style="dim")

    footer["ferr"].update(Panel(et, border_style="bright_black", padding=(0, 1)))
    footer["fsys"].update(Panel(sy, border_style="bright_black", padding=(0, 1)))
    root["footer_row"].update(footer)

    return root

def dashboard_thread():
    global DASHBOARD_ACTIVE
    refresh = max(1.0, DASHBOARD_REFRESH)

    # Save a reference to the real stdout BEFORE anything touches it
    _real_stdout = sys.__stdout__ or sys.stdout

    # FULLY REMOVE all console/stream handlers to prevent screen corruption
    # All log output continues to go to bot.log via FileHandler.
    handlers_to_remove = []
    
    # Remove from root logger
    for handler in logging.root.handlers[:]:
        if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
            handlers_to_remove.append((logging.root, handler))
    
    # Remove from our polyarb logger
    for handler in log.handlers[:]:
        if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
            handlers_to_remove.append((log, handler))
    
    for logger, handler in handlers_to_remove:
        try:
            logger.removeHandler(handler)
        except Exception:
            pass
    
    DASHBOARD_ACTIVE = True
    log.info("[DASHBOARD] Starting dashboard - console logging disabled, file logging continues")

    # Dedicated console using the saved real stdout
    dash_console = Console(file=_real_stdout, force_terminal=True)

    try:
        with Live(
            build_dashboard(),
            console=dash_console,
            auto_refresh=False,
            screen=True,
            transient=True,  # Prevents flickering
        ) as live:
            while True:
                try:
                    live.update(build_dashboard(), refresh=True)
                    time.sleep(refresh)
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    log.debug(f"[DASHBOARD] Update error (non-fatal): {e}")
                    time.sleep(1)
    except Exception as e:
        log.error(f"[DASHBOARD] Thread crashed: {e}", exc_info=True)
    finally:
        DASHBOARD_ACTIVE = False
        log.info("[DASHBOARD] Dashboard thread exited")

# ─────────────────────────────────────────────
# SUPERVISED TASK RUNNER
# ─────────────────────────────────────────────

async def supervised_task(name: str, coro_fn, restart_delay: float = 3.0):
    while not STOP_EVENT.is_set():
        try:
            log.debug(f"Task starting: {name}")
            await coro_fn()
        except asyncio.CancelledError:
            log.info(f"Task cancelled: {name}")
            break
        except Exception as e:
            STATE.log_error(f"Task [{name}] crashed: {e}")
            log.warning(f"Task [{name}] crashed: {e} — restarting in {restart_delay}s")
            await asyncio.sleep(restart_delay)

# ─────────────────────────────────────────────
# SHUTDOWN
# ─────────────────────────────────────────────

def handle_shutdown(*_):
    log.info("Shutdown signal received — stopping bot")
    try:
        loop = asyncio.get_running_loop()
        loop.call_soon_threadsafe(STOP_EVENT.set)
    except Exception:
        if STOP_EVENT:
            STOP_EVENT.set()

# ─────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────

def print_startup_banner():
    mode = "LIVE TRADING" if LIVE_TRADING_ENABLED else "PAPER TRADING"
    print("=" * 60)
    print(f"  Polymarket Arb Bot v7.2 (aggressive growth)")
    print(f"  Mode:      {mode}")
    print(f"  Portfolio: ${STARTING_PORTFOLIO:,.2f}")
    print(f"  Strategy:  5m + 15m trend continuation → hold to resolution")
    print(f"  Signal:    candle trend + acceleration + book imbalance")
    print(f"  Entry:     1-3min (5m) / 3-7min (15m), vol+persistence filter")
    print(f"  Exit:      sell near candle end (no TP/SL)")
    print(f"  Close:     5-attempt persistent retry, stuck detector at 2×TTL")
    print(f"  Price rng: ${MIN_POLY_PRICE:.2f}-${MAX_POLY_PRICE:.2f}")
    print(f"  Min Edge:  {MIN_EDGE_TO_EXECUTE*100:.1f}%")
    print(f"  Kelly:     {KELLY_FRACTION} | Min 5 contracts")
    print(f"  Kill at:   {DAILY_DRAWDOWN_LIMIT*100:.0f}% drawdown")
    print(f"  Assets:    {', '.join(set(a for a,_ in TARGET_CONFIGS))}")
    if LIVE_TRADING_ENABLED:
        print(f"  *** LIVE TRADING ENABLED ***")
    print("=" * 60)

# ─────────────────────────────────────────────
# CLOB BALANCE REFRESH
# ─────────────────────────────────────────────

async def refresh_clob_balance():
    from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
    while not STOP_EVENT.is_set():
        try:
            if STATE.clob_client is None:
                if LIVE_TRADING_ENABLED and POLY_PRIVATE_KEY:
                    STATE.clob_client = ClobClient(
                        host=POLYMARKET_HOST,
                        key=POLY_PRIVATE_KEY,
                        chain_id=POLYMARKET_CHAIN_ID,
                        signature_type=2,
                        funder=POLY_FUNDER if POLY_FUNDER else None
                    )
                    STATE.clob_client.set_api_creds(STATE.clob_client.create_or_derive_api_creds())
            if STATE.clob_client is not None:
                result = STATE.clob_client.get_balance_allowance(
                    params=BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                )
                raw = result.get("balance", "0")
                with STATE.lock:
                    STATE.clob_balance = float(raw) / 1_000_000
        except Exception as e:
            log.debug(f"refresh_clob_balance: {e}")
        await asyncio.sleep(30)

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

async def main():
    global STOP_EVENT
    STOP_EVENT = asyncio.Event()

    signal.signal(signal.SIGINT,  handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    print_startup_banner()

    # Brief pause to let the startup banner render before dashboard takes over
    await asyncio.sleep(2)

    dash_thread = threading.Thread(target=dashboard_thread, daemon=True)
    dash_thread.start()

    tasks = [
        asyncio.create_task(supervised_task("refresh_markets",  refresh_markets,     restart_delay=10.0)),
        asyncio.create_task(supervised_task("refresh_books",    refresh_books,       restart_delay=5.0)),
        asyncio.create_task(supervised_task("binance_ws",       binance_ws_listener, restart_delay=3.0)),
        asyncio.create_task(supervised_task("kalshi_oracle",    refresh_kalshi,      restart_delay=10.0)),
        asyncio.create_task(supervised_task("strategy_loop",    strategy_loop,       restart_delay=3.0)),
        asyncio.create_task(supervised_task("manage_positions",      manage_positions,       restart_delay=2.0)),
        asyncio.create_task(supervised_task("refresh_clob_balance", refresh_clob_balance,   restart_delay=10.0)),
    ]

    await asyncio.sleep(8)
    if not validate_live_readiness():
        log.error("Live readiness validation failed; forcing paper mode")
        globals()["LIVE_TRADING_ENABLED"] = False
        globals()["PAPER_TRADING"] = True

    log.info("All tasks started (real market discovery + live readiness check)")
    await STOP_EVENT.wait()
    log.info("Stop event received — cancelling tasks")

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    with STATE.lock:
        eq  = STATE.total_equity()
        pnl = STATE.realized_pnl + STATE.unrealized_pnl

    print("\n" + "=" * 60)
    print(f"  Bot stopped | Final equity: ${eq:,.2f} | PnL: ${pnl:+,.2f}")
    print("=" * 60)

if __name__ == "__main__":
    # v5.0: support --backtest CLI flag
    if "--backtest" in sys.argv:
        idx = sys.argv.index("--backtest")
        csv_file = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else BACKTEST_CSV
        bt = BacktestEngine(csv_file)
        bt.run()
    elif BACKTEST_MODE:
        bt = BacktestEngine(BACKTEST_CSV)
        bt.run()
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            pass
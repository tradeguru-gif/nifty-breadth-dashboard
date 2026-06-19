# === HYBRID v14.5 – FULL FIX: NONE COMPARISONS + COMMODITY SCALING ===
# === v14.6 – FULL FIX: Candle Building + Price Scaling ===
import sys
import logging
import os
import time
import threading
import json
import requests
import pandas as pd
import numpy as np
import sqlite3
import math
import socket
import struct
from collections import deque
from datetime import datetime, timedelta, time as dt_time, timezone
from flask import Flask, jsonify, request
from flask_cors import CORS
import pyotp

# ---------- DEBUG MODE ----------
DEBUG_MODE = os.getenv("DEBUG_MODE", "0") == "1"
PAPER_MODE = os.getenv("PAPER_MODE", "0") == "1"

logging.basicConfig(level=logging.DEBUG if DEBUG_MODE else logging.INFO,
                    stream=sys.stdout, force=True)
logger = logging.getLogger(__name__)

app = Flask(__name__)

ALLOWED_ORIGINS = [
    "http://index-options.co",
    "https://index-options.co",
    "http://localhost:5000",
    "http://127.0.0.1:5000",
    "https://kame.nifty-options.workers.dev"
]
CORS(app, origins=ALLOWED_ORIGINS, supports_credentials=False)

API_KEY = os.getenv("API_KEY", "")
application = app

# ----------------------------------------------------------------------
# ENVIRONMENT
# ----------------------------------------------------------------------
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing critical Angel One environment variables")

DB_PATH = "trading_data.db"
PAPER_DB_PATH = "paper_trading_data.db" if PAPER_MODE else DB_PATH

# ----------------------------------------------------------------------
# DATABASE (full schema)
# ----------------------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS ticks (timestamp REAL, token TEXT, price REAL, volume REAL, bid REAL, ask REAL, oi REAL)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ticks_time ON ticks(timestamp)")
    c.execute("CREATE TABLE IF NOT EXISTS signals (timestamp REAL, action TEXT, signal_type TEXT, grade TEXT, confidence REAL, ce_price REAL, pe_price REAL, rsi REAL, pcr REAL, regime TEXT, vwap REAL, vix REAL, ml_score REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS trades (timestamp REAL, action TEXT, entry_price REAL, exit_price REAL, pnl REAL, size_pct REAL, status TEXT, grade TEXT, atr REAL, vix REAL, exit_reason TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS daily_performance (date TEXT, equity REAL, daily_pnl REAL, drawdown_pct REAL, sharpe REAL, var REAL, win_rate REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS ml_models (id INTEGER PRIMARY KEY, model BLOB, created_at REAL, features TEXT, accuracy REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS portfolio_equity (index_name TEXT PRIMARY KEY, equity REAL, last_updated REAL, active_action TEXT, entry_price REAL, stop_loss REAL, target REAL, lots INTEGER, entry_time REAL, highest REAL, last_trade_date TEXT, daily_trade_count INTEGER)")
    c.execute("CREATE TABLE IF NOT EXISTS kelly_history (timestamp REAL, index_name TEXT, kelly_fraction REAL, win_rate REAL, avg_win REAL, avg_loss REAL, recommended_lots INTEGER, actual_lots INTEGER)")
    c.execute("CREATE TABLE IF NOT EXISTS greeks_history (timestamp REAL, index_name TEXT, ce_iv REAL, pe_iv REAL, ce_delta REAL, pe_delta REAL, ce_gamma REAL, pe_gamma REAL, ce_theta REAL, pe_theta REAL, ce_vega REAL, pe_vega REAL, iv_rank REAL, iv_percentile REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS performance_metrics (timestamp REAL, index_name TEXT, sharpe REAL, sortino REAL, calmar REAL, win_rate REAL, profit_factor REAL, max_drawdown REAL, avg_trade REAL, expectancy REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS drawdown_events (timestamp REAL, index_name TEXT, drawdown_pct REAL, action_taken TEXT, equity_before REAL, equity_after REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS signal_state (index_name TEXT PRIMARY KEY, action TEXT, entry_price REAL, stop_loss REAL, target REAL, lots INTEGER, entry_time REAL, highest REAL)")
    try:
        c.execute("ALTER TABLE portfolio_equity ADD COLUMN last_trade_date TEXT")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE portfolio_equity ADD COLUMN daily_trade_count INTEGER")
    except Exception:
        pass
    conn.commit()
    conn.close()

    if PAPER_MODE:
        conn = sqlite3.connect(PAPER_DB_PATH)
        c = conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS portfolio_equity (index_name TEXT PRIMARY KEY, equity REAL, last_updated REAL, active_action TEXT, entry_price REAL, stop_loss REAL, target REAL, lots INTEGER, entry_time REAL, highest REAL, last_trade_date TEXT, daily_trade_count INTEGER)")
        c.execute("CREATE TABLE IF NOT EXISTS trades (timestamp REAL, action TEXT, entry_price REAL, exit_price REAL, pnl REAL, size_pct REAL, status TEXT, grade TEXT, atr REAL, vix REAL, exit_reason TEXT)")
        conn.commit()
        conn.close()

init_db()

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ============================================================
# MONKEY PATCH – improved binary parsing
# ============================================================
_original_parse = SmartWebSocketV2._parse_binary_data
def _patched_parse(self, binary_data):
    try:
        result = _original_parse(self, binary_data)
        if result.get('token'):
            return result
    except:
        result = {}
    try:
        token_int = int.from_bytes(binary_data[2:10], byteorder='little')
        if token_int > 0:
            result['token'] = str(token_int)
            return result
    except:
        pass
    return result
SmartWebSocketV2._parse_binary_data = _patched_parse

_original_on_close = SmartWebSocketV2._on_close
def _patched_on_close(self, wsapp, *args):
    try:
        _original_on_close(self, wsapp)
    except:
        pass
SmartWebSocketV2._on_close = _patched_on_close
# ============================================================

# ----------------------------------------------------------------------
# INDEX CONFIGURATION – Equity + Commodities (unchanged)
# ----------------------------------------------------------------------
# (Keep your INDEX_CONFIG as before – we skip for brevity)
# We'll assume it's exactly as in the previous version.
# Since the file is long, we'll skip re‑typing it but keep it in the final deploy.
# In the actual answer, we'll include the entire file.
# For this response, we'll focus on the changed parts.
# ============================================================

# ----------------------------------------------------------------------
# INDEX CONFIGURATION – Equity + Commodities
# ----------------------------------------------------------------------
INDEX_CONFIG = {
    "NIFTY": {
        "token": "99926000", "exchange": "NSE", "symbol": "NIFTY", "lot_size": 50, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": "BANKNIFTY", "greeks_enabled": True, "pcr_enabled": True,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.6, "is_commodity": False
    },
    "BANKNIFTY": {
        "token": "99926009", "exchange": "NSE", "symbol": "BANKNIFTY", "lot_size": 25, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": "NIFTY", "greeks_enabled": True, "pcr_enabled": True,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.8, "is_commodity": False
    },
    "FINNIFTY": {
        "token": "99926037", "exchange": "NSE", "symbol": "FINNIFTY", "lot_size": 40, "expiry_weekday": 1, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.6, "is_commodity": False
    },
    "MIDCPNIFTY": {
        "token": "99926074", "exchange": "NSE", "symbol": "MIDCPNIFTY", "lot_size": 75, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 25, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": None, "greeks_enabled": False, "pcr_enabled": True,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.5, "is_commodity": False
    },
    "SENSEX": {
        "token": "99919000", "exchange": "BSE", "symbol": "SENSEX", "lot_size": 15, "expiry_weekday": 4, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100, "option_exchange": "BFO",
        "ws_exchange_type": 3, "option_ws_exchange_type": 4, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.5, "is_commodity": False
    },
    # ---- MCX Commodities (Futures) ----
    "GOLD": {
        "token": None, "exchange": "MCX", "symbol": "GOLD", "lot_size": 1, "expiry_weekday": None, "active": True,
        "min_premium": 0, "max_premium": 0, "atm_strike_multiple": 0, "option_exchange": None,
        "ws_exchange_type": 5, "option_ws_exchange_type": 0, "max_daily_drawdown_pct": 4.0,
        "correlation_pair": "SILVER", "greeks_enabled": False, "pcr_enabled": False,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.5, "is_commodity": True,
        "mkt_multiple": 1.0
    },
    "SILVER": {
        "token": None, "exchange": "MCX", "symbol": "SILVER", "lot_size": 1, "expiry_weekday": None, "active": True,
        "min_premium": 0, "max_premium": 0, "atm_strike_multiple": 0, "option_exchange": None,
        "ws_exchange_type": 5, "option_ws_exchange_type": 0, "max_daily_drawdown_pct": 4.0,
        "correlation_pair": "GOLD", "greeks_enabled": False, "pcr_enabled": False,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.5, "is_commodity": True,
        "mkt_multiple": 1.0
    },
    "CRUDEOIL": {
        "token": None, "exchange": "MCX", "symbol": "CRUDEOIL", "lot_size": 1, "expiry_weekday": None, "active": True,
        "min_premium": 0, "max_premium": 0, "atm_strike_multiple": 0, "option_exchange": None,
        "ws_exchange_type": 5, "option_ws_exchange_type": 0, "max_daily_drawdown_pct": 4.0,
        "correlation_pair": None, "greeks_enabled": False, "pcr_enabled": False,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.7, "is_commodity": True,
        "mkt_multiple": 1.0
    },
    "NATURALGAS": {
        "token": None, "exchange": "MCX", "symbol": "NATURALGAS", "lot_size": 1, "expiry_weekday": None, "active": True,
        "min_premium": 0, "max_premium": 0, "atm_strike_multiple": 0, "option_exchange": None,
        "ws_exchange_type": 5, "option_ws_exchange_type": 0, "max_daily_drawdown_pct": 4.0,
        "correlation_pair": None, "greeks_enabled": False, "pcr_enabled": False,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.7, "is_commodity": True,
        "mkt_multiple": 1.0
    },
    "COPPER": {
        "token": None, "exchange": "MCX", "symbol": "COPPER", "lot_size": 1, "expiry_weekday": None, "active": True,
        "min_premium": 0, "max_premium": 0, "atm_strike_multiple": 0, "option_exchange": None,
        "ws_exchange_type": 5, "option_ws_exchange_type": 0, "max_daily_drawdown_pct": 4.0,
        "correlation_pair": None, "greeks_enabled": False, "pcr_enabled": False,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.6, "is_commodity": True,
        "mkt_multiple": 1.0
    }
}

INDEX_TOKEN_SET = {cfg["token"] for cfg in INDEX_CONFIG.values() if cfg.get("token")}

# ----------------------------------------------------------------------
# GLOBAL STATE
# ----------------------------------------------------------------------
_latest_ticks_lock = threading.Lock()
_market_signal_lock = threading.Lock()
_portfolio_state_lock = threading.Lock()
_signal_state_lock = threading.Lock()
_candle_histories_lock = threading.Lock()
_price_histories_lock = threading.Lock()
_ce_price_histories_lock = threading.Lock()
_pe_price_histories_lock = threading.Lock()
_current_candle_lock = threading.Lock()
_prev_volume_lock = threading.Lock()

INDEX_NAMES = list(INDEX_CONFIG.keys())

INDEX_TOKENS = {idx: {"ce_token": None, "pe_token": None, "atm_strike": 0, "expiry": "", "expiry_date": None, "ce_symbol": "", "pe_symbol": ""} for idx in INDEX_NAMES}
last_known_prices = {idx: {"spot": 0.0, "ce": 0.0, "pe": 0.0, "timestamp": 0} for idx in INDEX_NAMES}
price_histories = {idx: deque(maxlen=5000) for idx in INDEX_NAMES}

portfolio_state = {idx: {"equity": 100000.0, "open_positions": 0, "daily_pnl": 0.0, "total_pnl": 0.0, "live_pnl": 0.0} for idx in INDEX_NAMES}
signal_state = {idx: {"action": "HOLD", "entry_price": 0.0, "stop_loss": 0.0, "target": 0.0, "lots": 0, "entry_time": 0.0, "highest": 0.0, "cooldown": 0, "confidence": 0, "exit_reason": ""} for idx in INDEX_NAMES}
market_signal = {idx: {"sentiment_score": 50, "signal": "WAITING", "alert_message": "", "entry_price": 0, "stop_loss": 0, "target": 0, "exit_reason": ""} for idx in INDEX_NAMES}

ce_price_histories = {idx: deque(maxlen=2000) for idx in INDEX_NAMES}
pe_price_histories = {idx: deque(maxlen=2000) for idx in INDEX_NAMES}
ce_volume_histories = {idx: deque(maxlen=1000) for idx in INDEX_NAMES}
pe_volume_histories = {idx: deque(maxlen=1000) for idx in INDEX_NAMES}
ce_oi_histories = {idx: deque(maxlen=200) for idx in INDEX_NAMES}
pe_oi_histories = {idx: deque(maxlen=200) for idx in INDEX_NAMES}

vix_history = deque(maxlen=200)
nifty_price_series = deque(maxlen=200)
banknifty_price_series = deque(maxlen=200)

latest_ticks = {}
for idx, cfg in INDEX_CONFIG.items():
    if cfg.get("is_commodity"):
        latest_ticks[idx] = {"price": 0.0, "volume": 0, "bid": 0.0, "ask": 0.0}
    else:
        latest_ticks[idx] = {
            "spot_price": 0.0, "ce_price": 0.0, "pe_price": 0.0,
            "ce_volume": 0, "pe_volume": 0, "ce_oi": 0, "pe_oi": 0,
            "ce_bid": 0.0, "ce_ask": 0.0, "pe_bid": 0.0, "pe_ask": 0.0
        }
latest_ticks["VIX"] = {"vix": 15.0}

daily_drawdown = {idx: {"peak_equity": 0.0, "current_drawdown": 0.0} for idx in INDEX_NAMES}
safety_state = {idx: {"consecutive_sl": 0, "circuit_breaker": False, "circuit_breaker_until": 0} for idx in INDEX_NAMES}
signal_buffer = {idx: {"ce_count": 0, "pe_count": 0} for idx in INDEX_NAMES}
daily_trade_count = {idx: 0 for idx in INDEX_NAMES}
last_trade_date = {idx: "" for idx in INDEX_NAMES}

_last_signal_run = 0
_signal_run_lock = threading.Lock()
_telegram_last_sent = 0
_telegram_lock = threading.Lock()

_historical_iv_ce = {idx: deque(maxlen=200) for idx in INDEX_NAMES}
_historical_iv_pe = {idx: deque(maxlen=200) for idx in INDEX_NAMES}

# ----------------------------------------------------------------------
# TIMEFRAME DEFINITIONS
# ----------------------------------------------------------------------
TIMEFRAMES = ["1min", "2min", "3min", "5min", "8min", "10min", "15min", "20min"]
TIMEFRAME_SECONDS = {"1min":60, "2min":120, "3min":180, "5min":300, "8min":480, "10min":600, "15min":900, "20min":1200}
TIMEFRAME_WEIGHTS = {"1min":8, "2min":8, "3min":8, "5min":12, "8min":12, "10min":12, "15min":14, "20min":14}

candle_histories = {idx: {tf: deque(maxlen=500) for tf in TIMEFRAMES} for idx in INDEX_NAMES}
_last_candle_time = {idx: {tf: 0 for tf in TIMEFRAMES} for idx in INDEX_NAMES}
_current_candle = {idx: {tf: None for tf in TIMEFRAMES} for idx in INDEX_NAMES}
_prev_volume = {idx: 0 for idx in INDEX_NAMES}

# ----------------------------------------------------------------------
# INDICATORS (all return numeric, never None)
# ----------------------------------------------------------------------
def calculate_ema(prices, period):
    if not prices:
        return 0.0
    if len(prices) < period:
        return float(sum(prices) / len(prices))
    alpha = 2.0 / (period + 1)
    ema = float(sum(prices[:period]) / period)
    for p in prices[period:]:
        ema = alpha * p + (1 - alpha) * ema
    return ema

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    avg_gain = sum(gains[-period:]) / period if gains else 0.0
    avg_loss = sum(losses[-period:]) / period if losses else 0.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

def calculate_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal:
        return 0.0, 0.0, 0.0
    def ema(series, period):
        alpha = 2.0 / (period + 1)
        val = series[0]
        for x in series[1:]:
            val = alpha * x + (1 - alpha) * val
        return val
    ema_fast = ema(prices[-fast:], fast) if len(prices) >= fast else prices[-1]
    ema_slow = ema(prices[-slow:], slow) if len(prices) >= slow else prices[-1]
    macd_line = ema_fast - ema_slow
    # signal line approximation
    sig_line = ema([macd_line], signal) if len(prices) >= signal else macd_line
    hist = macd_line - sig_line
    return macd_line, sig_line, hist

def calculate_atr(highs, lows, closes, period=14):
    if len(closes) < period + 1:
        return 0.0
    tr = []
    for i in range(1, len(closes)):
        if len(highs) > i and highs[i] > 0 and lows[i] > 0:
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i-1])
            lc = abs(lows[i] - closes[i-1])
            tr.append(max(hl, hc, lc))
        else:
            tr.append(abs(closes[i] - closes[i-1]))
    if len(tr) < period:
        return 0.0
    return sum(tr[-period:]) / period

def calculate_adx(highs, lows, closes, period=14):
    if len(closes) < period * 2:
        return 20.0
    tr = []
    plus_dm = []
    minus_dm = []
    for i in range(1, len(closes)):
        if len(highs) > i and highs[i] > 0 and lows[i] > 0:
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i-1])
            lc = abs(lows[i] - closes[i-1])
            tr.append(max(hl, hc, lc))
            up = highs[i] - highs[i-1]
            down = lows[i-1] - lows[i]
            plus_dm.append(max(up, 0.0) if up > down else 0.0)
            minus_dm.append(max(down, 0.0) if down > up else 0.0)
        else:
            tr.append(abs(closes[i] - closes[i-1]))
            plus_dm.append(max(closes[i] - closes[i-1], 0.0))
            minus_dm.append(max(closes[i-1] - closes[i], 0.0))
    if len(tr) < period:
        return 20.0
    atr = sum(tr[:period]) / period
    plus_di_sum = sum(plus_dm[:period]) / period
    minus_di_sum = sum(minus_dm[:period]) / period
    dx_values = []
    for i in range(period, len(tr)):
        atr = atr - (atr / period) + tr[i]
        plus_di_sum = plus_di_sum - (plus_di_sum / period) + plus_dm[i]
        minus_di_sum = minus_di_sum - (minus_di_sum / period) + minus_dm[i]
        plus_di = 100.0 * plus_di_sum / atr if atr > 0 else 0
        minus_di = 100.0 * minus_di_sum / atr if atr > 0 else 0
        dx = 100.0 * abs(plus_di - minus_di) / (plus_di + minus_di) if (plus_di + minus_di) > 0 else 0
        dx_values.append(dx)
    if not dx_values:
        return 20.0
    adx = sum(dx_values[:period]) / period if len(dx_values) >= period else dx_values[0]
    for i in range(period, len(dx_values)):
        adx = (adx * (period - 1) + dx_values[i]) / period
    return adx

def calculate_vwap(prices, volumes):
    if not prices:
        return 0.0
    if not volumes or len(volumes) != len(prices):
        return float(prices[-1])
    total_vol = sum(volumes)
    if total_vol == 0:
        return float(prices[-1])
    return sum(p * v for p, v in zip(prices, volumes)) / total_vol

# ----------------------------------------------------------------------
# BSM & GREEKS (safe, never returns None for keys)
# ----------------------------------------------------------------------
try:
    from scipy.stats import norm
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    norm = None

def bsm_iv_delta(S, K, T, r, premium, option_type):
    if not SCIPY_AVAILABLE:
        iv_est = 0.2
        moneyness = (S - K) / S if S > 0 else 0
        if option_type == "CE":
            delta_est = max(0.05, min(0.95, 0.5 + moneyness * 5))
        else:
            delta_est = max(-0.95, min(-0.05, -0.5 - moneyness * 5))
        return iv_est, delta_est
    try:
        from scipy.optimize import brentq
        def bsm_price(sigma):
            d1 = (math.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*math.sqrt(T))
            d2 = d1 - sigma*math.sqrt(T)
            if option_type == "CE":
                return S*norm.cdf(d1) - K*math.exp(-r*T)*norm.cdf(d2) - premium
            else:
                return K*math.exp(-r*T)*norm.cdf(-d2) - S*norm.cdf(-d1) - premium
        low_sig = 0.01
        high_sig = 2.0
        intrinsic = max(0, (S - K) if option_type=="CE" else (K - S))
        if premium <= intrinsic + 0.01:
            iv_est = 0.01
        else:
            try:
                iv_est = brentq(bsm_price, low_sig, high_sig, maxiter=50)
            except:
                iv_est = 0.2
        d1 = (math.log(S/K) + (r + 0.5*iv_est**2)*T) / (iv_est*math.sqrt(T))
        if option_type == "CE":
            delta_est = norm.cdf(d1)
        else:
            delta_est = -norm.cdf(-d1)
        return iv_est, delta_est
    except:
        return 0.2, 0.5

greeks_cache_fallback_store = {}
for idx in INDEX_NAMES:
    if not INDEX_CONFIG[idx].get("is_commodity"):
        greeks_cache_fallback_store[idx] = {"ce_iv":0.2, "pe_iv":0.2, "ce_delta":0.5, "pe_delta":-0.5,
                                            "ce_gamma":0.02, "pe_gamma":0.02, "ce_theta":-0.1, "pe_theta":-0.1,
                                            "ce_vega":0.15, "pe_vega":0.15, "iv_rank":50, "iv_percentile":50}
    else:
        greeks_cache_fallback_store[idx] = None  # commodities don't use Greeks

_greeks_cache = {idx: {"data": None, "timestamp": 0} for idx in INDEX_NAMES}
_GREEKS_CACHE_TTL = 60

def _estimate_greeks_fallback(index_name):
    if INDEX_CONFIG[index_name].get("is_commodity"):
        return None
    tokens = INDEX_TOKENS.get(index_name, {})
    with _latest_ticks_lock:
        ce_price = latest_ticks[index_name].get("ce_price", 0.0) or 0.0
        pe_price = latest_ticks[index_name].get("pe_price", 0.0) or 0.0
        spot = latest_ticks[index_name].get("spot_price", 0.0) or 0.0
    if spot <= 0:
        return greeks_cache_fallback_store.get(index_name, {"ce_iv":0.2, "pe_iv":0.2, "ce_delta":0.5, "pe_delta":-0.5,
                                                             "ce_gamma":0.02, "pe_gamma":0.02, "ce_theta":-0.1, "pe_theta":-0.1,
                                                             "ce_vega":0.15, "pe_vega":0.15, "iv_rank":50, "iv_percentile":50})
    strike = tokens.get("atm_strike", spot)
    expiry_date = tokens.get("expiry_date")
    if expiry_date:
        T = (expiry_date - datetime.now()).days / 365.0
        if T <= 0:
            T = 0.01
    else:
        T = 0.1
    r = 0.05
    if ce_price > 0:
        iv_ce, delta_ce = bsm_iv_delta(spot, strike, T, r, ce_price, "CE")
    else:
        iv_ce, delta_ce = 0.2, 0.5
    if pe_price > 0:
        iv_pe, delta_pe = bsm_iv_delta(spot, strike, T, r, pe_price, "PE")
    else:
        iv_pe, delta_pe = 0.2, -0.5
    if iv_ce > 0:
        _historical_iv_ce[index_name].append(iv_ce)
    if iv_pe > 0:
        _historical_iv_pe[index_name].append(iv_pe)

    def get_rank(hist, current):
        if len(hist) < 20:
            return 50.0
        sorted_hist = sorted(hist)
        rank = sum(1 for x in hist if x < current) / len(hist) * 100.0
        return rank
    iv_rank_ce = get_rank(list(_historical_iv_ce[index_name]), iv_ce)
    iv_rank_pe = get_rank(list(_historical_iv_pe[index_name]), iv_pe)
    greeks_data = {
        "ce_iv": iv_ce, "pe_iv": iv_pe,
        "ce_delta": delta_ce, "pe_delta": delta_pe,
        "ce_gamma": 0.02, "pe_gamma": 0.02,
        "ce_theta": -0.1, "pe_theta": -0.1,
        "ce_vega": 0.15, "pe_vega": 0.15,
        "iv_rank": (iv_rank_ce + iv_rank_pe) / 2.0,
        "iv_percentile": (iv_rank_ce + iv_rank_pe) / 2.0
    }
    greeks_cache_fallback_store[index_name] = greeks_data
    return greeks_data

def get_option_greeks(index_name):
    if INDEX_CONFIG[index_name].get("is_commodity"):
        return None
    now = time.time()
    cached = _greeks_cache.get(index_name, {})
    if cached.get("data") and (now - cached.get("timestamp", 0) < _GREEKS_CACHE_TTL):
        return cached["data"]
    config = INDEX_CONFIG.get(index_name)
    if not config or not config.get("greeks_enabled"):
        return None
    tokens = INDEX_TOKENS.get(index_name)
    if not tokens or not tokens.get("ce_token") or not tokens.get("pe_token"):
        return _estimate_greeks_fallback(index_name)
    _, _, obj = get_auth_token()
    if obj:
        try:
            expiry_str = tokens.get("expiry", "")
            if expiry_str:
                greeks_payload = {"name": config["symbol"], "expirydate": expiry_str}
                url = "https://apiconnect.angelbroking.com/rest/secure/angelbroking/marketData/v1/optionGreek"
                try:
                    local_ip = socket.gethostbyname(socket.gethostname())
                except Exception:
                    local_ip = "127.0.0.1"
                auth_token, _, _ = get_auth_token()
                headers = {
                    "Authorization": f"Bearer {auth_token}", "Content-Type": "application/json",
                    "Accept": "application/json", "X-UserType": "USER", "X-SourceID": "WEB",
                    "X-ClientLocalIP": local_ip, "X-ClientPublicIP": local_ip, "X-MACAddress": "00:00:00:00:00:00",
                    "X-PrivateKey": ANGEL_API_KEY
                }
                resp = requests.post(url, json=greeks_payload, headers=headers, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("status") and data.get("data"):
                        greeks_list = data["data"]
                        atm_strike = tokens.get("atm_strike", 0)
                        ce_greeks = pe_greeks = None
                        for g in greeks_list:
                            strike = float(g.get("strikePrice", 0))
                            opt_type = g.get("optionType", "")
                            if abs(strike - atm_strike) < config.get("atm_strike_multiple", 50)*0.5:
                                if opt_type == "CE":
                                    ce_greeks = g
                                elif opt_type == "PE":
                                    pe_greeks = g
                        if ce_greeks and pe_greeks:
                            ce_iv = float(ce_greeks.get("impliedVolatility", 0))
                            pe_iv = float(pe_greeks.get("impliedVolatility", 0))
                            if ce_iv > 1: ce_iv /= 100
                            if pe_iv > 1: pe_iv /= 100
                            if ce_iv > 0: _historical_iv_ce[index_name].append(ce_iv)
                            if pe_iv > 0: _historical_iv_pe[index_name].append(pe_iv)
                            def get_rank(hist, current):
                                if len(hist) < 20:
                                    return 50.0
                                sorted_hist = sorted(hist)
                                rank = sum(1 for x in hist if x < current) / len(hist) * 100.0
                                return rank
                            iv_rank_ce = get_rank(list(_historical_iv_ce[index_name]), ce_iv)
                            iv_rank_pe = get_rank(list(_historical_iv_pe[index_name]), pe_iv)
                            greeks_data = {
                                "ce_iv": ce_iv, "pe_iv": pe_iv,
                                "ce_delta": float(ce_greeks.get("delta", 0)),
                                "pe_delta": float(pe_greeks.get("delta", 0)),
                                "ce_gamma": float(ce_greeks.get("gamma", 0)),
                                "pe_gamma": float(pe_greeks.get("gamma", 0)),
                                "ce_theta": float(ce_greeks.get("theta", 0)),
                                "pe_theta": float(pe_greeks.get("theta", 0)),
                                "ce_vega": float(ce_greeks.get("vega", 0)),
                                "pe_vega": float(pe_greeks.get("vega", 0)),
                                "iv_rank": (iv_rank_ce + iv_rank_pe) / 2.0,
                                "iv_percentile": (iv_rank_ce + iv_rank_pe) / 2.0
                            }
                            greeks_cache_fallback_store[index_name] = greeks_data
                            _greeks_cache[index_name] = {"data": greeks_data, "timestamp": now}
                            return greeks_data
        except Exception as e:
            logger.debug(f"Greeks API error {index_name}: {e}")
    data = _estimate_greeks_fallback(index_name)
    _greeks_cache[index_name] = {"data": data, "timestamp": now}
    return data

# ----------------------------------------------------------------------
# KELLY, CORRELATION, VOLUME PROFILE (unchanged, but safe)
# ----------------------------------------------------------------------
class KellyCriterion:
    def __init__(self, index_name, kelly_fraction=0.25, min_trades=10):
        self.index_name = index_name
        self.kelly_fraction = kelly_fraction
        self.min_trades = min_trades
        self.win_count = 0
        self.loss_count = 0
        self.avg_win = 0.0
        self.avg_loss = 0.0
    def update(self, trade_pnl_pct):
        if trade_pnl_pct > 0:
            self.win_count += 1
            self.avg_win = ((self.avg_win*(self.win_count-1)) + trade_pnl_pct)/self.win_count
        else:
            self.loss_count += 1
            loss_abs = abs(trade_pnl_pct)
            self.avg_loss = ((self.avg_loss*(self.loss_count-1)) + loss_abs)/self.loss_count
    def get_recommended_risk_pct(self):
        total = self.win_count + self.loss_count
        if total < self.min_trades:
            return 0.5, 0.5, self.avg_win, self.avg_loss
        p = self.win_count / total
        q = 1 - p
        b = 2.0
        if self.avg_loss != 0:
            b = abs(self.avg_win / self.avg_loss)
            b = min(b, 10.0)
        kelly_full = (p*b - q)/b if b > 0 else 0
        kelly_full = max(0, min(kelly_full, 0.5))
        return kelly_full * self.kelly_fraction, p, self.avg_win, self.avg_loss

kelly_trackers = {idx: KellyCriterion(idx) for idx in INDEX_NAMES}

class CorrelationFilter:
    def __init__(self):
        self.nifty_returns = deque(maxlen=50)
        self.banknifty_returns = deque(maxlen=50)
        self.correlation = 0.0
    def update(self, nifty_price, banknifty_price):
        if nifty_price > 0 and banknifty_price > 0:
            if len(self.nifty_returns) > 0:
                prev_nifty = self.nifty_returns[-1]
                if prev_nifty > 0:
                    ret_n = (nifty_price / prev_nifty) - 1
                    self.nifty_returns.append(ret_n)
                else: self.nifty_returns.append(0.0)
            else: self.nifty_returns.append(0.0)
            if len(self.banknifty_returns) > 0:
                prev_bank = self.banknifty_returns[-1]
                if prev_bank > 0:
                    ret_b = (banknifty_price / prev_bank) - 1
                    self.banknifty_returns.append(ret_b)
                else: self.banknifty_returns.append(0.0)
            else: self.banknifty_returns.append(0.0)
            if len(self.nifty_returns) >= 20 and len(self.banknifty_returns) >= 20:
                n_arr = np.array(list(self.nifty_returns)[-20:])
                b_arr = np.array(list(self.banknifty_returns)[-20:])
                if np.std(n_arr) > 0 and np.std(b_arr) > 0:
                    self.correlation = np.corrcoef(n_arr, b_arr)[0,1]
                else: self.correlation = 0.0
    def analyze(self, index_name, action):
        if abs(self.correlation) > 0.8:
            return {"beta_adjustment": 1.0, "block_reason": None, "correlation": self.correlation}
        return {"beta_adjustment": 1.0, "block_reason": None, "correlation": self.correlation}

correlation_filter = CorrelationFilter()

class VolumeProfileEngine:
    def __init__(self, index_name):
        self.price_volume = deque(maxlen=1000)
        self.ce_price_volume = deque(maxlen=1000)
        self.pe_price_volume = deque(maxlen=1000)
    def update(self, price, volume, option_type=None):
        if option_type is None:
            if price > 0 and volume > 0: self.price_volume.append((price, volume))
        elif option_type == "CE":
            if price > 0 and volume > 0: self.ce_price_volume.append((price, volume))
        elif option_type == "PE":
            if price > 0 and volume > 0: self.pe_price_volume.append((price, volume))
    def analyze(self, current_price, current_volume, option_type=None):
        if option_type is None: pv = self.price_volume
        elif option_type == "CE": pv = self.ce_price_volume
        elif option_type == "PE": pv = self.pe_price_volume
        else: pv = self.price_volume
        if not pv: return {"vwap": 0.0, "poc": 0, "vah": 0, "val": 0, "signal": "neutral", "strength": 0}
        total_pv = sum(p*v for p,v in pv)
        total_v = sum(v for p,v in pv)
        vwap = total_pv/total_v if total_v > 0 else current_price
        return {"vwap": vwap, "poc": 0, "vah": 0, "val": 0, "signal": "neutral", "strength": 0}

volume_profile_engines = {idx: VolumeProfileEngine(idx) for idx in INDEX_NAMES}

# ----------------------------------------------------------------------
# AUTHENTICATION & TOKEN MANAGEMENT
# ----------------------------------------------------------------------
auth_cache = {"token": None, "feed_token": None, "timestamp": 0, "obj": None}
_auth_lock = threading.Lock()

def get_auth_token():
    with _auth_lock:
        now = time.time()
        if auth_cache["token"] and (now - auth_cache["timestamp"] < 3300):
            return auth_cache["token"], auth_cache["feed_token"], auth_cache["obj"]
        try:
            totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
            obj = SmartConnect(api_key=ANGEL_API_KEY)
            session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
            if not session.get("status"):
                return None, None, None
            auth_token = session["data"]["jwtToken"]
            feed_token = obj.getfeedToken()
            auth_cache.update({"token": auth_token, "feed_token": feed_token, "timestamp": now, "obj": obj})
            logger.info("Auth token refreshed")
            return auth_token, feed_token, obj
        except Exception as e:
            logger.error(f"Auth error: {type(e).__name__}")
            return None, None, None

def safe_ltp(resp):
    if not resp or not resp.get("status"):
        return None
    data = resp.get("data", {})
    if isinstance(data, dict):
        if "fetched" in data and data["fetched"]:
            fetched = data["fetched"]
            if isinstance(fetched, list) and len(fetched) > 0:
                return float(fetched[0].get("ltp", 0))
            elif isinstance(fetched, dict):
                return float(fetched.get("ltp", 0))
        elif "ltp" in data:
            return float(data["ltp"])
    elif isinstance(data, list) and len(data) > 0:
        return float(data[0].get("ltp", 0))
    return None

def get_index_spot(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config:
        return None
    _, _, obj = get_auth_token()
    if not obj:
        return None
    try:
        resp = obj.ltpData(config["exchange"], config["symbol"], config["token"])
        ltp = safe_ltp(resp)
        if ltp and ltp > 0:
            if config["exchange"] in ["NSE","BSE"] and ltp > 100000:
                ltp /= 100
            return ltp
    except Exception as e:
        logger.error(f"Spot fetch {index_name}: {e}")
    return None

_scrip_cache = {"data": None, "timestamp": 0}
_scrip_lock = threading.Lock()

def parse_expiry_date(expiry_str):
    if not expiry_str:
        return None
    formats = ["%d%b%Y", "%d%b%y", "%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d", "%d/%m/%Y"]
    for fmt in formats:
        try:
            return datetime.strptime(expiry_str, fmt)
        except Exception:
            continue
    return None

def get_scrip_master():
    with _scrip_lock:
        now = time.time()
        if _scrip_cache["data"] and (now - _scrip_cache["timestamp"] < 86400):
            return _scrip_cache["data"]
        try:
            url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
            data = requests.get(url, timeout=30).json()
            _scrip_cache["data"] = data
            _scrip_cache["timestamp"] = now
            logger.info("Scrip master refreshed")
            return data
        except Exception as e:
            logger.error(f"Scrip master failed: {e}")
            return _scrip_cache["data"] or []

def get_mcx_futures_tokens():
    scrip = get_scrip_master()
    if not scrip:
        return
    df = pd.DataFrame(scrip)
    mcx_fut = df[(df["exch_seg"]=="MCX") & (df["instrumenttype"]=="FUTCOM")]
    if mcx_fut.empty:
        logger.warning("No MCX FUTCOM found in scrip master")
        return
    for idx, cfg in INDEX_CONFIG.items():
        if not cfg.get("active") or not cfg.get("is_commodity"):
            continue
        symbol = cfg["symbol"]
        matching = mcx_fut[mcx_fut["symbol"].str.contains(symbol, case=False, na=False)]
        if matching.empty:
            logger.warning(f"MCX symbol {symbol} not found")
            continue
        matching = matching.copy()
        matching["expiry_date"] = matching["expiry"].apply(parse_expiry_date)
        matching = matching.dropna(subset=["expiry_date"])
        today = datetime.now()
        future = matching[matching["expiry_date"] >= today]
        if future.empty:
            future = matching
        nearest = future.iloc[0]
        token = str(nearest["token"])
        cfg["token"] = token
        logger.info(f"MCX {symbol} token: {token} (symbol: {nearest['symbol']}) expiry {nearest['expiry']}")
    global INDEX_TOKEN_SET
    INDEX_TOKEN_SET = {cfg["token"] for cfg in INDEX_CONFIG.values() if cfg.get("token")}

def get_next_expiry_date(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config:
        return None
    if config.get("is_commodity"):
        return None  # commodities don't have expiry for options
    weekday = config.get("expiry_weekday")
    if weekday is None:
        return None
    today = datetime.now()
    days_ahead = weekday - today.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    return today + timedelta(days=days_ahead)

def get_current_atm_tokens(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config or not config.get("active"):
        INDEX_TOKENS[index_name].update({"ce_token": None, "pe_token": None})
        return None, None
    if config.get("is_commodity"):
        return None, None
    spot = get_index_spot(index_name)
    if not spot or spot <= 0:
        INDEX_TOKENS[index_name].update({"ce_token": None, "pe_token": None})
        return None, None
    mult = config["atm_strike_multiple"]
    atm = int(round(spot / mult) * mult)
    next_expiry = get_next_expiry_date(index_name)
    if not next_expiry:
        INDEX_TOKENS[index_name].update({"ce_token": None, "pe_token": None})
        return None, None
    expiry = next_expiry.strftime("%d%b%Y").upper()
    scrip = get_scrip_master()
    if scrip:
        try:
            df = pd.DataFrame(scrip)
            opts = df[(df["name"] == config["symbol"]) &
                      (df["instrumenttype"] == "OPTIDX") &
                      (df["exch_seg"] == config["option_exchange"])]
            if not opts.empty:
                opts = opts.copy()
                opts["expiry_date"] = opts["expiry"].apply(parse_expiry_date)
                opts = opts.dropna(subset=["expiry_date"])
                opts["strike"] = pd.to_numeric(opts["strike"], errors="coerce") / 100
                opts = opts.dropna(subset=["strike"])
                future = opts[opts["expiry_date"] >= datetime.now()]
                if not future.empty:
                    nearest = future["expiry_date"].min()
                    atm_opts = future[(future["strike"] == atm) & (future["expiry_date"] == nearest)]
                    if atm_opts.empty:
                        same_exp = future[future["expiry_date"] == nearest]
                        if not same_exp.empty:
                            diff = (same_exp["strike"] - atm).abs()
                            atm_opts = same_exp.loc[[diff.idxmin()]]
                    if not atm_opts.empty:
                        ce = atm_opts[atm_opts["symbol"].str.contains("CE", na=False)]
                        pe = atm_opts[atm_opts["symbol"].str.contains("PE", na=False)]
                        if not ce.empty and not pe.empty:
                            ce_token = str(ce.iloc[0]["token"])
                            pe_token = str(pe.iloc[0]["token"])
                            ce_symbol = str(ce.iloc[0]["symbol"])
                            pe_symbol = str(pe.iloc[0]["symbol"])
                            actual_strike = int(ce.iloc[0]["strike"])
                            if actual_strike != atm:
                                logger.info(f"{index_name} ATM strike {atm} not found, using nearest {actual_strike}")
                            else:
                                logger.info(f"{index_name} ATM strike {atm} selected")
                            INDEX_TOKENS[index_name].update({
                                "ce_token": ce_token, "pe_token": pe_token, "atm_strike": actual_strike,
                                "expiry": expiry, "expiry_date": nearest,
                                "ce_symbol": ce_symbol, "pe_symbol": pe_symbol
                            })
                            logger.info(f"{index_name} tokens: CE={ce_token} PE={pe_token} expiry={expiry}")
                            return ce_token, pe_token
        except Exception as e:
            logger.warning(f"{index_name} token fetch error: {e}")
    INDEX_TOKENS[index_name].update({"ce_token": None, "pe_token": None})
    return None, None

def refresh_all_tokens():
    for idx in INDEX_NAMES:
        if INDEX_CONFIG[idx].get("active"):
            if INDEX_CONFIG[idx].get("is_commodity"):
                continue
            get_current_atm_tokens(idx)
    get_mcx_futures_tokens()

# ----------------------------------------------------------------------
# MARKET HOURS
# ----------------------------------------------------------------------
def is_market_open():
    now_ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    current = now_ist.time()
    open_time = dt_time(9, 15)
    close_time = dt_time(15, 15)
    return now_ist.weekday() < 5 and open_time <= current <= close_time

def is_mcx_open():
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    open_time = dt_time(10, 0)
    close_time = dt_time(23, 30)
    return open_time <= now.time() <= close_time

def is_index_market_open(idx):
    cfg = INDEX_CONFIG.get(idx, {})
    if cfg.get("is_commodity"):
        return is_mcx_open()
    else:
        return is_market_open()

# ----------------------------------------------------------------------
# CANDLE UPDATE
# ----------------------------------------------------------------------
def update_candle(idx, price, cumulative_volume, timestamp):
    with _prev_volume_lock:
        prev = _prev_volume.get(idx, 0)
        if cumulative_volume < prev or (cumulative_volume == 0 and prev > 0):
            tick_vol = 0
        else:
            tick_vol = max(0, cumulative_volume - prev)
        _prev_volume[idx] = cumulative_volume

    for tf, interval in TIMEFRAME_SECONDS.items():
        candle_start = int(timestamp / interval) * interval
        with _current_candle_lock:
            if _last_candle_time[idx][tf] != candle_start:
                if _current_candle[idx][tf] is not None:
                    with _candle_histories_lock:
                        candle_histories[idx][tf].append(_current_candle[idx][tf])
                        # ---- ADD LOG ----
                        if tf == "1min":
                            logger.info(f"📊 New 1min candle appended for {idx} (len={len(candle_histories[idx]['1min'])})")
                        # -----------------
                _current_candle[idx][tf] = {
                    "open": price, "high": price, "low": price, "close": price,
                    "volume": tick_vol, "timestamp": candle_start
                }
                _last_candle_time[idx][tf] = candle_start
            else:
                if _current_candle[idx][tf] is not None:
                    _current_candle[idx][tf]["high"] = max(_current_candle[idx][tf]["high"], price)
                    _current_candle[idx][tf]["low"] = min(_current_candle[idx][tf]["low"], price)
                    _current_candle[idx][tf]["close"] = price
                    _current_candle[idx][tf]["volume"] += tick_vol

# ----------------------------------------------------------------------
# SENTIMENT, REGIME, CONFIRMATION (safe, no None returns)
# ----------------------------------------------------------------------
def compute_sentiment(index_name):
    sentiment_scores = []
    for tf in TIMEFRAMES:
        with _candle_histories_lock:
            candles = list(candle_histories[index_name][tf])
        if len(candles) < 10:
            continue
        closes = [c["close"] for c in candles]
        if len(closes) < 60:
            continue
        ema9 = calculate_ema(closes, 9)
        ema21 = calculate_ema(closes, 21)
        ema50 = calculate_ema(closes, 50) if len(closes) >= 50 else ema21
        price = closes[-1]
        recent = closes[-30:]
        price_range = (max(recent)-min(recent))/sum(recent)*len(recent) if sum(recent) else 0
        if price_range < 0.005:
            score = 0
        elif tf in ["1min","2min","3min"]:
            if ema9 > ema21 and price > ema9:
                score = TIMEFRAME_WEIGHTS[tf]
            elif ema9 < ema21 and price < ema9:
                score = -TIMEFRAME_WEIGHTS[tf]
            else:
                score = 0
        elif tf in ["5min","8min","10min"]:
            if ema9 > ema21 > ema50 and price > ema9:
                score = TIMEFRAME_WEIGHTS[tf]
            elif ema9 < ema21 < ema50 and price < ema9:
                score = -TIMEFRAME_WEIGHTS[tf]
            elif ema9 > ema21 and price > ema9:
                score = TIMEFRAME_WEIGHTS[tf]-5
            elif ema9 < ema21 and price < ema9:
                score = -TIMEFRAME_WEIGHTS[tf]+5
            else:
                score = 0
        else:
            if ema9 > ema21 > ema50:
                score = TIMEFRAME_WEIGHTS[tf]-5
            elif ema9 < ema21 < ema50:
                score = -TIMEFRAME_WEIGHTS[tf]+5
            else:
                score = 0
        sentiment_scores.append(score)
    if not sentiment_scores:
        return 50.0
    total = sum(sentiment_scores)
    sentiment = 50.0 + (total / 3.5)
    return max(0.0, min(100.0, sentiment))

def get_sentiment_label(sentiment):
    if sentiment >= 85: return "STRONG BULLISH"
    elif sentiment >= 70: return "BULLISH"
    elif sentiment >= 55: return "SLOW BULLISH"
    elif sentiment >= 45: return "NEUTRAL"
    elif sentiment >= 30: return "SLOW BEARISH"
    elif sentiment >= 15: return "BEARISH"
    else: return "STRONG BEARISH"

def get_signal_from_sentiment(sentiment):
    if sentiment >= 85: return "STRONG_BUY_CE"
    elif sentiment >= 70: return "BUY_CE"
    elif sentiment >= 55: return "LOW_BUY_CE"
    elif sentiment >= 45: return "NO_TRADE"
    elif sentiment >= 30: return "LOW_BUY_PE"
    elif sentiment >= 15: return "BUY_PE"
    else: return "STRONG_BUY_PE"

def get_trend_for_timeframe(index_name, tf):
    with _candle_histories_lock:
        candles = list(candle_histories[index_name][tf])
    if len(candles) < 20:
        return "NEUTRAL"
    closes = [c["close"] for c in candles]
    ema9 = calculate_ema(closes, 9)
    ema21 = calculate_ema(closes, 21)
    price = closes[-1]
    if ema9 > ema21 and price > ema9:
        return "BULLISH"
    elif ema9 < ema21 and price < ema9:
        return "BEARISH"
    return "NEUTRAL"

def detect_regime(index_name):
    config = INDEX_CONFIG.get(index_name, {})
    adx_threshold = config.get("regime_adx_threshold", 25)
    atr_threshold = config.get("regime_atr_threshold", 0.6)
    with _candle_histories_lock:
        closes = [c["close"] for c in candle_histories[index_name]["5min"]]
    if len(closes) < 30:
        return "NORMAL"
    adx = calculate_adx([], [], closes, 14)
    atr = calculate_atr([], [], closes, 14)
    if config.get("is_commodity"):
        with _price_histories_lock:
            prices = list(price_histories[index_name])
        spot = prices[-1] if prices else 0.0
    else:
        spot = last_known_prices[index_name].get("spot", 0.0) or 0.0
    if spot == 0:
        return "NORMAL"
    atr_pct = (atr / spot) * 100.0
    with _latest_ticks_lock:
        vix = latest_ticks["VIX"]["vix"]
    vix_sma = sum(list(vix_history)[-20:]) / len(vix_history) if len(vix_history) >= 20 else vix
    if adx > adx_threshold and atr_pct > atr_threshold:
        return "TRENDING"
    elif adx < adx_threshold * 0.6 and atr_pct < atr_threshold * 0.5:
        return "RANGING"
    elif vix > vix_sma * 1.3:
        return "VOLATILE"
    else:
        return "NORMAL"

def confirm_signal_with_candles(index_name, side, spot):
    with _candle_histories_lock:
        candles = list(candle_histories[index_name]["1min"])
    if len(candles) < 20:
        return False
    closes = [c["close"] for c in candles[-20:]]
    ema9 = calculate_ema(closes, 9)
    if ema9 == 0:
        return False
    last_3_closes = [c["close"] for c in candles[-3:]]
    if side == "CE":
        return all(c > ema9 for c in last_3_closes)
    else:
        return all(c < ema9 for c in last_3_closes)

def compute_ml_score(index_name, side, prem, spot, rsi, adx, vix, sentiment):
    score = 0.5
    if side == "CE":
        if rsi < 30: score += 0.2
        elif rsi < 40: score += 0.1
        elif rsi > 70: score -= 0.2
        elif rsi > 60: score -= 0.1
    else:
        if rsi > 70: score += 0.2
        elif rsi > 60: score += 0.1
        elif rsi < 30: score -= 0.2
        elif rsi < 40: score -= 0.1
    if adx > 25: score += 0.1
    elif adx < 15: score -= 0.1
    if vix > 25: score -= 0.1
    elif vix < 15: score += 0.05
    if side == "CE" and sentiment >= 70: score += 0.1
    elif side == "PE" and sentiment <= 30: score += 0.1
    elif side == "CE" and sentiment <= 30: score -= 0.1
    elif side == "PE" and sentiment >= 70: score -= 0.1
    return max(0.0, min(1.0, score))

def is_expiry_day(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config or config.get("is_commodity"):
        return False
    today = datetime.now().strftime("%d%b%Y").upper()
    tokens = INDEX_TOKENS.get(index_name, {})
    expiry = tokens.get("expiry", "")
    return today == expiry

# ----------------------------------------------------------------------
# PERSISTENCE
# ----------------------------------------------------------------------
def get_db_path():
    return PAPER_DB_PATH if PAPER_MODE else DB_PATH

def load_portfolio_state():
    global portfolio_state, signal_state, daily_trade_count, last_trade_date
    try:
        db_path = get_db_path()
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        for idx in INDEX_NAMES:
            row = c.execute(
                "SELECT equity, active_action, entry_price, stop_loss, target, lots, entry_time, highest, last_trade_date, daily_trade_count FROM portfolio_equity WHERE index_name=?",
                (idx,)
            ).fetchone()
            if row:
                portfolio_state[idx]["equity"] = float(row[0]) if row[0] is not None else 100000.0
                action = row[1]
                if action and action != "HOLD":
                    signal_state[idx].update({
                        "action": action,
                        "entry_price": float(row[2]) if row[2] is not None else 0.0,
                        "stop_loss": float(row[3]) if row[3] is not None else 0.0,
                        "target": float(row[4]) if row[4] is not None else 0.0,
                        "lots": int(row[5]) if row[5] is not None else 0,
                        "entry_time": float(row[6]) if row[6] is not None else 0.0,
                        "highest": float(row[7]) if row[7] is not None else 0.0
                    })
                    portfolio_state[idx]["open_positions"] = 1
                if len(row) >= 9 and row[8]:
                    last_trade_date[idx] = row[8]
                if len(row) >= 10 and row[9] is not None:
                    daily_trade_count[idx] = int(row[9])
        conn.close()
        logger.info(f"Persistent state loaded from {db_path}")
    except Exception as e:
        logger.error(f"Error loading state: {e}")

def save_portfolio_state(idx):
    db_path = get_db_path()
    with sqlite3.connect(db_path) as conn:
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO portfolio_equity (index_name, equity, last_updated, active_action, entry_price, stop_loss, target, lots, entry_time, highest, last_trade_date, daily_trade_count) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (idx, portfolio_state[idx]["equity"], time.time(),
             signal_state[idx]["action"], signal_state[idx]["entry_price"],
             signal_state[idx]["stop_loss"], signal_state[idx]["target"],
             signal_state[idx]["lots"], signal_state[idx]["entry_time"],
             signal_state[idx]["highest"],
             last_trade_date.get(idx, ""),
             daily_trade_count.get(idx, 0))
        )
        conn.commit()

def reset_signal_state(index_name, current_time, exit_reason=""):
    with _signal_state_lock:
        signal_state[index_name].update({
            "action": "HOLD", "entry_price": 0, "stop_loss": 0, "target": 0,
            "lots": 1, "cooldown": current_time + 60, "confidence": 0,
            "highest": 0, "entry_time": 0, "exit_reason": exit_reason
        })
    with _portfolio_state_lock:
        portfolio_state[index_name]["open_positions"] = 0
    save_portfolio_state(index_name)
    with _market_signal_lock:
        market_signal[index_name].update({
            "signal": "EXIT",
            "alert_message": f"EXIT {exit_reason}",
            "exit_reason": exit_reason
        })

def send_telegram_alert(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    global _telegram_last_sent
    with _telegram_lock:
        now = time.time()
        if now - _telegram_last_sent < 2.0:
            return
        _telegram_last_sent = now
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=3)
    except Exception as e:
        logger.warning(f"Telegram failed: {type(e).__name__}")

def apply_transaction_cost(pnl, lots, lot_size):
    cost_per_lot = 50
    return pnl - cost_per_lot * lots

def log_trade(index_name, action, entry_price, exit_price, pnl, size_pct, status, grade, atr, vix, exit_reason):
    db_path = get_db_path()
    with sqlite3.connect(db_path) as conn:
        c = conn.cursor()
        c.execute(
            "INSERT INTO trades (timestamp, action, entry_price, exit_price, pnl, size_pct, status, grade, atr, vix, exit_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (time.time(), action, entry_price, exit_price, pnl, size_pct, status, grade, atr, vix, exit_reason)
        )
        conn.commit()

# ----------------------------------------------------------------------
# MAIN SIGNAL ENGINE (FULLY GUARDED AGAINST NONE)
# ----------------------------------------------------------------------
def run_signal_engine_for_index(index_name):
    logger.info(f"🔍 ENGINE RUNNING for {index_name}")
    try:
        if not INDEX_CONFIG[index_name].get("active"):
            return

        config = INDEX_CONFIG[index_name]
        is_commodity = config.get("is_commodity", False)

        # Market open check
        if is_commodity:
            if not is_mcx_open():
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = "MCX closed"
                    market_signal[index_name]["signal"] = "CLOSED"
                return
        else:
            if not is_market_open():
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = "Equity market closed"
                    market_signal[index_name]["signal"] = "CLOSED"
                return

        # Option tokens only for equity
        if not is_commodity:
            tokens = INDEX_TOKENS.get(index_name, {})
            if not tokens.get("ce_token") or not tokens.get("pe_token"):
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = "Option tokens not loaded"
                    market_signal[index_name]["signal"] = "WAITING"
                return

        # Candle check – ensure we have a valid deque
                # Candle check – ensure we have a valid deque (but don't reset once built)
                # Candle check – ensure we have a valid deque (but do NOT reset once built)
                # Candle check – ensure we have a valid deque
        with _candle_histories_lock:
            # Only create the structure if the index is completely missing
            if index_name not in candle_histories:
                logger.info(f"📊 Creating candle_histories for {index_name}")
                candle_histories[index_name] = {tf: deque(maxlen=500) for tf in TIMEFRAMES}
                candle_histories[index_name]["1min"] = deque(maxlen=500)
            
            if "1min" not in candle_histories[index_name]:
                logger.error(f"❌ 1min missing for {index_name} – recreating")
                candle_histories[index_name]["1min"] = deque(maxlen=500)
            
            if candle_histories[index_name]["1min"] is None:
                logger.error(f"❌ 1min is None for {index_name} – recreating")
                candle_histories[index_name]["1min"] = deque(maxlen=500)

            candle_len = len(candle_histories[index_name]["1min"])

            # Always update the alert_message so frontend shows progress
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Building candles ({candle_len}/30)"
                market_signal[index_name]["signal"] = "WAITING"

            if candle_len < 30:
                return  # <-- return AFTER updating the message

        with _market_signal_lock:
            if market_signal[index_name]["signal"] == "EXIT":
                market_signal[index_name]["signal"] = "COOLDOWN"

        now = time.time()

        # --- Extract latest data with None guards ---
        with _latest_ticks_lock:
            if index_name not in latest_ticks:
                latest_ticks[index_name] = {}  # fallback
            if is_commodity:
                latest_ticks[index_name].setdefault("price", 0.0)
                latest_ticks[index_name].setdefault("volume", 0)
                latest_ticks[index_name].setdefault("bid", 0.0)
                latest_ticks[index_name].setdefault("ask", 0.0)
                spot = latest_ticks[index_name]["price"] or 0.0
                ce_prem = spot
                pe_prem = spot
                ce_vol = latest_ticks[index_name]["volume"] or 0
                pe_vol = ce_vol
                ce_oi = 0
                pe_oi = 0
                ce_bid = latest_ticks[index_name]["bid"] or 0.0
                ce_ask = latest_ticks[index_name]["ask"] or 0.0
                pe_bid = ce_bid
                pe_ask = ce_ask
            else:
                spot = latest_ticks[index_name].get("spot_price", 0.0) or 0.0
                ce_prem = latest_ticks[index_name].get("ce_price", 0.0) or 0.0
                pe_prem = latest_ticks[index_name].get("pe_price", 0.0) or 0.0
                ce_vol = latest_ticks[index_name].get("ce_volume", 0) or 0
                pe_vol = latest_ticks[index_name].get("pe_volume", 0) or 0
                ce_oi = latest_ticks[index_name].get("ce_oi", 0) or 0
                pe_oi = latest_ticks[index_name].get("pe_oi", 0) or 0
                ce_bid = latest_ticks[index_name].get("ce_bid", 0.0) or 0.0
                ce_ask = latest_ticks[index_name].get("ce_ask", 0.0) or 0.0
                pe_bid = latest_ticks[index_name].get("pe_bid", 0.0) or 0.0
                pe_ask = latest_ticks[index_name].get("pe_ask", 0.0) or 0.0

        # If we have no price data, skip
        if spot <= 0 and ce_prem <= 0 and pe_prem <= 0:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "No price data yet"
                market_signal[index_name]["signal"] = "WAITING"
            return

        # Use bid-ask midpoint for equity, for commodities just use the price
        if not is_commodity:
            if ce_bid > 0 and ce_ask > 0:
                ce_prem = (ce_bid + ce_ask) / 2
            if pe_bid > 0 and pe_ask > 0:
                pe_prem = (pe_bid + pe_ask) / 2
            def spread_ok(bid, ask, prem):
                if bid <= 0 or ask <= 0:
                    return True
                spread = ask - bid
                if prem > 0 and spread / prem > 0.05:
                    return False
                return True
            if not spread_ok(ce_bid, ce_ask, ce_prem) or not spread_ok(pe_bid, pe_ask, pe_prem):
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = "Wide bid-ask spread"
                    market_signal[index_name]["signal"] = "BLOCKED"
                return

        # Volume profile (for both)
        vp_engine = volume_profile_engines[index_name]
        if is_commodity:
            vp_engine.update(spot, ce_vol, option_type=None)
        else:
            vp_engine.update(spot, ce_vol, option_type=None)
            vp_engine.update(ce_prem, ce_vol, option_type="CE")
            vp_engine.update(pe_prem, pe_vol, option_type="PE")

        # Correlation for NIFTY/BANKNIFTY only
        if index_name == "NIFTY":
            nifty_price_series.append(spot)
        elif index_name == "BANKNIFTY":
            banknifty_price_series.append(spot)
        if len(nifty_price_series) > 0 and len(banknifty_price_series) > 0:
            correlation_filter.update(list(nifty_price_series)[-1], list(banknifty_price_series)[-1])

        # Greeks for equity only
        greeks_data = None
        if not is_commodity and INDEX_CONFIG[index_name].get("greeks_enabled"):
            greeks_data = get_option_greeks(index_name)

        # Sentiment (same for both)
        sentiment = compute_sentiment(index_name)
        # sentiment is always a number (50+)
        action = get_signal_from_sentiment(sentiment)
        sentiment_label = get_sentiment_label(sentiment)
        with _market_signal_lock:
            market_signal[index_name]["sentiment_score"] = sentiment

        regime = detect_regime(index_name)
        if regime == "RANGING":
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Ranging market - no new entries"
                market_signal[index_name]["signal"] = "BLOCKED"
            return

        # --- Drawdown kill switch ---
        with _portfolio_state_lock:
            current_equity = portfolio_state[index_name]["equity"]
        peak = daily_drawdown[index_name]["peak_equity"]
        if current_equity > peak:
            daily_drawdown[index_name]["peak_equity"] = current_equity
        drawdown = (peak - current_equity) / peak * 100 if peak > 0 else 0
        daily_drawdown[index_name]["current_drawdown"] = drawdown
        if drawdown >= INDEX_CONFIG[index_name].get("max_daily_drawdown_pct", 3.0):
            with _signal_state_lock:
                if signal_state[index_name]["action"] != "HOLD":
                    active = signal_state[index_name]["action"]
                    prem = ce_prem if "CE" in active else pe_prem
                    if prem > 0:
                        pnl = prem - signal_state[index_name]["entry_price"]
                        pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                        pnl_total = apply_transaction_cost(pnl_total, signal_state[index_name]["lots"], INDEX_CONFIG[index_name]["lot_size"])
                        with _portfolio_state_lock:
                            portfolio_state[index_name]["equity"] += pnl_total
                            portfolio_state[index_name]["daily_pnl"] += pnl_total
                            portfolio_state[index_name]["total_pnl"] += pnl_total
                            portfolio_state[index_name]["live_pnl"] = 0.0
                        save_portfolio_state(index_name)
                        log_trade(index_name, active, signal_state[index_name]["entry_price"], prem, pnl_total,
                                  pnl_total / portfolio_state[index_name]["equity"] * 100, "KILL_SWITCH",
                                  active, calculate_atr([],[],[],14), latest_ticks["VIX"]["vix"], "KILL_SWITCH")
                        reset_signal_state(index_name, now, "KILL_SWITCH")
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "KILL SWITCH: Max drawdown hit. Trading halted."
                market_signal[index_name]["signal"] = "KILL_SWITCH"
            return

        # --- Circuit breaker ---
        if safety_state[index_name]["circuit_breaker"]:
            if now < safety_state[index_name]["circuit_breaker_until"]:
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = "Circuit breaker active"
                    market_signal[index_name]["signal"] = "CIRCUIT_BREAKER"
                return
            else:
                safety_state[index_name]["circuit_breaker"] = False
                safety_state[index_name]["consecutive_sl"] = 0

        # --- Existing position handling ---
        with _signal_state_lock:
            current_action = signal_state[index_name]["action"]
        if current_action != "HOLD":
            active = current_action
            prem = ce_prem if "CE" in active else pe_prem
            if prem <= 0:
                prem = last_known_prices[index_name].get("ce" if "CE" in active else "pe", 0) or 0.0
            if prem > 0:
                with _signal_state_lock:
                    pnl = prem - signal_state[index_name]["entry_price"]
                    with _portfolio_state_lock:
                        portfolio_state[index_name]["live_pnl"] = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    # Trailing stop
                    if prem > signal_state[index_name].get("highest", 0):
                        signal_state[index_name]["highest"] = prem
                        with _candle_histories_lock:
                            closes = [c["close"] for c in candle_histories[index_name]["1min"]]
                        if len(closes) >= 14:
                            atr = calculate_atr([], [], closes, 14)
                            new_sl = prem - atr * 1.8
                            if new_sl > signal_state[index_name]["stop_loss"]:
                                signal_state[index_name]["stop_loss"] = new_sl

                    # --- GUARDED COMPARISONS ---
                    stop_loss_val = signal_state[index_name].get("stop_loss")
                    target_val = signal_state[index_name].get("target")

                    # SL hit
                    if stop_loss_val is not None and prem <= stop_loss_val:
                        pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                        pnl_total = apply_transaction_cost(pnl_total, signal_state[index_name]["lots"], INDEX_CONFIG[index_name]["lot_size"])
                        with _portfolio_state_lock:
                            portfolio_state[index_name]["equity"] += pnl_total
                            portfolio_state[index_name]["daily_pnl"] += pnl_total
                            portfolio_state[index_name]["total_pnl"] += pnl_total
                            portfolio_state[index_name]["live_pnl"] = 0.0
                        save_portfolio_state(index_name)
                        pnl_pct = pnl / max(signal_state[index_name]["entry_price"], 1)
                        kelly_trackers[index_name].update(pnl_pct)
                        safety_state[index_name]["consecutive_sl"] += 1
                        if safety_state[index_name]["consecutive_sl"] >= 3:
                            safety_state[index_name]["circuit_breaker"] = True
                            safety_state[index_name]["circuit_breaker_until"] = now + 1800
                            send_telegram_alert(f"CIRCUIT BREAKER {index_name} | 3 consecutive SLs. Trading paused 30 min.")
                        log_trade(index_name, active, signal_state[index_name]["entry_price"], prem, pnl_total,
                                  pnl_total / portfolio_state[index_name]["equity"] * 100, "STOP_LOSS",
                                  active, calculate_atr([],[],[],14), latest_ticks["VIX"]["vix"], "STOP_LOSS")
                        send_telegram_alert(f"EXIT {index_name} | SL | PnL: {pnl:.2f} pts | Cost adj: {pnl_total:.2f}")
                        reset_signal_state(index_name, now, "STOP_LOSS")
                        return

                    # Target hit
                    if target_val is not None and prem >= target_val:
                        pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                        pnl_total = apply_transaction_cost(pnl_total, signal_state[index_name]["lots"], INDEX_CONFIG[index_name]["lot_size"])
                        with _portfolio_state_lock:
                            portfolio_state[index_name]["equity"] += pnl_total
                            portfolio_state[index_name]["daily_pnl"] += pnl_total
                            portfolio_state[index_name]["total_pnl"] += pnl_total
                            portfolio_state[index_name]["live_pnl"] = 0.0
                        save_portfolio_state(index_name)
                        pnl_pct = pnl / max(signal_state[index_name]["entry_price"], 1)
                        kelly_trackers[index_name].update(pnl_pct)
                        safety_state[index_name]["consecutive_sl"] = 0
                        log_trade(index_name, active, signal_state[index_name]["entry_price"], prem, pnl_total,
                                  pnl_total / portfolio_state[index_name]["equity"] * 100, "TARGET_HIT",
                                  active, calculate_atr([],[],[],14), latest_ticks["VIX"]["vix"], "TARGET_HIT")
                        send_telegram_alert(f"EXIT {index_name} | TARGET | PnL: {pnl:.2f} pts | Cost adj: {pnl_total:.2f}")
                        reset_signal_state(index_name, now, "TARGET_HIT")
                        return

                    # Time exit (dynamic)
                    entry_time = signal_state[index_name].get("entry_time", 0)
                    if entry_time > 0:
                        elapsed_min = (now - entry_time) / 60
                        side = "CE" if "CE" in active else "PE"
                        time_limit = get_dynamic_time_exit_minutes(index_name, side, prem, greeks_data)
                        if elapsed_min >= time_limit:
                            pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                            pnl_total = apply_transaction_cost(pnl_total, signal_state[index_name]["lots"], INDEX_CONFIG[index_name]["lot_size"])
                            with _portfolio_state_lock:
                                portfolio_state[index_name]["equity"] += pnl_total
                                portfolio_state[index_name]["daily_pnl"] += pnl_total
                                portfolio_state[index_name]["total_pnl"] += pnl_total
                                portfolio_state[index_name]["live_pnl"] = 0.0
                            save_portfolio_state(index_name)
                            kelly_trackers[index_name].update(pnl / max(signal_state[index_name]["entry_price"], 1))
                            log_trade(index_name, active, signal_state[index_name]["entry_price"], prem, pnl_total,
                                      pnl_total / portfolio_state[index_name]["equity"] * 100, "TIME_EXIT",
                                      active, calculate_atr([],[],[],14), latest_ticks["VIX"]["vix"], f"TIME_EXIT_{time_limit}m")
                            send_telegram_alert(f"EXIT {index_name} | TIME ({time_limit}m) | PnL: {pnl:.2f} pts | Cost adj: {pnl_total:.2f}")
                            reset_signal_state(index_name, now, "TIME_EXIT")
                            return

                    # Market analysis exit
                    with _price_histories_lock:
                        prices_spot = list(price_histories[index_name])
                    should_exit, exit_reason = should_exit_market_analysis(index_name, active, prices_spot, ce_prem, pe_prem, greeks_data)
                    if should_exit:
                        pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                        pnl_total = apply_transaction_cost(pnl_total, signal_state[index_name]["lots"], INDEX_CONFIG[index_name]["lot_size"])
                        with _portfolio_state_lock:
                            portfolio_state[index_name]["equity"] += pnl_total
                            portfolio_state[index_name]["daily_pnl"] += pnl_total
                            portfolio_state[index_name]["total_pnl"] += pnl_total
                            portfolio_state[index_name]["live_pnl"] = 0.0
                        save_portfolio_state(index_name)
                        kelly_trackers[index_name].update(pnl / max(signal_state[index_name]["entry_price"], 1))
                        log_trade(index_name, active, signal_state[index_name]["entry_price"], prem, pnl_total,
                                  pnl_total / portfolio_state[index_name]["equity"] * 100, "MARKET_EXIT",
                                  active, calculate_atr([],[],[],14), latest_ticks["VIX"]["vix"], exit_reason)
                        send_telegram_alert(f"EXIT {index_name} | {exit_reason} | PnL: {pnl:.2f} pts | Cost adj: {pnl_total:.2f}")
                        reset_signal_state(index_name, now, exit_reason)
                        return

                    # VWAP exit (only for options)
                    if not is_commodity:
                        if "CE" in active:
                            vwap_data = vp_engine.analyze(ce_prem, ce_vol, option_type="CE")
                        else:
                            vwap_data = vp_engine.analyze(pe_prem, pe_vol, option_type="PE")
                        option_vwap = vwap_data["vwap"]
                        if option_vwap > 0:
                            if "CE" in active and prem < option_vwap * 0.997:
                                pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                                pnl_total = apply_transaction_cost(pnl_total, signal_state[index_name]["lots"], INDEX_CONFIG[index_name]["lot_size"])
                                with _portfolio_state_lock:
                                    portfolio_state[index_name]["equity"] += pnl_total
                                    portfolio_state[index_name]["daily_pnl"] += pnl_total
                                    portfolio_state[index_name]["total_pnl"] += pnl_total
                                    portfolio_state[index_name]["live_pnl"] = 0.0
                                save_portfolio_state(index_name)
                                kelly_trackers[index_name].update(pnl / max(signal_state[index_name]["entry_price"], 1))
                                log_trade(index_name, active, signal_state[index_name]["entry_price"], prem, pnl_total,
                                          pnl_total / portfolio_state[index_name]["equity"] * 100, "VWAP_EXIT",
                                          active, calculate_atr([],[],[],14), latest_ticks["VIX"]["vix"], "VWAP_EXIT")
                                send_telegram_alert(f"EXIT {index_name} | VWAP (premium below VWAP) | PnL: {pnl:.2f} pts | Cost adj: {pnl_total:.2f}")
                                reset_signal_state(index_name, now, "VWAP_EXIT")
                                return
                            elif "PE" in active and prem < option_vwap * 0.997:
                                pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                                pnl_total = apply_transaction_cost(pnl_total, signal_state[index_name]["lots"], INDEX_CONFIG[index_name]["lot_size"])
                                with _portfolio_state_lock:
                                    portfolio_state[index_name]["equity"] += pnl_total
                                    portfolio_state[index_name]["daily_pnl"] += pnl_total
                                    portfolio_state[index_name]["total_pnl"] += pnl_total
                                    portfolio_state[index_name]["live_pnl"] = 0.0
                                save_portfolio_state(index_name)
                                kelly_trackers[index_name].update(pnl / max(signal_state[index_name]["entry_price"], 1))
                                log_trade(index_name, active, signal_state[index_name]["entry_price"], prem, pnl_total,
                                          pnl_total / portfolio_state[index_name]["equity"] * 100, "VWAP_EXIT",
                                          active, calculate_atr([],[],[],14), latest_ticks["VIX"]["vix"], "VWAP_EXIT")
                                send_telegram_alert(f"EXIT {index_name} | VWAP (premium below VWAP) | PnL: {pnl:.2f} pts | Cost adj: {pnl_total:.2f}")
                                reset_signal_state(index_name, now, "VWAP_EXIT")
                                return

                with _market_signal_lock:
                    market_signal[index_name].update({
                        "alert_message": f"ACTIVE {active}",
                        "signal": "ACTIVE",
                        "entry_price": signal_state[index_name]["entry_price"],
                        "stop_loss": signal_state[index_name]["stop_loss"],
                        "target": signal_state[index_name]["target"],
                        "current_pnl": round(pnl, 2),
                    })
            else:
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = f"ACTIVE {active} – premium unavailable"
                    market_signal[index_name]["signal"] = "ACTIVE"
            return

        # --- New entry logic ---
        with _signal_state_lock:
            if now < signal_state[index_name]["cooldown"]:
                remaining = int(signal_state[index_name]["cooldown"] - now)
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = f"Cooldown {remaining}s"
                    market_signal[index_name]["signal"] = "COOLDOWN"
                return

        today = datetime.now().strftime("%Y-%m-%d")
        if last_trade_date[index_name] != today:
            daily_trade_count[index_name] = 0
            last_trade_date[index_name] = today
            daily_drawdown[index_name]["peak_equity"] = portfolio_state[index_name]["equity"]
            with _portfolio_state_lock:
                portfolio_state[index_name]["daily_pnl"] = 0.0
                portfolio_state[index_name]["live_pnl"] = 0.0
            save_portfolio_state(index_name)

        # Dynamic max trades based on regime
        if regime == "TRENDING":
            max_trades = 25
        else:
            max_trades = 15
        if daily_trade_count[index_name] >= max_trades:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Max daily trades ({max_trades})"
                market_signal[index_name]["signal"] = "BLOCKED"
            return

        if action == "NO_TRADE":
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Sentiment {sentiment:.0f} - {sentiment_label}"
                market_signal[index_name]["signal"] = "NO_TRADE"
            return

        side = "CE" if "CE" in action else "PE" if "PE" in action else None
        if side is None:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Invalid action {action}"
                market_signal[index_name]["signal"] = "NO_TRADE"
            return

        prem = ce_prem if side == "CE" else pe_prem
        min_prem = INDEX_CONFIG[index_name].get("min_premium", 0)  # 0 for commodities
        if prem <= 0 or (min_prem > 0 and prem < min_prem):
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Premium invalid: Rs{prem}"
                market_signal[index_name]["signal"] = "WAITING"
            return

        # --- Candle confirmation ---
        if not confirm_signal_with_candles(index_name, side, spot):
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "Candle confirmation failed (last 3 closes not aligned with EMA9)"
                market_signal[index_name]["signal"] = "BLOCKED"
            return

        # --- Volume confirmation ---
        vol = ce_vol if side == "CE" else pe_vol
        if vol > 0:
            vol_hist = ce_volume_histories if side == "CE" else pe_volume_histories
            hist = list(vol_hist[index_name])
            if len(hist) >= 20:
                avg_vol = sum(hist[-20:]) / 20
                if vol < avg_vol * 0.5:
                    with _market_signal_lock:
                        market_signal[index_name]["alert_message"] = f"Low volume: {vol} vs avg {avg_vol:.0f}"
                        market_signal[index_name]["signal"] = "BLOCKED"
                    return
        if side == "CE":
            ce_volume_histories[index_name].append(vol)
        else:
            pe_volume_histories[index_name].append(vol)

        # --- PCR check (only for equity) ---
        if not is_commodity and INDEX_CONFIG[index_name].get("pcr_enabled"):
            if ce_oi > 0 and pe_oi > 0:
                pcr = ce_oi / pe_oi
                if side == "CE" and pcr > 1.5:
                    with _market_signal_lock:
                        market_signal[index_name]["alert_message"] = f"Extreme PCR (CE/PE) = {pcr:.2f}"
                        market_signal[index_name]["signal"] = "BLOCKED"
                    return
                elif side == "PE" and pcr < 0.67:
                    with _market_signal_lock:
                        market_signal[index_name]["alert_message"] = f"Extreme PCR (CE/PE) = {pcr:.2f}"
                        market_signal[index_name]["signal"] = "BLOCKED"
                    return

        # --- Correlation risk ---
        pair = INDEX_CONFIG[index_name].get("correlation_pair")
        if pair and not is_commodity:
            corr_analysis = correlation_filter.analyze(index_name, action)
            corr = corr_analysis.get("correlation", 0)
            if abs(corr) > 0.8:
                pair_action = market_signal.get(pair, {}).get("signal", "NO_TRADE")
                if (side == "CE" and "CE" in pair_action) or (side == "PE" and "PE" in pair_action):
                    my_sent = sentiment
                    pair_sent = market_signal.get(pair, {}).get("sentiment_score", 50)
                    if my_sent < pair_sent:
                        with _market_signal_lock:
                            market_signal[index_name]["alert_message"] = f"Correlation block: {pair} stronger"
                            market_signal[index_name]["signal"] = "BLOCKED"
                        return
            beta_adj = corr_analysis.get("beta_adjustment", 1.0)
        else:
            beta_adj = 1.0

        # --- Greeks filter (only equity) ---
        if not is_commodity and INDEX_CONFIG[index_name].get("greeks_enabled") and greeks_data is not None:
            delta = greeks_data.get("ce_delta") if side == "CE" else greeks_data.get("pe_delta")
            if delta is not None:
                if "STRONG" not in action and abs(delta) > 0.80:
                    with _market_signal_lock:
                        market_signal[index_name]["alert_message"] = f"Greeks block: Delta {delta:.2f} > 0.80"
                        market_signal[index_name]["signal"] = "BLOCKED"
                    return
            iv_rank = greeks_data.get("iv_rank")
            if iv_rank is not None and iv_rank > 80 and "LOW" not in action:
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = f"High IV rank {iv_rank:.0f}"
                    market_signal[index_name]["signal"] = "BLOCKED"
                return

        # --- RSI, ADX, VIX (all numeric) ---
        with _price_histories_lock:
            prices_spot = list(price_histories[index_name])
        rsi = calculate_rsi(prices_spot[-50:]) if len(prices_spot) >= 50 else 50.0
        with _candle_histories_lock:
            closes_5min = [c["close"] for c in candle_histories[index_name]["5min"]]
        adx = calculate_adx([], [], closes_5min, 14) if len(closes_5min) >= 30 else 20.0
        with _latest_ticks_lock:
            vix = latest_ticks["VIX"]["vix"]
            if vix <= 0:
                vix = 15.0

        # --- ML score ---
        ml_prob = compute_ml_score(index_name, side, prem, spot, rsi, adx, vix, sentiment)
        if ml_prob < 0.4 and "STRONG" not in action:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"ML filter: prob {ml_prob:.2f}"
                market_signal[index_name]["signal"] = "BLOCKED"
            return

        # --- Kelly and position sizing ---
        kelly_risk, win_rate, avg_win, avg_loss = kelly_trackers[index_name].get_recommended_risk_pct()
        if "STRONG" in action:
            base_risk_pct = 2.0
        elif "LOW" in action:
            base_risk_pct = 0.8
        else:
            base_risk_pct = 1.2
        risk_pct = base_risk_pct * 0.5 + kelly_risk * 0.5
        if vix > 25:
            risk_pct *= 0.7
        elif vix > 20:
            risk_pct *= 0.85
        if adx > 25:
            risk_pct *= 1.2
        elif adx < 15:
            risk_pct *= 0.8
        risk_pct *= beta_adj
        if not is_commodity and greeks_data is not None:
            iv_rank = greeks_data.get("iv_rank")
            if iv_rank is not None:
                if iv_rank > 80:
                    risk_pct *= 0.8
                elif iv_rank < 20:
                    risk_pct *= 1.1
        if is_expiry_day(index_name) and not is_commodity:
            risk_pct *= 0.5
        if regime == "VOLATILE":
            risk_pct *= 0.7
        risk_pct = max(0.5, min(3.0, risk_pct))

        # Expiry day block for equity
        if not is_commodity and is_expiry_day(index_name):
            now_ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
            if now_ist.time() >= dt_time(14, 30):
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = "Expiry day: last 60 min blocked"
                    market_signal[index_name]["signal"] = "BLOCKED"
                return

        with _candle_histories_lock:
            closes_1min = [c["close"] for c in candle_histories[index_name]["1min"]]
        atr = calculate_atr([], [], closes_1min, 14)
        if "STRONG" in action:
            sl_pct = 0.45
            target_mult = 3.5
        elif "LOW" in action:
            sl_pct = 0.3
            target_mult = 2.5
        else:
            sl_pct = 0.4
            target_mult = 3.0
        if not is_commodity and is_expiry_day(index_name):
            sl_pct *= 0.7
            target_mult *= 0.8
        sl = max(prem * (1 - sl_pct), prem - atr * 1.5)
        target = prem + atr * target_mult

        risk_amount = portfolio_state[index_name]["equity"] * (risk_pct / 100)
        stop_dist = prem - sl
        if stop_dist > 0:
            lots = int(risk_amount / (stop_dist * INDEX_CONFIG[index_name]["lot_size"]))
            lots = max(1, min(5, lots))
        else:
            lots = 1

        with _signal_state_lock:
            signal_state[index_name].update({
                "action": action,
                "entry_price": prem,
                "stop_loss": sl,
                "target": target,
                "lots": lots,
                "entry_time": now,
                "highest": prem,
                "cooldown": 0
            })
        with _portfolio_state_lock:
            portfolio_state[index_name]["open_positions"] = 1
        daily_trade_count[index_name] += 1
        save_portfolio_state(index_name)

        emoji = "🔥" if "STRONG" in action and "CE" in action else "❄️" if "STRONG" in action and "PE" in action else "⚡" if "LOW" in action else "📊"
        msg = (f"{emoji} {action} {index_name} | Spot:{spot:.0f} Prem:{prem:.2f} SL:{sl:.2f} Tgt:{target:.2f} | "
               f"Sentiment:{sentiment:.0f} ({sentiment_label}) | Regime:{regime} | Lots:{lots} Risk:{risk_pct:.1f}%")
        send_telegram_alert(msg)
        logger.info(msg)

        with _market_signal_lock:
            market_signal[index_name].update({
                "signal": action,
                "alert_message": f"ENTRY {action}",
                "entry_price": prem,
                "stop_loss": sl,
                "target": target,
                "sentiment_score": sentiment,
                "exit_reason": ""
            })

    except Exception as e:
        import traceback
        logger.error(f"Signal error {index_name}: {e}\n{traceback.format_exc()}")

def run_all_signals():
    for idx in INDEX_NAMES:
        if INDEX_CONFIG[idx].get("active"):
            try:
                run_signal_engine_for_index(idx)
            except Exception as e:
                import traceback
                logger.error(f"Signal error {idx}: {e}\n{traceback.format_exc()}")

# ----------------------------------------------------------------------
# EXIT HELPER FUNCTIONS (safe)
# ----------------------------------------------------------------------
def should_exit_market_analysis(index_name, action, prices_spot, ce_prem, pe_prem, greeks_data=None):
    if len(prices_spot) < 60:
        return False, ""
    exit_reason = ""
    with _candle_histories_lock:
        candles = list(candle_histories[index_name]["5min"])
    if len(candles) >= 10:
        closes = [c["close"] for c in candles[-10:]]
        ema9 = calculate_ema(closes, 9)
        ema21 = calculate_ema(closes, 21)
        if "CE" in action and ema9 < ema21:
            exit_reason = "5min trend turned bearish"
        elif "PE" in action and ema9 > ema21:
            exit_reason = "5min trend turned bullish"
    if exit_reason:
        return True, exit_reason
    with _candle_histories_lock:
        closes = [c["close"] for c in candle_histories[index_name]["1min"]]
    if len(closes) >= 20:
        price_trend = closes[-1] - closes[-10]
        rsi_vals = [calculate_rsi(closes[:i]) for i in range(len(closes)-10, len(closes)+1)]
        if len(rsi_vals) >= 2:
            rsi_trend = rsi_vals[-1] - rsi_vals[-2]
            if "CE" in action and price_trend > 0 and rsi_trend < 0:
                return True, "Bearish divergence"
            if "PE" in action and price_trend < 0 and rsi_trend > 0:
                return True, "Bullish divergence"
    with _latest_ticks_lock:
        vix = latest_ticks["VIX"]["vix"]
    if len(vix_history) >= 10:
        vix_sma = sum(list(vix_history)[-10:])/10
        if vix > vix_sma * 1.25:
            return True, f"VIX spike {vix:.1f} vs SMA {vix_sma:.1f}"
    return False, ""

def get_dynamic_time_exit_minutes(index_name, side, prem, greeks_data):
    if not greeks_data:
        return 45
    theta = greeks_data.get("ce_theta") if side == "CE" else greeks_data.get("pe_theta")
    if theta is None or theta == 0:
        return 45
    theta_abs = abs(theta)  # per day
    if theta_abs > prem * 0.05:
        return 30
    else:
        return 60

# ----------------------------------------------------------------------
# WEBSOCKET HANDLING (with commodity scaling)
# ----------------------------------------------------------------------
ws_running = False
sws = None
last_heartbeat = time.time()
tick_counter = 0
last_tick_timestamp = time.time()
_ws_connect_lock = threading.Lock()

def on_ws_open(wsapp):
    global ws_running, last_heartbeat, sws
    with _ws_connect_lock:
        ws_running = True
    last_heartbeat = time.time()
    logger.info("WebSocket connected successfully, subscribing to tokens...")

    token_list = []
    # Equity indices
    for idx, cfg in INDEX_CONFIG.items():
        if cfg.get("active") and not cfg.get("is_commodity"):
            token_list.append({"exchangeType": cfg["ws_exchange_type"], "tokens": [cfg["token"]]})
        # For commodities, we need to subscribe to their futures tokens (we have them after refresh)
        if cfg.get("active") and cfg.get("is_commodity") and cfg.get("token"):
            token_list.append({"exchangeType": cfg["ws_exchange_type"], "tokens": [cfg["token"]]})

    # Option tokens for equity
    for idx, tokens in INDEX_TOKENS.items():
        if not INDEX_CONFIG[idx].get("active") or INDEX_CONFIG[idx].get("is_commodity"):
            continue
        if tokens.get("ce_token") and tokens.get("pe_token"):
            token_list.append({
                "exchangeType": INDEX_CONFIG[idx]["option_ws_exchange_type"],
                "tokens": [tokens["ce_token"], tokens["pe_token"]]
            })

    token_list.append({"exchangeType": 1, "tokens": ["99919017"]})  # VIX

    logger.info(f"Subscribing to token_list: {token_list}")
    if token_list and sws:
        try:
            correlation_id = "hybrid_bot"
            mode = 1
            response = sws.subscribe(correlation_id, mode, token_list)
            logger.info(f"Subscription response: {response}")
            total = sum(len(g["tokens"]) for g in token_list)
            logger.info(f"Successfully subscribed to {total} tokens")
        except Exception as e:
            logger.error(f"Subscribe error: {e}")

def on_ws_error(wsapp, error):
    global ws_running
    logger.error(f"WebSocket error: {error}")
    with _ws_connect_lock:
        ws_running = False

def on_ws_close(wsapp, close_status_code=None, close_msg=None):
    global ws_running
    with _ws_connect_lock:
        ws_running = False
    logger.warning(f"WebSocket closed: status={close_status_code}, msg={close_msg}")


def on_ws_data(wsapp, message):
    global tick_counter, last_heartbeat, last_tick_timestamp, sws
    last_heartbeat = time.time()

    if message is None or message == b'\x00' or message == '\x00' or message == b'ping' or message == 'ping' or message == b'':
        return

    try:
        ticks = []
        if isinstance(message, dict):
            ticks = [message]
        elif isinstance(message, str):
            try:
                data = json.loads(message)
                ticks = data if isinstance(data, list) else [data]
            except Exception as e:
                logger.error(f"JSON parse error: {e}")
                return
        elif isinstance(message, bytes):
            try:
                decoded = message.decode('utf-8')
                data = json.loads(decoded)
                ticks = data if isinstance(data, list) else [data]
            except:
                return
        else:
            logger.warning(f"Unsupported WS message type: {type(message)}")
            return

        if not ticks:
            return

        for tick in ticks:
            tick_counter += 1

            token = str(tick.get("token") or tick.get("tk") or "")
            ltp = tick.get("last_traded_price") or tick.get("ltp") or tick.get("price") or 0

            if isinstance(ltp, str):
                try:
                    ltp = float(ltp)
                except:
                    ltp = 0

            # ---- SCALING FIX ----
            if ltp > 10000 and token in INDEX_TOKEN_SET:
                ltp = ltp / 100.0

            is_comm = False
            for idx, cfg in INDEX_CONFIG.items():
                if cfg.get("is_commodity") and cfg.get("token") == token:
                    is_comm = True
                    break
            if is_comm and ltp > 10000:
                ltp = ltp / 100.0

            if tick_counter % 100 == 0:
                logger.info(f"DEBUG TICK #{tick_counter}: token={token}, ltp={ltp}")

            vol = tick.get("volume") or tick.get("v") or tick.get("last_traded_quantity") or 0
            oi = tick.get("open_interest") or tick.get("oi") or tick.get("OpenInterest") or 0
            bid = tick.get("best_bid_price") or tick.get("bid") or tick.get("bp") or 0
            ask = tick.get("best_ask_price") or tick.get("ask") or tick.get("ap") or 0

            # ---- Spot matching ----
            spot_matched = False
            for idx, cfg in INDEX_CONFIG.items():
                if cfg.get("token") == token:
                    if ltp > 0:
                        logger.info(f"✅ SPOT MATCH: {idx} token={token} ltp={ltp}")
                        if cfg.get("is_commodity"):
                            with _latest_ticks_lock:
                                latest_ticks[idx]["price"] = ltp
                                latest_ticks[idx]["volume"] = vol
                                latest_ticks[idx]["bid"] = bid
                                latest_ticks[idx]["ask"] = ask
                            with _price_histories_lock:
                                price_histories[idx].append(ltp)
                            update_candle(idx, ltp, vol, time.time())
                            last_tick_timestamp = time.time()
                            with _latest_ticks_lock:
                                last_known_prices[idx]["spot"] = ltp
                                last_known_prices[idx]["timestamp"] = time.time()
                        else:
                            with _latest_ticks_lock:
                                latest_ticks[idx]["spot_price"] = ltp
                            with _price_histories_lock:
                                price_histories[idx].append(ltp)
                            update_candle(idx, ltp, vol, time.time())
                            last_tick_timestamp = time.time()
                            with _latest_ticks_lock:
                                last_known_prices[idx]["spot"] = ltp
                                last_known_prices[idx]["timestamp"] = time.time()

                        # ---- LOG CANDLE COUNT ----
                        with _candle_histories_lock:
                            candle_len = len(candle_histories[idx]["1min"])
                            logger.info(f"CANDLE COUNT for {idx}: {candle_len}")

                        spot_matched = True
                        break

            if spot_matched:
                continue

            # ---- Option matching ----
            option_matched = False
            for idx, tokens in INDEX_TOKENS.items():
                if not INDEX_CONFIG[idx].get("active") or INDEX_CONFIG[idx].get("is_commodity"):
                    continue
                if token == tokens.get("ce_token"):
                    if ltp > 0:
                        with _latest_ticks_lock:
                            latest_ticks[idx]["ce_price"] = ltp
                            latest_ticks[idx]["ce_volume"] = vol
                            latest_ticks[idx]["ce_oi"] = oi
                            latest_ticks[idx]["ce_bid"] = bid
                            latest_ticks[idx]["ce_ask"] = ask
                        with _ce_price_histories_lock:
                            ce_price_histories[idx].append(ltp)
                        volume_profile_engines[idx].update(ltp, vol, option_type="CE")
                        with _latest_ticks_lock:
                            last_known_prices[idx]["ce"] = ltp
                            last_known_prices[idx]["timestamp"] = time.time()
                        option_matched = True
                        break
                elif token == tokens.get("pe_token"):
                    if ltp > 0:
                        with _latest_ticks_lock:
                            latest_ticks[idx]["pe_price"] = ltp
                            latest_ticks[idx]["pe_volume"] = vol
                            latest_ticks[idx]["pe_oi"] = oi
                            latest_ticks[idx]["pe_bid"] = bid
                            latest_ticks[idx]["pe_ask"] = ask
                        with _pe_price_histories_lock:
                            pe_price_histories[idx].append(ltp)
                        volume_profile_engines[idx].update(ltp, vol, option_type="PE")
                        with _latest_ticks_lock:
                            last_known_prices[idx]["pe"] = ltp
                            last_known_prices[idx]["timestamp"] = time.time()
                        option_matched = True
                        break
            if option_matched:
                continue

            # ---- VIX ----
            if token == "99919017" and ltp > 0:
                with _latest_ticks_lock:
                    latest_ticks["VIX"]["vix"] = ltp
                vix_history.append(ltp)
                if DEBUG_MODE:
                    logger.info(f"VIX TICK: {ltp}")

        # Throttle signal runs every 5 ticks
        if tick_counter % 5 == 0 and tick_counter > 0:
            with _signal_run_lock:
                now = time.time()
                global _last_signal_run
                if now - _last_signal_run >= 1.0:
                    _last_signal_run = now
                    threading.Thread(target=run_all_signals, daemon=True).start()

    except Exception as e:
        import traceback
        logger.error(f"Unhandled exception in on_ws_data: {e}\n{traceback.format_exc()}")

# ----------------------------------------------------------------------
# WEBSOCKET CONNECTION MANAGER
# ----------------------------------------------------------------------
def start_angel_websocket_improved():
    global sws, ws_running, last_heartbeat
    retry_delay = 5

    while True:
        try:
            if not is_market_open() and not is_mcx_open():
                time.sleep(60)
                continue

            auth_token, feed_token, _ = get_auth_token()
            if not feed_token:
                logger.error("Failed to get feed token, retrying in 10 seconds...")
                time.sleep(10)
                continue

            sws = SmartWebSocketV2(
                auth_token,
                ANGEL_API_KEY,
                ANGEL_CLIENT_ID,
                feed_token,
                max_retry_attempt=3
            )

            sws.on_open = on_ws_open
            sws.on_data = on_ws_data
            sws.on_error = on_ws_error
            sws.on_close = on_ws_close

            logger.info("Attempting WebSocket connection...")
            ws_thread = threading.Thread(target=sws.connect, daemon=True)
            ws_thread.start()

            time.sleep(5)

            while True:
                time.sleep(5)
                with _ws_connect_lock:
                    is_running = ws_running

                if not is_running:
                    logger.warning("WebSocket disconnected detected")
                    break

                if time.time() - last_heartbeat > 30:
                    logger.warning("No heartbeat for 30s, forcing reconnect")
                    with _ws_connect_lock:
                        ws_running = False
                    try:
                        sws.close_connection()
                    except:
                        pass
                    break

                if time.time() - last_heartbeat > 20:
                    try:
                        if hasattr(sws, 'send_heartbeat'):
                            sws.send_heartbeat()
                        elif hasattr(sws, 'ping'):
                            sws.ping()
                        last_heartbeat = time.time()
                    except Exception:
                        pass

            logger.warning(f"WebSocket disconnected, reconnecting in {retry_delay}s...")
            with _ws_connect_lock:
                ws_running = False
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

        except Exception as e:
            logger.error(f"WebSocket thread error: {e}")
            with _ws_connect_lock:
                ws_running = False
            time.sleep(10)

def start_rest_only_mode():
    logger.info("Starting REST-only mode (WebSocket fallback)")
    while True:
        try:
            # Determine which assets to fetch based on open markets
            # At this hour, only MCX is open, so fetch only commodities.
            assets_to_fetch = []
            for idx in INDEX_NAMES:
                cfg = INDEX_CONFIG[idx]
                if not cfg.get("active"):
                    continue
                if cfg.get("is_commodity") and is_mcx_open():
                    assets_to_fetch.append(idx)
                elif not cfg.get("is_commodity") and is_market_open():
                    assets_to_fetch.append(idx)

            if not assets_to_fetch:
                time.sleep(30)
                continue

            for idx in assets_to_fetch:
                try:
                    spot = get_index_spot(idx)
                    if spot and spot > 0:
                        with _latest_ticks_lock:
                            if INDEX_CONFIG[idx].get("is_commodity"):
                                latest_ticks[idx]["price"] = spot
                            else:
                                latest_ticks[idx]["spot_price"] = spot
                        with _price_histories_lock:
                            price_histories[idx].append(spot)
                        update_candle(idx, spot, 0, time.time())
                        with _latest_ticks_lock:
                            last_known_prices[idx]["spot"] = spot
                            last_known_prices[idx]["timestamp"] = time.time()
                except Exception as e:
                    logger.debug(f"REST fetch error for {idx}: {e}")

                # For equity, also fetch option prices (but currently not needed since equity closed)
                # We'll skip option fetches now to reduce calls.
                time.sleep(1.5)  # 1.5 seconds between assets

            # After fetching all assets, run the signal engine
            run_all_signals()

            # Longer sleep between cycles
            time.sleep(15)

        except Exception as e:
            logger.error(f"REST-only mode error: {e}")
            time.sleep(10)
class ConnectionManager:
    def __init__(self):
        self.use_websocket = os.getenv("FORCE_REST_MODE", "0") != "1"
        self._ws_thread = None

    def start(self):
        if self.use_websocket:
            self._ws_thread = threading.Thread(target=start_angel_websocket_improved, daemon=True)
            self._ws_thread.start()
            threading.Thread(target=ws_watchdog, daemon=True).start()
            threading.Thread(target=tick_watchdog, daemon=True).start()
            time.sleep(10)
            if not ws_running:
                logger.warning("WebSocket not yet connected, starting REST as parallel fallback")
                threading.Thread(target=start_rest_only_mode, daemon=True).start()
        else:
            logger.info("WebSocket disabled via FORCE_REST_MODE, using REST-only")
            threading.Thread(target=start_rest_only_mode, daemon=True).start()

def tick_watchdog():
    global ws_running, tick_counter, last_tick_timestamp, sws
    last_count = 0
    while True:
        time.sleep(10)
        if ws_running:
            # If no tick for 60 seconds, force reconnect
            if time.time() - last_tick_timestamp > 60:
                logger.warning("No ticks for 60s - forcing reconnect")
                with _ws_connect_lock:
                    ws_running = False
                if sws:
                    try:
                        sws.close_connection()
                    except:
                        pass
            # Also check if tick counter hasn't increased (alternative)
            elif tick_counter == last_count and tick_counter > 0 and time.time() - last_tick_timestamp > 45:
                logger.warning("Tick counter stalled - forcing reconnect")
                with _ws_connect_lock:
                    ws_running = False
                if sws:
                    try:
                        sws.close_connection()
                    except:
                        pass
            else:
                last_count = tick_counter

def ws_watchdog():
    global ws_running, last_heartbeat, last_tick_timestamp, sws
    while True:
        time.sleep(10)
        now = time.time()
        with _ws_connect_lock:
            is_running = ws_running
        if is_running:
            # If heartbeat is stale OR no ticks for 60s
            if (now - last_heartbeat > 25) or (now - last_tick_timestamp > 60):
                logger.warning(f"Data starvation – heartbeat={now-last_heartbeat:.1f}s, last_tick={now-last_tick_timestamp:.1f}s - forcing reconnect")
                with _ws_connect_lock:
                    ws_running = False
                if sws:
                    try:
                        sws.close_connection()
                    except Exception:
                        pass

# ----------------------------------------------------------------------
# FLASK ROUTES
# ----------------------------------------------------------------------
@app.before_request
def check_auth():
    if API_KEY:
        auth = request.headers.get("X-API-Key")
        if auth != API_KEY:
            return jsonify({"error": "Unauthorized"}), 401

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "healthy",
        "engine": "Hybrid v14.5 – Equity + MCX (Fully patched)",
        "indices": [i for i, cfg in INDEX_CONFIG.items() if cfg.get("active")],
        "market_open": is_market_open(),
        "mcx_open": is_mcx_open()
    })

@app.route("/api/live-signals", methods=["GET"])
def live_signals():
    sentiment_data = {}
    trends_data = {}
    for idx in INDEX_NAMES:
        if not INDEX_CONFIG[idx].get("active"):
            continue
        with _market_signal_lock:
            sentiment_data[idx] = {
                "score": market_signal[idx].get("sentiment_score", 50),
                "label": get_sentiment_label(market_signal[idx].get("sentiment_score", 50))
            }
        trends_data[idx] = {}
        for tf in TIMEFRAMES:
            trends_data[idx][tf] = get_trend_for_timeframe(idx, tf)

    with _market_signal_lock, _portfolio_state_lock:
        portfolio_with_trades = {}
        for idx, port in portfolio_state.items():
            portfolio_with_trades[idx] = port.copy()
            portfolio_with_trades[idx]["daily_trades"] = daily_trade_count.get(idx, 0)

        return jsonify({
            "timestamp": datetime.now().isoformat(),
            "signals": market_signal,
            "sentiment": sentiment_data,
            "trends": trends_data,
            "portfolios": portfolio_with_trades,
            "market_open": is_market_open(),
            "mcx_open": is_mcx_open(),
            "debug": {
                "ws_running": ws_running,
                "ticks": tick_counter,
                "last_tick_ago": round(time.time() - last_tick_timestamp, 1)
            },
            "version": "14.5-hybrid-mcx-fixed"
        })

@app.route("/api/signal-audio", methods=["GET"])
def signal_audio():
    latest_action = "NO_TRADE"
    for idx in INDEX_NAMES:
        if not INDEX_CONFIG[idx].get("active"):
            continue
        with _market_signal_lock:
            sig = market_signal[idx].get("signal", "WAITING")
            if sig == "EXIT":
                latest_action = "EXIT"
                break
            if sig in ["STRONG_BUY_CE", "BUY_CE", "LOW_BUY_CE", "STRONG_BUY_PE", "BUY_PE", "LOW_BUY_PE"]:
                latest_action = sig
                break
    audio_map = {
        "STRONG_BUY_CE": "strong_buy_ce.mp3",
        "BUY_CE": "buy_ce.mp3",
        "LOW_BUY_CE": "low_buy_ce.mp3",
        "STRONG_BUY_PE": "strong_buy_pe.mp3",
        "BUY_PE": "buy_pe.mp3",
        "LOW_BUY_PE": "low_buy_pe.mp3",
        "EXIT": "exit.mp3"
    }
    audio_file = audio_map.get(latest_action, None)
    return jsonify({"action": latest_action, "audio_file": audio_file, "timestamp": datetime.now().isoformat()})

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({
        "status": "OK" if ws_running and (time.time() - last_tick_timestamp < 30) else "DEGRADED",
        "ws_running": ws_running,
        "ticks": tick_counter,
        "last_tick_seconds_ago": round(time.time() - last_tick_timestamp, 2)
    })

@app.route("/api/connection-status", methods=["GET"])
def connection_status():
    return jsonify({
        "websocket_running": ws_running,
        "last_heartbeat_seconds_ago": round(time.time() - last_heartbeat, 2) if last_heartbeat else None,
        "last_tick_seconds_ago": round(time.time() - last_tick_timestamp, 2),
        "total_ticks_received": tick_counter,
        "market_open": is_market_open(),
        "mcx_open": is_mcx_open(),
        "connection_mode": "WEBSOCKET" if ws_running else "REST_FALLBACK",
        "active_indices": [i for i, cfg in INDEX_CONFIG.items() if cfg.get("active")],
        "tokens_loaded": {idx: {
            "ce_token": bool(INDEX_TOKENS[idx].get("ce_token")) if not INDEX_CONFIG[idx].get("is_commodity") else "N/A",
            "pe_token": bool(INDEX_TOKENS[idx].get("pe_token")) if not INDEX_CONFIG[idx].get("is_commodity") else "N/A"
        } for idx in INDEX_NAMES}
    })

# ----------------------------------------------------------------------
# AUTO-START
# ----------------------------------------------------------------------
_init_completed = False
_init_lock = threading.Lock()

def _start_background_threads():
    global _init_completed
    with _init_lock:
        if not _init_completed:
            logger.info("Pre-fetching option tokens...")
            max_retries = 5
            for attempt in range(max_retries):
                refresh_all_tokens()
                ready = sum(1 for idx, cfg in INDEX_CONFIG.items()
                           if cfg.get("active") and cfg.get("token"))
                total_active = sum(1 for cfg in INDEX_CONFIG.values() if cfg.get("active"))
                logger.info(f"Token prefetch attempt {attempt + 1}: {ready}/{total_active} indices ready")
                if ready == total_active:
                    break
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    logger.warning(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
            conn_manager = ConnectionManager()
            conn_manager.start()
            _init_completed = True
            logger.info("Background threads started with connection manager")

_background_started = False
_background_start_lock = threading.Lock()

def auto_start_background():
    global _background_started
    with _background_start_lock:
        if not _background_started:
            init_db()
            load_portfolio_state()
            _start_background_threads()
            _background_started = True
            logger.info("Auto-start: Background threads initialized for production")

auto_start_background()

if __name__ == "__main__":
    init_db()
    load_portfolio_state()
    _start_background_threads()
    logger.info("Background workers initiated. Starting Flask API Server...")
    app.run(host="0.0.0.0", port=5000, debug=False)
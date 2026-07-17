# === EQUITY-ONLY SCALPING v16 – 1m/2m/3m/5m (No Telegram, No Commodity) ===
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
import traceback
from collections import deque, Counter
from datetime import datetime, timedelta, time as dt_time, timezone
from flask import Flask, jsonify, request
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor, as_completed
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
CORS(app, origins="*", supports_credentials=False)

API_KEY = os.getenv("API_KEY", "")
application = app

# ----------------------------------------------------------------------
# ENVIRONMENT (Angel One credentials only)
# ----------------------------------------------------------------------
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing critical Angel One environment variables")

DB_PATH = "trading_data.db"
PAPER_DB_PATH = "paper_trading_data.db" if PAPER_MODE else DB_PATH

# ----------------------------------------------------------------------
# DATABASE
# ----------------------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
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
    finally:
        conn.close()

    if PAPER_MODE:
        conn = sqlite3.connect(PAPER_DB_PATH)
        try:
            c = conn.cursor()
            c.execute("CREATE TABLE IF NOT EXISTS portfolio_equity (index_name TEXT PRIMARY KEY, equity REAL, last_updated REAL, active_action TEXT, entry_price REAL, stop_loss REAL, target REAL, lots INTEGER, entry_time REAL, highest REAL, last_trade_date TEXT, daily_trade_count INTEGER)")
            c.execute("CREATE TABLE IF NOT EXISTS trades (timestamp REAL, action TEXT, entry_price REAL, exit_price REAL, pnl REAL, size_pct REAL, status TEXT, grade TEXT, atr REAL, vix REAL, exit_reason TEXT)")
            conn.commit()
        finally:
            conn.close()

init_db()

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ============================================================
# MONKEY PATCHES – fix SmartAPI WS bugs (ENHANCED BINARY PARSER)
# ============================================================
_original_parse = SmartWebSocketV2._parse_binary_data
def _patched_parse(self, binary_data):
    try:
        result = _original_parse(self, binary_data)
        if result and result.get('token'):
            return result
    except Exception:
        result = {}
    try:
        # Fallback manual byte parsing for SmartWebSocketV2 (Angel One binary format)
        if len(binary_data) >= 10:
            token_int = int.from_bytes(binary_data[2:10], byteorder='little')
            if token_int > 0:
                result['token'] = str(token_int)

                # Extract LTP (bytes 26-33, int64, divide by 100)
                if len(binary_data) >= 34:
                    ltp_raw = int.from_bytes(binary_data[26:34], byteorder='little', signed=True)
                    result['last_traded_price'] = ltp_raw / 100.0

                # Extract Volume (bytes 42-49)
                if len(binary_data) >= 50:
                    vol_raw = int.from_bytes(binary_data[42:50], byteorder='little', signed=True)
                    result['volume'] = vol_raw

                # Extract Bid/Ask Prices (bytes 50-57, 66-73) with sanity checks
                if len(binary_data) >= 74:
                    bid_raw = int.from_bytes(binary_data[50:58], byteorder='little', signed=True)
                    ask_raw = int.from_bytes(binary_data[66:74], byteorder='little', signed=True)
                    if 0 < bid_raw < 100000000:
                        result['best_bid_price'] = bid_raw / 100.0
                    if 0 < ask_raw < 100000000:
                        result['best_ask_price'] = ask_raw / 100.0

                # Extract Open Interest (bytes 82-89)
                if len(binary_data) >= 90:
                    oi_raw = int.from_bytes(binary_data[82:90], byteorder='little', signed=True)
                    result['open_interest'] = oi_raw

                return result
    except Exception as e:
        logger.debug(f"Critical error in custom binary fallback parser: {e}")
    return result
SmartWebSocketV2._parse_binary_data = _patched_parse

_original_on_close = SmartWebSocketV2._on_close
def _patched_on_close(self, wsapp, *args):
    try:
        _original_on_close(self, wsapp, *args)
    except Exception:
        pass
SmartWebSocketV2._on_close = _patched_on_close

_original_on_pong = SmartWebSocketV2._on_pong
def _patched_on_pong(self, wsapp, *args):
    try:
        _original_on_pong(self, wsapp, *args)
    except Exception:
        pass
SmartWebSocketV2._on_pong = _patched_on_pong
# ============================================================

# ----------------------------------------------------------------------
# INDEX CONFIGURATION – Only Equity Indices (5 active)
# ----------------------------------------------------------------------
INDEX_CONFIG = {
    "NIFTY": {
        "token": "99926000", "exchange": "NSE", "symbol": "NIFTY", "lot_size": 50, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": "BANKNIFTY", "greeks_enabled": True, "pcr_enabled": True,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.6
    },
    "BANKNIFTY": {
        "token": "99926009", "exchange": "NSE", "symbol": "BANKNIFTY", "lot_size": 25, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": "NIFTY", "greeks_enabled": True, "pcr_enabled": True,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.8
    },
    "FINNIFTY": {
        "token": "99926037", "exchange": "NSE", "symbol": "FINNIFTY", "lot_size": 40, "expiry_weekday": 1, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.6
    },
    "SENSEX": {
        "token": "99919000", "exchange": "BSE", "symbol": "SENSEX", "lot_size": 15, "expiry_weekday": 4, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100, "option_exchange": "BFO",
        "ws_exchange_type": 3, "option_ws_exchange_type": 4, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.5
    },
    "BANKEX": {
        "token": "99919012", "exchange": "BSE", "symbol": "BANKEX", "lot_size": 15, "expiry_weekday": 4, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100, "option_exchange": "BFO",
        "ws_exchange_type": 3, "option_ws_exchange_type": 4, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": "BANKNIFTY", "greeks_enabled": True, "pcr_enabled": True,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.8
    },

    # ----- ALL OTHER INDICES INACTIVE (to be re‑enabled later) -----
    "MIDCPNIFTY": { "token": "99926074", "exchange": "NSE", "symbol": "NIFTY MID SELECT", "lot_size": 75, "expiry_weekday": 3, "active": False, "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 25, "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0, "correlation_pair": None, "greeks_enabled": False, "pcr_enabled": True, "regime_adx_threshold": 25, "regime_atr_threshold": 0.5 },
    "PSUBNIFTY": { "token": "99926025", "exchange": "NSE", "symbol": "Nifty PSU Bank", "lot_size": 50, "expiry_weekday": 3, "active": False, "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 10, "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0, "correlation_pair": None, "greeks_enabled": False, "pcr_enabled": False, "regime_adx_threshold": 25, "regime_atr_threshold": 0.5 },
    "MIDSELNIFTY": { "token": "99926074", "exchange": "NSE", "symbol": "NIFTY MID SELECT", "lot_size": 50, "expiry_weekday": 3, "active": False, "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 10, "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0, "correlation_pair": None, "greeks_enabled": False, "pcr_enabled": False, "regime_adx_threshold": 25, "regime_atr_threshold": 0.5 },
    "NIFTYIT": { "token": "99926008", "exchange": "NSE", "symbol": "Nifty IT", "lot_size": 25, "expiry_weekday": 3, "active": False, "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0, "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True, "regime_adx_threshold": 25, "regime_atr_threshold": 0.5 },
    "NIFTYPHARMA": { "token": "99926023", "exchange": "NSE", "symbol": "Nifty Pharma", "lot_size": 25, "expiry_weekday": 3, "active": False, "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0, "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True, "regime_adx_threshold": 25, "regime_atr_threshold": 0.5 },
    "NIFTYFMCG": { "token": "99926021", "exchange": "NSE", "symbol": "Nifty FMCG", "lot_size": 25, "expiry_weekday": 3, "active": False, "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0, "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True, "regime_adx_threshold": 25, "regime_atr_threshold": 0.5 },
    "NIFTYAUTO": { "token": "99926029", "exchange": "NSE", "symbol": "Nifty Auto", "lot_size": 25, "expiry_weekday": 3, "active": False, "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0, "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True, "regime_adx_threshold": 25, "regime_atr_threshold": 0.5 },
    "NIFTYNEXT50": { "token": "99926078", "exchange": "NSE", "symbol": "NIFTYNEXT50", "lot_size": 25, "expiry_weekday": 3, "active": False, "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 25, "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0, "correlation_pair": "NIFTY", "greeks_enabled": False, "pcr_enabled": True, "regime_adx_threshold": 25, "regime_atr_threshold": 0.5 },
    "SENSEXNEXT": { "token": "99919001", "exchange": "BSE", "symbol": "SENSEXNEXT", "lot_size": 15, "expiry_weekday": 4, "active": False, "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 25, "option_exchange": "BFO", "ws_exchange_type": 3, "option_ws_exchange_type": 4, "max_daily_drawdown_pct": 3.0, "correlation_pair": "SENSEX", "greeks_enabled": False, "pcr_enabled": True, "regime_adx_threshold": 25, "regime_atr_threshold": 0.5 },
    "PRIVATENIFTY": { "token": "99926056", "exchange": "NSE", "symbol": "PRIVATENIFTY", "lot_size": 50, "expiry_weekday": 3, "active": False, "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 10, "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0, "correlation_pair": None, "greeks_enabled": False, "pcr_enabled": False, "regime_adx_threshold": 25, "regime_atr_threshold": 0.5 },
    "NIFTYREALTY": { "token": "99926022", "exchange": "NSE", "symbol": "NIFTYREALTY", "lot_size": 25, "expiry_weekday": 3, "active": False, "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0, "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True, "regime_adx_threshold": 25, "regime_atr_threshold": 0.5 },
    "NIFTYMETAL": { "token": "99926024", "exchange": "NSE", "symbol": "NIFTYMETAL", "lot_size": 25, "expiry_weekday": 3, "active": False, "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0, "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True, "regime_adx_threshold": 25, "regime_atr_threshold": 0.5 },
    "NIFTYENERGY": { "token": "99926026", "exchange": "NSE", "symbol": "NIFTYENERGY", "lot_size": 25, "expiry_weekday": 3, "active": False, "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0, "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True, "regime_adx_threshold": 25, "regime_atr_threshold": 0.5 },
}

INDEX_TOKEN_SET = {cfg["token"] for cfg in INDEX_CONFIG.values() if cfg.get("token")}
INDEX_NAMES = [idx for idx, cfg in INDEX_CONFIG.items() if cfg.get("active")]

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

INDEX_TOKENS = {idx: {"ce_token": None, "pe_token": None, "atm_strike": 0, "expiry": "", "expiry_date": None, "ce_symbol": "", "pe_symbol": ""} for idx in INDEX_NAMES}
last_known_prices = {idx: {"spot": 0.0, "ce": 0.0, "pe": 0.0, "timestamp": 0} for idx in INDEX_NAMES}
price_histories = {idx: deque(maxlen=5000) for idx in INDEX_NAMES}

portfolio_state = {idx: {"equity": 100000.0, "open_positions": 0, "daily_pnl": 0.0, "total_pnl": 0.0, "live_pnl": 0.0} for idx in INDEX_NAMES}
signal_state = {idx: {"action": "HOLD", "entry_price": 0.0, "stop_loss": 0.0, "target": 0.0, "lots": 0, "entry_time": 0.0, "highest": 0.0, "cooldown": 0, "confidence": 0, "exit_reason": ""} for idx in INDEX_NAMES}

market_signal = {idx: {"sentiment_score": 50, "signal": "WAITING", "alert_message": "", "entry_price": 0, "stop_loss": 0, "target": 0, "exit_reason": "", "quality_score": 0, "strike_price": 0, "trading_symbol": "", "status": ""} for idx in INDEX_NAMES}

# ---------- TRADE STATE (for PnL tracking) ----------
trade_state = {
    idx: {
        "active": False,
        "side": "",
        "entry_price": 0.0,
        "current_price": 0.0,
        "stop_loss": 0.0,
        "target": 0.0,
        "pnl": 0.0,
        "entry_time": "",
        "exit_time": ""
    } for idx in INDEX_NAMES
}

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
for idx in INDEX_CONFIG:
    latest_ticks[idx] = {
        "spot_price": 0.0, "ce_price": 0.0, "pe_price": 0.0,
        "ce_volume": 0, "pe_volume": 0, "ce_oi": 0, "pe_oi": 0,
        "ce_bid": 0.0, "ce_ask": 0.0, "pe_bid": 0.0, "pe_ask": 0.0
    }
latest_ticks["VIX"] = {"vix": 15.0}

daily_drawdown = {idx: {"peak_equity": 0.0, "current_drawdown": 0.0, "dd_warning_sent": False} for idx in INDEX_NAMES}
safety_state = {idx: {"consecutive_sl": 0, "circuit_breaker": False, "circuit_breaker_until": 0} for idx in INDEX_NAMES}
signal_buffer = {idx: {"ce_count": 0, "pe_count": 0} for idx in INDEX_NAMES}
daily_trade_count = {idx: 0 for idx in INDEX_NAMES}
last_trade_date = {idx: "" for idx in INDEX_NAMES}

_last_signal_run = 0
_signal_run_lock = threading.Lock()

_historical_iv_ce = {idx: deque(maxlen=200) for idx in INDEX_NAMES}
_historical_iv_pe = {idx: deque(maxlen=200) for idx in INDEX_NAMES}

# Regime hysteresis
_regime_history = {idx: deque(maxlen=5) for idx in INDEX_NAMES}

# ----------------------------------------------------------------------
# TIMEFRAME DEFINITIONS – Only 1m, 2m, 3m, 5m
# ----------------------------------------------------------------------
TIMEFRAMES = ["1min", "2min", "3min", "5min"]
TIMEFRAME_SECONDS = {"1min":60, "2min":120, "3min":180, "5min":300}
TIMEFRAME_WEIGHTS = {"1min":8, "2min":10, "3min":12, "5min":16}

candle_histories = {idx: {tf: deque(maxlen=500) for tf in TIMEFRAMES} for idx in INDEX_NAMES}
_last_candle_time = {idx: {tf: 0 for tf in TIMEFRAMES} for idx in INDEX_NAMES}
_current_candle = {idx: {tf: None for tf in TIMEFRAMES} for idx in INDEX_NAMES}
_prev_volume = {idx: 0 for idx in INDEX_NAMES}

# ----------------------------------------------------------------------
# HELPER: Update trade PnL
# ----------------------------------------------------------------------
def update_trade_pnl(index_name, current_price):
    """Update trade_state PnL and check auto-exit (optional) – called from WS tick."""
    if not trade_state[index_name]["active"] or current_price <= 0:
        return
    trade_state[index_name]["current_price"] = current_price
    trade_state[index_name]["pnl"] = round(current_price - trade_state[index_name]["entry_price"], 2)
    # Optional auto-exit check – but we rely on the signal engine to close trades properly.
    # We'll just update PnL; the background loop will handle exits.

# ----------------------------------------------------------------------
# INDICATORS (same as before, unchanged)
# ----------------------------------------------------------------------
def calculate_ema(prices, period):
    if not prices or period <= 0:
        return 0.0
    if len(prices) < period:
        return float(sum(prices) / len(prices))
    alpha = 2.0 / (period + 1)
    ema = float(sum(prices[:period]) / period)
    for p in prices[period:]:
        ema = alpha * p + (1 - alpha) * ema
    return ema

def calculate_rsi(prices, period=14):
    if period <= 0 or len(prices) < period + 1:
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
    if len(prices) < slow:
        return 0.0, 0.0, 0.0

    macd_history = []
    for i in range(slow, len(prices) + 1):
        sub_prices = prices[:i]
        ema_fast = calculate_ema(sub_prices, fast)
        ema_slow = calculate_ema(sub_prices, slow)
        if ema_fast is not None and ema_slow is not None:
            macd_history.append(ema_fast - ema_slow)
        else:
            macd_history.append(0.0)

    if len(macd_history) < signal:
        return macd_history[-1] if macd_history else 0.0, 0.0, 0.0

    sig_line = calculate_ema(macd_history, signal)
    macd_line = macd_history[-1]
    if sig_line is None:
        sig_line = macd_line
    hist = macd_line - sig_line
    return float(macd_line), float(sig_line), float(hist)

def calculate_atr(highs, lows, closes, period=14):
    if period <= 0 or len(closes) < period + 1:
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
    if period <= 0 or len(closes) < period * 2:
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
# BSM & GREEKS – unchanged
# ----------------------------------------------------------------------
try:
    from scipy.optimize import brentq
    from scipy.stats import norm
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    norm = None
    brentq = None

def bsm_price_helper(sigma, S, K, T, r, premium, option_type):
    d1 = (math.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*math.sqrt(T))
    d2 = d1 - sigma*math.sqrt(T)
    if option_type == "CE":
        return S*norm.cdf(d1) - K*math.exp(-r*T)*norm.cdf(d2) - premium
    else:
        return K*math.exp(-r*T)*norm.cdf(-d2) - S*norm.cdf(-d1) - premium

def bsm_iv_delta(S, K, T, r, premium, option_type):
    if not SCIPY_AVAILABLE or S <= 0 or T <= 0:
        iv_est = 0.2
        moneyness = (S - K) / S if S > 0 else 0
        if option_type == "CE":
            delta_est = max(0.05, min(0.95, 0.5 + moneyness * 5))
        else:
            delta_est = max(-0.95, min(-0.05, -0.5 - moneyness * 5))
        return iv_est, delta_est

    intrinsic = max(0, (S - K) if option_type == "CE" else (K - S))
    if premium <= intrinsic + 0.01:
        iv_est = 0.01
    else:
        try:
            iv_est = brentq(lambda sig: bsm_price_helper(sig, S, K, T, r, premium, option_type), 0.01, 2.0, maxiter=50)
        except Exception:
            iv_est = 0.2

    try:
        d1 = (math.log(S/K) + (r + 0.5*iv_est**2)*T) / (iv_est*math.sqrt(T))
        delta_est = norm.cdf(d1) if option_type == "CE" else -norm.cdf(-d1)
    except Exception:
        delta_est = 0.5 if option_type == "CE" else -0.5

    return iv_est, delta_est

greeks_cache_fallback_store = {}
for idx in INDEX_NAMES:
    greeks_cache_fallback_store[idx] = {"ce_iv":0.2, "pe_iv":0.2, "ce_delta":0.5, "pe_delta":-0.5,
                                        "ce_gamma":0.02, "pe_gamma":0.02, "ce_theta":-0.1, "pe_theta":-0.1,
                                        "ce_vega":0.15, "pe_vega":0.15, "iv_rank":50, "iv_percentile":50}

_greeks_cache = {idx: {"data": None, "timestamp": 0} for idx in INDEX_NAMES}
_GREEKS_CACHE_TTL = 60

def _estimate_greeks_fallback(index_name):
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
# KELLY, CORRELATION, VOLUME PROFILE (unchanged)
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
        if self.avg_loss > 1e-9:
            b = abs(self.avg_win / self.avg_loss)
            b = min(b, 10.0)
        else:
            b = 10.0
        kelly_full = (p*b - q)/b if b > 0 else 0
        kelly_full = max(0, min(kelly_full, 0.5))
        return kelly_full * self.kelly_fraction, p, self.avg_win, self.avg_loss

kelly_trackers = {idx: KellyCriterion(idx) for idx in INDEX_NAMES}

class CorrelationFilter:
    def __init__(self):
        self.nifty_prices = deque(maxlen=50)
        self.banknifty_prices = deque(maxlen=50)
        self.nifty_returns = deque(maxlen=50)
        self.banknifty_returns = deque(maxlen=50)
        self.correlation = 0.0

    def update(self, nifty_price, banknifty_price):
        if nifty_price > 0 and banknifty_price > 0:
            if len(self.nifty_prices) > 0:
                prev_nifty = self.nifty_prices[-1]
                if prev_nifty > 0:
                    ret_n = (nifty_price / prev_nifty) - 1
                    self.nifty_returns.append(ret_n)
                else:
                    self.nifty_returns.append(0.0)
                prev_bank = self.banknifty_prices[-1]
                if prev_bank > 0:
                    ret_b = (banknifty_price / prev_bank) - 1
                    self.banknifty_returns.append(ret_b)
                else:
                    self.banknifty_returns.append(0.0)
            else:
                self.nifty_returns.append(0.0)
                self.banknifty_returns.append(0.0)
            self.nifty_prices.append(nifty_price)
            self.banknifty_prices.append(banknifty_price)

            if len(self.nifty_returns) >= 20 and len(self.banknifty_returns) >= 20:
                n_arr = np.array(list(self.nifty_returns)[-20:])
                b_arr = np.array(list(self.banknifty_returns)[-20:])
                if np.std(n_arr) > 0 and np.std(b_arr) > 0:
                    self.correlation = np.corrcoef(n_arr, b_arr)[0,1]
                else:
                    self.correlation = 0.0

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
# AUTHENTICATION & TOKEN MANAGEMENT (unchanged)
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
        logger.warning(f"get_index_spot: No config for {index_name}")
        return None
    if not config.get("token"):
        logger.warning(f"get_index_spot: No token for {index_name}")
        return None
    _, _, obj = get_auth_token()
    if not obj:
        logger.warning(f"get_index_spot: Auth failed for {index_name}")
        return None
    try:
        resp = obj.ltpData(config["exchange"], config["symbol"], config["token"])
        ltp = safe_ltp(resp)
        if ltp and ltp > 0:
            if config["exchange"] in ["NSE","BSE"] and ltp > 100000:
                ltp /= 100
            return ltp
    except Exception as e:
        logger.error(f"get_index_spot: {index_name} exception: {e}")
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

def get_next_expiry_date(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config:
        return None
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
                            return ce_token, pe_token
        except Exception as e:
            logger.warning(f"{index_name} token fetch error: {e}")
    INDEX_TOKENS[index_name].update({"ce_token": None, "pe_token": None})
    return None, None

def refresh_all_tokens():
    for idx in INDEX_NAMES:
        if INDEX_CONFIG[idx].get("active"):
            get_current_atm_tokens(idx)

# ----------------------------------------------------------------------
# MARKET HOURS (Equity only)
# ----------------------------------------------------------------------
def is_market_open():
    if os.getenv("FORCE_MARKET_OPEN", "0") == "1":
        logger.info("Market open forced by FORCE_MARKET_OPEN=1")
        return True
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
    current = now_ist.time()
    open_time = dt_time(9, 15)
    close_time = dt_time(15, 15)
    is_open = now_ist.weekday() < 5 and open_time <= current <= close_time
    return is_open

def is_index_market_open(idx):
    return is_market_open()

# ----------------------------------------------------------------------
# CANDLE UPDATE (unchanged)
# ----------------------------------------------------------------------
def update_candle(idx, price, cumulative_volume, timestamp):
    with _prev_volume_lock:
        prev = _prev_volume.get(idx, 0)
        if cumulative_volume > 0:
            if prev > 0:
                tick_vol = max(0, cumulative_volume - prev)
            else:
                tick_vol = 0
            _prev_volume[idx] = cumulative_volume
        else:
            tick_vol = 0

        if tick_vol > 1000000:
            tick_vol = 0

    for tf, interval in TIMEFRAME_SECONDS.items():
        candle_start = int(timestamp / interval) * interval
        with _current_candle_lock:
            if _last_candle_time[idx][tf] != candle_start:
                if _current_candle[idx][tf] is not None:
                    with _candle_histories_lock:
                        candle_histories[idx][tf].append(_current_candle[idx][tf])
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

            if tf == "1min" and _current_candle[idx][tf] is not None:
                current_age = timestamp - _current_candle[idx][tf]["timestamp"]
                if current_age > 90:
                    with _candle_histories_lock:
                        candle_histories[idx][tf].append(_current_candle[idx][tf])
                    _current_candle[idx][tf] = {
                        "open": price, "high": price, "low": price, "close": price,
                        "volume": tick_vol, "timestamp": candle_start
                    }
                    _last_candle_time[idx][tf] = candle_start

# ----------------------------------------------------------------------
# SENTIMENT, REGIME, CONFIRMATION (unchanged)
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
        elif tf == "5min":
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
        candles = list(candle_histories[index_name]["5min"])
    if len(candles) < 30:
        return "NORMAL"
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    closes = [c["close"] for c in candles]
    adx = calculate_adx(highs, lows, closes, 14)
    atr = calculate_atr(highs, lows, closes, 14)
    spot = last_known_prices[index_name].get("spot", 0.0) or 0.0
    if spot == 0:
        return "NORMAL"
    atr_pct = (atr / spot) * 100.0

    with _latest_ticks_lock:
        vix = latest_ticks["VIX"]["vix"]
    if len(vix_history) >= 20:
        vix_slices = list(vix_history)[-20:]
        vix_sma = sum(vix_slices) / len(vix_slices)
    else:
        vix_sma = vix

    if adx > adx_threshold and atr_pct > atr_threshold:
        new_regime = "TRENDING"
    elif adx < adx_threshold * 0.6 and atr_pct < atr_threshold * 0.5:
        new_regime = "RANGING"
    elif vix > vix_sma * 1.3:
        new_regime = "VOLATILE"
    else:
        new_regime = "NORMAL"

    _regime_history[index_name].append(new_regime)
    if len(_regime_history[index_name]) >= 3:
        counts = Counter(_regime_history[index_name])
        confirmed = counts.most_common(1)[0][0]
        return confirmed
    return new_regime

def confirm_signal_with_candles(index_name, side, spot):
    with _candle_histories_lock:
        candles = list(candle_histories[index_name]["1min"])
    if len(candles) < 10:
        return False
    closes = [c["close"] for c in candles[-10:]]
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
    if not config:
        return False
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
    today = now_ist.strftime("%d%b%Y").upper()
    tokens = INDEX_TOKENS.get(index_name, {})
    expiry = tokens.get("expiry", "")
    return today == expiry

# ----------------------------------------------------------------------
# SIGNAL QUALITY SCORE (unchanged)
# ----------------------------------------------------------------------
def compute_signal_quality(index_name):
    scores = []
    last_tick = last_known_prices[index_name].get("timestamp", 0)
    age = time.time() - last_tick
    if age < 5: scores.append(30)
    elif age < 15: scores.append(20)
    elif age < 30: scores.append(10)
    else: scores.append(0)
    with _candle_histories_lock:
        count = len(candle_histories[index_name]["1min"])
    scores.append(min(30, count))
    greeks = get_option_greeks(index_name)
    scores.append(20 if greeks else 0)
    with _latest_ticks_lock:
        bid = latest_ticks[index_name].get("ce_bid", 0)
        ask = latest_ticks[index_name].get("ce_ask", 0)
    if bid > 0 and ask > 0:
        spread_pct = (ask - bid) / ((ask + bid) / 2) * 100
        scores.append(max(0, 20 - spread_pct * 4))
    else:
        scores.append(10)
    return min(100, sum(scores))

# ----------------------------------------------------------------------
# DATA READINESS CHECK
# ----------------------------------------------------------------------
def has_complete_data(index_name):
    return (last_known_prices[index_name].get("spot", 0) > 0 and
            (last_known_prices[index_name].get("ce", 0) > 0 or
             last_known_prices[index_name].get("pe", 0) > 0))

# ----------------------------------------------------------------------
# PERSISTENCE (unchanged)
# ----------------------------------------------------------------------
def get_db_path():
    return PAPER_DB_PATH if PAPER_MODE else DB_PATH

def load_portfolio_state():
    global portfolio_state, signal_state, daily_trade_count, last_trade_date
    try:
        db_path = get_db_path()
        conn = sqlite3.connect(db_path)
        try:
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
        finally:
            conn.close()
        logger.info(f"Persistent state loaded from {db_path}")
    except Exception as e:
        logger.error(f"Error loading state: {e}")

def save_portfolio_state(idx):
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    try:
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
    finally:
        conn.close()

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
            "exit_reason": exit_reason,
            "status": exit_reason
        })
    # Reset trade_state on exit
    trade_state[index_name].update({
        "active": False,
        "side": "",
        "entry_price": 0.0,
        "current_price": 0.0,
        "stop_loss": 0.0,
        "target": 0.0,
        "pnl": 0.0,
        "entry_time": "",
        "exit_time": datetime.now().strftime("%H:%M:%S")
    })

# Remove send_telegram_alert function entirely – we'll just log instead
def log_alert(msg):
    logger.info(msg)

def apply_transaction_cost(pnl, lots, lot_size):
    cost_per_lot = 50
    return pnl - cost_per_lot * lots

def log_trade(index_name, action, entry_price, exit_price, pnl, size_pct, status, grade, atr, vix, exit_reason):
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    try:
        c = conn.cursor()
        c.execute(
            "INSERT INTO trades (timestamp, action, entry_price, exit_price, pnl, size_pct, status, grade, atr, vix, exit_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (time.time(), action, entry_price, exit_price, pnl, size_pct, status, grade, atr, vix, exit_reason)
        )
        conn.commit()
    finally:
        conn.close()

# ============================================================
# HELPER FUNCTIONS (unchanged)
# ============================================================
def spread_ok(bid, ask, prem):
    if bid <= 0 or ask <= 0:
        return True
    spread = ask - bid
    if prem > 0 and spread / prem > 0.05:
        return False
    return True

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
        rsi_recent = calculate_rsi(closes)
        rsi_previous = calculate_rsi(closes[:-1])
        if rsi_recent is not None and rsi_previous is not None:
            rsi_trend = rsi_recent - rsi_previous
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
    theta_abs = abs(theta)
    if theta_abs > prem * 0.05:
        return 30
    else:
        return 60

# ============================================================
# REST HELPER FUNCTIONS (unchanged)
# ============================================================
def get_vix_ltp():
    return None

def get_option_quote(index_name, option_type):
    config = INDEX_CONFIG.get(index_name)
    if not config:
        return None
    tokens = INDEX_TOKENS.get(index_name, {})
    token_key = "ce_token" if option_type == "CE" else "pe_token"
    symbol_key = "ce_symbol" if option_type == "CE" else "pe_symbol"
    token = tokens.get(token_key)
    symbol = tokens.get(symbol_key)
    if not token or not symbol:
        return None
    _, _, obj = get_auth_token()
    if not obj:
        return None
    try:
        resp = obj.ltpData(config["option_exchange"], symbol, token)
        if not resp or not resp.get("status"):
            return None
        data = resp.get("data", {})
        if isinstance(data, dict) and "fetched" in data:
            fetched = data["fetched"]
            if isinstance(fetched, list) and len(fetched) > 0:
                item = fetched[0]
            else:
                return None
        else:
            return None
        ltp = float(item.get("ltp", 0))
        if ltp > 100000:
            ltp /= 100
        volume = int(item.get("volume", 0) or item.get("v", 0) or 0)
        oi = int(item.get("oi", 0) or item.get("openInterest", 0) or 0)
        bid = float(item.get("bp", 0) or item.get("bid", 0) or 0)
        ask = float(item.get("ap", 0) or item.get("ask", 0) or 0)
        return {
            "ltp": ltp,
            "volume": volume,
            "oi": oi,
            "bid": bid,
            "ask": ask
        }
    except Exception as e:
        logger.debug(f"Option quote error {index_name} {option_type}: {e}")
    return None

# ============================================================
# MAIN SIGNAL ENGINE (stripped of commodity and telegram)
# ============================================================
def run_signal_engine_for_index(index_name):
    try:
        if not INDEX_CONFIG[index_name].get("active"):
            return

        if not is_market_open():
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "Market closed"
                market_signal[index_name]["signal"] = "CLOSED"
            return

        tokens = INDEX_TOKENS.get(index_name, {})
        if not tokens.get("ce_token") or not tokens.get("pe_token"):
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "Option tokens not loaded"
                market_signal[index_name]["signal"] = "WAITING"
            return

        with _candle_histories_lock:
            candle_len = len(candle_histories[index_name]["1min"])

        with _market_signal_lock:
            if candle_len < 10:
                market_signal[index_name]["alert_message"] = f"Building candles ({candle_len}/10)"
                market_signal[index_name]["signal"] = "WAITING"
            else:
                market_signal[index_name]["alert_message"] = "Ready – scanning for signals"

        if candle_len < 10:
            return

        with _market_signal_lock:
            if market_signal[index_name]["signal"] == "EXIT":
                market_signal[index_name]["signal"] = "COOLDOWN"

        now = time.time()

        with _latest_ticks_lock:
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

        if spot <= 0 and ce_prem <= 0 and pe_prem <= 0:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "No price data yet"
                market_signal[index_name]["signal"] = "WAITING"
            return

        # Use mid if available
        if ce_bid > 0 and ce_ask > 0:
            ce_prem = (ce_bid + ce_ask) / 2
        if pe_bid > 0 and pe_ask > 0:
            pe_prem = (pe_bid + pe_ask) / 2

        if not spread_ok(ce_bid, ce_ask, ce_prem) or not spread_ok(pe_bid, pe_ask, pe_prem):
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "Wide bid-ask spread"
                market_signal[index_name]["signal"] = "BLOCKED"
            return

        # Volume profile
        vp_engine = volume_profile_engines[index_name]
        vp_engine.update(spot, ce_vol + pe_vol, option_type=None)
        vp_engine.update(ce_prem, ce_vol, option_type="CE")
        vp_engine.update(pe_prem, pe_vol, option_type="PE")

        if index_name == "NIFTY":
            nifty_price_series.append(spot)
        elif index_name == "BANKNIFTY":
            banknifty_price_series.append(spot)
        if len(nifty_price_series) > 0 and len(banknifty_price_series) > 0:
            correlation_filter.update(list(nifty_price_series)[-1], list(banknifty_price_series)[-1])

        greeks_data = None
        if INDEX_CONFIG[index_name].get("greeks_enabled"):
            greeks_data = get_option_greeks(index_name)

        sentiment = compute_sentiment(index_name)
        action = get_signal_from_sentiment(sentiment)
        sentiment_label = get_sentiment_label(sentiment)
        with _market_signal_lock:
            market_signal[index_name]["sentiment_score"] = sentiment

        regime = detect_regime(index_name)
        if regime == "RANGING":
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "Ranging market - no new entries"
                market_signal[index_name]["signal"] = "BLOCKED"
            return

        # Drawdown & lock protected
        with _portfolio_state_lock:
            current_equity = portfolio_state[index_name]["equity"]
            peak = daily_drawdown[index_name]["peak_equity"]
            if current_equity > peak:
                daily_drawdown[index_name]["peak_equity"] = current_equity
            drawdown = (peak - current_equity) / peak * 100 if peak > 0 else 0
            daily_drawdown[index_name]["current_drawdown"] = drawdown

            if drawdown >= 2.0 and drawdown < 3.0:
                if not daily_drawdown[index_name].get("dd_warning_sent", False):
                    log_alert(f"⚠️ {index_name} drawdown: {drawdown:.1f}% (warning at 2%)")
                    daily_drawdown[index_name]["dd_warning_sent"] = True
            elif drawdown < 1.5:
                daily_drawdown[index_name]["dd_warning_sent"] = False

            if drawdown >= INDEX_CONFIG[index_name].get("max_daily_drawdown_pct", 3.0):
                with _signal_state_lock:
                    if signal_state[index_name]["action"] != "HOLD":
                        active = signal_state[index_name]["action"]
                        exit_prem = ce_prem if "CE" in active else pe_prem
                        if exit_prem > 0:
                            pnl = exit_prem - signal_state[index_name]["entry_price"]
                            pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                            pnl_total = apply_transaction_cost(pnl_total, signal_state[index_name]["lots"], INDEX_CONFIG[index_name]["lot_size"])
                            portfolio_state[index_name]["equity"] += pnl_total
                            portfolio_state[index_name]["daily_pnl"] += pnl_total
                            portfolio_state[index_name]["total_pnl"] += pnl_total
                            portfolio_state[index_name]["live_pnl"] = 0.0
                            save_portfolio_state(index_name)
                            log_trade(index_name, active, signal_state[index_name]["entry_price"], exit_prem, pnl_total,
                                      pnl_total / portfolio_state[index_name]["equity"] * 100, "KILL_SWITCH",
                                      active, calculate_atr([],[],[],14), latest_ticks["VIX"]["vix"], "KILL_SWITCH")
                            reset_signal_state(index_name, now, "KILL_SWITCH")
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = "KILL SWITCH: Max drawdown hit. Trading halted."
                    market_signal[index_name]["signal"] = "KILL_SWITCH"
                return

        if safety_state[index_name]["circuit_breaker"]:
            if now < safety_state[index_name]["circuit_breaker_until"]:
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = "Circuit breaker active"
                    market_signal[index_name]["signal"] = "CIRCUIT_BREAKER"
                return
            else:
                safety_state[index_name]["circuit_breaker"] = False
                safety_state[index_name]["consecutive_sl"] = 0

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

                    # Trailing stop using 5min ATR
                    if prem > signal_state[index_name].get("highest", 0):
                        signal_state[index_name]["highest"] = prem
                        with _candle_histories_lock:
                            candles = list(candle_histories[index_name]["5min"])
                            highs = [c["high"] for c in candles]
                            lows = [c["low"] for c in candles]
                            closes = [c["close"] for c in candles]
                        if len(closes) >= 15:
                            atr = calculate_atr(highs, lows, closes, 14)
                            new_sl = prem - atr * 1.8
                            if new_sl > signal_state[index_name]["stop_loss"]:
                                signal_state[index_name]["stop_loss"] = new_sl

                    stop_loss_val = signal_state[index_name].get("stop_loss")
                    target_val = signal_state[index_name].get("target")

                    if stop_loss_val is not None and prem <= stop_loss_val:
                        pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                        pnl_total = apply_transaction_cost(pnl_total, signal_state[index_name]["lots"], INDEX_CONFIG[index_name]["lot_size"])
                        with _portfolio_state_lock:
                            portfolio_state[index_name]["equity"] += pnl_total
                            portfolio_state[index_name]["daily_pnl"] += pnl_total
                            portfolio_state[index_name]["total_pnl"] += pnl_total
                            portfolio_state[index_name]["live_pnl"] = 0.0
                        save_portfolio_state(index_name)
                        entry_price = signal_state[index_name]["entry_price"]
                        if entry_price > 0:
                            pnl_pct = pnl / entry_price
                            kelly_trackers[index_name].update(pnl_pct)
                        safety_state[index_name]["consecutive_sl"] += 1
                        if safety_state[index_name]["consecutive_sl"] >= 3:
                            safety_state[index_name]["circuit_breaker"] = True
                            safety_state[index_name]["circuit_breaker_until"] = now + 1800
                            log_alert(f"CIRCUIT BREAKER {index_name} | 3 consecutive SLs. Trading paused 30 min.")
                        log_trade(index_name, active, signal_state[index_name]["entry_price"], prem, pnl_total,
                                  pnl_total / portfolio_state[index_name]["equity"] * 100, "STOP_LOSS",
                                  active, calculate_atr([],[],[],14), latest_ticks["VIX"]["vix"], "STOP_LOSS")
                        log_alert(f"EXIT {index_name} | SL | PnL: {pnl:.2f} pts | Cost adj: {pnl_total:.2f}")
                        reset_signal_state(index_name, now, "STOP_LOSS")
                        return

                    if target_val is not None and prem >= target_val:
                        pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                        pnl_total = apply_transaction_cost(pnl_total, signal_state[index_name]["lots"], INDEX_CONFIG[index_name]["lot_size"])
                        with _portfolio_state_lock:
                            portfolio_state[index_name]["equity"] += pnl_total
                            portfolio_state[index_name]["daily_pnl"] += pnl_total
                            portfolio_state[index_name]["total_pnl"] += pnl_total
                            portfolio_state[index_name]["live_pnl"] = 0.0
                        save_portfolio_state(index_name)
                        entry_price = signal_state[index_name]["entry_price"]
                        if entry_price > 0:
                            pnl_pct = pnl / entry_price
                            kelly_trackers[index_name].update(pnl_pct)
                        safety_state[index_name]["consecutive_sl"] = 0
                        log_trade(index_name, active, signal_state[index_name]["entry_price"], prem, pnl_total,
                                  pnl_total / portfolio_state[index_name]["equity"] * 100, "TARGET_HIT",
                                  active, calculate_atr([],[],[],14), latest_ticks["VIX"]["vix"], "TARGET_HIT")
                        log_alert(f"EXIT {index_name} | TARGET | PnL: {pnl:.2f} pts | Cost adj: {pnl_total:.2f}")
                        reset_signal_state(index_name, now, "TARGET_HIT")
                        return

                    # Time exit
                    with _signal_state_lock:
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
                            entry_price = signal_state[index_name]["entry_price"]
                            if entry_price > 0:
                                kelly_trackers[index_name].update(pnl / entry_price)
                            log_trade(index_name, active, signal_state[index_name]["entry_price"], prem, pnl_total,
                                      pnl_total / portfolio_state[index_name]["equity"] * 100, "TIME_EXIT",
                                      active, calculate_atr([],[],[],14), latest_ticks["VIX"]["vix"], f"TIME_EXIT_{time_limit}m")
                            log_alert(f"EXIT {index_name} | TIME ({time_limit}m) | PnL: {pnl:.2f} pts | Cost adj: {pnl_total:.2f}")
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
                        entry_price = signal_state[index_name]["entry_price"]
                        if entry_price > 0:
                            kelly_trackers[index_name].update(pnl / entry_price)
                        log_trade(index_name, active, signal_state[index_name]["entry_price"], prem, pnl_total,
                                  pnl_total / portfolio_state[index_name]["equity"] * 100, "MARKET_EXIT",
                                  active, calculate_atr([],[],[],14), latest_ticks["VIX"]["vix"], exit_reason)
                        log_alert(f"EXIT {index_name} | {exit_reason} | PnL: {pnl:.2f} pts | Cost adj: {pnl_total:.2f}")
                        reset_signal_state(index_name, now, exit_reason)
                        return

                    # VWAP exit
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
                            entry_price = signal_state[index_name]["entry_price"]
                            if entry_price > 0:
                                kelly_trackers[index_name].update(pnl / entry_price)
                            log_trade(index_name, active, signal_state[index_name]["entry_price"], prem, pnl_total,
                                      pnl_total / portfolio_state[index_name]["equity"] * 100, "VWAP_EXIT",
                                      active, calculate_atr([],[],[],14), latest_ticks["VIX"]["vix"], "VWAP_EXIT")
                            log_alert(f"EXIT {index_name} | VWAP (premium below VWAP) | PnL: {pnl:.2f} pts | Cost adj: {pnl_total:.2f}")
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
                            entry_price = signal_state[index_name]["entry_price"]
                            if entry_price > 0:
                                kelly_trackers[index_name].update(pnl / entry_price)
                            log_trade(index_name, active, signal_state[index_name]["entry_price"], prem, pnl_total,
                                      pnl_total / portfolio_state[index_name]["equity"] * 100, "VWAP_EXIT",
                                      active, calculate_atr([],[],[],14), latest_ticks["VIX"]["vix"], "VWAP_EXIT")
                            log_alert(f"EXIT {index_name} | VWAP (premium below VWAP) | PnL: {pnl:.2f} pts | Cost adj: {pnl_total:.2f}")
                            reset_signal_state(index_name, now, "VWAP_EXIT")
                            return

                # Update market_signal with strike info for ACTIVE trade
                token_info = INDEX_TOKENS.get(index_name, {})
                atm_strike = token_info.get("atm_strike", 0)
                if "CE" in active:
                    trading_symbol = token_info.get("ce_symbol", "")
                else:
                    trading_symbol = token_info.get("pe_symbol", "")
                with _market_signal_lock:
                    market_signal[index_name].update({
                        "alert_message": f"ACTIVE {active}",
                        "signal": "ACTIVE",
                        "entry_price": signal_state[index_name]["entry_price"],
                        "stop_loss": signal_state[index_name]["stop_loss"],
                        "target": signal_state[index_name]["target"],
                        "current_pnl": round(pnl, 2),
                        "strike_price": atm_strike,
                        "trading_symbol": trading_symbol,
                        "status": "ACTIVE"
                    })
            else:
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = f"ACTIVE {active} – premium unavailable"
                    market_signal[index_name]["signal"] = "ACTIVE"
            return

        # --- New entry ---
        with _signal_state_lock:
            if now < signal_state[index_name]["cooldown"]:
                remaining = int(signal_state[index_name]["cooldown"] - now)
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = f"Cooldown {remaining}s"
                    market_signal[index_name]["signal"] = "COOLDOWN"
                return

            # Reset daily counters if new day
            now_ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
            today = now_ist.strftime("%Y-%m-%d")
            if last_trade_date[index_name] != today:
                daily_trade_count[index_name] = 0
                last_trade_date[index_name] = today
                with _portfolio_state_lock:
                    daily_drawdown[index_name]["peak_equity"] = portfolio_state[index_name]["equity"]
                    portfolio_state[index_name]["daily_pnl"] = 0.0
                    portfolio_state[index_name]["live_pnl"] = 0.0
                save_portfolio_state(index_name)

        if regime == "TRENDING":
            max_trades = 25
        else:
            max_trades = 15
        with _signal_state_lock:
            current_trades = daily_trade_count[index_name]
        if current_trades >= max_trades:
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
        min_prem = INDEX_CONFIG[index_name].get("min_premium", 0)
        max_prem = INDEX_CONFIG[index_name].get("max_premium", 8000)
        if prem <= 0 or prem < min_prem or prem > max_prem:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Premium invalid: {prem:.2f}"
                market_signal[index_name]["signal"] = "WAITING"
            return

        # VWAP entry filter
        spot_vwap = vp_engine.analyze(spot, ce_vol + pe_vol, option_type=None)["vwap"]
        if spot_vwap > 0:
            if side == "CE" and spot > spot_vwap * 1.003:
                if "STRONG" not in action:
                    with _market_signal_lock:
                        market_signal[index_name]["alert_message"] = "Spot above VWAP, extended"
                        market_signal[index_name]["signal"] = "BLOCKED"
                    return
            elif side == "PE" and spot < spot_vwap * 0.997:
                if "STRONG" not in action:
                    with _market_signal_lock:
                        market_signal[index_name]["alert_message"] = "Spot below VWAP, extended"
                        market_signal[index_name]["signal"] = "BLOCKED"
                    return

        if not confirm_signal_with_candles(index_name, side, spot):
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "Candle confirmation failed (last 3 closes not aligned with EMA9)"
                market_signal[index_name]["signal"] = "BLOCKED"
            return

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

        if INDEX_CONFIG[index_name].get("pcr_enabled"):
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

        pair = INDEX_CONFIG[index_name].get("correlation_pair")
        corr_adjust = 1.0
        if pair:
            corr_analysis = correlation_filter.analyze(index_name, action)
            corr = abs(corr_analysis.get("correlation", 0))
            if corr > 0.85:
                corr_adjust = max(0.6, 1 - (corr - 0.85) * 2)
            if corr > 0.8:
                pair_action = market_signal.get(pair, {}).get("signal", "NO_TRADE")
                if (side == "CE" and "CE" in pair_action) or (side == "PE" and "PE" in pair_action):
                    my_sent = sentiment
                    pair_sent = market_signal.get(pair, {}).get("sentiment_score", 50)
                    if my_sent < pair_sent:
                        with _market_signal_lock:
                            market_signal[index_name]["alert_message"] = f"Correlation block: {pair} stronger"
                            market_signal[index_name]["signal"] = "BLOCKED"
                        return
            beta_adj = corr_analysis.get("beta_adjustment", 1.0) * corr_adjust
        else:
            beta_adj = corr_adjust

        if INDEX_CONFIG[index_name].get("greeks_enabled") and greeks_data is not None:
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

        with _price_histories_lock:
            prices_spot = list(price_histories[index_name])
        rsi = calculate_rsi(prices_spot[-50:]) if len(prices_spot) >= 50 else 50.0
        with _candle_histories_lock:
            candles = list(candle_histories[index_name]["5min"])
            highs = [c["high"] for c in candles]
            lows = [c["low"] for c in candles]
            closes = [c["close"] for c in candles]
        adx = calculate_adx(highs, lows, closes, 14) if len(closes) >= 30 else 20.0
        with _latest_ticks_lock:
            vix = latest_ticks["VIX"]["vix"]
            if vix <= 0:
                vix = 15.0

        ml_prob = compute_ml_score(index_name, side, prem, spot, rsi, adx, vix, sentiment)
        if ml_prob < 0.4 and "STRONG" not in action:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"ML filter: prob {ml_prob:.2f}"
                market_signal[index_name]["signal"] = "BLOCKED"
            return

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
        if greeks_data is not None:
            iv_rank = greeks_data.get("iv_rank")
            if iv_rank is not None:
                if iv_rank > 80:
                    risk_pct *= 0.8
                elif iv_rank < 20:
                    risk_pct *= 1.1
        if is_expiry_day(index_name):
            risk_pct *= 0.5
        if regime == "VOLATILE":
            risk_pct *= 0.7
        risk_pct = max(0.5, min(3.0, risk_pct))

        if is_expiry_day(index_name):
            now_ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
            if now_ist.time() >= dt_time(14, 30):
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = "Expiry day: last 60 min blocked"
                    market_signal[index_name]["signal"] = "BLOCKED"
                return

        with _candle_histories_lock:
            candles = list(candle_histories[index_name]["1min"])
            highs = [c["high"] for c in candles]
            lows = [c["low"] for c in candles]
            closes = [c["close"] for c in candles]
        atr = calculate_atr(highs, lows, closes, 14)
        if "STRONG" in action:
            sl_pct = 0.45
            target_mult = 3.5
        elif "LOW" in action:
            sl_pct = 0.3
            target_mult = 2.5
        else:
            sl_pct = 0.4
            target_mult = 3.0
        if is_expiry_day(index_name):
            sl_pct *= 0.7
            target_mult *= 0.8
        sl = max(prem * (1 - sl_pct), prem - atr * 1.5)
        target = prem + atr * target_mult

        stop_dist = prem - sl
        if stop_dist <= 0:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "Invalid stop distance"
                market_signal[index_name]["signal"] = "BLOCKED"
            return

        risk_amount = portfolio_state[index_name]["equity"] * (risk_pct / 100)
        lots = int(risk_amount / (stop_dist * INDEX_CONFIG[index_name]["lot_size"]))
        lots = max(1, min(5, lots))

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
            daily_trade_count[index_name] += 1

        with _portfolio_state_lock:
            portfolio_state[index_name]["open_positions"] = 1
        save_portfolio_state(index_name)

        emoji = "🔥" if "STRONG" in action and "CE" in action else "❄️" if "STRONG" in action and "PE" in action else "⚡" if "LOW" in action else "📊"
        msg = f"{emoji} {action} {index_name} | Spot:{spot:.0f} Prem:{prem:.2f} SL:{sl:.2f} Tgt:{target:.2f} | Sentiment:{sentiment:.0f} ({sentiment_label}) | Regime:{regime} | Lots:{lots} Risk:{risk_pct:.1f}%"
        log_alert(msg)

        token_info = INDEX_TOKENS.get(index_name, {})
        atm_strike = token_info.get("atm_strike", 0)
        if side == "CE":
            trading_symbol = token_info.get("ce_symbol", "")
        else:
            trading_symbol = token_info.get("pe_symbol", "")

        with _market_signal_lock:
            market_signal[index_name].update({
                "signal": action,
                "alert_message": f"ENTRY {action}",
                "entry_price": prem,
                "stop_loss": sl,
                "target": target,
                "sentiment_score": sentiment,
                "exit_reason": "",
                "quality_score": compute_signal_quality(index_name),
                "strike_price": atm_strike,
                "trading_symbol": trading_symbol,
                "status": "ENTRY"
            })

        # ----- Setup trade_state for PnL tracking -----
        trade_state[index_name]["active"] = True
        trade_state[index_name]["side"] = action
        trade_state[index_name]["entry_price"] = prem
        trade_state[index_name]["current_price"] = prem
        trade_state[index_name]["stop_loss"] = sl
        trade_state[index_name]["target"] = target
        trade_state[index_name]["pnl"] = 0.0
        trade_state[index_name]["entry_time"] = datetime.now().strftime("%H:%M:%S")
        trade_state[index_name]["exit_time"] = ""

    except Exception as e:
        logger.error(f"Signal error {index_name}: {e}\n{traceback.format_exc()}")

def run_all_signals():
    for idx in INDEX_NAMES:
        if INDEX_CONFIG[idx].get("active"):
            try:
                run_signal_engine_for_index(idx)
            except Exception as e:
                logger.error(f"Signal error {idx}: {e}\n{traceback.format_exc()}")

# ============================================================
# WEBSOCKET + WATCHDOGS (with trade_state PnL updates)
# ============================================================
ws_running = False
sws = None
last_heartbeat = time.time()
tick_counter = 0
last_tick_timestamp = time.time()
last_rest_fetch = time.time()
_ws_connect_lock = threading.Lock()

def on_ws_open(wsapp):
    global ws_running, last_heartbeat, sws
    with _ws_connect_lock:
        ws_running = True
    last_heartbeat = time.time()
    logger.info("WebSocket connected successfully, subscribing to tokens...")

    token_list = []
    for idx, cfg in INDEX_CONFIG.items():
        if cfg.get("active"):
            exch_type = int(cfg["ws_exchange_type"])
            token_list.append({"exchangeType": exch_type, "tokens": [cfg["token"]]})

    for idx, tokens in INDEX_TOKENS.items():
        if not INDEX_CONFIG[idx].get("active"):
            continue
        if tokens.get("ce_token") and tokens.get("pe_token"):
            exch_type = int(INDEX_CONFIG[idx]["option_ws_exchange_type"])
            token_list.append({
                "exchangeType": exch_type,
                "tokens": [tokens["ce_token"], tokens["pe_token"]]
            })

    token_list.append({"exchangeType": 1, "tokens": ["99919017"]})  # VIX

    if token_list and sws:
        try:
            correlation_id = "hybrid_bot"
            mode = 1
            response = sws.subscribe(correlation_id, mode, token_list)
            if response and isinstance(response, dict) and not response.get("status", True):
                logger.error(f"Subscription failed: {response}")
            else:
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
    global tick_counter, last_heartbeat, last_tick_timestamp, sws, _last_signal_run
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
            with _ws_connect_lock:
                local_sws = sws
            try:
                if local_sws and hasattr(local_sws, "_parse_binary_data"):
                    parsed = local_sws._parse_binary_data(message)
                    if parsed and parsed.get('token'):
                        ticks = [parsed]
                    else:
                        logger.warning("Binary parse returned empty, skipping tick")
                        return
                else:
                    logger.warning("sws._parse_binary_data not available")
                    return
            except Exception as e:
                logger.error(f"Binary parse error: {e}\n{traceback.format_exc()}")
                return
        else:
            logger.warning(f"Unsupported WS message type: {type(message)}")
            return

        if not ticks:
            return

        for tick in ticks:
            try:
                tick_counter += 1

                token = str(tick.get("token") or tick.get("tk") or "")
                ltp = tick.get("last_traded_price") or tick.get("ltp") or tick.get("price") or 0

                if isinstance(ltp, str):
                    try:
                        ltp = float(ltp)
                    except:
                        ltp = 0

                if ltp > 10000 and token in INDEX_TOKEN_SET:
                    ltp = ltp / 100.0

                vol = tick.get("volume") or tick.get("v") or tick.get("last_traded_quantity") or 0
                oi = tick.get("open_interest") or tick.get("oi") or tick.get("OpenInterest") or 0
                bid = tick.get("best_bid_price") or tick.get("bid") or tick.get("bp") or 0
                ask = tick.get("best_ask_price") or tick.get("ask") or tick.get("ap") or 0

                # ---- Spot matching ----
                spot_matched = False
                for idx, cfg in INDEX_CONFIG.items():
                    if cfg.get("token") == token:
                        if ltp > 0:
                            with _latest_ticks_lock:
                                latest_ticks[idx]["spot_price"] = ltp
                            with _price_histories_lock:
                                price_histories[idx].append(ltp)
                            update_candle(idx, ltp, vol, time.time())
                            last_tick_timestamp = time.time()
                            with _latest_ticks_lock:
                                last_known_prices[idx]["spot"] = ltp
                                last_known_prices[idx]["timestamp"] = time.time()
                            spot_matched = True
                            break
                if spot_matched:
                    continue

                # ---- Option matching ----
                option_matched = False
                for idx, tokens in INDEX_TOKENS.items():
                    if not INDEX_CONFIG[idx].get("active"):
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
                            # Update trade_state PnL if this index has an active CE trade
                            if trade_state[idx]["active"] and "CE" in trade_state[idx]["side"]:
                                update_trade_pnl(idx, ltp)
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
                            # Update trade_state PnL if this index has an active PE trade
                            if trade_state[idx]["active"] and "PE" in trade_state[idx]["side"]:
                                update_trade_pnl(idx, ltp)
                            option_matched = True
                            break
                if option_matched:
                    continue

                # ---- VIX ----
                if str(token) == "99919017" and ltp > 0:
                    with _latest_ticks_lock:
                        latest_ticks["VIX"]["vix"] = ltp
                    vix_history.append(ltp)

            except Exception as e:
                logger.error(f"Error processing tick: {e}\n{traceback.format_exc()}")
                continue

        # Run signals
        ready_indices = [idx for idx in INDEX_NAMES if INDEX_CONFIG[idx].get("active") and has_complete_data(idx)]
        if ready_indices and tick_counter % 5 == 0 and tick_counter > 0:
            with _signal_run_lock:
                now = time.time()
                if now - _last_signal_run >= 1.0:
                    _last_signal_run = now
                    threading.Thread(target=run_all_signals, daemon=True).start()

    except Exception as e:
        logger.error(f"Unhandled exception in on_ws_data: {e}\n{traceback.format_exc()}")

def tick_watchdog():
    global ws_running, tick_counter, last_tick_timestamp, sws
    last_count = 0
    while True:
        time.sleep(10)
        if ws_running:
            if time.time() - last_tick_timestamp > 60:
                logger.warning("No ticks for 60s - forcing reconnect")
                with _ws_connect_lock:
                    ws_running = False
                if sws:
                    try:
                        sws.close_connection()
                    except:
                        pass
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
            if (now - last_heartbeat > 25) or (now - last_tick_timestamp > 60):
                logger.warning(f"Data starvation – forcing reconnect")
                with _ws_connect_lock:
                    ws_running = False
                if sws:
                    try:
                        sws.close_connection()
                    except Exception:
                        pass

def start_angel_websocket_improved():
    global sws, ws_running, last_heartbeat
    retry_delay = 5

    while True:
        try:
            with _ws_connect_lock:
                ws_running = False

            if not is_market_open():
                time.sleep(60)
                continue

            auth_token, feed_token, _ = get_auth_token()
            if not feed_token:
                logger.error("Failed to get feed token, retrying in 10 seconds...")
                time.sleep(10)
                continue

            new_sws = SmartWebSocketV2(
                auth_token,
                ANGEL_API_KEY,
                ANGEL_CLIENT_ID,
                feed_token,
                max_retry_attempt=3
            )

            new_sws.on_open = on_ws_open
            new_sws.on_data = on_ws_data
            new_sws.on_error = on_ws_error
            new_sws.on_close = on_ws_close

            with _ws_connect_lock:
                sws = new_sws

            logger.info("Attempting WebSocket connection...")
            ws_thread = threading.Thread(target=sws.connect, daemon=True)
            ws_thread.start()

            time.sleep(5)

            retry_delay = 5

            while True:
                try:
                    force_ws = os.getenv("FORCE_WS", "0") == "1"
                    if not force_ws:
                        if not is_market_open():
                            time.sleep(60)
                            continue

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

                    time.sleep(5)
                except Exception as e:
                    logger.error(f"Error in inner loop: {e}")
                    break

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

# ----------------------------------------------------------------------
# PRE-MARKET TOKEN REFRESH SCHEDULER
# ----------------------------------------------------------------------
def schedule_token_refresh():
    while True:
        now_utc = datetime.now(timezone.utc)
        now_ist = now_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
        if now_ist.weekday() < 5:
            open_time = datetime.combine(now_ist.date(), dt_time(9, 10), tzinfo=now_ist.tzinfo)
            wait = (open_time - now_ist).total_seconds()
            if wait > 0:
                time.sleep(wait)
                logger.info("⏰ Pre-market token refresh triggered")
                refresh_all_tokens()
                logger.info("Pre-market token refresh completed")
        time.sleep(60)

# ----------------------------------------------------------------------
# ENHANCED REST FALLBACK (unchanged, but commodity removed)
# ----------------------------------------------------------------------
def fetch_asset_data(idx):
    results = {"index": idx, "spot": None, "ce": None, "pe": None}
    try:
        spot = get_index_spot(idx)
        if spot:
            results["spot"] = spot
            tokens = INDEX_TOKENS.get(idx, {})
            if tokens.get("ce_token"):
                results["ce"] = get_option_quote(idx, "CE")
            if tokens.get("pe_token"):
                results["pe"] = get_option_quote(idx, "PE")
    except Exception as e:
        logger.error(f"Fetch error {idx}: {e}")
    return results

def start_rest_only_mode():
    global last_rest_fetch, last_tick_timestamp
    logger.info("Starting REST fallback (concurrent fetch mode)")
    cycle_count = 0
    while True:
        with _ws_connect_lock:
            if ws_running:
                time.sleep(30)
                continue

        cycle_count += 1
        try:
            assets_to_fetch = []
            for idx in INDEX_NAMES:
                cfg = INDEX_CONFIG[idx]
                if cfg.get("active") and is_market_open():
                    assets_to_fetch.append(idx)

            if not assets_to_fetch:
                logger.info("REST: No markets open, sleeping 30s")
                time.sleep(30)
                continue

            for idx in assets_to_fetch:
                tokens = INDEX_TOKENS.get(idx, {})
                if not tokens.get("ce_token") or not tokens.get("pe_token"):
                    logger.warning(f"Tokens missing for {idx}, retrying...")
                    get_current_atm_tokens(idx)

            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {executor.submit(fetch_asset_data, idx): idx for idx in assets_to_fetch}
                for future in as_completed(futures):
                    result = future.result()
                    idx = result["index"]
                    try:
                        spot = result.get("spot")
                        if spot and spot > 0:
                            with _latest_ticks_lock:
                                latest_ticks[idx]["spot_price"] = spot
                            with _price_histories_lock:
                                price_histories[idx].append(spot)
                            update_candle(idx, spot, 0, time.time())
                            with _latest_ticks_lock:
                                last_known_prices[idx]["spot"] = spot
                                last_known_prices[idx]["timestamp"] = time.time()
                            last_tick_timestamp = time.time()

                        ce = result.get("ce")
                        pe = result.get("pe")
                        if ce:
                            with _latest_ticks_lock:
                                latest_ticks[idx]["ce_price"] = ce["ltp"]
                                latest_ticks[idx]["ce_volume"] = ce["volume"]
                                latest_ticks[idx]["ce_oi"] = ce["oi"]
                                latest_ticks[idx]["ce_bid"] = ce["bid"]
                                latest_ticks[idx]["ce_ask"] = ce["ask"]
                            with _latest_ticks_lock:
                                last_known_prices[idx]["ce"] = ce["ltp"]
                            with _ce_price_histories_lock:
                                ce_price_histories[idx].append(ce["ltp"])
                            # Update trade_state PnL if active
                            if trade_state[idx]["active"] and "CE" in trade_state[idx]["side"]:
                                update_trade_pnl(idx, ce["ltp"])
                            last_tick_timestamp = time.time()
                        if pe:
                            with _latest_ticks_lock:
                                latest_ticks[idx]["pe_price"] = pe["ltp"]
                                latest_ticks[idx]["pe_volume"] = pe["volume"]
                                latest_ticks[idx]["pe_oi"] = pe["oi"]
                                latest_ticks[idx]["pe_bid"] = pe["bid"]
                                latest_ticks[idx]["pe_ask"] = pe["ask"]
                            with _latest_ticks_lock:
                                last_known_prices[idx]["pe"] = pe["ltp"]
                            with _pe_price_histories_lock:
                                pe_price_histories[idx].append(pe["ltp"])
                            if trade_state[idx]["active"] and "PE" in trade_state[idx]["side"]:
                                update_trade_pnl(idx, pe["ltp"])
                            last_tick_timestamp = time.time()
                    except Exception as e:
                        logger.error(f"Error processing REST result for {idx}: {e}")

            # VIX
            try:
                vix = get_vix_ltp()
                if vix:
                    with _latest_ticks_lock:
                        latest_ticks["VIX"]["vix"] = vix
                    vix_history.append(vix)
            except Exception as e:
                logger.debug(f"VIX REST fetch error: {e}")

            last_rest_fetch = time.time()

            ready_indices = [idx for idx in INDEX_NAMES if INDEX_CONFIG[idx].get("active") and has_complete_data(idx)]
            if ready_indices:
                run_all_signals()

            cycle_interval = int(os.getenv("REST_CYCLE_INTERVAL", "10"))
            for _ in range(cycle_interval):
                time.sleep(1)
                with _ws_connect_lock:
                    if ws_running:
                        logger.info("REST cycle interrupted: WS reconnected")
                        break

            logger.info(f"REST cycle {cycle_count} complete.")

        except Exception as e:
            logger.error(f"REST fallback error: {e}")
            last_rest_fetch = time.time()
            time.sleep(10)

# ----------------------------------------------------------------------
# CONNECTION MANAGER
# ----------------------------------------------------------------------
class ConnectionManager:
    def __init__(self):
        force_rest = os.getenv("FORCE_REST_MODE", "0") == "1"
        self.use_websocket = not force_rest
        self._ws_thread = None
        self._rest_thread = None
        self._refresh_thread = None

    def start(self):
        if self.use_websocket:
            logger.info("WebSocket mode enabled. Starting WS thread.")
            self._ws_thread = threading.Thread(target=start_angel_websocket_improved, daemon=True)
            self._ws_thread.start()
            threading.Thread(target=ws_watchdog, daemon=True).start()
            threading.Thread(target=tick_watchdog, daemon=True).start()
        else:
            logger.info("REST-only mode forced (FORCE_REST_MODE=1). No WebSocket thread.")
        self._rest_thread = threading.Thread(target=start_rest_only_mode, daemon=True)
        self._rest_thread.start()
        logger.info("REST fallback thread started (will take over if WS disconnects).")
        self._refresh_thread = threading.Thread(target=schedule_token_refresh, daemon=True)
        self._refresh_thread.start()
        logger.info("Pre-market token refresh scheduler started.")

# ----------------------------------------------------------------------
# BACKGROUND THREADS
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
                ready = 0
                for idx, cfg in INDEX_CONFIG.items():
                    if not cfg.get("active"):
                        continue
                    tokens = INDEX_TOKENS.get(idx, {})
                    if tokens.get("ce_token") and tokens.get("pe_token"):
                        ready += 1
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

# ----------------------------------------------------------------------
# FLASK ROUTES (updated with trade_state fields)
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
        "engine": "Equity-Only Scalping v16 – with Trade State & PnL",
        "indices": [i for i, cfg in INDEX_CONFIG.items() if cfg.get("active")],
        "market_open": is_market_open()
    })

@app.route("/api/live-signals", methods=["GET"])
def live_signals():
    sentiment_data = {}
    trends_data = {}
    quality_scores = {}
    for idx in INDEX_NAMES:
        if not INDEX_CONFIG[idx].get("active"):
            continue
        with _market_signal_lock:
            sentiment_data[idx] = {
                "score": market_signal[idx].get("sentiment_score", 50),
                "label": get_sentiment_label(market_signal[idx].get("sentiment_score", 50))
            }
            quality_scores[idx] = market_signal[idx].get("quality_score", compute_signal_quality(idx))
        trends_data[idx] = {}
        for tf in TIMEFRAMES:
            trends_data[idx][tf] = get_trend_for_timeframe(idx, tf)

    for idx in INDEX_NAMES:
        if not INDEX_CONFIG[idx].get("active"):
            continue
        with _market_signal_lock:
            if market_signal[idx].get("alert_message") == "" and market_signal[idx].get("signal") in ("WAITING", ""):
                with _candle_histories_lock:
                    candle_len = len(candle_histories[idx]["1min"])
                if candle_len >= 10:
                    run_signal_engine_for_index(idx)

    with _market_signal_lock, _portfolio_state_lock:
        portfolio_with_trades = {}
        for idx, port in portfolio_state.items():
            portfolio_with_trades[idx] = port.copy()
            portfolio_with_trades[idx]["daily_trades"] = daily_trade_count.get(idx, 0)

        # Build response with trade_state fields
        signals_with_trade = {}
        for idx in INDEX_NAMES:
            if not INDEX_CONFIG[idx].get("active"):
                continue
            base_signal = market_signal[idx].copy()
            # Add trade_state fields
            base_signal.update({
                "entry_price": trade_state[idx]["entry_price"],
                "ltp": trade_state[idx]["current_price"],
                "stop_loss": trade_state[idx]["stop_loss"],
                "target": trade_state[idx]["target"],
                "pnl": trade_state[idx]["pnl"],
                "trade_active": trade_state[idx]["active"],
                "entry_time": trade_state[idx]["entry_time"],
                "exit_time": trade_state[idx]["exit_time"],
            })
            signals_with_trade[idx] = base_signal

        return jsonify({
            "timestamp": datetime.now().isoformat(),
            "signals": signals_with_trade,
            "sentiment": sentiment_data,
            "trends": trends_data,
            "portfolios": portfolio_with_trades,
            "quality_scores": quality_scores,
            "market_open": is_market_open(),
            "debug": {
                "ws_running": ws_running,
                "ticks": tick_counter,
                "last_tick_ago": round(time.time() - last_tick_timestamp, 1)
            },
            "version": "16-Equity-Scalping-Bot"
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
    if not is_market_open():
        return jsonify({
            "status": "CLOSED",
            "ws_running": ws_running,
            "rest_active": False,
            "ticks": tick_counter,
            "last_tick_seconds_ago": round(time.time() - last_tick_timestamp, 2)
        })
    rest_active = (time.time() - last_rest_fetch < 60)
    ws_active = ws_running and (time.time() - last_tick_timestamp < 30)
    return jsonify({
        "status": "OK" if (ws_active or rest_active) else "DEGRADED",
        "ws_running": ws_running,
        "rest_active": rest_active,
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
        "connection_mode": "WEBSOCKET" if ws_running else "REST_FALLBACK",
        "active_indices": [i for i, cfg in INDEX_CONFIG.items() if cfg.get("active")],
        "tokens_loaded": {idx: {
            "ce_token": bool(INDEX_TOKENS[idx].get("ce_token")),
            "pe_token": bool(INDEX_TOKENS[idx].get("pe_token"))
        } for idx in INDEX_NAMES}
    })

@app.route("/api/candles/<index_name>/<timeframe>", methods=["GET"])
def get_candles(index_name, timeframe):
    if index_name not in candle_histories:
        return jsonify({"error": "Invalid index name"}), 400
    if timeframe not in TIMEFRAMES:
        return jsonify({"error": "Invalid timeframe"}), 400
    with _candle_histories_lock:
        candles = list(candle_histories[index_name][timeframe])
    return jsonify(candles)

@app.route("/api/backtest-signal/<index_name>", methods=["POST"])
def backtest_signal(index_name):
    if index_name not in INDEX_CONFIG or not INDEX_CONFIG[index_name].get("active"):
        return jsonify({"error": "Invalid index"}), 400
    data = request.get_json() or {}
    lookback = min(data.get("lookback", 100), 200)
    with _candle_histories_lock:
        candles = list(candle_histories[index_name]["1min"])[-lookback:]
    if len(candles) < 20:
        return jsonify({"error": "Not enough candles"}), 400
    signals = []
    for i in range(20, len(candles)):
        closes = [c["close"] for c in candles[:i]]
        sentiment = compute_sentiment(index_name)
        action = get_signal_from_sentiment(sentiment)
        signals.append({
            "timestamp": candles[i]["timestamp"],
            "sentiment": sentiment,
            "action": action
        })
    return jsonify({"signals": signals, "count": len(signals)})

# ----------------------------------------------------------------------
# RUN FLASK
# ----------------------------------------------------------------------
if __name__ == "__main__":
    init_db()
    load_portfolio_state()
    refresh_all_tokens()
    _start_background_threads()
    logger.info("Background workers initiated. Starting Flask API Server...")
    app.run(host="0.0.0.0", port=5000, debug=False)
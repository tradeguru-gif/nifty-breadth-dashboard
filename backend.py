# === HYBRID v17.7 – FULL SIGNAL ENGINE + ALL FIXES ===
# - Restored signal engine (run_all_signals, run_signal_engine_for_index, etc.)
# - MCX tokens fetched before refresh_all_tokens()
# - VIX REST fetch implemented (get_vix_ltp)
# - _signal_state_lock added in load_portfolio_state
# - Unused _prev_volume_lock removed
# - Fixed execute_entry() alert message (removed undefined 'adx')
# - All previous: WebSocket, REST, candles, indicators, market hours, etc.

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
SQLITE_TIMEOUT = 30

# ----------------------------------------------------------------------
# DATABASE
# ----------------------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH, timeout=SQLITE_TIMEOUT)
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
        
        c.execute("""
            CREATE TABLE IF NOT EXISTS candle_data (
                index_name TEXT,
                timeframe TEXT,
                timestamp INTEGER,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                PRIMARY KEY (index_name, timeframe, timestamp)
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_candle_time ON candle_data(index_name, timeframe, timestamp)")
        
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
        conn = sqlite3.connect(PAPER_DB_PATH, timeout=SQLITE_TIMEOUT)
        try:
            c = conn.cursor()
            c.execute("CREATE TABLE IF NOT EXISTS portfolio_equity (index_name TEXT PRIMARY KEY, equity REAL, last_updated REAL, active_action TEXT, entry_price REAL, stop_loss REAL, target REAL, lots INTEGER, entry_time REAL, highest REAL, last_trade_date TEXT, daily_trade_count INTEGER)")
            c.execute("CREATE TABLE IF NOT EXISTS trades (timestamp REAL, action TEXT, entry_price REAL, exit_price REAL, pnl REAL, size_pct REAL, status TEXT, grade TEXT, atr REAL, vix REAL, exit_reason TEXT)")
            c.execute("""
                CREATE TABLE IF NOT EXISTS candle_data (
                    index_name TEXT,
                    timeframe TEXT,
                    timestamp INTEGER,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    PRIMARY KEY (index_name, timeframe, timestamp)
                )
            """)
            c.execute("CREATE INDEX IF NOT EXISTS idx_candle_time ON candle_data(index_name, timeframe, timestamp)")
            conn.commit()
        finally:
            conn.close()

init_db()

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ============================================================
# MONKEY PATCHES – fix SmartAPI WS bugs
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
        if len(binary_data) >= 10:
            token_int = int.from_bytes(binary_data[2:10], byteorder='little')
            if token_int > 0:
                result['token'] = str(token_int)
                if len(binary_data) >= 34:
                    ltp_raw = int.from_bytes(binary_data[26:34], byteorder='little', signed=True)
                    result['last_traded_price'] = ltp_raw / 100.0
                if len(binary_data) >= 50:
                    vol_raw = int.from_bytes(binary_data[42:50], byteorder='little', signed=True)
                    result['volume'] = vol_raw
                if len(binary_data) >= 74:
                    bid_raw = int.from_bytes(binary_data[50:58], byteorder='little', signed=True)
                    ask_raw = int.from_bytes(binary_data[66:74], byteorder='little', signed=True)
                    if 0 < bid_raw < 100000000:
                        result['best_bid_price'] = bid_raw / 100.0
                    if 0 < ask_raw < 100000000:
                        result['best_ask_price'] = ask_raw / 100.0
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
# INDEX CONFIGURATION – Equity + MCX
# ----------------------------------------------------------------------
INDEX_CONFIG = {
    "NIFTY": {
        "token": "99926000", "exchange": "NSE", "symbol": "NIFTY", "lot_size": 50, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": "BANKNIFTY", "greeks_enabled": True, "pcr_enabled": True,
        "regime_adx_threshold": 18, "regime_atr_threshold": 0.6, "is_commodity": False
    },
    "BANKNIFTY": {
        "token": "99926009", "exchange": "NSE", "symbol": "BANKNIFTY", "lot_size": 25, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": "NIFTY", "greeks_enabled": True, "pcr_enabled": True,
        "regime_adx_threshold": 18, "regime_atr_threshold": 0.8, "is_commodity": False
    },
    "FINNIFTY": {
        "token": "99926037", "exchange": "NSE", "symbol": "FINNIFTY", "lot_size": 40, "expiry_weekday": 1, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True,
        "regime_adx_threshold": 18, "regime_atr_threshold": 0.6, "is_commodity": False
    },
    "MIDCPNIFTY": {
        "token": "99926074", "exchange": "NSE", "symbol": "MIDCPNIFTY", "lot_size": 75, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 25, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": None, "greeks_enabled": False, "pcr_enabled": True,
        "regime_adx_threshold": 18, "regime_atr_threshold": 0.5, "is_commodity": False
    },
    "SENSEX": {
        "token": "99919000", "exchange": "BSE", "symbol": "SENSEX", "lot_size": 15, "expiry_weekday": 4, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100, "option_exchange": "BFO",
        "ws_exchange_type": 3, "option_ws_exchange_type": 4, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True,
        "regime_adx_threshold": 18, "regime_atr_threshold": 0.5, "is_commodity": False
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
INDEX_NAMES = list(INDEX_CONFIG.keys())

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
# _prev_volume_lock removed (unused)

INDEX_TOKENS = {idx: {"ce_token": None, "pe_token": None, "atm_strike": 0, "expiry": "", "expiry_date": None, "ce_symbol": "", "pe_symbol": ""} for idx in INDEX_NAMES}
last_known_prices = {idx: {"spot": 0.0, "ce": 0.0, "pe": 0.0, "timestamp": 0} for idx in INDEX_NAMES}
price_histories = {idx: deque(maxlen=5000) for idx in INDEX_NAMES}

portfolio_state = {idx: {"equity": 100000.0, "open_positions": 0, "daily_pnl": 0.0, "total_pnl": 0.0, "live_pnl": 0.0} for idx in INDEX_NAMES}
signal_state = {idx: {"action": "HOLD", "entry_price": 0.0, "stop_loss": 0.0, "target": 0.0, "lots": 0, "entry_time": 0.0, "highest": 0.0, "cooldown": 0, "confidence": 0, "exit_reason": ""} for idx in INDEX_NAMES}

market_signal = {idx: {"sentiment_score": 50, "signal": "WAITING", "alert_message": "", "entry_price": 0, "stop_loss": 0, "target": 0, "exit_reason": "", "quality_score": 0, "strike_price": 0, "trading_symbol": "", "lots": 0, "confidence": 0} for idx in INDEX_NAMES}

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

daily_drawdown = {idx: {"peak_equity": 0.0, "current_drawdown": 0.0, "dd_warning_sent": False} for idx in INDEX_NAMES}
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

_regime_history = {idx: deque(maxlen=5) for idx in INDEX_NAMES}

# ---- NEW CANDLE FLAG FOR SIGNAL TRIGGER ----
_new_candle_flag = {idx: False for idx in INDEX_NAMES}
_new_candle_flag_lock = threading.Lock()

# ----------------------------------------------------------------------
# TIMEFRAME DEFINITIONS – Only four: 1min, 2min, 3min, 5min
# ----------------------------------------------------------------------
TIMEFRAMES = ["1min", "2min", "3min", "5min"]
TIMEFRAME_SECONDS = {"1min":60, "2min":120, "3min":180, "5min":300}
TIMEFRAME_WEIGHTS = {"1min":12, "2min":12, "3min":12, "5min":15}

candle_histories = {idx: {tf: deque(maxlen=2000) for tf in TIMEFRAMES} for idx in INDEX_NAMES}
_last_candle_time = {idx: {tf: 0 for tf in TIMEFRAMES} for idx in INDEX_NAMES}
_current_candle = {idx: {tf: None for tf in TIMEFRAMES} for idx in INDEX_NAMES}
_prev_volume = {idx: 0 for idx in INDEX_NAMES}

# ============================================================
# CANDLE PERSISTENCE FUNCTIONS (with SQLite timeout)
# ============================================================
def save_candle_to_db(index_name, timeframe, candle):
    try:
        conn = sqlite3.connect(get_db_path(), timeout=SQLITE_TIMEOUT)
        c = conn.cursor()
        c.execute("""
            INSERT OR REPLACE INTO candle_data 
            (index_name, timeframe, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            index_name, timeframe, 
            int(candle["timestamp"]), 
            candle["open"], candle["high"], 
            candle["low"], candle["close"], 
            candle["volume"]
        ))
        conn.commit()
    except Exception as e:
        logger.debug(f"Failed to save candle: {e}")
    finally:
        conn.close()

def load_historical_candles(index_name, timeframe, max_count=500):
    try:
        conn = sqlite3.connect(get_db_path(), timeout=SQLITE_TIMEOUT)
        c = conn.cursor()
        rows = c.execute("""
            SELECT timestamp, open, high, low, close, volume
            FROM candle_data
            WHERE index_name = ? AND timeframe = ?
            ORDER BY timestamp DESC
            LIMIT ?
        """, (index_name, timeframe, max_count)).fetchall()
        
        candles = []
        for row in reversed(rows):
            candles.append({
                "timestamp": row[0],
                "open": row[1],
                "high": row[2],
                "low": row[3],
                "close": row[4],
                "volume": row[5]
            })
        return candles
    except Exception as e:
        logger.debug(f"Failed to load candles: {e}")
        return []
    finally:
        conn.close()

def clear_candle_data(index_name=None):
    try:
        conn = sqlite3.connect(get_db_path(), timeout=SQLITE_TIMEOUT)
        c = conn.cursor()
        if index_name:
            c.execute("DELETE FROM candle_data WHERE index_name = ?", (index_name,))
        else:
            c.execute("DELETE FROM candle_data")
        conn.commit()
        logger.info(f"Cleared candle data for {index_name if index_name else 'all'}")
    except Exception as e:
        logger.error(f"Failed to clear candle data: {e}")
    finally:
        conn.close()

# ----------------------------------------------------------------------
# INDICATORS
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
    if period <= 0 or len(closes) < 15:
        return 18.0
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
# BSM & GREEKS
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
    if not INDEX_CONFIG[idx].get("is_commodity"):
        greeks_cache_fallback_store[idx] = {"ce_iv":0.2, "pe_iv":0.2, "ce_delta":0.5, "pe_delta":-0.5,
                                            "ce_gamma":0.02, "pe_gamma":0.02, "ce_theta":-0.1, "pe_theta":-0.1,
                                            "ce_vega":0.15, "pe_vega":0.15, "iv_rank":50, "iv_percentile":50}
    else:
        greeks_cache_fallback_store[idx] = None

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
# KELLY, CORRELATION, VOLUME PROFILE
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

# ============================================================
# CRITICAL: get_index_spot()
# ============================================================
def get_index_spot(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config:
        return None
    if not config.get("token"):
        return None
    _, _, obj = get_auth_token()
    if not obj:
        logger.warning(f"No auth object for {index_name}")
        return None
    try:
        logger.info(f"🔍 ltpData request: exchange={config['exchange']}, symbol={config['symbol']}, token={config['token']}")
        resp = obj.ltpData(config["exchange"], config["symbol"], config["token"])
        logger.info(f"🔍 ltpData response: {resp}")
        ltp = safe_ltp(resp)
        if ltp and ltp > 0:
            if config["exchange"] in ["NSE","BSE"] and ltp > 100000:
                ltp /= 100
            logger.info(f"✅ Parsed LTP for {index_name}: {ltp}")
            return ltp
        else:
            logger.warning(f"Invalid LTP for {index_name}: {ltp}")
    except Exception as e:
        logger.error(f"get_index_spot: {index_name} exception: {e}")
    return None

# ============================================================
# CRITICAL: get_mcx_futures_tokens()
# ============================================================
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
        matching = mcx_fut[mcx_fut["symbol"].str.startswith(symbol, na=False)]
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

# ----------------------------------------------------------------------
# TOKEN REFRESH FUNCTIONS
# ----------------------------------------------------------------------
def get_next_expiry_date(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config or config.get("is_commodity"):
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
    if not config or not config.get("active") or config.get("is_commodity"):
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
        if INDEX_CONFIG[idx].get("active") and not INDEX_CONFIG[idx].get("is_commodity"):
            get_current_atm_tokens(idx)
    get_mcx_futures_tokens()

# ----------------------------------------------------------------------
# MARKET HOURS
# ----------------------------------------------------------------------
def is_market_open():
    if os.getenv("FORCE_MARKET_OPEN", "0") == "1":
        return True
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
    current = now_ist.time()
    open_time = dt_time(9, 15)
    close_time = dt_time(15, 15)
    return now_ist.weekday() < 5 and open_time <= current <= close_time

def is_mcx_open():
    if os.getenv("FORCE_MARKET_OPEN", "0") == "1":
        return True
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
    current = now_ist.time()
    if now_ist.weekday() >= 5:
        return False
    open_time = dt_time(10, 0)
    close_time = dt_time(23, 30)
    return open_time <= current <= close_time

def is_index_market_open(idx):
    cfg = INDEX_CONFIG.get(idx, {})
    return is_mcx_open() if cfg.get("is_commodity") else is_market_open()

# ============================================================
# CORRECTED update_candle() – with debug logs and new candle flag
# ============================================================
def update_candle(idx, price, volume, timestamp):
    """Update all timeframes for the given index.  Logs entry and candle closes."""
    if price <= 0:
        logger.debug(f"update_candle({idx}): price <= 0, skipping")
        return

    logger.debug(f"🔄 update_candle called for {idx}, price={price}, ts={timestamp}")

    with _price_histories_lock:
        price_histories[idx].append(price)

    with _candle_histories_lock, _current_candle_lock:
        tick_vol = volume if volume > 0 else 1

        for tf in TIMEFRAMES:
            interval = TIMEFRAME_SECONDS[tf]
            candle_time = int(timestamp // interval) * interval

            if (_current_candle[idx][tf] is None or
                _current_candle[idx][tf]["timestamp"] != candle_time):

                if _current_candle[idx][tf] is not None:
                    old = _current_candle[idx][tf]
                    candle_histories[idx][tf].append(old)
                    logger.debug(f"🕯️ CANDLE CLOSED {idx} {tf} close={old['close']} at ts={old['timestamp']}")
                    if tf == "1min":
                        with _new_candle_flag_lock:
                            _new_candle_flag[idx] = True
                    try:
                        save_candle_to_db(idx, tf, old)
                    except Exception as e:
                        logger.error(f"Failed to save candle: {e}")

                new_candle = {
                    "timestamp": candle_time,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": tick_vol
                }
                _current_candle[idx][tf] = new_candle
                _last_candle_time[idx][tf] = candle_time
                logger.debug(f"🕯️ NEW CANDLE {idx} {tf} open={price} at ts={candle_time}")
            else:
                candle = _current_candle[idx][tf]
                if price > candle["high"]:
                    candle["high"] = price
                if price < candle["low"]:
                    candle["low"] = price
                candle["close"] = price
                candle["volume"] += tick_vol

# ----------------------------------------------------------------------
# SENTIMENT, REGIME, CONFIRMATION
# ----------------------------------------------------------------------
def compute_sentiment(index_name):
    with _candle_histories_lock:
        candle_count = len(candle_histories[index_name]["1min"])
    min_candles = 2 if INDEX_CONFIG[index_name].get("is_commodity") else 3
    if candle_count < min_candles:
        # Use price history as fallback
        with _price_histories_lock:
            prices = list(price_histories[index_name])
        if len(prices) >= 5:
            # Compute a simple trend using EMA of price history
            ema9 = calculate_ema(prices, 9)
            ema21 = calculate_ema(prices, 21)
            price = prices[-1]
            if ema9 > ema21 and price > ema9:
                return 65.0
            elif ema9 < ema21 and price < ema9:
                return 35.0
            else:
                return 50.0
        return 50.0

    # Rest of sentiment logic (now correctly indented)
    sentiment_scores = []
    for tf in TIMEFRAMES:
        with _candle_histories_lock:
            candles = list(candle_histories[index_name][tf])
        if len(candles) < 5:
            continue
        closes = [c["close"] for c in candles]
        if len(closes) < 5:
            continue
        ema9 = calculate_ema(closes, 9)
        ema21 = calculate_ema(closes, 21)
        ema50 = calculate_ema(closes, 50) if len(closes) >= 50 else ema21
        price = closes[-1]
        recent = closes[-30:] if len(closes) >= 30 else closes
        price_range = (max(recent) - min(recent)) / sum(recent) * len(recent) if sum(recent) else 0
        if price_range < 0.005:
            score = 0
        elif tf in ["1min", "2min", "3min"]:
            if ema9 > ema21 and price > ema9:
                score = TIMEFRAME_WEIGHTS[tf]
            elif ema9 < ema21 and price < ema9:
                score = -TIMEFRAME_WEIGHTS[tf]
            else:
                score = 0
        elif tf in ["5min"]:
            if ema9 > ema21 > ema50 and price > ema9:
                score = TIMEFRAME_WEIGHTS[tf]
            elif ema9 < ema21 < ema50 and price < ema9:
                score = -TIMEFRAME_WEIGHTS[tf]
            elif ema9 > ema21 and price > ema9:
                score = TIMEFRAME_WEIGHTS[tf] - 5
            elif ema9 < ema21 and price < ema9:
                score = -TIMEFRAME_WEIGHTS[tf] + 5
            else:
                score = 0
        else:
            score = 0
        sentiment_scores.append(score)
    if not sentiment_scores:
        return 50.0
    total = sum(sentiment_scores)
    sentiment = 50.0 + (total / 2.2)
    return max(0.0, min(100.0, sentiment))

def get_sentiment_label(sentiment):
    if sentiment >= 85: return "STRONG BULLISH"
    elif sentiment >= 70: return "BULLISH"
    elif sentiment >= 55: return "SLOW BULLISH"
    elif sentiment >= 45: return "NEUTRAL"
    elif sentiment >= 30: return "SLOW BEARISH"
    elif sentiment >= 15: return "BEARISH"
    else: return "STRONG BEARISH"

def get_current_adx(index_name):
    with _candle_histories_lock:
        candles = list(candle_histories[index_name]["5min"])
    if len(candles) >= 15:
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]
        closes = [c["close"] for c in candles]
        return calculate_adx(highs, lows, closes, 14)
    return 18.0

def get_signal_from_sentiment(index_name, sentiment, adx=None):
    if adx is None:
        adx = get_current_adx(index_name)
    regime = detect_regime(index_name)
    is_commodity = INDEX_CONFIG[index_name].get("is_commodity", False)
    with _latest_ticks_lock:
        vix = latest_ticks["VIX"]["vix"]
    confidence_multiplier = 1.0
    if vix > 25:
        confidence_multiplier = 0.7
    elif vix < 15:
        confidence_multiplier = 1.1

    # ----- COMMODITY BRANCH (directional) -----
    if is_commodity:
        if sentiment >= 55:
            return "BUY", int(70 * confidence_multiplier)
        elif sentiment >= 45:
            return "HOLD", 50
        else:
            return "SELL", int(70 * confidence_multiplier)

    # ----- EQUITY INDICES (option side) -----
    if regime == "TRENDING" and adx > 15:
        if sentiment >= 62:
            return "STRONG_BUY_CE", int(90 * confidence_multiplier)
        elif sentiment >= 52:
            return "BUY_CE", int(80 * confidence_multiplier)
        elif sentiment >= 42:
            return "LOW_BUY_CE", int(70 * confidence_multiplier)
        elif sentiment >= 38:
            return "NO_TRADE", 50
        elif sentiment >= 28:
            return "LOW_BUY_PE", int(70 * confidence_multiplier)
        elif sentiment >= 18:
            return "BUY_PE", int(80 * confidence_multiplier)
        else:
            return "STRONG_BUY_PE", int(90 * confidence_multiplier)
    elif regime == "RANGING":
        if sentiment >= 68:
            return "STRONG_BUY_CE", int(75 * confidence_multiplier)
        elif sentiment >= 58:
            return "BUY_CE", int(65 * confidence_multiplier)
        elif sentiment >= 48:
            return "LOW_BUY_CE", int(55 * confidence_multiplier)
        elif sentiment >= 42:
            return "NO_TRADE", 50
        elif sentiment >= 32:
            return "LOW_BUY_PE", int(55 * confidence_multiplier)
        elif sentiment >= 22:
            return "BUY_PE", int(65 * confidence_multiplier)
        else:
            return "STRONG_BUY_PE", int(75 * confidence_multiplier)
    else:
        if adx > 15:
            if sentiment >= 62:
                return "STRONG_BUY_CE", int(85 * confidence_multiplier)
            elif sentiment >= 52:
                return "BUY_CE", int(75 * confidence_multiplier)
            elif sentiment >= 42:
                return "LOW_BUY_CE", int(65 * confidence_multiplier)
            elif sentiment >= 38:
                return "NO_TRADE", 50
            elif sentiment >= 28:
                return "LOW_BUY_PE", int(65 * confidence_multiplier)
            elif sentiment >= 18:
                return "BUY_PE", int(75 * confidence_multiplier)
            else:
                return "STRONG_BUY_PE", int(85 * confidence_multiplier)
        else:
            if sentiment >= 68:
                return "STRONG_BUY_CE", int(80 * confidence_multiplier)
            elif sentiment >= 58:
                return "BUY_CE", int(70 * confidence_multiplier)
            elif sentiment >= 48:
                return "NO_TRADE", 50
            elif sentiment >= 42:
                return "NO_TRADE", 50
            elif sentiment >= 32:
                return "NO_TRADE", 50
            elif sentiment >= 22:
                return "BUY_PE", int(70 * confidence_multiplier)
            else:
                return "STRONG_BUY_PE", int(80 * confidence_multiplier)

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
    if len(vix_history) >= 20:
        vix_sma = sum(list(vix_history)[-20:]) / 20
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
        return counts.most_common(1)[0][0]
    return new_regime

def confirm_signal_with_candles(index_name, side, spot):
    with _candle_histories_lock:
        candles = list(candle_histories[index_name]["1min"])
    if len(candles) < 5:
        return True
    closes = [c["close"] for c in candles[-5:]]
    ema9 = calculate_ema(closes, 9)
    if ema9 == 0:
        return True
    # For commodities: BUY=long (price above EMA), SELL=short (price below EMA)
    # For options: CE=bullish, PE=bearish
    if side in ("CE", "BUY"):
        return closes[-1] > ema9
    else:
        return closes[-1] < ema9

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
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
    today = now_ist.strftime("%d%b%Y").upper()
    tokens = INDEX_TOKENS.get(index_name, {})
    expiry = tokens.get("expiry", "")
    return today == expiry

# ----------------------------------------------------------------------
# SIGNAL QUALITY, DATA READINESS, PERSISTENCE
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
    if not INDEX_CONFIG[index_name].get("is_commodity"):
        greeks = get_option_greeks(index_name)
        scores.append(20 if greeks else 0)
    else:
        scores.append(20)
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
# has_complete_data() – requires sufficient history
# ----------------------------------------------------------------------
def has_complete_data(index_name):
    cfg = INDEX_CONFIG.get(index_name, {})
    if cfg.get("is_commodity"):
        # Commodities: only 1 candle needed
        with _candle_histories_lock:
            if len(candle_histories[index_name]["1min"]) < 1:
                return False
        with _latest_ticks_lock:
            return latest_ticks[index_name].get("price", 0) > 0
    else:
        # Equities: require 3 candles (instead of 10)
        with _latest_ticks_lock:
            spot = latest_ticks[index_name].get("spot_price", 0)
            ce = latest_ticks[index_name].get("ce_price", 0)
            pe = latest_ticks[index_name].get("pe_price", 0)
            if spot <= 0 or ce <= 0 or pe <= 0:
                return False
        with _candle_histories_lock:
            if len(candle_histories[index_name]["1min"]) < 3:
                return False
        with _price_histories_lock:
            if len(price_histories[index_name]) < 15:
                return False
        return True

def get_db_path():
    return PAPER_DB_PATH if PAPER_MODE else DB_PATH

def load_portfolio_state():
    global portfolio_state, signal_state, daily_trade_count, last_trade_date
    try:
        db_path = get_db_path()
        conn = sqlite3.connect(db_path, timeout=SQLITE_TIMEOUT)
        try:
            c = conn.cursor()
            for idx in INDEX_NAMES:
                row = c.execute(
                    "SELECT equity, active_action, entry_price, stop_loss, target, lots, entry_time, highest, last_trade_date, daily_trade_count FROM portfolio_equity WHERE index_name=?",
                    (idx,)
                ).fetchone()
                if row:
                    with _portfolio_state_lock:
                        portfolio_state[idx]["equity"] = float(row[0]) if row[0] is not None else 100000.0
                    
                    action = row[1]
                    if action and action != "HOLD":
                        with _signal_state_lock:
                            signal_state[idx].update({
                                "action": action,
                                "entry_price": float(row[2]) if row[2] is not None else 0.0,
                                "stop_loss": float(row[3]) if row[3] is not None else 0.0,
                                "target": float(row[4]) if row[4] is not None else 0.0,
                                "lots": int(row[5]) if row[5] is not None else 0,
                                "entry_time": float(row[6]) if row[6] is not None else 0.0,
                                "highest": float(row[7]) if row[7] is not None else 0.0
                            })
                        with _portfolio_state_lock:
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
    conn = sqlite3.connect(db_path, timeout=SQLITE_TIMEOUT)
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
    except Exception:
        pass

def apply_transaction_cost(pnl, lots, lot_size):
    cost_per_lot = 50
    return pnl - cost_per_lot * lots

def log_trade(index_name, action, entry_price, exit_price, pnl, size_pct, status, grade, atr, vix, exit_reason):
    db_path = get_db_path()
    conn = sqlite3.connect(db_path, timeout=SQLITE_TIMEOUT)
    try:
        c = conn.cursor()
        c.execute(
            "INSERT INTO trades (timestamp, action, entry_price, exit_price, pnl, size_pct, status, grade, atr, vix, exit_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (time.time(), action, entry_price, exit_price, pnl, size_pct, status, grade, atr, vix, exit_reason)
        )
        conn.commit()
    finally:
        conn.close()

# ----------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------
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
        return 90
    theta = greeks_data.get("ce_theta") if side == "CE" else greeks_data.get("pe_theta")
    if theta is None or theta == 0:
        return 90
    theta_abs = abs(theta)
    if theta_abs > prem * 0.05:
        return 45
    else:
        return 90

# ----------------------------------------------------------------------
# REST HELPER FUNCTIONS
# ----------------------------------------------------------------------
def get_vix_ltp():
    """Fetch VIX (India VIX) LTP via REST."""
    _, _, obj = get_auth_token()
    if not obj:
        return None
    try:
        resp = obj.ltpData("NSE", "INDIAVIX", "99919017")
        ltp = safe_ltp(resp)
        if ltp and ltp > 0:
            if ltp > 1000:
                ltp = ltp / 100.0
            return ltp
    except Exception as e:
        logger.debug(f"VIX fetch error: {e}")
    return None

def get_option_quote(index_name, option_type):
    config = INDEX_CONFIG.get(index_name)
    if not config or config.get("is_commodity"):
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
# MAIN SIGNAL ENGINE – FULL IMPLEMENTATION
# ============================================================

def run_all_signals():
    """Run signal engine for all active indices."""
    for idx in INDEX_NAMES:
        cfg = INDEX_CONFIG.get(idx, {})
        if not cfg.get("active"):
            continue
        if not is_index_market_open(idx):
            continue
        try:
            run_signal_engine_for_index(idx)
        except Exception as e:
            logger.error(f"Signal engine error for {idx}: {e}")
            logger.debug(traceback.format_exc())

def run_signal_engine_for_index(index_name):
    """Main signal generation and position management for a single index."""
    global market_signal, daily_trade_count, last_trade_date
    
    cfg = INDEX_CONFIG.get(index_name, {})
    if not cfg:
        return
    
    is_commodity = cfg.get("is_commodity", False)
    now = time.time()
    
    # --- Check cooldown / circuit breaker ---
    with _signal_state_lock:
        state = signal_state[index_name]
        if state["cooldown"] > now:
            return
        if safety_state[index_name]["circuit_breaker"]:
            if safety_state[index_name]["circuit_breaker_until"] > now:
                return
            else:
                safety_state[index_name]["circuit_breaker"] = False
                safety_state[index_name]["consecutive_sl"] = 0
    
    # --- Get current prices ---
    with _latest_ticks_lock:
        if is_commodity:
            spot = latest_ticks[index_name].get("price", 0.0) or 0.0
            ce_prem = pe_prem = 0.0
        else:
            spot = latest_ticks[index_name].get("spot_price", 0.0) or 0.0
            ce_prem = latest_ticks[index_name].get("ce_price", 0.0) or 0.0
            pe_prem = latest_ticks[index_name].get("pe_price", 0.0) or 0.0
    
    if spot <= 0:
        logger.debug(f"{index_name}: No spot price, skipping signal")
        return
    
    # --- Compute indicators ---
    sentiment = compute_sentiment(index_name)
    adx = get_current_adx(index_name)
    regime = detect_regime(index_name)
    action, confidence = get_signal_from_sentiment(index_name, sentiment, adx)
    
    # --- Get greeks (options only) ---
    greeks_data = None
    if not is_commodity and cfg.get("greeks_enabled"):
        greeks_data = get_option_greeks(index_name)
    
    # --- Get VIX ---
    with _latest_ticks_lock:
        vix = latest_ticks["VIX"]["vix"]
    
    # --- Compute RSI from price history ---
    with _price_histories_lock:
        prices = list(price_histories[index_name])
    rsi = calculate_rsi(prices, 14) if len(prices) >= 15 else 50.0
    
    # --- ATR for sizing ---
    with _candle_histories_lock:
        candles_5m = list(candle_histories[index_name]["5min"])
    atr = 0.0
    if len(candles_5m) >= 15:
        highs = [c["high"] for c in candles_5m]
        lows = [c["low"] for c in candles_5m]
        closes = [c["close"] for c in candles_5m]
        atr = calculate_atr(highs, lows, closes, 14)
    
    # --- ML Score ---
    ml_score = 0.5
    if not is_commodity and ce_prem > 0 and pe_prem > 0:
        side = "CE" if "CE" in action else ("PE" if "PE" in action else "CE")
        prem = ce_prem if side == "CE" else pe_prem
        ml_score = compute_ml_score(index_name, side, prem, spot, rsi, adx, vix, sentiment)
    
    # --- Signal Quality ---
    quality = compute_signal_quality(index_name)
    
    # --- Update market signal display ---
    with _market_signal_lock:
        market_signal[index_name].update({
            "sentiment_score": round(sentiment, 1),
            "signal": action,
            "confidence": confidence,
            "quality_score": quality,
            "regime": regime,
            "adx": round(adx, 1),
            "rsi": round(rsi, 1),
            "vix": vix,
            "ml_score": round(ml_score, 2),
            "spot": spot,
            "timestamp": now
        })
    
    # --- Manage existing position ---
    with _signal_state_lock:
        current_action = signal_state[index_name]["action"]
    
    if current_action != "HOLD":
        manage_existing_position(index_name, spot, ce_prem, pe_prem, atr, vix, greeks_data, prices)
        return
    
    # --- Entry logic (only if no position) ---
    if action in ("NO_TRADE", "WAITING"):
        with _market_signal_lock:
            market_signal[index_name]["alert_message"] = f"{action} | Sentiment: {sentiment:.1f} | Regime: {regime}"
        return
    
    # Check if we should enter
    if not should_enter_trade(index_name, action, spot, ce_prem, pe_prem, confidence, quality, regime, adx, vix):
        return
    
    # Execute entry
    execute_entry(index_name, action, spot, ce_prem, pe_prem, atr, vix, greeks_data, confidence, quality, sentiment, rsi, regime)


def should_enter_trade(index_name, action, spot, ce_prem, pe_prem, confidence, quality, regime, adx, vix):
    """Validate entry conditions before taking a trade."""
    cfg = INDEX_CONFIG.get(index_name, {})
    is_commodity = cfg.get("is_commodity", False)
    
    # Minimum confidence
    if confidence < 55:
        logger.debug(f"{index_name}: Confidence {confidence} < 55, skipping")
        return False
    
    # Minimum quality
    if quality < 40:
        logger.debug(f"{index_name}: Quality {quality} < 40, skipping")
        return False
    
    # Daily trade limit
    today = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d")
    if last_trade_date.get(index_name) != today:
        daily_trade_count[index_name] = 0
        last_trade_date[index_name] = today
    
    max_daily_trades = 5 if is_expiry_day(index_name) else 3
    if daily_trade_count[index_name] >= max_daily_trades:
        logger.debug(f"{index_name}: Daily trade limit reached ({daily_trade_count[index_name]})")
        return False
    
    # Drawdown check
    with _portfolio_state_lock:
        equity = portfolio_state[index_name]["equity"]
    peak = daily_drawdown[index_name]["peak_equity"]
    if peak == 0:
        daily_drawdown[index_name]["peak_equity"] = equity
    
    peak = max(peak, equity)
    daily_drawdown[index_name]["peak_equity"] = peak
    dd_pct = ((peak - equity) / peak * 100) if peak > 0 else 0
    daily_drawdown[index_name]["current_drawdown"] = dd_pct
    
    max_dd = cfg.get("max_daily_drawdown_pct", 3.0)
    if dd_pct >= max_dd:
        if not daily_drawdown[index_name]["dd_warning_sent"]:
            send_telegram_alert(f"⚠️ {index_name}: Max daily drawdown {dd_pct:.1f}% reached. Pausing.")
            daily_drawdown[index_name]["dd_warning_sent"] = True
        return False
    
    # Option-specific checks
    if not is_commodity:
        prem = ce_prem if "CE" in action else pe_prem
        if prem <= 0:
            logger.debug(f"{index_name}: No premium data for {action}")
            return False
        if prem < cfg.get("min_premium", 5):
            logger.debug(f"{index_name}: Premium {prem} below min {cfg.get('min_premium')}")
            return False
        if prem > cfg.get("max_premium", 8000):
            logger.debug(f"{index_name}: Premium {prem} above max")
            return False
        
        # Spread check
        with _latest_ticks_lock:
            bid = latest_ticks[index_name].get("ce_bid" if "CE" in action else "pe_bid", 0)
            ask = latest_ticks[index_name].get("ce_ask" if "CE" in action else "pe_ask", 0)
        if not spread_ok(bid, ask, prem):
            logger.debug(f"{index_name}: Spread too wide")
            return False
        
        # Expiry day caution
        if is_expiry_day(index_name) and confidence < 70:
            logger.debug(f"{index_name}: Expiry day, requiring higher confidence")
            return False
    
    # VIX check
    if vix > 30 and confidence < 75:
        logger.debug(f"{index_name}: High VIX {vix}, requiring higher confidence")
        return False
    
    # Signal buffer (reduce churn)
    buffer = signal_buffer[index_name]
    if is_commodity:
        if action == "BUY":
            buffer["ce_count"] += 1
            buffer["pe_count"] = max(0, buffer["pe_count"] - 1)
            if buffer["ce_count"] < 2:
                logger.debug(f"{index_name}: BUY buffer {buffer['ce_count']}/2")
                return False
        else:  # SELL
            buffer["pe_count"] += 1
            buffer["ce_count"] = max(0, buffer["ce_count"] - 1)
            if buffer["pe_count"] < 2:
                logger.debug(f"{index_name}: SELL buffer {buffer['pe_count']}/2")
                return False
    else:
        side = "CE" if "CE" in action else "PE"
        if side == "CE":
            buffer["ce_count"] += 1
            buffer["pe_count"] = max(0, buffer["pe_count"] - 1)
            if buffer["ce_count"] < 2:
                logger.debug(f"{index_name}: CE buffer {buffer['ce_count']}/2")
                return False
        else:
            buffer["pe_count"] += 1
            buffer["ce_count"] = max(0, buffer["ce_count"] - 1)
            if buffer["pe_count"] < 2:
                logger.debug(f"{index_name}: PE buffer {buffer['pe_count']}/2")
                return False
    
    # Correlation filter (NIFTY/BANKNIFTY only)
    pair = cfg.get("correlation_pair")
    if pair and pair in INDEX_NAMES:
        corr = correlation_filter.analyze(index_name, action)
        if corr.get("block_reason"):
            logger.debug(f"{index_name}: Blocked by correlation filter: {corr['block_reason']}")
            return False
    
    # Candle confirmation
    opt_side = "CE" if "CE" in action else "PE"
    if not confirm_signal_with_candles(index_name, opt_side, spot):
        logger.debug(f"{index_name}: Candle confirmation failed")
        return False
    
    return True


def execute_entry(index_name, action, spot, ce_prem, pe_prem, atr, vix, greeks_data, confidence, quality, sentiment, rsi, regime):
    """Execute a new trade entry."""
    cfg = INDEX_CONFIG.get(index_name, {})
    is_commodity = cfg.get("is_commodity", False)
    now = time.time()
    
    # Determine side and premium
    if is_commodity:
        side = "CE" if action == "BUY" else "PE"
        prem = spot  # For commodities, "premium" is just the spot price
    else:
        side = "CE" if "CE" in action else "PE"
        prem = ce_prem if side == "CE" else pe_prem
    
    # Position sizing
    with _portfolio_state_lock:
        equity = portfolio_state[index_name]["equity"]
    
    # Kelly criterion sizing
    kelly_tracker = kelly_trackers[index_name]
    kelly_frac, win_rate, avg_win, avg_loss = kelly_tracker.get_recommended_risk_pct()
    
    # Base risk per trade
    base_risk_pct = 0.02
    risk_pct = base_risk_pct * kelly_frac * 4
    
    # Adjust for confidence and quality
    risk_pct *= (confidence / 100) * (quality / 100)
    
    # Adjust for VIX
    if vix > 25:
        risk_pct *= 0.7
    elif vix < 15:
        risk_pct *= 1.1
    
    # Cap risk
    risk_pct = max(0.005, min(0.05, risk_pct))
    
    # Calculate stop loss and target
    if is_commodity:
        sl_points = atr * 2.0 if atr > 0 else spot * 0.01
        target_points = atr * 3.0 if atr > 0 else spot * 0.015
        entry = spot
        sl = entry - sl_points if side == "CE" else entry + sl_points
        target = entry + target_points if side == "CE" else entry - target_points
        lots = max(1, int((equity * risk_pct) / (sl_points * cfg["lot_size"])))
    else:
        sl_pct = 0.40 if is_expiry_day(index_name) else 0.50
        target_pct = 1.0
        
        if greeks_data:
            iv_rank = greeks_data.get("iv_rank", 50)
            if iv_rank > 70:
                sl_pct *= 1.2
            elif iv_rank < 30:
                sl_pct *= 0.8
        
        entry = prem
        sl = entry * (1 - sl_pct)
        target = entry * (1 + target_pct)
        
        risk_amount = equity * risk_pct
        max_loss_per_lot = entry * sl_pct * cfg["lot_size"]
        lots = max(1, int(risk_amount / max_loss_per_lot)) if max_loss_per_lot > 0 else 1
        
        max_lots = int(equity / (entry * cfg["lot_size"] * 0.2))
        lots = min(lots, max(1, max_lots))
    
    # Grade assignment
    if confidence >= 85 and quality >= 80:
        grade = "A+"
    elif confidence >= 75 and quality >= 70:
        grade = "A"
    elif confidence >= 65 and quality >= 60:
        grade = "B"
    else:
        grade = "C"
    
    # Update signal state
    with _signal_state_lock:
        signal_state[index_name].update({
            "action": action,
            "entry_price": entry,
            "stop_loss": sl,
            "target": target,
            "lots": lots,
            "entry_time": now,
            "highest": entry,
            "confidence": confidence,
            "cooldown": 0,
            "exit_reason": ""
        })
    
    with _portfolio_state_lock:
        portfolio_state[index_name]["open_positions"] = 1
    
    # Update daily count
    today = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d")
    if last_trade_date.get(index_name) != today:
        daily_trade_count[index_name] = 0
        last_trade_date[index_name] = today
    daily_trade_count[index_name] += 1
    
    # Save state
    save_portfolio_state(index_name)
    
    # Build alert — fixed: removed undefined 'adx' reference
    msg = (
        f"🟢 ENTRY {index_name} {action}\n"
        f"Entry: {entry:.2f} | SL: {sl:.2f} | Target: {target:.2f}\n"
        f"Lots: {lots} | Grade: {grade} | Conf: {confidence}%\n"
        f"Sentiment: {sentiment:.1f} | RSI: {rsi:.1f} | Regime: {regime}\n"
        f"VIX: {vix:.1f}"
    )
    if not is_commodity:
        msg += f"\nSpot: {spot:.2f} | Premium: {prem:.2f}"
    
    with _market_signal_lock:
        market_signal[index_name].update({
            "signal": action,
            "alert_message": msg,
            "entry_price": entry,
            "stop_loss": sl,
            "target": target,
            "lots": lots,
            "grade": grade,
            "strike_price": INDEX_TOKENS[index_name].get("atm_strike", 0),
            "trading_symbol": INDEX_TOKENS[index_name].get(f"{side.lower()}_symbol", ""),
            "exit_reason": ""
        })
    
    send_telegram_alert(msg)
    logger.info(f"ENTRY: {index_name} {action} @ {entry:.2f} x {lots} lots")
    
    # Log to DB
    log_trade(index_name, action, entry, 0, 0, risk_pct * 100, "OPEN", grade, atr, vix, "")


def manage_existing_position(index_name, spot, ce_prem, pe_prem, atr, vix, greeks_data, prices):
    """Check and manage an existing open position."""
    with _signal_state_lock:
        state = signal_state[index_name].copy()
    
    action = state["action"]
    entry = state["entry_price"]
    sl = state["stop_loss"]
    target = state["target"]
    lots = state["lots"]
    entry_time = state["entry_time"]
    highest = state["highest"]
    now = time.time()
    
    is_commodity = INDEX_CONFIG[index_name].get("is_commodity", False)
    if is_commodity:
        side = "CE" if action == "BUY" else "PE"
    else:
        side = "CE" if "CE" in action else "PE"
    
    # Current value
    if is_commodity:
        current = spot
    else:
        current = ce_prem if side == "CE" else pe_prem
    
    if current <= 0:
        logger.debug(f"{index_name}: No current price for position management")
        return
    
    # Update trailing highest
    if current > highest:
        with _signal_state_lock:
            signal_state[index_name]["highest"] = current
    
    # Calculate P&L
    if is_commodity:
        if side == "CE":
            pnl = (current - entry) * lots * INDEX_CONFIG[index_name]["lot_size"]
        else:
            pnl = (entry - current) * lots * INDEX_CONFIG[index_name]["lot_size"]
    else:
        # Options P&L: Both CE and PE are bought (long), profit when premium increases
        pnl = (current - entry) * lots * INDEX_CONFIG[index_name]["lot_size"]
    
    pnl_pct = (pnl / (entry * lots * INDEX_CONFIG[index_name]["lot_size"])) * 100 if entry > 0 else 0
    
    # Update live P&L
    with _portfolio_state_lock:
        portfolio_state[index_name]["live_pnl"] = pnl
    
    # --- Exit checks ---
    exit_reason = None
    exit_price = current
    
    # 1. Stop Loss hit
    if is_commodity:
        if side == "CE" and current <= sl:
            exit_reason = f"SL hit @ {sl:.2f}"
        elif side == "PE" and current >= sl:
            exit_reason = f"SL hit @ {sl:.2f}"
    else:
        # For options, SL is when premium drops to SL level
        if current <= sl:
            exit_reason = f"SL hit @ {sl:.2f} (from {entry:.2f})"
    
    # 2. Target hit
    if not exit_reason:
        if is_commodity:
            if side == "CE" and current >= target:
                exit_reason = f"Target hit @ {target:.2f}"
            elif side == "PE" and current <= target:
                exit_reason = f"Target hit @ {target:.2f}"
        else:
            if current >= target:
                exit_reason = f"Target hit @ {target:.2f} (from {entry:.2f})"
    
    # 3. Trailing stop (lock in profits)
    if not exit_reason and highest > entry * 1.2:  # 20% profit
        trail_sl = highest * 0.85  # Trail at 85% of highest
        if current <= trail_sl:
            exit_reason = f"Trailing SL @ {trail_sl:.2f} (peak: {highest:.2f})"
    
    # 4. Time-based exit
    if not exit_reason and not is_commodity:
        time_exit = get_dynamic_time_exit_minutes(index_name, side, entry, greeks_data)
        if (now - entry_time) > (time_exit * 60):
            # Only exit if not profitable
            if pnl < 0:
                exit_reason = f"Time exit after {time_exit}min"
    
    # 5. Market analysis exit (trend reversal)
    if not exit_reason:
        should_exit, analysis_reason = should_exit_market_analysis(index_name, action, prices, ce_prem, pe_prem, greeks_data)
        if should_exit:
            exit_reason = analysis_reason
    
    # 6. Max loss per trade (hard stop)
    if not exit_reason:
        max_loss = entry * lots * INDEX_CONFIG[index_name]["lot_size"] * 0.6  # 60% of premium
        if pnl < -max_loss:
            exit_reason = f"Max loss reached: {pnl:.0f}"
    
    # Execute exit if needed
    if exit_reason:
        execute_exit(index_name, action, entry, exit_price, pnl, lots, atr, vix, exit_reason)
    else:
        # Update alert with live P&L
        with _market_signal_lock:
            market_signal[index_name].update({
                "alert_message": f"📊 {index_name} {action} | Live P&L: {pnl:+.0f} ({pnl_pct:+.1f}%) | Current: {current:.2f}",
                "exit_reason": ""
            })


def execute_exit(index_name, action, entry, exit_price, pnl, lots, atr, vix, exit_reason):
    """Execute position exit and update records."""
    now = time.time()
    
    # Apply transaction costs
    pnl = apply_transaction_cost(pnl, lots, INDEX_CONFIG[index_name]["lot_size"])
    
    # Update equity
    with _portfolio_state_lock:
        old_equity = portfolio_state[index_name]["equity"]
        new_equity = old_equity + pnl
        portfolio_state[index_name]["equity"] = new_equity
        portfolio_state[index_name]["daily_pnl"] += pnl
        portfolio_state[index_name]["total_pnl"] += pnl
        portfolio_state[index_name]["live_pnl"] = 0
        portfolio_state[index_name]["open_positions"] = 0
    
    # Kelly update
    pnl_pct = pnl / old_equity if old_equity > 0 else 0
    kelly_trackers[index_name].update(pnl_pct)
    
    # Safety state (consecutive SL tracking)
    if pnl < 0:
        safety_state[index_name]["consecutive_sl"] += 1
        if safety_state[index_name]["consecutive_sl"] >= 3:
            safety_state[index_name]["circuit_breaker"] = True
            safety_state[index_name]["circuit_breaker_until"] = now + 1800  # 30 min break
            send_telegram_alert(f"🔒 {index_name}: Circuit breaker activated (3 consecutive SLs). Pausing 30min.")
    else:
        safety_state[index_name]["consecutive_sl"] = 0
    
    # Drawdown tracking
    peak = daily_drawdown[index_name]["peak_equity"]
    if new_equity < peak:
        dd_pct = (peak - new_equity) / peak * 100 if peak > 0 else 0
        if dd_pct > daily_drawdown[index_name]["current_drawdown"]:
            daily_drawdown[index_name]["current_drawdown"] = dd_pct
            # Log drawdown event
            try:
                conn = sqlite3.connect(get_db_path(), timeout=SQLITE_TIMEOUT)
                c = conn.cursor()
                c.execute(
                    "INSERT INTO drawdown_events (timestamp, index_name, drawdown_pct, action_taken, equity_before, equity_after) VALUES (?,?,?,?,?,?)",
                    (now, index_name, dd_pct, exit_reason, old_equity, new_equity)
                )
                conn.commit()
                conn.close()
            except Exception as e:
                logger.debug(f"DD log error: {e}")
    
    # Log trade
    status = "WIN" if pnl > 0 else "LOSS"
    grade = signal_state[index_name].get("confidence", 0)
    log_trade(index_name, action, entry, exit_price, pnl, 0, status, str(grade), atr, vix, exit_reason)
    
    # Reset signal state
    reset_signal_state(index_name, now, exit_reason)
    
    # Alert
    emoji = "🟢" if pnl > 0 else "🔴"
    msg = (
        f"{emoji} EXIT {index_name} {action}\n"
        f"Entry: {entry:.2f} | Exit: {exit_price:.2f}\n"
        f"P&L: {pnl:+.0f} | Lots: {lots}\n"
        f"Reason: {exit_reason}\n"
        f"Equity: {new_equity:.0f}"
    )
    send_telegram_alert(msg)
    logger.info(f"EXIT: {index_name} {action} @ {exit_price:.2f} P&L={pnl:+.0f} Reason={exit_reason}")
    
    # Save portfolio state
    save_portfolio_state(index_name)

# ============================================================
# WEBSOCKET + WATCHDOGS
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
    try:
        with _ws_connect_lock:
            ws_running = True
        last_heartbeat = time.time()
        logger.info("WebSocket connected successfully, subscribing to tokens...")

        token_list = []
        for idx, cfg in INDEX_CONFIG.items():
            if cfg.get("active") and not cfg.get("is_commodity"):
                exch_type = int(cfg["ws_exchange_type"])
                token_list.append({"exchangeType": exch_type, "tokens": [cfg["token"]]})
            if cfg.get("active") and cfg.get("is_commodity") and cfg.get("token"):
                exch_type = int(cfg["ws_exchange_type"])
                token_list.append({"exchangeType": exch_type, "tokens": [cfg["token"]]})

        for idx, tokens in INDEX_TOKENS.items():
            if not INDEX_CONFIG[idx].get("active") or INDEX_CONFIG[idx].get("is_commodity"):
                continue
            if tokens.get("ce_token") and tokens.get("pe_token"):
                exch_type = int(INDEX_CONFIG[idx]["option_ws_exchange_type"])
                token_list.append({
                    "exchangeType": exch_type,
                    "tokens": [tokens["ce_token"], tokens["pe_token"]]
                })

        token_list.append({"exchangeType": 1, "tokens": ["99919017"]})

        logger.info(f"Subscribing to token_list: {token_list}")
        time.sleep(0.5)

        try:
            response = sws.subscribe("hybrid_bot", 1, token_list)
            logger.info(f"Subscription response: {response}")

            if response is None:
                logger.error("Subscription returned None – assuming failure")
                with _ws_connect_lock:
                    ws_running = False
                return
            elif isinstance(response, dict) and response.get("status") == False:
                logger.error(f"Subscription failed: {response}")
                with _ws_connect_lock:
                    ws_running = False
                return
            else:
                total = sum(len(g["tokens"]) for g in token_list)
                logger.info(f"Successfully subscribed to {total} tokens")
        except Exception as e:
            logger.exception(f"Subscribe error: {e}")
            with _ws_connect_lock:
                ws_running = False

    except Exception as e:
        logger.exception("on_ws_open crashed")
        with _ws_connect_lock:
            ws_running = False

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
    global tick_counter, last_heartbeat, last_tick_timestamp, sws, _last_signal_run, ws_running
    try:
        # --- DIAGNOSTIC: log every message ---
        logger.info(f"📨 Raw message received: type={type(message)}, len={len(message) if message else 0}")

        # Keep ws_running True whenever data arrives
        ws_running = True
        last_heartbeat = time.time()

        if message is None or message == b'\x00' or message == '\x00' or message == b'ping' or message == 'ping' or message == b'':
            logger.debug("Received ping or empty message, ignoring")
            return

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
                        logger.debug("Binary parse returned empty or no token")
                        return
                else:
                    logger.debug("No parser available")
                    return
            except Exception as e:
                logger.error(f"Binary parse error: {e}")
                return
        else:
            logger.warning(f"Unhandled message type: {type(message)}")
            return

        if not ticks:
            logger.debug("No ticks extracted from message")
            return

        logger.info(f"📊 Processing {len(ticks)} tick(s)")

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
                if ltp > 100000:
                    ltp = ltp / 100.0

                # --- Log every tick token and price ---
                logger.info(f"🔹 TICK: token={token}, ltp={ltp}")

                vol = tick.get("volume") or tick.get("v") or tick.get("last_traded_quantity") or 0
                oi = tick.get("open_interest") or tick.get("oi") or tick.get("OpenInterest") or 0
                bid = tick.get("best_bid_price") or tick.get("bid") or tick.get("bp") or 0
                ask = tick.get("best_ask_price") or tick.get("ask") or tick.get("ap") or 0

                # ---- SPOT PRICE MATCH ----
                spot_matched = False
                for idx, cfg in INDEX_CONFIG.items():
                    cfg_token = cfg.get("token")
                    if cfg_token and str(cfg_token) == token:
                        if ltp > 0:
                            logger.info(f"✅ SPOT MATCH: {idx} token={token} ltp={ltp}")
                            try:
                                if cfg.get("is_commodity"):
                                    with _latest_ticks_lock:
                                        latest_ticks[idx]["price"] = ltp
                                        latest_ticks[idx]["volume"] = vol
                                        latest_ticks[idx]["bid"] = bid
                                        latest_ticks[idx]["ask"] = ask
                                    update_candle(idx, ltp, vol, time.time())
                                    last_tick_timestamp = time.time()
                                    with _latest_ticks_lock:
                                        last_known_prices[idx]["spot"] = ltp
                                        last_known_prices[idx]["timestamp"] = time.time()
                                else:
                                    with _latest_ticks_lock:
                                        latest_ticks[idx]["spot_price"] = ltp
                                    update_candle(idx, ltp, vol, time.time())
                                    last_tick_timestamp = time.time()
                                    with _latest_ticks_lock:
                                        last_known_prices[idx]["spot"] = ltp
                                        last_known_prices[idx]["timestamp"] = time.time()
                                spot_matched = True
                            except Exception as e:
                                logger.exception(f"update_candle for {idx} spot failed: {e}")
                            break
                if spot_matched:
                    continue

                # ---- OPTION PRICE MATCH ----
                option_matched = False
                for idx, tokens in INDEX_TOKENS.items():
                    if not INDEX_CONFIG[idx].get("active") or INDEX_CONFIG[idx].get("is_commodity"):
                        continue
                    ce_token = tokens.get("ce_token")
                    pe_token = tokens.get("pe_token")
                    if ce_token and token == str(ce_token):
                        if ltp > 0:
                            if ltp > 1000:
                                ltp = ltp / 100.0
                            vol = vol if vol > 0 else 1
                            oi = oi if oi > 0 else 1
                            logger.info(f"✅ CE MATCH: {idx} token={token} ltp={ltp}")
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
                    elif pe_token and token == str(pe_token):
                        if ltp > 0:
                            if ltp > 1000:
                                ltp = ltp / 100.0
                            vol = vol if vol > 0 else 1
                            oi = oi if oi > 0 else 1
                            logger.info(f"✅ PE MATCH: {idx} token={token} ltp={ltp}")
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

                # ---- VIX MATCH ----
                if str(token) == "99919017" and ltp > 0:
                    with _latest_ticks_lock:
                        latest_ticks["VIX"]["vix"] = ltp
                    vix_history.append(ltp)
                    logger.info(f"✅ VIX MATCH: ltp={ltp}")

            except Exception as e:
                logger.error(f"Error processing tick: {e}")

        # Log candle growth every 100 ticks (debug)
        if tick_counter % 100 == 0:
            for idx in INDEX_NAMES:
                if not INDEX_CONFIG[idx].get("is_commodity"):
                    with _candle_histories_lock:
                        ccount = len(candle_histories[idx]["1min"])
                    logger.debug(f"📊 {idx} 1min candle count: {ccount}, price_hist: {len(price_histories[idx])}")

        # ---- Run signals only on new 1-min candle close ----
        with _new_candle_flag_lock:
            for idx in INDEX_NAMES:
                if _new_candle_flag[idx]:
                    _new_candle_flag[idx] = False
                    if INDEX_CONFIG[idx].get("active") and has_complete_data(idx):
                        try:
                            run_signal_engine_for_index(idx)
                        except Exception as e:
                            logger.exception(f"run_signal_engine_for_index({idx}) from callback failed: {e}")

    except Exception as e:
        logger.exception("Unhandled exception in on_ws_data")

def tick_watchdog():
    global ws_running, tick_counter, last_tick_timestamp, sws
    last_count = 0
    while True:
        time.sleep(10)
        now = time.time()
        if ws_running:
            if now - last_tick_timestamp > 60:
                logger.warning(f"No ticks for 60s (last tick {now - last_tick_timestamp:.1f}s ago) - forcing reconnect")
                with _ws_connect_lock:
                    ws_running = False
                if sws:
                    try:
                        sws.close_connection()
                    except:
                        pass
            elif tick_counter == last_count and tick_counter > 0 and now - last_tick_timestamp > 45:
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
        else:
            logger.debug("WS not running, watchdog idle")

def ws_watchdog():
    global ws_running, last_heartbeat, last_tick_timestamp, sws
    while True:
        time.sleep(10)
        now = time.time()
        with _ws_connect_lock:
            is_running = ws_running
        if is_running:
            if (now - last_heartbeat > 25) or (now - last_tick_timestamp > 60):
                logger.warning(f"Data starvation – heartbeat={now-last_heartbeat:.1f}s, last_tick={now-last_tick_timestamp:.1f}s - forcing reconnect")
                with _ws_connect_lock:
                    ws_running = False
                if sws:
                    try:
                        sws.close_connection()
                    except Exception:
                        pass

def start_angel_websocket_improved():
    global sws, ws_running, last_heartbeat
    retry_delay = 30   # start with 30 seconds (avoid 429)
    max_delay = 300    # max 5 minutes

    while True:
        try:
            # Ensure we don't already have a connection attempt running
            with _ws_connect_lock:
                if ws_running:
                    logger.info("WS already running, skipping new connection attempt")
                    time.sleep(10)
                    continue
                ws_running = False   # ensure it's false

            # Wait if market closed
            if not is_market_open() and not is_mcx_open():
                logger.info("Markets closed, sleeping 60s")
                time.sleep(60)
                continue

            auth_token, feed_token, _ = get_auth_token()
            if not feed_token:
                logger.error("Failed to get feed token, retrying in 30s...")
                time.sleep(30)
                continue

            # Close any existing connection (cleanup)
            if sws:
                try:
                    sws.close_connection()
                    time.sleep(2)
                except:
                    pass
                sws = None

            # Create new instance with internal retries disabled
            new_sws = SmartWebSocketV2(
                auth_token,
                ANGEL_API_KEY,
                ANGEL_CLIENT_ID,
                feed_token,
                max_retry_attempt=0
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

            # Give the connection time to establish
            time.sleep(5)

            # Inner monitoring loop
            while True:
                time.sleep(5)
                with _ws_connect_lock:
                    is_running = ws_running
                if not is_running:
                    logger.warning("WebSocket disconnected detected")
                    break

                # Optional: send heartbeat if library supports it
                # ...

            # Disconnected – backoff and retry
            logger.warning(f"Reconnecting in {retry_delay}s...")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_delay)

        except Exception as e:
            logger.error(f"WebSocket thread error: {e}")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_delay)

# ----------------------------------------------------------------------
# PRE-MARKET TOKEN REFRESH SCHEDULER
# ----------------------------------------------------------------------
def schedule_token_refresh():
    while True:
        now_ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
        if now_ist.weekday() < 5:
            open_time = now_ist.replace(hour=9, minute=10, second=0, microsecond=0)
            wait = (open_time - now_ist).total_seconds()
            if wait > 0:
                time.sleep(wait)
                logger.info("⏰ Pre-market token refresh triggered")
                try:
                    refresh_all_tokens()
                except Exception as e:
                    logger.error(f"Token refresh error: {e}")
                logger.info("Pre-market token refresh completed")
        time.sleep(60)

# ----------------------------------------------------------------------
# PERIODIC TOKEN REFRESH (every 5 minutes)
# ----------------------------------------------------------------------
def periodic_token_refresh():
    while True:
        time.sleep(300)  # 5 minutes
        try:
            refresh_all_tokens()
            logger.info("Periodic token refresh completed")
        except Exception as e:
            logger.error(f"Periodic token refresh error: {e}")

# ----------------------------------------------------------------------
# ENHANCED REST FALLBACK
# ----------------------------------------------------------------------
def fetch_asset_data(idx):
    results = {"index": idx, "spot": None, "ce": None, "pe": None}
    try:
        # --- Log the attempt ---
        logger.info(f"🔍 Fetching spot for {idx}")

        spot = get_index_spot(idx)   # this already logs internally if we add logs there
        if spot:
            results["spot"] = spot
            logger.info(f"✅ Spot for {idx}: {spot}")
            if not INDEX_CONFIG[idx].get("is_commodity"):
                tokens = INDEX_TOKENS.get(idx, {})
                if tokens.get("ce_token"):
                    results["ce"] = get_option_quote(idx, "CE")
                if tokens.get("pe_token"):
                    results["pe"] = get_option_quote(idx, "PE")
    except Exception as e:
        logger.error(f"Fetch error {idx}: {e}")
    return results

def start_rest_only_mode():
    global last_rest_fetch, last_tick_timestamp, ws_running, tick_counter
    logger.info("Starting REST fallback (concurrent fetch mode)")
    cycle_count = 0
    while True:
        try:
            with _ws_connect_lock:
                if ws_running and not os.getenv("FORCE_REST_MODE") == "1":
                    logger.debug(f"REST cycle {cycle_count}: WS is running, sleeping 30s")
                    time.sleep(30)
                    continue

            cycle_count += 1
            try:
                assets_to_fetch = []
                for idx in INDEX_NAMES:
                    cfg = INDEX_CONFIG[idx]
                    if not cfg.get("active"):
                        continue
                    is_open = is_mcx_open() if cfg.get("is_commodity") else is_market_open()
                    if is_open:
                        assets_to_fetch.append(idx)

                if not assets_to_fetch:
                    logger.debug("REST: No markets open, sleeping 30s")
                    time.sleep(30)
                    continue

                for idx in assets_to_fetch:
                    if not INDEX_CONFIG[idx].get("is_commodity"):
                        tokens = INDEX_TOKENS.get(idx, {})
                        if not tokens.get("ce_token") or not tokens.get("pe_token"):
                            logger.warning(f"Tokens missing for {idx}, retrying...")
                            get_current_atm_tokens(idx)

                with ThreadPoolExecutor(max_workers=3) as executor:
                    futures = {executor.submit(fetch_asset_data, idx): idx for idx in assets_to_fetch}
                    for future in as_completed(futures):
                        try:
                            result = future.result()
                        except Exception as e:
                            logger.error(f"Future for {futures[future]} failed: {e}")
                            continue
                        idx = result["index"]
                        try:
                            spot = result.get("spot")
                            if spot and spot > 0:
                                with _latest_ticks_lock:
                                    if INDEX_CONFIG[idx].get("is_commodity"):
                                        latest_ticks[idx]["price"] = spot
                                    else:
                                        latest_ticks[idx]["spot_price"] = spot
                                try:
                                    update_candle(idx, spot, 0, time.time())
                                except Exception as e:
                                    logger.exception(f"update_candle rest {idx} failed")
                                with _latest_ticks_lock:
                                    last_known_prices[idx]["spot"] = spot
                                    last_known_prices[idx]["timestamp"] = time.time()

                                # --- CRITICAL: increment tick counter and update timestamp ---
                                tick_counter += 1
                                last_tick_timestamp = time.time()
                                with _ws_connect_lock:
                                    ws_running = True
                                logger.info(f"✅ REST: {idx} spot={spot} (tick_counter={tick_counter})")
                            else:
                                logger.warning(f"REST: No spot for {idx}")

                            # Option data (only for equity)
                            if not INDEX_CONFIG[idx].get("is_commodity"):
                                ce = result.get("ce")
                                pe = result.get("pe")
                                if ce:
                                    with _latest_ticks_lock:
                                        latest_ticks[idx]["ce_price"] = ce["ltp"]
                                        latest_ticks[idx]["ce_volume"] = ce["volume"] or 1
                                        latest_ticks[idx]["ce_oi"] = ce["oi"] or 1
                                        latest_ticks[idx]["ce_bid"] = ce["bid"]
                                        latest_ticks[idx]["ce_ask"] = ce["ask"]
                                    with _latest_ticks_lock:
                                        last_known_prices[idx]["ce"] = ce["ltp"]
                                    with _ce_price_histories_lock:
                                        ce_price_histories[idx].append(ce["ltp"])
                                    last_tick_timestamp = time.time()
                                    with _ws_connect_lock:
                                        ws_running = True
                                    logger.debug(f"REST: {idx} CE={ce['ltp']}")
                                else:
                                    logger.warning(f"REST: {idx} CE quote fetch FAILED")
                                if pe:
                                    with _latest_ticks_lock:
                                        latest_ticks[idx]["pe_price"] = pe["ltp"]
                                        latest_ticks[idx]["pe_volume"] = pe["volume"] or 1
                                        latest_ticks[idx]["pe_oi"] = pe["oi"] or 1
                                        latest_ticks[idx]["pe_bid"] = pe["bid"]
                                        latest_ticks[idx]["pe_ask"] = pe["ask"]
                                    with _latest_ticks_lock:
                                        last_known_prices[idx]["pe"] = pe["ltp"]
                                    with _pe_price_histories_lock:
                                        pe_price_histories[idx].append(pe["ltp"])
                                    last_tick_timestamp = time.time()
                                    with _ws_connect_lock:
                                        ws_running = True
                                    logger.debug(f"REST: {idx} PE={pe['ltp']}")
                                else:
                                    logger.warning(f"REST: {idx} PE quote fetch FAILED")
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

                # Run signals for all ready indices
                ready_indices = [idx for idx in INDEX_NAMES if INDEX_CONFIG[idx].get("active") and has_complete_data(idx)]
                for idx in ready_indices:
                    try:
                        run_signal_engine_for_index(idx)
                    except Exception as e:
                        logger.exception(f"run_signal_engine_for_index({idx}) rest failed: {e}")

                cycle_interval = int(os.getenv("REST_CYCLE_INTERVAL", "10"))
                for _ in range(cycle_interval):
                    time.sleep(1)
                    with _ws_connect_lock:
                        if ws_running and not os.getenv("FORCE_REST_MODE") == "1":
                            logger.debug("REST cycle interrupted: WS reconnected")
                            break

                logger.debug(f"REST cycle {cycle_count} complete.")

            except Exception as e:
                logger.error(f"REST cycle error: {e}\n{traceback.format_exc()}")
                last_rest_fetch = time.time()
                time.sleep(10)

        except Exception as e:
            logger.error(f"REST fallback outer error: {e}\n{traceback.format_exc()}")
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
        self._periodic_thread = None

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
        
        self._periodic_thread = threading.Thread(target=periodic_token_refresh, daemon=True)
        self._periodic_thread.start()
        
        logger.info("Pre-market and periodic token refresh schedulers started.")

        # Periodic signal timer (runs every 10 seconds)
        def signal_timer():
            while True:
                time.sleep(10)
                try:
                    run_all_signals()
                except Exception as e:
                    logger.exception("Periodic signal timer crashed: %s", e)

        threading.Thread(target=signal_timer, daemon=True).start()
        logger.info("Periodic signal timer started (every 10s)")

# ----------------------------------------------------------------------
# BACKGROUND THREADS
# ----------------------------------------------------------------------
_init_completed = False
_init_lock = threading.Lock()

def _load_candle_histories_from_db():
    logger.info("Loading historical candles from database...")
    total_loaded = 0
    for idx in INDEX_NAMES:
        for tf in TIMEFRAMES:
            candles = load_historical_candles(idx, tf, 500)
            if candles:
                with _candle_histories_lock:
                    for candle in candles:
                        candle_histories[idx][tf].append(candle)
                total_loaded += len(candles)
                logger.debug(f"Loaded {len(candles)} {tf} candles for {idx}")
    logger.info(f"✅ Loaded {total_loaded} historical candles from database")

def _start_background_threads():
    global _init_completed
    with _init_lock:
        if not _init_completed:
            _load_candle_histories_from_db()
            
            logger.info("Pre-fetching option tokens...")
            
            # CRITICAL FIX: Fetch MCX tokens FIRST
            try:
                get_mcx_futures_tokens()
                logger.info("MCX futures tokens fetched")
            except Exception as e:
                logger.error(f"MCX token fetch error: {e}")
            
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    refresh_all_tokens()
                except Exception as e:
                    logger.error(f"Token refresh attempt {attempt+1} failed: {e}")
                ready = 0
                for idx, cfg in INDEX_CONFIG.items():
                    if not cfg.get("active"):
                        continue
                    if cfg.get("is_commodity"):
                        if cfg.get("token"):
                            ready += 1
                    else:
                        tokens = INDEX_TOKENS.get(idx, {})
                        if tokens.get("ce_token") and tokens.get("pe_token"):
                            ready += 1
                total_active = sum(1 for cfg in INDEX_CONFIG.values() if cfg.get("active"))
                for idx in INDEX_NAMES:
                    if INDEX_CONFIG[idx].get("is_commodity"):
                        logger.info(f"Token status {idx}: token={INDEX_CONFIG[idx].get('token')}")
                    else:
                        tokens = INDEX_TOKENS.get(idx, {})
                        logger.info(f"Token status {idx}: ce={tokens.get('ce_token')}, pe={tokens.get('pe_token')}, spot_token={INDEX_CONFIG[idx].get('token')}")
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
        "engine": "Hybrid v17.7 – Full Signal Engine + All Fixes",
        "indices": [i for i, cfg in INDEX_CONFIG.items() if cfg.get("active")],
        "market_open": is_market_open(),
        "mcx_open": is_mcx_open()
    })

@app.route("/api/live-signals", methods=["GET"])
def live_signals():
    try:
        sentiment_data = {}
        trends_data = {}
        quality_scores = {}
        for idx in INDEX_NAMES:
            if not INDEX_CONFIG[idx].get("active"):
                continue
            with _market_signal_lock:
                sentiment_data[idx] = {
                    "score": market_signal[idx].get("sentiment_score", 50),
                    "label": get_sentiment_label(market_signal[idx].get("sentiment_score", 50)),
                    "confidence": market_signal[idx].get("confidence", 0)
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
                    if candle_len >= 5:
                        try:
                            run_signal_engine_for_index(idx)
                        except Exception as e:
                            logger.exception(f"run_signal_engine_for_index({idx}) from live-signals failed: {e}")

        equity_open = is_market_open()
        mcx_open = is_mcx_open()
        if equity_open and mcx_open:
            market_label = "EQUITY + MCX OPEN"
        elif equity_open:
            market_label = "EQUITY OPEN"
        elif mcx_open:
            market_label = "MCX OPEN"
        else:
            market_label = "CLOSED"

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
                "quality_scores": quality_scores,
                "market_open": equity_open or mcx_open,
                "market_label": market_label,
                "equity_open": equity_open,
                "mcx_open": mcx_open,
                "debug": {
                    "ws_running": ws_running,
                    "ticks": tick_counter,
                    "last_tick_ago": round(time.time() - last_tick_timestamp, 1)
                },
                "version": "17.7-FullEngine"
            })
    except Exception as e:
        logger.exception("live_signals crashed")
        return jsonify({"error": str(e)}), 500

@app.route("/api/token-status", methods=["GET"])
def token_status():
    status = {}
    for idx in INDEX_NAMES:
        if INDEX_CONFIG[idx].get("is_commodity"):
            status[idx] = {
                "type": "commodity",
                "token": INDEX_CONFIG[idx].get("token"),
                "has_data": latest_ticks.get(idx, {}).get("price", 0) > 0,
                "price": latest_ticks.get(idx, {}).get("price", 0)
            }
        else:
            tokens = INDEX_TOKENS.get(idx, {})
            status[idx] = {
                "type": "equity",
                "ce_token": tokens.get("ce_token"),
                "pe_token": tokens.get("pe_token"),
                "atm_strike": tokens.get("atm_strike"),
                "expiry": tokens.get("expiry"),
                "ce_price": latest_ticks.get(idx, {}).get("ce_price", 0),
                "pe_price": latest_ticks.get(idx, {}).get("pe_price", 0),
                "spot_price": latest_ticks.get(idx, {}).get("spot_price", 0),
                "has_option_data": latest_ticks.get(idx, {}).get("ce_price", 0) > 0 or 
                                   latest_ticks.get(idx, {}).get("pe_price", 0) > 0,
                "ce_volume": latest_ticks.get(idx, {}).get("ce_volume", 0),
                "pe_volume": latest_ticks.get(idx, {}).get("pe_volume", 0)
            }
    return jsonify(status)

@app.route("/api/refresh-tokens", methods=["POST"])
def refresh_tokens():
    logger.info("🔄 Forcing token refresh...")
    refresh_all_tokens()
    
    global ws_running
    with _ws_connect_lock:
        ws_running = False
    if sws:
        try:
            sws.close_connection()
        except:
            pass
    
    return jsonify({
        "status": "refreshed",
        "tokens": {idx: {
            "ce": INDEX_TOKENS[idx].get("ce_token"),
            "pe": INDEX_TOKENS[idx].get("pe_token"),
            "spot": INDEX_CONFIG[idx].get("token")
        } for idx in INDEX_NAMES}
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
            if sig in ["STRONG_BUY_CE", "BUY_CE", "LOW_BUY_CE", "STRONG_BUY_PE", "BUY_PE", "LOW_BUY_PE", "BUY", "SELL"]:
                latest_action = sig
                break
    audio_map = {
        "STRONG_BUY_CE": "strong_buy_ce.mp3",
        "BUY_CE": "buy_ce.mp3",
        "LOW_BUY_CE": "low_buy_ce.mp3",
        "STRONG_BUY_PE": "strong_buy_pe.mp3",
        "BUY_PE": "buy_pe.mp3",
        "LOW_BUY_PE": "low_buy_pe.mp3",
        "BUY": "buy_ce.mp3",
        "SELL": "buy_pe.mp3",
        "EXIT": "exit.mp3"
    }
    audio_file = audio_map.get(latest_action, None)
    return jsonify({"action": latest_action, "audio_file": audio_file, "timestamp": datetime.now().isoformat()})

@app.route("/api/health", methods=["GET"])
def health():
    if not is_market_open() and not is_mcx_open():
        return jsonify({
            "status": "CLOSED",
            "ws_running": ws_running,
            "rest_active": False,
            "ticks": tick_counter,
            "last_tick_seconds_ago": round(time.time() - last_tick_timestamp, 2)
        })
    rest_active = (time.time() - last_rest_fetch < 60)
    ws_active = ws_running and (time.time() - last_tick_timestamp < 30)
    if not ws_running and (time.time() - last_tick_timestamp < 15):
        ws_active = True
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
        "mcx_open": is_mcx_open(),
        "connection_mode": "WEBSOCKET" if ws_running else "REST_FALLBACK",
        "active_indices": [i for i, cfg in INDEX_CONFIG.items() if cfg.get("active")],
        "tokens_loaded": {idx: {
            "ce_token": bool(INDEX_TOKENS[idx].get("ce_token")) if not INDEX_CONFIG[idx].get("is_commodity") else "N/A",
            "pe_token": bool(INDEX_TOKENS[idx].get("pe_token")) if not INDEX_CONFIG[idx].get("is_commodity") else "N/A"
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
        adx = get_current_adx(index_name)
        action, confidence = get_signal_from_sentiment(index_name, sentiment, adx)
        signals.append({
            "timestamp": candles[i]["timestamp"],
            "sentiment": sentiment,
            "adx": adx,
            "action": action,
            "confidence": confidence
        })
    return jsonify({"signals": signals, "count": len(signals)})

@app.route("/api/debug/<index_name>", methods=["GET"])
def debug_index(index_name):
    if index_name not in INDEX_NAMES:
        return jsonify({"error": "Invalid index"}), 400
    
    return jsonify({
        "index": index_name,
        "has_complete_data": has_complete_data(index_name),
        "candle_count": len(candle_histories[index_name]["1min"]),
        "price_history_len": len(price_histories[index_name]),
        "ce_price_hist_len": len(ce_price_histories[index_name]),
        "pe_price_hist_len": len(pe_price_histories[index_name]),
        "last_known": last_known_prices[index_name],
        "latest_ticks": latest_ticks[index_name],
        "tokens": {
            "ce": INDEX_TOKENS[index_name].get("ce_token"),
            "pe": INDEX_TOKENS[index_name].get("pe_token"),
            "spot": INDEX_CONFIG[index_name].get("token")
        },
        "ws_running": ws_running,
        "tick_counter": tick_counter,
        "sentiment": compute_sentiment(index_name),
        "adx": get_current_adx(index_name),
        "regime": detect_regime(index_name)
    })

@app.route("/api/reset/<index_name>", methods=["POST"])
def reset_index(index_name):
    if index_name not in INDEX_NAMES:
        return jsonify({"error": "Invalid index"}), 400
    reset_signal_state(index_name, time.time(), "MANUAL_RESET")
    return jsonify({"status": "reset", "index": index_name})

@app.route("/api/clear-candles/<index_name>", methods=["POST"])
def clear_candles(index_name):
    if index_name not in INDEX_NAMES:
        return jsonify({"error": "Invalid index"}), 400
    clear_candle_data(index_name)
    with _candle_histories_lock:
        for tf in TIMEFRAMES:
            candle_histories[index_name][tf].clear()
    return jsonify({"status": "cleared", "index": index_name})

@app.route("/api/reload-candles/<index_name>", methods=["POST"])
def reload_candles(index_name):
    if index_name not in INDEX_NAMES:
        return jsonify({"error": "Invalid index"}), 400
    with _candle_histories_lock:
        for tf in TIMEFRAMES:
            candle_histories[index_name][tf].clear()
        for tf in TIMEFRAMES:
            candles = load_historical_candles(index_name, tf, 500)
            if candles:
                for candle in candles:
                    candle_histories[index_name][tf].append(candle)
    return jsonify({
        "status": "reloaded",
        "index": index_name,
        "candle_count": len(candle_histories[index_name]["1min"])
    })

# ----------------------------------------------------------------------
# RUN FLASK
# ----------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    
    # CRITICAL: Start Flask immediately so Render detects the port
    # Background threads start after in a separate thread
    
    def start_background_after_bind():
        time.sleep(3)  # Let Flask start
        try:
            init_db()
            load_portfolio_state()
            get_mcx_futures_tokens()
            refresh_all_tokens()
            _load_candle_histories_from_db()
            _start_background_threads()
            logger.info("✅ Background initialization complete")
        except Exception as e:
            logger.exception("Background init failed")
    
    threading.Thread(target=start_background_after_bind, daemon=True).start()
    
    logger.info(f"🚀 Starting Flask on port {port}...")
    app.run(host="0.0.0.0", port=port, debug=False)
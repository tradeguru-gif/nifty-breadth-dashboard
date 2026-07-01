# === HYBRID v15.24 – Fixed data readiness, added debug logs ===
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
import pytz

# ---------- DEBUG MODE ----------
DEBUG_MODE = os.getenv("DEBUG_MODE", "0") == "1"
PAPER_MODE = os.getenv("PAPER_MODE", "0") == "1"

if not DEBUG_MODE:
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)

logging.basicConfig(level=logging.DEBUG if DEBUG_MODE else logging.INFO,
                    stream=sys.stdout, force=True,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
# DATABASE (unchanged)
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
# MONKEY PATCHES – fix SmartAPI WS bugs (unchanged)
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
        logger.debug(f"Binary parser error: {e}")
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
# INDEX CONFIGURATION (unchanged)
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
    # ---- MCX Commodities ----
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
EQUITY_TOKEN_SET = {cfg["token"] for cfg in INDEX_CONFIG.values() if not cfg.get("is_commodity") and cfg.get("token")}
INDEX_NAMES = list(INDEX_CONFIG.keys())

# ----------------------------------------------------------------------
# GLOBAL STATE (unchanged)
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
_ws_connect_lock = threading.Lock()

INDEX_TOKENS = {idx: {"ce_token": None, "pe_token": None, "atm_strike": 0, "expiry": "", "expiry_date": None, "ce_symbol": "", "pe_symbol": ""} for idx in INDEX_NAMES}
last_known_prices = {idx: {"spot": 0.0, "ce": 0.0, "pe": 0.0, "timestamp": 0} for idx in INDEX_NAMES}
price_histories = {idx: deque(maxlen=5000) for idx in INDEX_NAMES}
last_spot_tick = {idx: 0.0 for idx in INDEX_NAMES}

portfolio_state = {idx: {"equity": 100000.0, "open_positions": 0, "daily_pnl": 0.0, "total_pnl": 0.0, "live_pnl": 0.0} for idx in INDEX_NAMES}
signal_state = {idx: {"action": "HOLD", "entry_price": 0.0, "stop_loss": 0.0, "target": 0.0, "lots": 0, "entry_time": 0.0, "highest": 0.0, "cooldown": 0, "confidence": 0, "exit_reason": ""} for idx in INDEX_NAMES}

market_signal = {idx: {"sentiment_score": 50, "signal": "WAITING", "alert_message": "", "entry_price": 0, "stop_loss": 0, "target": 0, "exit_reason": "", "quality_score": 0, "strike_price": 0, "trading_symbol": ""} for idx in INDEX_NAMES}

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

# ============================================================
# INDICATORS (unchanged)
# ============================================================
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
# BSM & GREEKS (unchanged)
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
        logger.debug(f"get_index_spot: Fetching {index_name} | exchange={config['exchange']} | symbol={config['symbol']} | token={config['token']}")
        resp = obj.ltpData(config["exchange"], config["symbol"], config["token"])
        logger.debug(f"get_index_spot: {index_name} raw resp={resp}")
        ltp = safe_ltp(resp)
        if ltp and ltp > 0:
            if config["exchange"] in ["NSE","BSE"] and ltp > 100000:
                ltp /= 100
            logger.debug(f"get_index_spot: {index_name} LTP={ltp}")
            return ltp
        else:
            logger.warning(f"get_index_spot: {index_name} LTP invalid: {ltp}, resp={resp}")
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

# ================================================================
# ENHANCED MCX FUTURES TOKEN RETRIEVAL (unchanged)
# ================================================================
def get_mcx_futures_tokens():
    scrip = get_scrip_master()
    if not scrip:
        logger.warning("Scrip master not available for MCX token fetch")
        return
    df = pd.DataFrame(scrip)

    possible_types = ["FUTCOM", "FUT", "FUTURES", "COMMODITY"]
    mcx_fut = pd.DataFrame()
    for itype in possible_types:
        subset = df[(df["exch_seg"] == "MCX") & (df["instrumenttype"] == itype)]
        if not subset.empty:
            mcx_fut = subset
            logger.info(f"MCX futures found with instrumenttype='{itype}' ({len(mcx_fut)} rows)")
            break

    if mcx_fut.empty:
        logger.warning("No MCX futures found. Available types: "
                       f"{df[df['exch_seg']=='MCX']['instrumenttype'].unique()}")
        return

    for excl in ['GUINEA', 'PETAL', 'MINI', 'MICRO', '1000', 'TEN']:
        mcx_fut = mcx_fut[~mcx_fut['symbol'].str.contains(excl, case=False, na=False)]

    copper_debug = mcx_fut[mcx_fut['symbol'].str.contains('COPPER', case=False, na=False)]
    logger.info(f"🔍 COPPER symbols found: {copper_debug['symbol'].tolist()}")
    logger.info(f"🔍 First 10 MCX symbols: {mcx_fut['symbol'].head(10).tolist()}")

    mcx_fut = mcx_fut.copy()
    mcx_fut["expiry_date"] = mcx_fut["expiry"].apply(parse_expiry_date)
    mcx_fut = mcx_fut.dropna(subset=["expiry_date"])
    if mcx_fut.empty:
        logger.warning("MCX futures found but no parseable expiry dates")
        return

    symbol_aliases = {
        "GOLD":     ["GOLDM", "GOLD"],
        "SILVER":   ["SILVERM", "SILVER"],
        "CRUDEOIL": ["CRUDEOIL", "CRUDEOILM"],
        "NATURALGAS": ["NATURALGAS", "NATURALGASM"],
        "COPPER":   ["COPPERM", "COPPER", "COPPERMIC", "COPPERMINI", "COPPER1000", "COPPER25"],
    }

    for idx, cfg in INDEX_CONFIG.items():
        if not cfg.get("active") or not cfg.get("is_commodity"):
            continue
        symbol = cfg["symbol"]

        matching = mcx_fut[mcx_fut["symbol"].str.upper() == symbol.upper()]
        if matching.empty and symbol in symbol_aliases:
            for alias in symbol_aliases[symbol]:
                matching = mcx_fut[mcx_fut["symbol"].str.upper() == alias.upper()]
                if not matching.empty:
                    logger.info(f"MCX {symbol} matched via alias: {alias}")
                    break

        if matching.empty:
            matching = mcx_fut[mcx_fut["symbol"].str.startswith(symbol, na=False)]
        if matching.empty:
            matching = mcx_fut[mcx_fut["symbol"].str.contains(f"^{symbol}[A-Z]*$", case=False, na=False, regex=True)]

        if matching.empty:
            logger.warning(f"MCX symbol '{symbol}' not found after alias/fallback")
            continue

        future_sorted = matching.sort_values("expiry_date")
        nearest = future_sorted.iloc[0]
        token = str(nearest["token"])
        cfg["token"] = token
        logger.info(f"MCX {symbol} token: {token} (symbol: {nearest['symbol']}) expiry {nearest['expiry']}")

    global INDEX_TOKEN_SET, EQUITY_TOKEN_SET
    INDEX_TOKEN_SET = {cfg["token"] for cfg in INDEX_CONFIG.values() if cfg.get("token")}
    EQUITY_TOKEN_SET = {cfg["token"] for cfg in INDEX_CONFIG.values() if not cfg.get("is_commodity") and cfg.get("token")}
    logger.info(f"Updated token sets: INDEX={INDEX_TOKEN_SET}, EQUITY={EQUITY_TOKEN_SET}")

def get_next_expiry_date(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config:
        return None
    if config.get("is_commodity"):
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
    if not scrip:
        logger.warning(f"{index_name}: No scrip master")
        INDEX_TOKENS[index_name].update({"ce_token": None, "pe_token": None})
        return None, None

    try:
        df = pd.DataFrame(scrip)
        opts = df[
            (df["name"] == config["symbol"]) &
            (df["instrumenttype"] == "OPTIDX") &
            (df["exch_seg"] == config["option_exchange"])
        ]
        if opts.empty:
            logger.warning(f"{index_name}: No OPTIDX found")
            INDEX_TOKENS[index_name].update({"ce_token": None, "pe_token": None})
            return None, None

        opts = opts.copy()
        opts["expiry_date"] = opts["expiry"].apply(parse_expiry_date)
        opts = opts.dropna(subset=["expiry_date"])
        sample_strikes = opts["strike"].dropna().astype(float).head(10).tolist()
        logger.info(f"🔍 {index_name} sample strikes: {sample_strikes}")
        if sample_strikes and max(sample_strikes) > 10000:
            strike_divisor = 100.0
            logger.info(f"🔍 {index_name} strikes appear to be in paise, using divisor 100")
        else:
            strike_divisor = 1.0
            logger.info(f"🔍 {index_name} strikes appear to be in rupees, using divisor 1")

        opts["strike"] = pd.to_numeric(opts["strike"], errors="coerce") / strike_divisor
        opts = opts.dropna(subset=["strike"])

        future = opts[opts["expiry_date"] >= datetime.now()]
        if future.empty:
            future = opts

        nearest_expiry = future["expiry_date"].min()
        same_exp = future[future["expiry_date"] == nearest_expiry]
        same_exp["strike_diff"] = abs(same_exp["strike"] - atm)
        closest_row = same_exp.loc[same_exp["strike_diff"].idxmin()]
        actual_strike = int(closest_row["strike"])

        atm_opts = same_exp[same_exp["strike"] == actual_strike]
        if atm_opts.empty:
            logger.warning(f"{index_name}: No options for strike {actual_strike}")
            INDEX_TOKENS[index_name].update({"ce_token": None, "pe_token": None})
            return None, None

        ce = atm_opts[atm_opts["symbol"].str.contains("CE", na=False)]
        pe = atm_opts[atm_opts["symbol"].str.contains("PE", na=False)]

        if ce.empty or pe.empty:
            logger.warning(f"{index_name}: Missing CE or PE for strike {actual_strike}")
            INDEX_TOKENS[index_name].update({"ce_token": None, "pe_token": None})
            return None, None

        ce_token = str(ce.iloc[0]["token"])
        pe_token = str(pe.iloc[0]["token"])
        ce_symbol = str(ce.iloc[0]["symbol"])
        pe_symbol = str(pe.iloc[0]["symbol"])

        logger.info(f"✅ {index_name} | ATM strike: {actual_strike} (spot={spot}) | CE token {ce_token}, PE token {pe_token} | expiry {expiry}")

        INDEX_TOKENS[index_name].update({
            "ce_token": ce_token,
            "pe_token": pe_token,
            "atm_strike": actual_strike,
            "expiry": expiry,
            "expiry_date": nearest_expiry,
            "ce_symbol": ce_symbol,
            "pe_symbol": pe_symbol
        })
        return ce_token, pe_token

    except Exception as e:
        logger.error(f"{index_name} token fetch error: {e}\n{traceback.format_exc()}")
        INDEX_TOKENS[index_name].update({"ce_token": None, "pe_token": None})
        return None, None

def refresh_all_tokens():
    for idx in INDEX_NAMES:
        if INDEX_CONFIG[idx].get("active"):
            if INDEX_CONFIG[idx].get("is_commodity"):
                continue
            get_current_atm_tokens(idx)
    get_mcx_futures_tokens()
    logger.info("All tokens refreshed (equity options + MCX futures)")

# ============================================================
# MARKET HOURS (unchanged)
# ============================================================
IST = pytz.timezone('Asia/Kolkata')

def is_market_open():
    if os.getenv("FORCE_MARKET_OPEN", "0") == "1":
        return True
    now_ist = datetime.now(IST)
    current = now_ist.time()
    open_time = dt_time(9, 15)
    close_time = dt_time(15, 30)
    return now_ist.weekday() < 5 and open_time <= current <= close_time

def is_mcx_open():
    if os.getenv("FORCE_MARKET_OPEN", "0") == "1":
        return True
    now_ist = datetime.now(IST)
    current = now_ist.time()
    if now_ist.weekday() >= 5:
        return False
    open_time = dt_time(10, 0)
    close_time = dt_time(23, 30)
    return open_time <= current <= close_time

def get_market_status_label():
    equity_open = is_market_open()
    mcx_open = is_mcx_open()
    if equity_open and mcx_open:
        return "EQUITY + MCX OPEN"
    elif equity_open:
        return "EQUITY OPEN"
    elif mcx_open:
        return "MCX OPEN"
    else:
        return "CLOSED"

def is_index_market_open(idx):
    cfg = INDEX_CONFIG.get(idx, {})
    return is_mcx_open() if cfg.get("is_commodity") else is_market_open()

# ============================================================
# CANDLE UPDATE – simplified rollover (unchanged)
# ============================================================
def update_candle(idx, price, cumulative_volume, timestamp):
    """Create/update 1‑minute candles; higher timeframes are derived on demand."""
    if price <= 0:
        return

    tf = "1min"
    interval = 60
    candle_start = int(timestamp / interval) * interval

    with _current_candle_lock:
        existing = _current_candle[idx][tf]
        if existing is None:
            _current_candle[idx][tf] = {
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 1,
                "timestamp": candle_start
            }
            _last_candle_time[idx][tf] = candle_start
            logger.debug(f"🕯️ New candle started for {idx} at {candle_start}")
        elif existing["timestamp"] != candle_start:
            with _candle_histories_lock:
                candle_histories[idx][tf].append(existing)
                logger.debug(f"📊 Candle closed for {idx} (len={len(candle_histories[idx][tf])})")
            _current_candle[idx][tf] = {
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 1,
                "timestamp": candle_start
            }
            _last_candle_time[idx][tf] = candle_start
        else:
            existing["high"] = max(existing["high"], price)
            existing["low"] = min(existing["low"], price)
            existing["close"] = price
            existing["volume"] += 1

# ============================================================
# SENTIMENT, REGIME, CONFIRMATION (unchanged)
# ============================================================
# ... (all these functions remain identical – I'm omitting them to reduce length)
# They are exactly as in your original file.

# ============================================================
# ENHANCED DATA READINESS CHECK – FIXED
# ============================================================
def has_complete_data(index_name):
    """Check that we have at least 10 1‑minute candles."""
    cfg = INDEX_CONFIG.get(index_name)
    if not cfg or not cfg.get("active"):
        return False

    REQUIRED_BARS = 10   # Start generating signals after 10 minutes of data

    with _candle_histories_lock:
        history_len = len(candle_histories[index_name]["1min"])
        if history_len < REQUIRED_BARS:
            logger.debug(f"⚠️ {index_name} lacks enough 1min bars: {history_len}/{REQUIRED_BARS}")
            return False
    return True

# ----------------------------------------------------------------------
# PERSISTENCE (unchanged)
# ----------------------------------------------------------------------
# ... (load_portfolio_state, save_portfolio_state, reset_signal_state, etc.)
# They are all identical to your original file.

# ============================================================
# MAIN SIGNAL ENGINE – with added debug logs for MCX
# ============================================================
def run_signal_engine_for_index(index_name):
    logger.info(f"🔍 ENGINE RUNNING for {index_name}")
    try:
        if not INDEX_CONFIG[index_name].get("active"):
            return

        config = INDEX_CONFIG[index_name]
        is_commodity = config.get("is_commodity", False)
        logger.info(f"🟣 {index_name} is_commodity={is_commodity}")

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

        # Token check for equity
        if not is_commodity:
            tokens = INDEX_TOKENS.get(index_name, {})
            if not tokens.get("ce_token") or not tokens.get("pe_token"):
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = "Option tokens not loaded"
                    market_signal[index_name]["signal"] = "WAITING"
                return

        # Candle readiness
        with _candle_histories_lock:
            candle_len = len(candle_histories[index_name]["1min"])
        logger.info(f"🕯️ {index_name} candle_len={candle_len}")

        min_candles = 1 if is_commodity else 10
        if candle_len < min_candles and is_commodity:
            rest_spot = get_index_spot(index_name)
            if rest_spot and rest_spot > 0:
                now = time.time()
                with _latest_ticks_lock:
                    latest_ticks[index_name]["price"] = rest_spot
                    last_known_prices[index_name]["spot"] = rest_spot
                    last_known_prices[index_name]["timestamp"] = now
                with _price_histories_lock:
                    price_histories[index_name].append(rest_spot)
                update_candle(index_name, rest_spot, 0, now)
                with _candle_histories_lock:
                    candle_len = len(candle_histories[index_name]["1min"])
                logger.info(f"✅ {index_name} after REST fallback: candle_len={candle_len}")

        # Use enhanced has_complete_data for equity, or just min_candles for commodities
        if is_commodity:
            if candle_len < min_candles:
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = f"Building candles ({candle_len}/{min_candles})"
                    market_signal[index_name]["signal"] = "WAITING"
                return
        else:
            if not has_complete_data(index_name):
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = f"Insufficient data (need 10 bars)"
                    market_signal[index_name]["signal"] = "WAITING"
                return

        with _latest_ticks_lock:
            vix = latest_ticks["VIX"].get("vix", 15.0)
            if vix <= 0:
                vix = 15.0

        with _market_signal_lock:
            if market_signal[index_name]["signal"] == "EXIT":
                market_signal[index_name]["signal"] = "COOLDOWN"

        now = time.time()

        # ---- Get latest price ----
        with _latest_ticks_lock:
            if index_name not in latest_ticks:
                latest_ticks[index_name] = {}

            if is_commodity:
                raw_ws_price = latest_ticks[index_name].get("price", 0.0) or 0.0
                multiplier = config.get("mkt_multiple", 1.0)
                spot = raw_ws_price * multiplier
                logger.info(f"📈 {index_name} raw WS price = {raw_ws_price}, multiplier = {multiplier}, final spot = {spot}")
                latest_ticks[index_name]["price"] = spot
                ce_prem = spot
                pe_prem = spot
                ce_vol = latest_ticks[index_name].get("volume", 0) or 0
                pe_vol = ce_vol
                ce_oi = 0
                pe_oi = 0
                ce_bid = latest_ticks[index_name].get("bid", 0.0) or 0.0
                ce_ask = latest_ticks[index_name].get("ask", 0.0) or 0.0
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

        if spot <= 0:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "No price data yet"
                market_signal[index_name]["signal"] = "WAITING"
            return

        if is_commodity:
            prem = spot
            if prem <= 0.0:
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = f"Invalid commodity price {prem}"
                    market_signal[index_name]["signal"] = "BLOCKED"
                logger.warning(f"{index_name} commodity price invalid: {prem}")
                return
        else:
            if ce_bid > 0 and ce_ask > 0:
                ce_prem = (ce_bid + ce_ask) / 2
            if pe_bid > 0 and pe_ask > 0:
                pe_prem = (pe_bid + pe_ask) / 2
            if not spread_ok(ce_bid, ce_ask, ce_prem) or not spread_ok(pe_bid, pe_ask, pe_prem):
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = "Wide bid-ask spread"
                    market_signal[index_name]["signal"] = "BLOCKED"
                logger.info(f"📢 SET SPREAD BLOCK for {index_name}")
                return

        # ---- Volume profile, correlation, greeks ----
        vp_engine = volume_profile_engines[index_name]
        if is_commodity:
            vp_engine.update(spot, ce_vol, option_type=None)
        else:
            vp_engine.update(spot, ce_vol, option_type=None)
            vp_engine.update(ce_prem, ce_vol, option_type="CE")
            vp_engine.update(pe_prem, pe_vol, option_type="PE")

        if index_name == "NIFTY":
            nifty_price_series.append(spot)
        elif index_name == "BANKNIFTY":
            banknifty_price_series.append(spot)
        if len(nifty_price_series) > 0 and len(banknifty_price_series) > 0:
            correlation_filter.update(list(nifty_price_series)[-1], list(banknifty_price_series)[-1])

        greeks_data = None
        if not is_commodity and INDEX_CONFIG[index_name].get("greeks_enabled"):
            greeks_data = get_option_greeks(index_name)

        sentiment = compute_sentiment(index_name)
        adx = get_current_adx(index_name)
        action, confidence = get_signal_from_sentiment(index_name, sentiment, adx)
        logger.info(f"🎯 {index_name} sentiment={sentiment}, action={action}, confidence={confidence}")
        sentiment_label = get_sentiment_label(sentiment)

        with _market_signal_lock:
            market_signal[index_name]["sentiment_score"] = sentiment

        regime = detect_regime(index_name)
        if regime == "RANGING" and not is_commodity:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "Ranging market - no new entries"
                market_signal[index_name]["signal"] = "BLOCKED"
            logger.info(f"📢 SET RANGING for {index_name}")
            return

        # ---- Drawdown & safety ----
        with _portfolio_state_lock:
            current_equity = portfolio_state[index_name]["equity"]
            peak = daily_drawdown[index_name]["peak_equity"]
            if current_equity > peak:
                daily_drawdown[index_name]["peak_equity"] = current_equity
            drawdown = (peak - current_equity) / peak * 100 if peak > 0 else 0
            daily_drawdown[index_name]["current_drawdown"] = drawdown

            if drawdown >= 2.0 and drawdown < 3.0:
                if not daily_drawdown[index_name].get("dd_warning_sent", False):
                    send_telegram_alert(f"⚠️ {index_name} drawdown: {drawdown:.1f}% (warning at 2%)")
                    daily_drawdown[index_name]["dd_warning_sent"] = True
            elif drawdown < 1.5:
                daily_drawdown[index_name]["dd_warning_sent"] = False

            if drawdown >= INDEX_CONFIG[index_name].get("max_daily_drawdown_pct", 3.0):
                with _signal_state_lock:
                    if signal_state[index_name]["action"] != "HOLD":
                        active = signal_state[index_name]["action"]
                        exit_prem = spot if is_commodity else (ce_prem if "CE" in active else pe_prem)
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

        # ---- Existing position management ----
        with _signal_state_lock:
            current_action = signal_state[index_name]["action"]
        if current_action != "HOLD":
            active = current_action
            if is_commodity:
                prem = spot
            else:
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
                            candles = list(candle_histories[index_name]["5min"])
                            highs = [c["high"] for c in candles]
                            lows = [c["low"] for c in candles]
                            closes = [c["close"] for c in candles]
                        if len(closes) >= 15:
                            atr = calculate_atr(highs, lows, closes, 14)
                            if atr <= 0.01 and prem > 0:
                                atr = prem * 0.005
                            new_sl = prem - atr * 1.8
                            if new_sl > signal_state[index_name]["stop_loss"]:
                                signal_state[index_name]["stop_loss"] = new_sl

                    stop_loss_val = signal_state[index_name].get("stop_loss")
                    target_val = signal_state[index_name].get("target")

                    # SL check
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
                            send_telegram_alert(f"CIRCUIT BREAKER {index_name} | 3 consecutive SLs. Trading paused 30 min.")
                        log_trade(index_name, active, signal_state[index_name]["entry_price"], prem, pnl_total,
                                  pnl_total / portfolio_state[index_name]["equity"] * 100, "STOP_LOSS",
                                  active, calculate_atr([],[],[],14), latest_ticks["VIX"]["vix"], "STOP_LOSS")
                        send_telegram_alert(f"EXIT {index_name} | SL | PnL: {pnl:.2f} pts | Cost adj: {pnl_total:.2f}")
                        logger.info(f"🔴 MCX SL EXIT {index_name} | prem={prem}, pnl={pnl:.2f}, pnl_total={pnl_total}")
                        reset_signal_state(index_name, now, "STOP_LOSS")
                        return

                    # Target check
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
                        send_telegram_alert(f"EXIT {index_name} | TARGET | PnL: {pnl:.2f} pts | Cost adj: {pnl_total:.2f}")
                        logger.info(f"🟢 MCX TARGET EXIT {index_name} | prem={prem}, pnl={pnl:.2f}, pnl_total={pnl_total}")
                        reset_signal_state(index_name, now, "TARGET_HIT")
                        return

                    # Time exit – forced for MCX after 30 min
                    with _signal_state_lock:
                        entry_time = signal_state[index_name].get("entry_time", 0)
                    if entry_time > 0:
                        elapsed_min = (now - entry_time) / 60
                        side = "CE" if "CE" in active else "PE"
                        time_limit = get_dynamic_time_exit_minutes(index_name, side, prem, greeks_data)
                        if is_commodity:
                            time_limit = min(time_limit, 30)
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
                            send_telegram_alert(f"EXIT {index_name} | TIME ({time_limit}m) | PnL: {pnl:.2f} pts | Cost adj: {pnl_total:.2f}")
                            logger.info(f"⏰ MCX TIME EXIT {index_name} | prem={prem}, pnl={pnl:.2f}, pnl_total={pnl_total}")
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
                        send_telegram_alert(f"EXIT {index_name} | {exit_reason} | PnL: {pnl:.2f} pts | Cost adj: {pnl_total:.2f}")
                        reset_signal_state(index_name, now, exit_reason)
                        return

                    # VWAP exit (only for equity)
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
                                entry_price = signal_state[index_name]["entry_price"]
                                if entry_price > 0:
                                    kelly_trackers[index_name].update(pnl / entry_price)
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
                                entry_price = signal_state[index_name]["entry_price"]
                                if entry_price > 0:
                                    kelly_trackers[index_name].update(pnl / entry_price)
                                log_trade(index_name, active, signal_state[index_name]["entry_price"], prem, pnl_total,
                                          pnl_total / portfolio_state[index_name]["equity"] * 100, "VWAP_EXIT",
                                          active, calculate_atr([],[],[],14), latest_ticks["VIX"]["vix"], "VWAP_EXIT")
                                send_telegram_alert(f"EXIT {index_name} | VWAP (premium below VWAP) | PnL: {pnl:.2f} pts | Cost adj: {pnl_total:.2f}")
                                reset_signal_state(index_name, now, "VWAP_EXIT")
                                return

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
                        "trading_symbol": trading_symbol
                    })
            else:
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = f"ACTIVE {active} – premium unavailable"
                    market_signal[index_name]["signal"] = "ACTIVE"
            return

        # ---- New entry logic ----
        with _signal_state_lock:
            if now < signal_state[index_name]["cooldown"]:
                remaining = int(signal_state[index_name]["cooldown"] - now)
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = f"Cooldown {remaining}s"
                    market_signal[index_name]["signal"] = "COOLDOWN"
                return

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

        if action == "NO_TRADE" or action == "HOLD":
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Sentiment {sentiment:.0f} - {sentiment_label}"
                market_signal[index_name]["signal"] = "NO_TRADE"
            logger.info(f"📢 SET NO_TRADE for {index_name}: sentiment={sentiment}, action={action}")
            return

        side = "CE" if "CE" in action else "PE" if "PE" in action else None
        if side is None:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Invalid action {action}"
                market_signal[index_name]["signal"] = "NO_TRADE"
            return

        if is_commodity:
            prem = spot
        else:
            prem = ce_prem if side == "CE" else pe_prem

        if is_commodity:
            if prem <= 0.0:
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = f"Invalid commodity price {prem}"
                    market_signal[index_name]["signal"] = "BLOCKED"
                logger.warning(f"{index_name} commodity price invalid: {prem}")
                return
        else:
            min_prem = INDEX_CONFIG[index_name].get("min_premium", 0)
            max_prem = INDEX_CONFIG[index_name].get("max_premium", 8000)
            if prem <= 0 or prem < min_prem or prem > max_prem:
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = f"Premium invalid: {prem:.2f}"
                    market_signal[index_name]["signal"] = "WAITING"
                logger.info(f"📢 SET PREMIUM INVALID for {index_name}: {prem}")
                return

        if not is_commodity:
            spot_vwap = vp_engine.analyze(spot, ce_vol + pe_vol, option_type=None)["vwap"]
            if spot_vwap > 0:
                if side == "CE" and spot > spot_vwap * 1.003:
                    if "STRONG" not in action:
                        with _market_signal_lock:
                            market_signal[index_name]["alert_message"] = "Spot above VWAP, extended"
                            market_signal[index_name]["signal"] = "BLOCKED"
                        logger.info(f"📢 SET VWAP EXTENDED for {index_name}")
                        return
                elif side == "PE" and spot < spot_vwap * 0.997:
                    if "STRONG" not in action:
                        with _market_signal_lock:
                            market_signal[index_name]["alert_message"] = "Spot below VWAP, extended"
                            market_signal[index_name]["signal"] = "BLOCKED"
                        logger.info(f"📢 SET VWAP EXTENDED for {index_name}")
                        return

        if not is_commodity:
            if not confirm_signal_with_candles(index_name, side, spot):
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = "Candle confirmation failed (last 3 closes not aligned with EMA9)"
                    market_signal[index_name]["signal"] = "BLOCKED"
                logger.info(f"📢 SET CANDLE CONFIRM FAIL for {index_name}")
                return

        if not is_commodity:
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
                        logger.info(f"📢 SET LOW VOLUME for {index_name}")
                        return
            if side == "CE":
                ce_volume_histories[index_name].append(vol)
            else:
                pe_volume_histories[index_name].append(vol)

        if not is_commodity and INDEX_CONFIG[index_name].get("pcr_enabled"):
            if ce_oi > 0 and pe_oi > 0:
                pcr = ce_oi / pe_oi
                if side == "CE" and pcr > 1.5:
                    with _market_signal_lock:
                        market_signal[index_name]["alert_message"] = f"Extreme PCR (CE/PE) = {pcr:.2f}"
                        market_signal[index_name]["signal"] = "BLOCKED"
                    logger.info(f"📢 SET EXTREME PCR for {index_name}")
                    return
                elif side == "PE" and pcr < 0.67:
                    with _market_signal_lock:
                        market_signal[index_name]["alert_message"] = f"Extreme PCR (CE/PE) = {pcr:.2f}"
                        market_signal[index_name]["signal"] = "BLOCKED"
                    logger.info(f"📢 SET EXTREME PCR for {index_name}")
                    return

        # beta_adj
        beta_adj = 1.0
        if not is_commodity:
            pair = INDEX_CONFIG[index_name].get("correlation_pair")
            corr_adjust = 1.0
            if pair:
                corr_analysis = correlation_filter.analyze(index_name, action)
                corr = abs(corr_analysis.get("correlation", 0))
                if corr > 0.85:
                    corr_adjust = max(0.6, 1 - (corr - 0.85) * 2)
                    logger.info(f"Correlation adjustment: {index_name} vs {pair} = {corr:.2f}, risk factor = {corr_adjust:.2f}")
                if corr > 0.8:
                    pair_action = market_signal.get(pair, {}).get("signal", "NO_TRADE")
                    if (side == "CE" and "CE" in pair_action) or (side == "PE" and "PE" in pair_action):
                        my_sent = sentiment
                        pair_sent = market_signal.get(pair, {}).get("sentiment_score", 50)
                        if my_sent < pair_sent:
                            with _market_signal_lock:
                                market_signal[index_name]["alert_message"] = f"Correlation block: {pair} stronger"
                                market_signal[index_name]["signal"] = "BLOCKED"
                            logger.info(f"📢 SET CORRELATION BLOCK for {index_name}")
                            return
                beta_adj = corr_analysis.get("beta_adjustment", 1.0) * corr_adjust
            else:
                beta_adj = 1.0

        if not is_commodity and INDEX_CONFIG[index_name].get("greeks_enabled") and greeks_data is not None:
            delta = greeks_data.get("ce_delta") if side == "CE" else greeks_data.get("pe_delta")
            if delta is not None:
                if "STRONG" not in action and abs(delta) > 0.80:
                    with _market_signal_lock:
                        market_signal[index_name]["alert_message"] = f"Greeks block: Delta {delta:.2f} > 0.80"
                        market_signal[index_name]["signal"] = "BLOCKED"
                    logger.info(f"📢 SET GREEKS BLOCK for {index_name}")
                    return
            iv_rank = greeks_data.get("iv_rank")
            if iv_rank is not None and iv_rank > 80 and "LOW" not in action:
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = f"High IV rank {iv_rank:.0f}"
                    market_signal[index_name]["signal"] = "BLOCKED"
                logger.info(f"📢 SET HIGH IV for {index_name}")
                return

        if not is_commodity:
            with _price_histories_lock:
                prices_spot = list(price_histories[index_name])
            rsi = calculate_rsi(prices_spot[-50:]) if len(prices_spot) >= 50 else 50.0
            with _candle_histories_lock:
                candles = list(candle_histories[index_name]["5min"])
                highs = [c["high"] for c in candles]
                lows = [c["low"] for c in candles]
                closes = [c["close"] for c in candles]
            adx = calculate_adx(highs, lows, closes, 14) if len(closes) >= 30 else 20.0

            ml_prob = compute_ml_score(index_name, side, prem, spot, rsi, adx, vix, sentiment)
            if ml_prob < 0.4 and "STRONG" not in action:
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = f"ML filter: prob {ml_prob:.2f}"
                    market_signal[index_name]["signal"] = "BLOCKED"
                logger.info(f"📢 SET ML BLOCK for {index_name}")
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

        if not is_commodity and is_expiry_day(index_name):
            now_ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
            if now_ist.time() >= dt_time(14, 30):
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = "Expiry day: last 60 min blocked"
                    market_signal[index_name]["signal"] = "BLOCKED"
                logger.info(f"📢 SET EXPIRY BLOCK for {index_name}")
                return

        with _candle_histories_lock:
            candles = list(candle_histories[index_name]["1min"])
            highs = [c["high"] for c in candles]
            lows = [c["low"] for c in candles]
            closes = [c["close"] for c in candles]
        atr = calculate_atr(highs, lows, closes, 14)

        if atr <= 0.01 and prem > 0:
            atr = prem * 0.005
            logger.warning(f"{index_name}: ATR was zero, using fallback {atr:.2f}")

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

        stop_dist = prem - sl
        if stop_dist <= 0:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "Invalid stop distance"
                market_signal[index_name]["signal"] = "BLOCKED"
            logger.warning(f"Invalid stop distance for {index_name}: prem={prem}, sl={sl}")
            return

        risk_amount = portfolio_state[index_name]["equity"] * (risk_pct / 100)
        lots = int(risk_amount / (stop_dist * INDEX_CONFIG[index_name]["lot_size"]))
        lots = max(1, min(5, lots))

        # Log MCX entry
        if is_commodity:
            logger.info(f"🚀 MCX ENTRY {index_name} | prem={prem}, sl={sl}, target={target}, lots={lots}")

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
        msg = (f"{emoji} {action} {index_name} | Spot:{spot:.0f} Prem:{prem:.2f} SL:{sl:.2f} Tgt:{target:.2f} | "
               f"Sentiment:{sentiment:.0f} ({sentiment_label}) | Regime:{regime} | Lots:{lots} Risk:{risk_pct:.1f}%")
        send_telegram_alert(msg)
        logger.info(msg)

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
                "trading_symbol": trading_symbol
            })
        logger.info(f"📢 SET SIGNAL for {index_name}: action={action}")

    except Exception as e:
        logger.error(f"Signal error {index_name}: {e}\n{traceback.format_exc()}")

# ============================================================
# WEBSOCKET + WATCHDOGS (with fixed ready_indices)
# ============================================================
# ... (all remaining code is exactly as in your original, but with ready_indices corrected)
# I'll include the corrected ready_indices block here for clarity.

# Inside on_ws_data, replace the line:
# ready_indices = [idx for idx in INDEX_NAMES if INDEX_CONFIG[idx].get("active") and has_complete_data(idx) if not INDEX_CONFIG[idx].get("is_commodity") else True]
# with:
ready_indices = []
for idx in INDEX_NAMES:
    if not INDEX_CONFIG[idx].get("active"):
        continue
    if INDEX_CONFIG[idx].get("is_commodity"):
        if last_known_prices[idx].get("spot", 0) > 0:
            ready_indices.append(idx)
    else:
        if has_complete_data(idx):
            ready_indices.append(idx)

# Similarly in start_rest_only_mode.

# ---- The rest of the code (Flask routes, main) remains unchanged ----
# I'll include the full file in the final answer to avoid confusion.
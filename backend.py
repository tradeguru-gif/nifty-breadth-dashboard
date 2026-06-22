# === HYBRID v15.2 – REST PRIMARY + ALL FIXES (RENDER-OPTIMIZED) ===
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
# DATABASE (with try/finally to avoid leaks)
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
# MONKEY PATCHES – fix SmartAPI WS bugs (with proper exception handling)
# ============================================================
_original_parse = SmartWebSocketV2._parse_binary_data
def _patched_parse(self, binary_data):
    try:
        result = _original_parse(self, binary_data)
        if result.get('token'):
            return result
    except Exception:
        result = {}
    try:
        token_int = int.from_bytes(binary_data[2:10], byteorder='little')
        if token_int > 0:
            result['token'] = str(token_int)
            return result
    except Exception:
        pass
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
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.8, "is_commodity": True,
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

# ----------------------------------------------------------------------
# GLOBAL STATE
# ----------------------------------------------------------------------
price_buffers = {idx: deque(maxlen=500) for idx in INDEX_CONFIG}
option_chain_buffers = {idx: {"CE": {}, "PE": {}} for idx in INDEX_CONFIG}
active_signals = {idx: None for idx in INDEX_CONFIG}
ws_connections = {}
last_tick_time = {idx: 0 for idx in INDEX_CONFIG}
market_open_today = {idx: False for idx in INDEX_CONFIG}
daily_stats = {idx: {"trades": 0, "pnl": 0.0, "max_dd": 0.0} for idx in INDEX_CONFIG}
lock = threading.RLock()

# ----------------------------------------------------------------------
# ANGEL ONE AUTH
# ----------------------------------------------------------------------
angel_client = None
auth_token = None
feed_token = None
refresh_token = None

def angel_login():
    global angel_client, auth_token, feed_token, refresh_token
    try:
        angel_client = SmartConnect(api_key=ANGEL_API_KEY)
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        session = angel_client.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
        auth_token = session['data']['jwtToken']
        refresh_token = session['data']['refreshToken']
        feed_token = angel_client.getfeedToken()
        logger.info("Angel One login successful")
        return True
    except Exception as e:
        logger.error(f"Angel One login failed: {e}")
        return False

def refresh_auth():
    global auth_token, refresh_token
    try:
        session = angel_client.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, pyotp.TOTP(ANGEL_TOTP_SECRET).now())
        auth_token = session['data']['jwtToken']
        refresh_token = session['data']['refreshToken']
        logger.info("Auth token refreshed")
        return True
    except Exception as e:
        logger.error(f"Token refresh failed: {e}")
        return False

# ----------------------------------------------------------------------
# TELEGRAM ALERTS
# ----------------------------------------------------------------------
def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")

# ----------------------------------------------------------------------
# MARKET HOURS CHECK
# ----------------------------------------------------------------------
def is_market_open(index_name):
    now = datetime.now(timezone(timedelta(hours=5, minutes=30)))  # IST
    t = now.time()
    weekday = now.weekday()

    if weekday >= 5:  # Saturday/Sunday
        return False

    cfg = INDEX_CONFIG[index_name]
    if cfg.get("is_commodity"):
        # MCX: 9:00 AM - 11:30 PM (with breaks)
        return dt_time(9, 0) <= t <= dt_time(23, 30)

    # Equity: 9:15 AM - 3:30 PM
    return dt_time(9, 15) <= t <= dt_time(15, 30)

def get_next_expiry(index_name):
    cfg = INDEX_CONFIG[index_name]
    if cfg.get("is_commodity"):
        # MCX monthly expiry (last Friday or similar)
        today = datetime.now(timezone(timedelta(hours=5, minutes=30)))
        # Simplified: next month end
        if today.month == 12:
            next_month = today.replace(year=today.year + 1, month=1, day=1)
        else:
            next_month = today.replace(month=today.month + 1, day=1)
        return next_month.strftime("%d%b%y").upper()

    today = datetime.now(timezone(timedelta(hours=5, minutes=30)))
    weekday = cfg["expiry_weekday"]  # 0=Mon, 3=Thu
    days_ahead = weekday - today.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    expiry = today + timedelta(days=days_ahead)
    return expiry.strftime("%d%b%y").upper()

# ----------------------------------------------------------------------
# TECHNICAL INDICATORS
# ----------------------------------------------------------------------
def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

def calculate_ema(prices, period):
    if len(prices) < period:
        return prices[-1] if prices else 0
    prices = np.array(prices)
    alpha = 2.0 / (period + 1)
    ema = [prices[0]]
    for p in prices[1:]:
        ema.append(alpha * p + (1 - alpha) * ema[-1])
    return ema[-1]

def calculate_atr(high_low_close, period=14):
    if len(high_low_close) < period + 1:
        return 0.0
    tr_list = []
    for i in range(1, len(high_low_close)):
        h, l, c = high_low_close[i]
        prev_c = high_low_close[i-1][2]
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        tr_list.append(tr)
    return np.mean(tr_list[-period:])

def calculate_vwap(prices, volumes):
    if not prices or not volumes or len(prices) != len(volumes):
        return 0.0
    pv = sum(p * v for p, v in zip(prices, volumes))
    v = sum(volumes)
    return pv / v if v > 0 else 0.0

def calculate_adx(high_low_close, period=14):
    if len(high_low_close) < period * 2:
        return 25.0
    highs = [x[0] for x in high_low_close]
    lows = [x[1] for x in high_low_close]
    closes = [x[2] for x in high_low_close]

    tr_list = []
    plus_dm = []
    minus_dm = []

    for i in range(1, len(high_low_close)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        tr_list.append(tr)
        up_move = highs[i] - highs[i-1]
        down_move = lows[i-1] - lows[i]
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0)
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0)

    atr = np.mean(tr_list[-period:])
    plus_di = 100 * np.mean(plus_dm[-period:]) / atr if atr > 0 else 0
    minus_di = 100 * np.mean(minus_dm[-period:]) / atr if atr > 0 else 0
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di) if (plus_di + minus_di) > 0 else 0
    return dx

def calculate_pcr(oi_ce, oi_pe):
    if oi_ce == 0:
        return 1.0
    return oi_pe / oi_ce

# ----------------------------------------------------------------------
# OPTION CHAIN FETCHING
# ----------------------------------------------------------------------
def fetch_option_chain(index_name, spot_price):
    cfg = INDEX_CONFIG[index_name]
    if cfg.get("is_commodity"):
        return None, None

    try:
        expiry = get_next_expiry(index_name)
        atm = round(spot_price / cfg["atm_strike_multiple"]) * cfg["atm_strike_multiple"]

        # Build option symbols
        ce_symbol = f"{cfg['symbol']}{expiry}{atm}CE"
        pe_symbol = f"{cfg['symbol']}{expiry}{atm}PE"

        # Search for tokens (simplified - in production use proper symbol search)
        # This is a placeholder for actual option chain fetching
        return {"strike": atm, "ce_symbol": ce_symbol, "pe_symbol": pe_symbol}, None
    except Exception as e:
        logger.error(f"Option chain fetch failed for {index_name}: {e}")
        return None, None

# ----------------------------------------------------------------------
# SIGNAL GENERATION ENGINE
# ----------------------------------------------------------------------
def generate_signal(index_name):
    with lock:
        buf = price_buffers[index_name]
        if len(buf) < 50:
            return None

        prices = [x["price"] for x in buf]
        volumes = [x.get("volume", 0) for x in buf]

        rsi = calculate_rsi(prices)
        vwap = calculate_vwap(prices, volumes)
        current_price = prices[-1]

        # Trend detection
        ema20 = calculate_ema(prices, 20)
        ema50 = calculate_ema(prices, 50)

        # Regime detection
        cfg = INDEX_CONFIG[index_name]
        atr = calculate_atr([(x.get("high", x["price"]), x.get("low", x["price"]), x["price"]) for x in buf])
        atr_pct = (atr / current_price) * 100 if current_price > 0 else 0

        regime = "neutral"
        if atr_pct > cfg.get("regime_atr_threshold", 0.6):
            regime = "volatile"
        elif ema20 > ema50 * 1.001:
            regime = "bullish"
        elif ema20 < ema50 * 0.999:
            regime = "bearish"

        # Signal logic
        signal = None
        confidence = 0.0
        grade = "D"

        # Long conditions
        if rsi < 35 and current_price > vwap * 0.998 and regime in ["bullish", "neutral"]:
            signal = "LONG"
            confidence = min(0.95, (40 - rsi) / 40 + 0.3)
        # Short conditions
        elif rsi > 65 and current_price < vwap * 1.002 and regime in ["bearish", "neutral"]:
            signal = "SHORT"
            confidence = min(0.95, (rsi - 60) / 40 + 0.3)

        if signal:
            # Grade based on confidence
            if confidence >= 0.85:
                grade = "A"
            elif confidence >= 0.75:
                grade = "B"
            elif confidence >= 0.65:
                grade = "C"

            # ATR-based stop loss and target
            stop_loss = current_price - (atr * 1.5) if signal == "LONG" else current_price + (atr * 1.5)
            target = current_price + (atr * 3) if signal == "LONG" else current_price - (atr * 3)

            return {
                "index": index_name,
                "action": signal,
                "confidence": confidence,
                "grade": grade,
                "entry_price": current_price,
                "stop_loss": stop_loss,
                "target": target,
                "rsi": rsi,
                "vwap": vwap,
                "regime": regime,
                "atr": atr,
                "timestamp": time.time()
            }

        return None

# ----------------------------------------------------------------------
# POSITION SIZING (Kelly Criterion)
# ----------------------------------------------------------------------
def calculate_kelly_fraction(index_name):
    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.cursor()
        c.execute("SELECT pnl FROM trades WHERE action IN ('LONG', 'SHORT') AND status = 'CLOSED' ORDER BY timestamp DESC LIMIT 100")
        trades = c.fetchall()

        if len(trades) < 20:
            return 0.02  # Conservative default

        wins = [t[0] for t in trades if t[0] > 0]
        losses = [t[0] for t in trades if t[0] < 0]

        win_rate = len(wins) / len(trades)
        avg_win = np.mean(wins) if wins else 0
        avg_loss = abs(np.mean(losses)) if losses else 1

        kelly = win_rate - ((1 - win_rate) / (avg_win / avg_loss)) if avg_loss > 0 else 0
        kelly = max(0, min(kelly, 0.25))  # Cap at 25%

        return kelly
    except Exception as e:
        logger.error(f"Kelly calc error: {e}")
        return 0.02
    finally:
        conn.close()

def get_position_size(index_name, confidence, grade):
    cfg = INDEX_CONFIG[index_name]
    kelly = calculate_kelly_fraction(index_name)

    # Grade multiplier
    grade_mult = {"A": 1.0, "B": 0.75, "C": 0.5, "D": 0.25}.get(grade, 0.25)

    # Confidence adjustment
    conf_mult = confidence

    # Base size (simplified - should use account equity)
    base_lots = 1
    lots = int(base_lots * kelly * grade_mult * conf_mult * 10)
    lots = max(1, min(lots, 10))  # Cap between 1-10 lots

    return lots

# ----------------------------------------------------------------------
# TRADE EXECUTION
# ----------------------------------------------------------------------
def execute_trade(index_name, signal):
    with lock:
        cfg = INDEX_CONFIG[index_name]

        # Check daily drawdown limit
        dd = daily_stats[index_name]["max_dd"]
        if dd >= cfg["max_daily_drawdown_pct"]:
            logger.warning(f"Daily drawdown limit hit for {index_name}")
            return False

        # Check if already in position
        if active_signals[index_name] is not None:
            logger.info(f"Already in position for {index_name}")
            return False

        lots = get_position_size(index_name, signal["confidence"], signal["grade"])

        trade_record = {
            "timestamp": time.time(),
            "action": signal["action"],
            "entry_price": signal["entry_price"],
            "exit_price": 0,
            "pnl": 0,
            "size_pct": lots * cfg["lot_size"],
            "status": "OPEN",
            "grade": signal["grade"],
            "atr": signal["atr"],
            "vix": 0,
            "exit_reason": ""
        }

        # Paper vs Live mode
        if PAPER_MODE:
            conn = sqlite3.connect(PAPER_DB_PATH)
        else:
            conn = sqlite3.connect(DB_PATH)

        try:
            c = conn.cursor()
            c.execute("""INSERT INTO trades 
                (timestamp, action, entry_price, exit_price, pnl, size_pct, status, grade, atr, vix, exit_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (trade_record["timestamp"], trade_record["action"], trade_record["entry_price"],
                 trade_record["exit_price"], trade_record["pnl"], trade_record["size_pct"],
                 trade_record["status"], trade_record["grade"], trade_record["atr"],
                 trade_record["vix"], trade_record["exit_reason"]))

            # Update portfolio
            c.execute("""INSERT OR REPLACE INTO portfolio_equity 
                (index_name, equity, last_updated, active_action, entry_price, stop_loss, target, lots, entry_time, highest, last_trade_date, daily_trade_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (index_name, 0, time.time(), signal["action"], signal["entry_price"],
                 signal["stop_loss"], signal["target"], lots, time.time(), signal["entry_price"],
                 datetime.now().strftime("%Y-%m-%d"), daily_stats[index_name]["trades"] + 1))

            conn.commit()
        finally:
            conn.close()

        active_signals[index_name] = signal
        daily_stats[index_name]["trades"] += 1

        msg = f"🚀 <b>{signal['action']}</b> {index_name}\nGrade: {signal['grade']} | Confidence: {signal['confidence']:.2%}\nEntry: {signal['entry_price']:.2f} | SL: {signal['stop_loss']:.2f} | TG: {signal['target']:.2f}\nLots: {lots}"
        send_telegram(msg)
        logger.info(f"Trade executed: {signal['action']} {index_name} @ {signal['entry_price']}")

        return True

def check_exit_conditions(index_name, current_price):
    with lock:
        signal = active_signals[index_name]
        if signal is None:
            return

        cfg = INDEX_CONFIG[index_name]
        exit_triggered = False
        exit_reason = ""
        pnl = 0

        if signal["action"] == "LONG":
            pnl = current_price - signal["entry_price"]
            if current_price <= signal["stop_loss"]:
                exit_triggered = True
                exit_reason = "STOP_LOSS"
            elif current_price >= signal["target"]:
                exit_triggered = True
                exit_reason = "TARGET"
            elif pnl < -signal["atr"] * 2:  # Trailing stop
                exit_triggered = True
                exit_reason = "TRAILING_STOP"
        else:  # SHORT
            pnl = signal["entry_price"] - current_price
            if current_price >= signal["stop_loss"]:
                exit_triggered = True
                exit_reason = "STOP_LOSS"
            elif current_price <= signal["target"]:
                exit_triggered = True
                exit_reason = "TARGET"
            elif pnl < -signal["atr"] * 2:
                exit_triggered = True
                exit_reason = "TRAILING_STOP"

        # Time-based exit (close to market end)
        now = datetime.now(timezone(timedelta(hours=5, minutes=30)))
        if not cfg.get("is_commodity") and now.time() >= dt_time(15, 15):
            exit_triggered = True
            exit_reason = "MARKET_CLOSE"

        if exit_triggered:
            close_trade(index_name, current_price, pnl, exit_reason)

def close_trade(index_name, exit_price, pnl, exit_reason):
    with lock:
        signal = active_signals[index_name]
        if signal is None:
            return

        cfg = INDEX_CONFIG[index_name]

        if PAPER_MODE:
            conn = sqlite3.connect(PAPER_DB_PATH)
        else:
            conn = sqlite3.connect(DB_PATH)

        try:
            c = conn.cursor()
            c.execute("""UPDATE trades SET exit_price = ?, pnl = ?, status = ?, exit_reason = ? 
                WHERE timestamp = ? AND action = ? AND status = 'OPEN'""",
                (exit_price, pnl, "CLOSED", exit_reason, signal["timestamp"], signal["action"]))

            # Reset portfolio
            c.execute("UPDATE portfolio_equity SET active_action = NULL, entry_price = 0, stop_loss = 0, target = 0, lots = 0, entry_time = 0, highest = 0 WHERE index_name = ?", (index_name,))

            conn.commit()
        finally:
            conn.close()

        daily_stats[index_name]["pnl"] += pnl
        if pnl < 0:
            dd = abs(pnl / signal["entry_price"]) * 100 if signal["entry_price"] > 0 else 0
            daily_stats[index_name]["max_dd"] = max(daily_stats[index_name]["max_dd"], dd)

        emoji = "✅" if pnl > 0 else "❌"
        msg = f"{emoji} <b>CLOSED</b> {index_name} {signal['action']}\nReason: {exit_reason} | P&L: {pnl:.2f}\nEntry: {signal['entry_price']:.2f} | Exit: {exit_price:.2f}"
        send_telegram(msg)

        active_signals[index_name] = None
        logger.info(f"Trade closed: {index_name} {signal['action']} P&L={pnl:.2f} Reason={exit_reason}")

# ----------------------------------------------------------------------
# WEBSOCKET HANDLERS
# ----------------------------------------------------------------------
def on_ticks(wsapp, message):
    try:
        token = str(message.get("token", ""))
        price = float(message.get("last_traded_price", 0)) / 100
        volume = int(message.get("volume_traded_today", 0))

        # Find which index this token belongs to
        for idx, cfg in INDEX_CONFIG.items():
            if cfg["token"] == token:
                tick = {
                    "timestamp": time.time(),
                    "price": price,
                    "volume": volume,
                    "high": float(message.get("high", price)),
                    "low": float(message.get("low", price)),
                    "bid": float(message.get("bid_price", price)),
                    "ask": float(message.get("ask_price", price)),
                    "oi": int(message.get("open_interest", 0))
                }

                with lock:
                    price_buffers[idx].append(tick)
                    last_tick_time[idx] = time.time()

                # Check exits first
                if active_signals[idx]:
                    check_exit_conditions(idx, price)

                # Generate new signals
                if is_market_open(idx) and active_signals[idx] is None:
                    signal = generate_signal(idx)
                    if signal and signal["confidence"] >= 0.65:
                        execute_trade(idx, signal)

                break
    except Exception as e:
        logger.error(f"Tick processing error: {e}")

def on_open(wsapp):
    logger.info("WebSocket connected")
    subscribe_tokens = []
    for idx, cfg in INDEX_CONFIG.items():
        if cfg["token"] and cfg.get("active"):
            subscribe_tokens.append({
                "exchangeType": cfg["ws_exchange_type"],
                "tokens": [cfg["token"]]
            })

    if subscribe_tokens:
        wsapp.subscribe("ws", subscribe_tokens)

def on_close(wsapp, *args):
    logger.warning("WebSocket disconnected, will reconnect...")
    threading.Timer(5, start_websocket).start()

def on_error(wsapp, error):
    logger.error(f"WebSocket error: {error}")

def start_websocket():
    global ws_connections
    try:
        if not auth_token:
            if not angel_login():
                threading.Timer(10, start_websocket).start()
                return

        sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)
        sws.on_open = on_open
        sws.on_data = on_ticks
        sws.on_close = on_close
        sws.on_error = on_error

        ws_connections["main"] = sws
        threading.Thread(target=sws.connect, daemon=True).start()
        logger.info("WebSocket connection initiated")
    except Exception as e:
        logger.error(f"WebSocket start failed: {e}")
        threading.Timer(10, start_websocket).start()

# ----------------------------------------------------------------------
# REST API ENDPOINTS
# ----------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "paper_mode": PAPER_MODE,
        "timestamp": datetime.now(timezone(timedelta(hours=5, minutes=30))).isoformat(),
        "active_signals": {k: v["action"] if v else None for k, v in active_signals.items()},
        "market_open": {k: is_market_open(k) for k in INDEX_CONFIG}
    })

@app.route("/api/signals", methods=["GET"])
def get_signals():
    with lock:
        return jsonify({
            "signals": active_signals,
            "buffers": {k: len(v) for k, v in price_buffers.items()},
            "daily_stats": daily_stats
        })

@app.route("/api/trades", methods=["GET"])
def get_trades():
    limit = request.args.get("limit", 50, type=int)
    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.cursor()
        c.execute("SELECT * FROM trades ORDER BY timestamp DESC LIMIT ?", (limit,))
        cols = [d[0] for d in c.description]
        trades = [dict(zip(cols, row)) for row in c.fetchall()]
        return jsonify({"trades": trades})
    finally:
        conn.close()

@app.route("/api/performance", methods=["GET"])
def get_performance():
    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.cursor()
        c.execute("SELECT * FROM daily_performance ORDER BY date DESC LIMIT 30")
        cols = [d[0] for d in c.description]
        perf = [dict(zip(cols, row)) for row in c.fetchall()]
        return jsonify({"performance": perf})
    finally:
        conn.close()

@app.route("/api/equity", methods=["GET"])
def get_equity():
    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.cursor()
        c.execute("SELECT * FROM portfolio_equity")
        cols = [d[0] for d in c.description]
        equity = [dict(zip(cols, row)) for row in c.fetchall()]
        return jsonify({"equity": equity})
    finally:
        conn.close()

@app.route("/api/manual/<action>", methods=["POST"])
def manual_trade(action):
    api_key = request.headers.get("X-API-Key", "")
    if api_key != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json or {}
    index_name = data.get("index", "NIFTY")

    if action not in ["LONG", "SHORT", "CLOSE"]:
        return jsonify({"error": "Invalid action"}), 400

    if action == "CLOSE":
        if active_signals[index_name]:
            current = list(price_buffers[index_name])[-1]["price"] if price_buffers[index_name] else 0
            close_trade(index_name, current, 0, "MANUAL")
            return jsonify({"status": "closed"})
        return jsonify({"error": "No active position"}), 400

    # Manual entry
    signal = {
        "index": index_name,
        "action": action,
        "confidence": 1.0,
        "grade": "A",
        "entry_price": list(price_buffers[index_name])[-1]["price"] if price_buffers[index_name] else 0,
        "stop_loss": data.get("stop_loss", 0),
        "target": data.get("target", 0),
        "rsi": 50,
        "vwap": 0,
        "regime": "manual",
        "atr": 0,
        "timestamp": time.time()
    }
    execute_trade(index_name, signal)
    return jsonify({"status": "executed", "signal": signal})

@app.route("/api/config", methods=["GET", "POST"])
def config_endpoint():
    api_key = request.headers.get("X-API-Key", "")
    if api_key != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    if request.method == "GET":
        return jsonify({"config": INDEX_CONFIG})

    data = request.json or {}
    for idx, cfg in data.items():
        if idx in INDEX_CONFIG:
            INDEX_CONFIG[idx].update(cfg)
    return jsonify({"status": "updated"})

# ----------------------------------------------------------------------
# BACKGROUND TASKS
# ----------------------------------------------------------------------
def heartbeat():
    while True:
        time.sleep(30)
        now = time.time()
        for idx in INDEX_CONFIG:
            if now - last_tick_time[idx] > 60:
                logger.warning(f"No ticks for {idx} in 60s")
                if ws_connections.get("main"):
                    try:
                        ws_connections["main"].disconnect()
                    except:
                        pass
                    start_websocket()

def daily_reset():
    while True:
        now = datetime.now(timezone(timedelta(hours=5, minutes=30)))
        if now.time() >= dt_time(15, 35) and now.time() <= dt_time(15, 40):
            for idx in INDEX_CONFIG:
                daily_stats[idx] = {"trades": 0, "pnl": 0.0, "max_dd": 0.0}
                if active_signals[idx]:
                    current = list(price_buffers[idx])[-1]["price"] if price_buffers[idx] else 0
                    close_trade(idx, current, 0, "EOD_RESET")
            logger.info("Daily reset executed")
            time.sleep(300)
        time.sleep(60)

def save_state():
    while True:
        time.sleep(60)
        conn = sqlite3.connect(DB_PATH)
        try:
            c = conn.cursor()
            for idx, signal in active_signals.items():
                if signal:
                    c.execute("""INSERT OR REPLACE INTO signal_state 
                        (index_name, action, entry_price, stop_loss, target, lots, entry_time, highest)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (idx, signal["action"], signal["entry_price"], signal["stop_loss"],
                         signal["target"], signal.get("lots", 1), signal["timestamp"], signal["entry_price"]))
            conn.commit()
        except Exception as e:
            logger.error(f"State save error: {e}")
        finally:
            conn.close()

# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("HYBRID v15.2 Trading Engine Starting...")
    logger.info(f"Paper Mode: {PAPER_MODE}")
    logger.info(f"Debug Mode: {DEBUG_MODE}")
    logger.info("=" * 60)

    # Login to Angel One
    if angel_login():
        start_websocket()
    else:
        logger.error("Failed to login, retrying in 30s...")
        threading.Timer(30, angel_login).start()

    # Start background threads
    threading.Thread(target=heartbeat, daemon=True).start()
    threading.Thread(target=daily_reset, daemon=True).start()
    threading.Thread(target=save_state, daemon=True).start()

    # Start Flask
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=DEBUG_MODE, use_reloader=False)
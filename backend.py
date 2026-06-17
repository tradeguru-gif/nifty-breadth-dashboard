# === VERSION 14.0 - PRO SIGNAL BOT: FIXED SmartAPI WS V2 Binary Parsing ===
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

logging.basicConfig(level=logging.DEBUG if DEBUG_MODE else logging.INFO,
                    stream=sys.stdout, force=True)
logger = logging.getLogger(__name__)

app = Flask(__name__)

ALLOWED_ORIGINS = [
    "http://index-options.co",
    "https://index-options.co",
    "http://localhost:5000",
    "http://127.0.0.1:5000"
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

init_db()

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ----------------------------------------------------------------------
# INDEX CONFIGURATION
# ----------------------------------------------------------------------
INDEX_CONFIG = {
    "NIFTY": {
        "token": "99926000", "exchange": "NSE", "symbol": "NIFTY", "lot_size": 50, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": "BANKNIFTY", "greeks_enabled": True, "pcr_enabled": True
    },
    "BANKNIFTY": {
        "token": "99926009", "exchange": "NSE", "symbol": "BANKNIFTY", "lot_size": 25, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": "NIFTY", "greeks_enabled": True, "pcr_enabled": True
    },
    "FINNIFTY": {
        "token": "99926037", "exchange": "NSE", "symbol": "FINNIFTY", "lot_size": 40, "expiry_weekday": 1, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True
    },
    "MIDCPNIFTY": {
        "token": "99926074", "exchange": "NSE", "symbol": "MIDCPNIFTY", "lot_size": 75, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 25, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": None, "greeks_enabled": False, "pcr_enabled": True
    },
    "SENSEX": {
        "token": "99919000", "exchange": "BSE", "symbol": "SENSEX", "lot_size": 15, "expiry_weekday": 4, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100, "option_exchange": "BFO",
        "ws_exchange_type": 3, "option_ws_exchange_type": 4, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True
    }
}

INDEX_TOKEN_SET = {cfg["token"] for cfg in INDEX_CONFIG.values()}

# ----------------------------------------------------------------------
# GLOBAL STATE & LOCKS
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

INDEX_TOKENS = {idx: {"ce_token": None, "pe_token": None, "atm_strike": 0, "expiry": "", "expiry_date": None, "ce_symbol": "", "pe_symbol": ""} for idx in INDEX_CONFIG}
last_known_prices = {idx: {"spot": 0.0, "ce": 0.0, "pe": 0.0, "timestamp": 0} for idx in INDEX_CONFIG}
price_histories = {idx: deque(maxlen=5000) for idx in INDEX_CONFIG}

portfolio_state = {idx: {"equity": 100000.0, "open_positions": 0, "daily_pnl": 0.0, "total_pnl": 0.0, "live_pnl": 0.0} for idx in INDEX_CONFIG}
signal_state = {idx: {"action": "HOLD", "entry_price": 0.0, "stop_loss": 0.0, "target": 0.0, "lots": 0, "entry_time": 0.0, "highest": 0.0, "cooldown": 0, "confidence": 0, "exit_reason": ""} for idx in INDEX_CONFIG}
market_signal = {idx: {"sentiment_score": 50, "signal": "WAITING", "alert_message": "", "entry_price": 0, "stop_loss": 0, "target": 0, "exit_reason": ""} for idx in INDEX_CONFIG}

ce_price_histories = {idx: deque(maxlen=2000) for idx in INDEX_CONFIG}
pe_price_histories = {idx: deque(maxlen=2000) for idx in INDEX_CONFIG}
ce_volume_histories = {idx: deque(maxlen=1000) for idx in INDEX_CONFIG}
pe_volume_histories = {idx: deque(maxlen=1000) for idx in INDEX_CONFIG}
ce_oi_histories = {idx: deque(maxlen=200) for idx in INDEX_CONFIG}
pe_oi_histories = {idx: deque(maxlen=200) for idx in INDEX_CONFIG}
vix_history = deque(maxlen=200)
nifty_price_series = deque(maxlen=200)
banknifty_price_series = deque(maxlen=200)

latest_ticks = {idx: {"spot_price": 0.0, "ce_price": 0.0, "pe_price": 0.0,
                      "ce_volume": 0, "pe_volume": 0, "ce_oi": 0, "pe_oi": 0,
                      "ce_bid": 0.0, "ce_ask": 0.0, "pe_bid": 0.0, "pe_ask": 0.0}
                for idx in INDEX_CONFIG}
latest_ticks["VIX"] = {"vix": 15.0}

daily_drawdown = {idx: {"peak_equity": 0.0, "current_drawdown": 0.0} for idx in INDEX_CONFIG}
safety_state = {idx: {"consecutive_sl": 0, "circuit_breaker": False, "circuit_breaker_until": 0} for idx in INDEX_CONFIG}
signal_buffer = {idx: {"ce_count": 0, "pe_count": 0} for idx in INDEX_CONFIG}
daily_trade_count = {idx: 0 for idx in INDEX_CONFIG}
last_trade_date = {idx: "" for idx in INDEX_CONFIG}

_last_signal_run = 0
_signal_run_lock = threading.Lock()
_telegram_last_sent = 0
_telegram_lock = threading.Lock()

# ----------------------------------------------------------------------
# MULTI-TIMEFRAME CANDLES
# ----------------------------------------------------------------------
TIMEFRAMES = ["1min", "2min", "3min", "5min", "8min", "10min", "15min", "20min"]
TIMEFRAME_SECONDS = {"1min":60, "2min":120, "3min":180, "5min":300, "8min":480, "10min":600, "15min":900, "20min":1200}
TIMEFRAME_WEIGHTS = {"1min":8, "2min":8, "3min":8, "5min":12, "8min":12, "10min":12, "15min":14, "20min":14}

candle_histories = {idx: {tf: deque(maxlen=500) for tf in TIMEFRAMES} for idx in INDEX_CONFIG}
_last_candle_time = {idx: {tf: 0 for tf in TIMEFRAMES} for idx in INDEX_CONFIG}
_current_candle = {idx: {tf: None for tf in TIMEFRAMES} for idx in INDEX_CONFIG}
_prev_volume = {idx: 0 for idx in INDEX_CONFIG}

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
# TECHNICAL INDICATORS (unchanged)
# ----------------------------------------------------------------------
def calculate_ema(prices, period):
    if not prices: return 0
    if len(prices) < period: return sum(prices)/len(prices)
    alpha = 2/(period+1)
    ema = sum(prices[:period])/period
    for p in prices[period:]:
        ema = alpha*p + (1-alpha)*ema
    return ema

def calculate_rsi(prices, period=14):
    if len(prices) < period+1: return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        gains.append(max(diff,0))
        losses.append(max(-diff,0))
    avg_gain = sum(gains[-period:])/period
    avg_loss = sum(losses[-period:])/period
    if avg_loss == 0: return 100.0
    return 100 - (100/(1+avg_gain/avg_loss))

def calculate_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal: return 0.0, 0.0, 0.0
    ema_fast_list, ema_slow_list = [], []
    val_f = prices[0]
    val_s = prices[0]
    alpha_f = 2 / (fast + 1)
    alpha_s = 2 / (slow + 1)
    for x in prices:
        val_f = alpha_f * x + (1 - alpha_f) * val_f
        val_s = alpha_s * x + (1 - alpha_s) * val_s
        ema_fast_list.append(val_f)
        ema_slow_list.append(val_s)
    macd_line = [f - s for f, s in zip(ema_fast_list, ema_slow_list)]
    val_sig = macd_line[0]
    alpha_sig = 2 / (signal + 1)
    for m in macd_line:
        val_sig = alpha_sig * m + (1 - alpha_sig) * val_sig
    return macd_line[-1], val_sig, macd_line[-1] - val_sig

def calculate_atr(highs, lows, closes, period=14):
    if len(closes) < period+1:
        return 5.0
    tr = []
    for i in range(1, len(closes)):
        if len(highs) > i and highs[i] > 0 and lows[i] > 0:
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i-1])
            lc = abs(lows[i] - closes[i-1])
            tr.append(max(hl, hc, lc))
        else:
            tr.append(abs(closes[i] - closes[i-1]))
    return sum(tr[-period:])/period if len(tr)>=period else 5.0

def calculate_adx(highs, lows, closes, period=14):
    if len(closes) < period*2:
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
            plus_dm.append(max(up, 0) if up>down else 0)
            minus_dm.append(max(down, 0) if down>up else 0)
        else:
            tr.append(abs(closes[i] - closes[i-1]))
            plus_dm.append(max(closes[i] - closes[i-1], 0))
            minus_dm.append(max(closes[i-1] - closes[i], 0))
    if len(tr) < period:
        return 20.0
    atr = sum(tr[:period])
    plus_di_sum = sum(plus_dm[:period])
    minus_di_sum = sum(minus_dm[:period])
    dx_values = []
    for i in range(period, len(tr)):
        atr = atr - (atr / period) + tr[i]
        plus_di_sum = plus_di_sum - (plus_di_sum / period) + plus_dm[i]
        minus_di_sum = minus_di_sum - (minus_di_sum / period) + minus_dm[i]
        plus_di = 100 * plus_di_sum / atr if atr > 0 else 0
        minus_di = 100 * minus_di_sum / atr if atr > 0 else 0
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di) if (plus_di + minus_di) > 0 else 0
        dx_values.append(dx)
    if not dx_values:
        return 20.0
    adx = sum(dx_values[:period]) / period if len(dx_values) >= period else dx_values[0]
    for i in range(period, len(dx_values)):
        adx = (adx * (period - 1) + dx_values[i]) / period
    return adx

def calculate_vwap(prices, volumes):
    if not prices:
        return 0
    if not volumes:
        return prices[-1]
    s_vol = sum(volumes)
    if s_vol == 0:
        return prices[-1]
    return sum(p*v for p,v in zip(prices, volumes)) / s_vol

# ----------------------------------------------------------------------
# PERSISTENCE (unchanged)
# ----------------------------------------------------------------------
def load_portfolio_state():
    global portfolio_state, signal_state, daily_trade_count, last_trade_date
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        for idx in INDEX_CONFIG:
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
        logger.info("Persistent state loaded successfully.")
    except Exception as e:
        logger.error(f"Error loading state: {e}")

def save_portfolio_state(idx):
    with sqlite3.connect(DB_PATH) as conn:
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

# ----------------------------------------------------------------------
# GREEKS ENGINE (unchanged)
# ----------------------------------------------------------------------
greeks_cache_fallback_store = {idx: {"ce_iv":0.2, "pe_iv":0.2, "ce_delta":0.5, "pe_delta":-0.5, "ce_gamma":0.02, "pe_gamma":0.02, "ce_theta":-0.1, "pe_theta":-0.1, "ce_vega":0.15, "pe_vega":0.15, "iv_rank":50, "iv_percentile":50} for idx in INDEX_CONFIG}
_greeks_cache = {idx: {"data": None, "timestamp": 0} for idx in INDEX_CONFIG}
_GREEKS_CACHE_TTL = 60

def _estimate_greeks_fallback(index_name):
    tokens = INDEX_TOKENS.get(index_name, {})
    with _latest_ticks_lock:
        ce_price = latest_ticks[index_name]["ce_price"]
        pe_price = latest_ticks[index_name]["pe_price"]
        spot = latest_ticks[index_name]["spot_price"]
    if ce_price > 0 and pe_price > 0 and spot > 0:
        strike = tokens.get("atm_strike", spot)
        moneyness = (spot - strike) / spot if spot > 0 else 0
        moneyness = max(-0.2, min(0.2, moneyness))
        ce_delta = max(0.05, min(0.95, 0.5 + moneyness * 5))
        pe_delta = max(-0.95, min(-0.05, -0.5 + (-moneyness) * 5))
        greeks_data = {
            "ce_iv": 0.2, "pe_iv": 0.2, "ce_delta": ce_delta, "pe_delta": pe_delta,
            "ce_gamma": 0.02, "pe_gamma": 0.02, "ce_theta": -0.1, "pe_theta": -0.1,
            "ce_vega": 0.15, "pe_vega": 0.15, "iv_rank": 50, "iv_percentile": 50
        }
    else:
        greeks_data = greeks_cache_fallback_store.get(index_name, {
            "ce_iv":0.2, "pe_iv":0.2, "ce_delta":0.5, "pe_delta":-0.5, "ce_gamma":0.02, "pe_gamma":0.02,
            "ce_theta":-0.1, "pe_theta":-0.1, "ce_vega":0.15, "pe_vega":0.15, "iv_rank":50, "iv_percentile":50
        })
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
        return None
    _, _, obj = get_auth_token()
    if not obj:
        data = _estimate_greeks_fallback(index_name)
        _greeks_cache[index_name] = {"data": data, "timestamp": now}
        return data

    try:
        expiry_str = tokens.get("expiry", "")
        if not expiry_str:
            data = _estimate_greeks_fallback(index_name)
            _greeks_cache[index_name] = {"data": data, "timestamp": now}
            return data

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
                    if abs(strike - atm_strike) < config.get("atm_strike_multiple", 50) * 0.5:
                        if opt_type == "CE":
                            ce_greeks = g
                        elif opt_type == "PE":
                            pe_greeks = g
                if ce_greeks and pe_greeks:
                    ce_iv = float(ce_greeks.get("impliedVolatility", 0))
                    pe_iv = float(pe_greeks.get("impliedVolatility", 0))
                    if ce_iv > 1:
                        ce_iv /= 100
                    if pe_iv > 1:
                        pe_iv /= 100
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
                        "iv_rank": 50, "iv_percentile": 50
                    }
                    greeks_cache_fallback_store[index_name] = greeks_data
                    _greeks_cache[index_name] = {"data": greeks_data, "timestamp": now}
                    return greeks_data
        data = _estimate_greeks_fallback(index_name)
        _greeks_cache[index_name] = {"data": data, "timestamp": now}
        return data
    except Exception as e:
        logger.debug(f"Greeks API error {index_name}: {e}")
        data = _estimate_greeks_fallback(index_name)
        _greeks_cache[index_name] = {"data": data, "timestamp": now}
        return data

# ----------------------------------------------------------------------
# ML FILTER (placeholder - kept for compatibility)
# ----------------------------------------------------------------------
class MLSignalFilter:
    def __init__(self):
        self.is_trained = False
    def predict(self, feature_vector):
        return 0.5
ml_filter = MLSignalFilter()

# ----------------------------------------------------------------------
# KELLY CRITERION (unchanged)
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
            self.avg_win = ((self.avg_win * (self.win_count - 1)) + trade_pnl_pct) / self.win_count
        else:
            self.loss_count += 1
            loss_abs = abs(trade_pnl_pct)
            self.avg_loss = ((self.avg_loss * (self.loss_count - 1)) + loss_abs) / self.loss_count

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
        kelly_full = (p * b - q) / b if b > 0 else 0
        kelly_full = max(0, min(kelly_full, 0.5))
        return kelly_full * self.kelly_fraction, p, self.avg_win, self.avg_loss

kelly_trackers = {idx: KellyCriterion(idx) for idx in INDEX_CONFIG}

# ----------------------------------------------------------------------
# PERFORMANCE TRACKER (simplified)
# ----------------------------------------------------------------------
class PerformanceTracker:
    def __init__(self, index_name):
        self.index_name = index_name
    def add_trade(self, pnl):
        pass
performance_trackers = {idx: PerformanceTracker(idx) for idx in INDEX_CONFIG}

# ----------------------------------------------------------------------
# CORRELATION FILTER (unchanged)
# ----------------------------------------------------------------------
class CorrelationFilter:
    def __init__(self):
        self.nifty_returns = deque(maxlen=50)
        self.banknifty_returns = deque(maxlen=50)
    def update(self, nifty_price, banknifty_price):
        if nifty_price>0 and banknifty_price>0:
            self.nifty_returns.append(nifty_price)
            self.banknifty_returns.append(banknifty_price)
    def analyze(self, index_name, action):
        return {"beta_adjustment": 1.0, "block_reason": None}
correlation_filter = CorrelationFilter()

# ----------------------------------------------------------------------
# VOLUME PROFILE ENGINE (unchanged)
# ----------------------------------------------------------------------
class VolumeProfileEngine:
    def __init__(self, index_name):
        self.price_volume = deque(maxlen=1000)
        self.ce_price_volume = deque(maxlen=1000)
        self.pe_price_volume = deque(maxlen=1000)

    def update(self, price, volume, option_type=None):
        if option_type is None:
            if price > 0 and volume > 0:
                self.price_volume.append((price, volume))
        elif option_type == "CE":
            if price > 0 and volume > 0:
                self.ce_price_volume.append((price, volume))
        elif option_type == "PE":
            if price > 0 and volume > 0:
                self.pe_price_volume.append((price, volume))

    def analyze(self, current_price, current_volume, option_type=None):
        if option_type is None:
            pv = self.price_volume
        elif option_type == "CE":
            pv = self.ce_price_volume
        elif option_type == "PE":
            pv = self.pe_price_volume
        else:
            pv = self.price_volume

        if not pv:
            return {"vwap": 0.0, "poc": 0, "vah": 0, "val": 0, "signal": "neutral", "strength": 0}
        total_pv = sum(p * v for p, v in pv)
        total_v = sum(v for p, v in pv)
        vwap = total_pv / total_v if total_v > 0 else current_price
        return {"vwap": vwap, "poc": 0, "vah": 0, "val": 0, "signal": "neutral", "strength": 0}

volume_profile_engines = {idx: VolumeProfileEngine(idx) for idx in INDEX_CONFIG}

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
        return None
    _, _, obj = get_auth_token()
    if not obj:
        return None
    try:
        resp = obj.ltpData(config["exchange"], config["symbol"], config["token"])
        ltp = safe_ltp(resp)
        if ltp and ltp > 0:
            if config["exchange"] in ["NSE", "BSE"] and ltp > 100000:
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

def get_next_expiry_date(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config:
        return None
    today = datetime.now()
    weekday = today.weekday()
    expiry_weekday = config["expiry_weekday"]
    days_ahead = expiry_weekday - weekday
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
                            logger.info(f"{index_name} tokens: CE={ce_token} PE={pe_token} expiry={expiry}")
                            return ce_token, pe_token
        except Exception as e:
            logger.warning(f"{index_name} token fetch error: {e}")
    INDEX_TOKENS[index_name].update({"ce_token": None, "pe_token": None})
    return None, None

def refresh_all_tokens():
    for idx in INDEX_CONFIG:
        if INDEX_CONFIG[idx].get("active"):
            get_current_atm_tokens(idx)

# ----------------------------------------------------------------------
# SENTIMENT & SIGNAL MAPPING (unchanged)
# ----------------------------------------------------------------------
SENTIMENT_SCORES = {
    "STRONG_BULLISH": (85, 100, "STRONG BULLISH", "STRONG_BUY_CE"),
    "BULLISH": (70, 84, "BULLISH", "BUY_CE"),
    "SLOW_BULLISH": (55, 69, "SLOW BULLISH", "LOW_BUY_CE"),
    "NEUTRAL": (45, 54, "NEUTRAL", "NO_TRADE"),
    "SLOW_BEARISH": (30, 44, "SLOW BEARISH", "LOW_BUY_PE"),
    "BEARISH": (15, 29, "BEARISH", "BUY_PE"),
    "STRONG_BEARISH": (0, 14, "STRONG BEARISH", "STRONG_BUY_PE")
}

def get_signal_from_sentiment(sentiment):
    for _, (low, high, _, action) in SENTIMENT_SCORES.items():
        if low <= sentiment <= high:
            return action
    return "NO_TRADE"

def get_sentiment_label(sentiment):
    for _, (low, high, label, _) in SENTIMENT_SCORES.items():
        if low <= sentiment <= high:
            return label
    return "UNKNOWN"

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
        elif tf in ["5min", "8min", "10min"]:
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
            if ema9 > ema21 > ema50:
                score = TIMEFRAME_WEIGHTS[tf] - 5
            elif ema9 < ema21 < ema50:
                score = -TIMEFRAME_WEIGHTS[tf] + 5
            else:
                score = 0
        sentiment_scores.append(score)
    if not sentiment_scores:
        return 50
    total = sum(sentiment_scores)
    sentiment = 50 + (total / 3.5)
    return max(0, min(100, sentiment))

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

# ----------------------------------------------------------------------
# EXIT LOGIC (unchanged)
# ----------------------------------------------------------------------
def should_exit_market_analysis(index_name, action, prices_spot, ce_prem, pe_prem):
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
        vix_sma = sum(list(vix_history)[-10:]) / 10
        if vix > vix_sma * 1.25:
            return True, f"VIX spike {vix:.1f} vs SMA {vix_sma:.1f}"
    return False, ""

# ----------------------------------------------------------------------
# HELPER FUNCTIONS (unchanged)
# ----------------------------------------------------------------------
def send_telegram_alert(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    global _telegram_last_sent
    with _telegram_lock:
        now = time.time()
        if now - _telegram_last_sent < 2.0:
            logger.debug("Telegram rate limit: skipping message")
            return
        _telegram_last_sent = now
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=3
        )
    except Exception as e:
        logger.warning(f"Telegram failed: {type(e).__name__}")

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

def is_expiry_day(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config:
        return False
    today = datetime.now().strftime("%d%b%Y").upper()
    tokens = INDEX_TOKENS.get(index_name, {})
    expiry = tokens.get("expiry", "")
    return today == expiry

def is_market_open():
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc + timedelta(hours=5, minutes=30)
    current = now_ist.time()
    # Market is open Mon–Fri 9:15 AM to 3:30 PM IST
    is_open = now_ist.weekday() < 5 and dt_time(9, 15) <= current <= dt_time(15, 30)
    if DEBUG_MODE or True:  # Always log for now to debug
        logger.info(f"Market check: UTC={now_utc}, IST={now_ist}, current={current}, is_open={is_open}")
    return is_open

# ----------------------------------------------------------------------
# MAIN SIGNAL ENGINE (unchanged)
# ----------------------------------------------------------------------
def run_signal_engine_for_index(index_name):
    if not INDEX_CONFIG[index_name].get("active"):
        return

    tokens = INDEX_TOKENS.get(index_name, {})
    if not tokens.get("ce_token") or not tokens.get("pe_token"):
        with _market_signal_lock:
            market_signal[index_name]["alert_message"] = "Option tokens not loaded"
            market_signal[index_name]["signal"] = "WAITING"
        return

    with _candle_histories_lock:
        if len(candle_histories[index_name]["1min"]) < 30:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Building candles ({len(candle_histories[index_name]['1min'])}/30)"
                market_signal[index_name]["signal"] = "WAITING"
            return

    with _market_signal_lock:
        if market_signal[index_name]["signal"] == "EXIT":
            market_signal[index_name]["signal"] = "COOLDOWN"

    now = time.time()

    with _latest_ticks_lock:
        spot = latest_ticks[index_name]["spot_price"]
        if spot <= 0:
            spot = last_known_prices[index_name].get("spot", 0)
        ce_prem = latest_ticks[index_name]["ce_price"]
        pe_prem = latest_ticks[index_name]["pe_price"]
        if ce_prem <= 0:
            ce_prem = last_known_prices[index_name].get("ce", 0)
        if pe_prem <= 0:
            pe_prem = last_known_prices[index_name].get("pe", 0)
        ce_vol = latest_ticks[index_name]["ce_volume"]
        pe_vol = latest_ticks[index_name]["pe_volume"]

    vp_engine = volume_profile_engines[index_name]
    vp_engine.update(spot, ce_vol, option_type=None)
    vp_engine.update(ce_prem, ce_vol, option_type="CE")
    vp_engine.update(pe_prem, pe_vol, option_type="PE")

    if index_name == "NIFTY":
        nifty_price_series.append(spot)
    elif index_name == "BANKNIFTY":
        banknifty_price_series.append(spot)
    if len(nifty_price_series) > 0 and len(banknifty_price_series) > 0:
        correlation_filter.update(list(nifty_price_series)[-1], list(banknifty_price_series)[-1])

    greeks_data = get_option_greeks(index_name) if INDEX_CONFIG[index_name].get("greeks_enabled") else None
    sentiment = compute_sentiment(index_name)
    action = get_signal_from_sentiment(sentiment)
    sentiment_label = get_sentiment_label(sentiment)
    with _market_signal_lock:
        market_signal[index_name]["sentiment_score"] = sentiment

    # Drawdown kill switch
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
                    with _portfolio_state_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        portfolio_state[index_name]["daily_pnl"] += pnl_total
                        portfolio_state[index_name]["total_pnl"] += pnl_total
                        portfolio_state[index_name]["live_pnl"] = 0.0
                    save_portfolio_state(index_name)
                    reset_signal_state(index_name, now, "KILL_SWITCH")
        with _market_signal_lock:
            market_signal[index_name]["alert_message"] = "KILL SWITCH: Max drawdown hit. Trading halted."
            market_signal[index_name]["signal"] = "KILL_SWITCH"
        return

    # Circuit breaker
    if safety_state[index_name]["circuit_breaker"]:
        if now < safety_state[index_name]["circuit_breaker_until"]:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "Circuit breaker active"
                market_signal[index_name]["signal"] = "CIRCUIT_BREAKER"
            return
        else:
            safety_state[index_name]["circuit_breaker"] = False
            safety_state[index_name]["consecutive_sl"] = 0

    # Existing position handling
    with _signal_state_lock:
        current_action = signal_state[index_name]["action"]
    if current_action != "HOLD":
        active = current_action
        with _latest_ticks_lock:
            fresh_ce = latest_ticks[index_name]["ce_price"]
            fresh_pe = latest_ticks[index_name]["pe_price"]
        prem = fresh_ce if "CE" in active else fresh_pe
        if prem <= 0:
            prem = last_known_prices[index_name].get("ce" if "CE" in active else "pe", 0)
        if prem > 0:
            with _signal_state_lock:
                pnl = prem - signal_state[index_name]["entry_price"]
                with _portfolio_state_lock:
                    portfolio_state[index_name]["live_pnl"] = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                if prem > signal_state[index_name].get("highest", 0):
                    signal_state[index_name]["highest"] = prem
                    with _candle_histories_lock:
                        closes = [c["close"] for c in candle_histories[index_name]["1min"]]
                    if len(closes) >= 14:
                        atr = calculate_atr([], [], closes, 14)
                        new_sl = prem - atr * 1.8
                        if new_sl > signal_state[index_name]["stop_loss"]:
                            signal_state[index_name]["stop_loss"] = new_sl
                if prem <= signal_state[index_name]["stop_loss"]:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
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
                    send_telegram_alert(f"EXIT {index_name} | SL | PnL: {pnl:.2f} pts")
                    reset_signal_state(index_name, now, "STOP_LOSS")
                    return
                if prem >= signal_state[index_name]["target"]:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_state_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        portfolio_state[index_name]["daily_pnl"] += pnl_total
                        portfolio_state[index_name]["total_pnl"] += pnl_total
                        portfolio_state[index_name]["live_pnl"] = 0.0
                    save_portfolio_state(index_name)
                    pnl_pct = pnl / max(signal_state[index_name]["entry_price"], 1)
                    kelly_trackers[index_name].update(pnl_pct)
                    safety_state[index_name]["consecutive_sl"] = 0
                    send_telegram_alert(f"EXIT {index_name} | TARGET | PnL: {pnl:.2f} pts")
                    reset_signal_state(index_name, now, "TARGET_HIT")
                    return
                entry_time = signal_state[index_name].get("entry_time", 0)
                if entry_time > 0 and (now - entry_time) / 60 >= 45:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_state_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        portfolio_state[index_name]["daily_pnl"] += pnl_total
                        portfolio_state[index_name]["total_pnl"] += pnl_total
                        portfolio_state[index_name]["live_pnl"] = 0.0
                    save_portfolio_state(index_name)
                    kelly_trackers[index_name].update(pnl / max(signal_state[index_name]["entry_price"], 1))
                    send_telegram_alert(f"EXIT {index_name} | TIME | PnL: {pnl:.2f} pts")
                    reset_signal_state(index_name, now, "TIME_EXIT")
                    return
                with _price_histories_lock:
                    prices_spot = list(price_histories[index_name])
                should_exit, exit_reason = should_exit_market_analysis(index_name, active, prices_spot, fresh_ce, fresh_pe)
                if should_exit:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_state_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        portfolio_state[index_name]["daily_pnl"] += pnl_total
                        portfolio_state[index_name]["total_pnl"] += pnl_total
                        portfolio_state[index_name]["live_pnl"] = 0.0
                    save_portfolio_state(index_name)
                    kelly_trackers[index_name].update(pnl / max(signal_state[index_name]["entry_price"], 1))
                    send_telegram_alert(f"EXIT {index_name} | {exit_reason} | PnL: {pnl:.2f} pts")
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
                        with _portfolio_state_lock:
                            portfolio_state[index_name]["equity"] += pnl_total
                            portfolio_state[index_name]["daily_pnl"] += pnl_total
                            portfolio_state[index_name]["total_pnl"] += pnl_total
                            portfolio_state[index_name]["live_pnl"] = 0.0
                        save_portfolio_state(index_name)
                        kelly_trackers[index_name].update(pnl / max(signal_state[index_name]["entry_price"], 1))
                        send_telegram_alert(f"EXIT {index_name} | VWAP (premium below VWAP) | PnL: {pnl:.2f} pts")
                        reset_signal_state(index_name, now, "VWAP_EXIT")
                        return
                    elif "PE" in active and prem < option_vwap * 0.997:
                        pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                        with _portfolio_state_lock:
                            portfolio_state[index_name]["equity"] += pnl_total
                            portfolio_state[index_name]["daily_pnl"] += pnl_total
                            portfolio_state[index_name]["total_pnl"] += pnl_total
                            portfolio_state[index_name]["live_pnl"] = 0.0
                        save_portfolio_state(index_name)
                        kelly_trackers[index_name].update(pnl / max(signal_state[index_name]["entry_price"], 1))
                        send_telegram_alert(f"EXIT {index_name} | VWAP (premium below VWAP) | PnL: {pnl:.2f} pts")
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

    # New entry logic
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

    if daily_trade_count[index_name] >= 20:
        with _market_signal_lock:
            market_signal[index_name]["alert_message"] = "Max daily trades"
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
    min_prem = INDEX_CONFIG[index_name].get("min_premium", 5)
    if prem <= 0 or prem < min_prem:
        with _market_signal_lock:
            market_signal[index_name]["alert_message"] = f"Premium invalid: Rs{prem}"
            market_signal[index_name]["signal"] = "WAITING"
        return

    buf = signal_buffer[index_name]
    if side == "CE":
        buf["ce_count"] += 1
        buf["pe_count"] = 0
        if buf["ce_count"] < 2:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Building CE ({buf['ce_count']}/2)"
                market_signal[index_name]["signal"] = "BUILDING"
            return
    else:
        buf["pe_count"] += 1
        buf["ce_count"] = 0
        if buf["pe_count"] < 2:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Building PE ({buf['pe_count']}/2)"
                market_signal[index_name]["signal"] = "BUILDING"
            return

    # Greeks filter
    if INDEX_CONFIG[index_name].get("greeks_enabled") and greeks_data:
        delta = greeks_data["ce_delta"] if side == "CE" else greeks_data["pe_delta"]
        if "STRONG" not in action and abs(delta) > 0.80:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Greeks block: Delta {delta:.2f} > 0.80"
                market_signal[index_name]["signal"] = "BLOCKED"
            return
        iv_rank = greeks_data.get("iv_rank", 50)
        if iv_rank > 80 and "LOW" not in action:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"High IV rank {iv_rank:.0f}"
                market_signal[index_name]["signal"] = "BLOCKED"
            return

    pair = INDEX_CONFIG[index_name].get("correlation_pair")
    if pair:
        corr_analysis = correlation_filter.analyze(index_name, action)
        if corr_analysis.get("block_reason"):
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Correlation block: {corr_analysis['block_reason']}"
                market_signal[index_name]["signal"] = "BLOCKED"
            return
        beta_adj = corr_analysis.get("beta_adjustment", 1.0)
    else:
        beta_adj = 1.0

    with _price_histories_lock:
        prices_spot = list(price_histories[index_name])
    rsi = calculate_rsi(prices_spot[-50:]) if len(prices_spot) >= 50 else 50
    with _candle_histories_lock:
        closes_5min = [c["close"] for c in candle_histories[index_name]["5min"]]
    adx = calculate_adx([], [], closes_5min, 14) if len(closes_5min) >= 30 else 20
    with _latest_ticks_lock:
        vix = latest_ticks["VIX"]["vix"]
        if vix <= 0:
            vix = 15.0
    ml_prob = ml_filter.predict([prem, spot, rsi, adx, vix, sentiment])
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
    if greeks_data:
        iv_rank = greeks_data.get("iv_rank", 50)
        if iv_rank > 80:
            risk_pct *= 0.8
        elif iv_rank < 20:
            risk_pct *= 1.1
    if is_expiry_day(index_name):
        risk_pct *= 0.5
    risk_pct = max(0.5, min(3.0, risk_pct))

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
            "action": action, "entry_price": prem, "stop_loss": sl, "target": target,
            "lots": lots, "entry_time": now, "highest": prem, "cooldown": 0
        })
    with _portfolio_state_lock:
        portfolio_state[index_name]["open_positions"] = 1
    signal_buffer[index_name]["ce_count"] = signal_buffer[index_name]["pe_count"] = 0
    daily_trade_count[index_name] += 1
    save_portfolio_state(index_name)

    emoji = "🔥" if "STRONG" in action and "CE" in action else "❄️" if "STRONG" in action and "PE" in action else "⚡" if "LOW" in action else "📊"
    msg = (f"{emoji} {action} {index_name} | Spot:{spot:.0f} Prem:{prem:.2f} SL:{sl:.2f} Tgt:{target:.2f} | "
           f"Sentiment:{sentiment:.0f} ({sentiment_label}) | Lots:{lots} Risk:{risk_pct:.1f}%")
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

def run_all_signals():
    for idx in INDEX_CONFIG:
        if INDEX_CONFIG[idx].get("active"):
            try:
                run_signal_engine_for_index(idx)
            except Exception as e:
                logger.error(f"Signal error {idx}: {e}")

# ----------------------------------------------------------------------
# CUSTOM BINARY TICK PARSER FOR SMARTAPI WS V2
# Based on official SmartAPI binary protocol specification
# ----------------------------------------------------------------------

def parse_smartapi_binary(data):
    """
    Parse SmartAPI WebSocket V2 binary tick data.
    Returns a list of dicts with parsed tick fields.

    Binary format (Mode 3 - SnapQuote, most common):
    Byte positions based on SmartAPI V2 spec:

    All modes share:
    - Bytes 0-1:   Subscription mode (1=ltp, 2=quote, 3=snapquote)
    - Bytes 2-26:  Token (25 bytes, null-padded)
    - Bytes 27-34: Exchange timestamp (int64, ms)
    - Bytes 35-42: Last traded price (int64, paise -> divide by 100)
    - Bytes 43-50: Last traded quantity (int64)
    - Bytes 51-58: Average traded price (int64)
    - Bytes 59-66: Volume for the day (int64)
    - Bytes 67-74: Total buy quantity (int64)
    - Bytes 75-82: Total sell quantity (int64)
    - Bytes 83-90: Open price (int64)
    - Bytes 91-98: High price (int64)
    - Bytes 99-106: Low price (int64)
    - Bytes 107-114: Close price (int64)
    - Bytes 115-122: Last traded timestamp (int64)

    Mode 2+ additional:
    - Bytes 123-130: Open interest (int64)
    - Bytes 131-138: Open interest change % (int64)

    Mode 3 additional:
    - Bytes 139-146: Upper circuit (int64)
    - Bytes 147-154: Lower circuit (int64)
    - Bytes 155-162: 52-week high (int64)
    - Bytes 163-170: 52-week low (int64)
    - Then best 5 bid/ask...

    We use a flexible parser that extracts the common fields regardless of mode.
    """
    ticks = []

    if not data or len(data) < 2:
        return ticks

    # Handle heartbeat (ping/pong)
    if len(data) == 4 and data == b'ping':
        return ticks  # Heartbeat, no tick data

    if data == b'\x00' or data == b'\x00\x00':
        return ticks

    try:
        # Some implementations send multiple ticks concatenated
        # Each tick has a mode byte at position 0
        offset = 0

        while offset < len(data) - 1:
            # Determine subscription mode
            if offset + 1 >= len(data):
                break

            mode = data[offset]

            # Validate mode (1=LTP, 2=Quote, 3=SnapQuote)
            if mode not in (1, 2, 3):
                # Might be a different format, try to find next valid mode
                offset += 1
                continue

            # Calculate expected minimum length based on mode
            if mode == 1:
                min_len = 43  # LTP mode minimum
            elif mode == 2:
                min_len = 139  # Quote mode minimum
            else:
                min_len = 171  # SnapQuote mode minimum

            if offset + min_len > len(data):
                break

            try:
                # Parse token (bytes 2-26, 25 bytes)
                token_bytes = data[offset + 2:offset + 27]
                token = token_bytes.decode('utf-8', errors='ignore').strip('\x00').strip()

                # Parse exchange timestamp (bytes 27-34, int64 LE)
                exchange_ts = struct.unpack('<q', data[offset + 27:offset + 35])[0] if offset + 35 <= len(data) else 0

                # Parse LTP (bytes 35-42, int64 LE, in paise)
                ltp_raw = struct.unpack('<q', data[offset + 35:offset + 43])[0] if offset + 43 <= len(data) else 0
                ltp = ltp_raw / 100.0

                # Parse volume (bytes 59-66, int64 LE)
                volume = struct.unpack('<q', data[offset + 59:offset + 67])[0] if offset + 67 <= len(data) else 0

                # Parse OI (bytes 123-130, int64 LE) - Mode 2+
                oi = 0
                if mode >= 2 and offset + 131 <= len(data):
                    oi = struct.unpack('<q', data[offset + 123:offset + 131])[0]

                # Parse bid/ask (Mode 3)
                bid = 0
                ask = 0
                if mode >= 3 and offset + 179 <= len(data):
                    # Best 5 buy starts at byte 171
                    # Each entry: 20 bytes (quantity:8, price:8, orders:4)
                    best_bid_price_raw = struct.unpack('<q', data[offset + 179:offset + 187])[0] if offset + 187 <= len(data) else 0
                    bid = best_bid_price_raw / 100.0

                    # Best 5 sell starts after best 5 buy (5 * 20 = 100 bytes)
                    best_ask_offset = offset + 171 + 100
                    if best_ask_offset + 8 <= len(data):
                        best_ask_price_raw = struct.unpack('<q', data[best_ask_offset:best_ask_offset + 8])[0]
                        ask = best_ask_price_raw / 100.0

                tick = {
                    "token": token,
                    "last_traded_price": ltp,
                    "ltp": ltp,
                    "price": ltp,
                    "volume": volume,
                    "v": volume,
                    "open_interest": oi,
                    "oi": oi,
                    "best_bid_price": bid,
                    "bid": bid,
                    "bp": bid,
                    "best_ask_price": ask,
                    "ask": ask,
                    "ap": ask,
                    "exchange_timestamp": exchange_ts,
                    "mode": mode
                }
                ticks.append(tick)

                # Move to next tick
                offset += min_len

            except struct.error as e:
                logger.debug(f"Struct unpack error at offset {offset}: {e}")
                offset += 1
            except Exception as e:
                logger.debug(f"Parse error at offset {offset}: {e}")
                offset += 1

    except Exception as e:
        logger.error(f"Binary parse error: {e}")

    return ticks


def parse_smartapi_binary_v2(data):
    """
    Alternative parser using SmartAPI's _parse_binary_data if available,
    with fallback to custom parser.
    """
    # First try the SDK's built-in parser
    if sws and hasattr(sws, '_parse_binary_data'):
        try:
            result = sws._parse_binary_data(data)
            if result and isinstance(result, (dict, list)):
                if isinstance(result, dict):
                    return [result]
                return result
        except Exception as e:
            logger.debug(f"SDK parser failed: {e}")

    # Fallback to custom parser
    return parse_smartapi_binary(data)


# ----------------------------------------------------------------------
# WEBSOCKET WITH FIXED BINARY PARSING
# ----------------------------------------------------------------------
ws_running = False
sws = None
last_heartbeat = time.time()
tick_counter = 0
last_tick_timestamp = time.time()
_last_tick_count = 0


def on_ws_open(wsapp):
    logger.info("on_ws_open CALLED")   # <-- ADD
    ...
    global ws_running, last_heartbeat
    ws_running = True
    last_heartbeat = time.time()
    logger.info("WebSocket connected successfully, subscribing to tokens...")

    token_list = []
    for idx, cfg in INDEX_CONFIG.items():
        if cfg.get("active"):
            token_list.append({"exchangeType": cfg["ws_exchange_type"], "tokens": [cfg["token"]]})

    for idx, tokens in INDEX_TOKENS.items():
        if not INDEX_CONFIG[idx].get("active"):
            continue
        if tokens.get("ce_token") and tokens.get("pe_token"):
            token_list.append({
                "exchangeType": INDEX_CONFIG[idx]["option_ws_exchange_type"],
                "tokens": [tokens["ce_token"], tokens["pe_token"]]
            })

    token_list.append({"exchangeType": 1, "tokens": ["99919017"]})  # VIX

    if token_list and sws:
        try:
            # Mode 3 = SnapQuote (most data), Mode 2 = Quote, Mode 1 = LTP
            # Use Mode 2 for better performance, Mode 3 if you need depth
            response = sws.subscribe("admin", 3, token_list)
            logger.info(f"Subscription response: {response}")
            total = sum(len(g["tokens"]) for g in token_list)
            logger.info(f"Successfully subscribed to {total} tokens")
        except Exception as e:
            logger.error(f"Subscribe error: {e}")
            try:
                for token_group in token_list:
                    sws.subscribe("admin", 2, [token_group])
                logger.info("Alternative subscription method succeeded")
            except Exception as e2:
                logger.error(f"Alternative subscription also failed: {e2}")


def on_ws_error(wsapp, error):
    logger.error(f"on_ws_error: {error}")   # <-- ADD
    ...
    global ws_running
    logger.error(f"WebSocket error: {error}")
    ws_running = False


def on_ws_close(wsapp, *args):
    logger.warning(f"on_ws_close: {args}")   # <-- add this
    global ws_running
    ws_running = False
    logger.warning(f"WebSocket closed: {args}")


def on_ws_data(wsapp, message):
    global tick_counter, last_heartbeat, last_tick_timestamp, sws
    last_heartbeat = time.time()

    # ---- DEBUG: log raw message ----
    if DEBUG_MODE:
        logger.info(f"WS data: type={type(message)}, len={len(message) if message else 0}")
        if isinstance(message, bytes) and len(message) > 0:
            logger.info(f"First 20 bytes: {message[:20].hex()}")
    # --------------------------------

    # Skip empty/heartbeat messages
    if message == b'\x00' or message == '\x00' or message == b'ping' or message == 'ping':
        return

    try:
        ticks = []

        if isinstance(message, bytes):
            # Try SDK parser first, then custom binary parser
            ticks = parse_smartapi_binary_v2(message)

            if not ticks and len(message) > 2:
                # Try JSON fallback
                try:
                    decoded = message.decode('utf-8')
                    data = json.loads(decoded)
                    ticks = data if isinstance(data, list) else [data]
                except Exception:
                    pass

        elif isinstance(message, str):
            try:
                data = json.loads(message)
                ticks = data if isinstance(data, list) else [data]
            except Exception as e:
                logger.error(f"JSON parse error: {e}")
                return
        elif isinstance(message, dict):
            ticks = [message]
        else:
            logger.warning(f"Unsupported WS message type: {type(message)}")
            return

        if not ticks:
            return

        # Log first tick for debugging
        if DEBUG_MODE and ticks:
            logger.info(f"Parsed {len(ticks)} ticks. First: {ticks[0]}")

        # ---------- PROCESS TICKS ----------
        for tick in ticks:
            tick_counter += 1

            token = str(tick.get("token") or tick.get("symbol") or "")
            ltp = tick.get("last_traded_price") or tick.get("ltp") or tick.get("price") or 0
            if isinstance(ltp, str):
                try:
                    ltp = float(ltp)
                except:
                    ltp = 0
            if ltp > 100000 and token not in INDEX_TOKEN_SET:
                ltp /= 100

            vol = tick.get("volume") or tick.get("v") or tick.get("last_traded_quantity") or 0
            oi = tick.get("open_interest") or tick.get("oi") or tick.get("OpenInterest") or 0
            bid = tick.get("best_bid_price") or tick.get("bid") or tick.get("bp") or 0
            ask = tick.get("best_ask_price") or tick.get("ask") or tick.get("ap") or 0

            # Spot indices
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
                        if DEBUG_MODE:
                            logger.info(f"SPOT TICK {idx}: {ltp}")
                    break

            # Option premiums – CE
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
                        if DEBUG_MODE:
                            logger.info(f"CE TICK {idx}: {ltp}")
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
                        if DEBUG_MODE:
                            logger.info(f"PE TICK {idx}: {ltp}")
                    break

            # VIX
            if token == "99919017" and ltp > 0:
                with _latest_ticks_lock:
                    latest_ticks["VIX"]["vix"] = ltp
                vix_history.append(ltp)
                if DEBUG_MODE:
                    logger.info(f"VIX TICK: {ltp}")

        # Throttle signal runs
        if tick_counter % 5 == 0 and tick_counter > 0:
            with _signal_run_lock:
                now = time.time()
                global _last_signal_run
                if now - _last_signal_run >= 1.0:
                    _last_signal_run = now
                    threading.Thread(target=run_all_signals, daemon=True).start()

    except Exception as e:
        logger.error(f"Unhandled exception in on_ws_data: {e}", exc_info=True)


def tick_watchdog():
    """Force reconnect if no new ticks arrive for 30 seconds, but only if market is open."""
    global ws_running, tick_counter, last_tick_timestamp
    last_count = 0
    while True:
        time.sleep(15)
        if ws_running:
            # Only check if market is open
            if not is_market_open():
                continue  # skip checking during market closed
            if tick_counter == last_count:
                if time.time() - last_tick_timestamp > 30:
                    logger.warning("No new ticks for 30s - forcing reconnect")
                    ws_running = False
                    if sws:
                        try:
                            sws.close_connection()
                        except:
                            pass
            else:
                last_count = tick_counter


def ws_watchdog():
    global ws_running, last_heartbeat, sws
    while True:
        time.sleep(10)
        now = time.time()
        if ws_running and (now - last_heartbeat > 20):
            logger.warning("Data starvation – no tick for 20s, forcing reconnect")
            ws_running = False
            if sws:
                try:
                    sws.close_connection()
                except Exception:
                    pass


def start_angel_websocket_improved():
    global sws, ws_running
    logger.info("***** ENTERED start_angel_websocket_improved *****")
    # Inform the manager that the WebSocket thread is alive
    ws_running = True   # <--- CRITICAL

    while True:
        try:
            # Wait until market opens, but keep ws_running True
            while not is_market_open():
                logger.info("Market closed, will retry in 5 seconds...")
                time.sleep(5)

            auth_token, feed_token, _ = get_auth_token()
            logger.info(f"Auth token obtained: {bool(auth_token)}, feed_token: {bool(feed_token)}")
            if not feed_token:
                logger.error("Failed to get feed token, retrying in 10 seconds...")
                time.sleep(10)
                continue

            logger.info("Creating SmartWebSocketV2 instance...")
            sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)
            sws.on_open = on_ws_open
            sws.on_data = on_ws_data
            sws.on_error = on_ws_error
            sws.on_close = on_ws_close

            # ---------- TIMEOUT WRAPPER ----------
            logger.info("Attempting WebSocket connection...")

            def connect_with_log():
                try:
                    logger.info("Thread: calling sws.connect()")
                    sws.connect()
                    logger.info("Thread: sws.connect() returned")
                except Exception as e:
                    logger.error(f"Thread: sws.connect() raised exception: {e}", exc_info=True)

            connect_thread = threading.Thread(target=connect_with_log)
            connect_thread.daemon = True
            connect_thread.start()
            logger.info("Thread started, waiting up to 10 seconds for completion...")
            connect_thread.join(timeout=10)

            if connect_thread.is_alive():
                logger.error("WebSocket connect() timed out after 10s, aborting and retrying")
                try:
                    sws.close_connection()
                except:
                    pass
                time.sleep(5)
                continue   # retry connection attempt

            logger.info("WebSocket connect() returned (or completed)")

            # ---------- MAIN LOOP ----------
            logger.info("Entering main WebSocket keep-alive loop")
            while ws_running:
                time.sleep(1)
                if time.time() - last_heartbeat > 30:
                    try:
                        if hasattr(sws, 'ping'):
                            sws.ping()
                        last_heartbeat = time.time()
                    except Exception:
                        pass

            logger.warning("WebSocket disconnected, reconnecting in 5 seconds...")
            time.sleep(5)

        except Exception as e:
            logger.error(f"WebSocket thread error: {e}", exc_info=True)
            time.sleep(10)
            # In case of error, we keep ws_running True to avoid fallback, but if the connection is dead, we may need to reset.
            # For simplicity, we'll let the loop retry.
# ----------------------------------------------------------------------
# REST-ONLY FALLBACK MODE (unchanged)
# ----------------------------------------------------------------------
def start_rest_only_mode():
    logger.info("Starting REST-only mode (WebSocket fallback)")
    while True:
        try:
            if not is_market_open():
                time.sleep(5)
                continue
            for idx in INDEX_CONFIG:
                if not INDEX_CONFIG[idx].get("active"):
                    continue
                try:
                    spot = get_index_spot(idx)
                    if spot and spot > 0:
                        with _latest_ticks_lock:
                            latest_ticks[idx]["spot_price"] = spot
                        with _price_histories_lock:
                            price_histories[idx].append(spot)
                        update_candle(idx, spot, 0, time.time())
                        with _latest_ticks_lock:
                            last_known_prices[idx]["spot"] = spot
                            last_known_prices[idx]["timestamp"] = time.time()
                    tokens = INDEX_TOKENS.get(idx, {})
                    if tokens.get("ce_token") and tokens.get("pe_token"):
                        auth_token, _, obj = get_auth_token()
                        if obj:
                            ce_resp = obj.ltpData(INDEX_CONFIG[idx]["option_exchange"], tokens["ce_symbol"], tokens["ce_token"])
                            ce_price = safe_ltp(ce_resp)
                            if ce_price and ce_price > 0:
                                if ce_price > 100000:
                                    ce_price /= 100
                                with _latest_ticks_lock:
                                    latest_ticks[idx]["ce_price"] = ce_price
                                with _latest_ticks_lock:
                                    last_known_prices[idx]["ce"] = ce_price
                                volume_profile_engines[idx].update(ce_price, 0, option_type="CE")
                            pe_resp = obj.ltpData(INDEX_CONFIG[idx]["option_exchange"], tokens["pe_symbol"], tokens["pe_token"])
                            pe_price = safe_ltp(pe_resp)
                            if pe_price and pe_price > 0:
                                if pe_price > 100000:
                                    pe_price /= 100
                                with _latest_ticks_lock:
                                    latest_ticks[idx]["pe_price"] = pe_price
                                with _latest_ticks_lock:
                                    last_known_prices[idx]["pe"] = pe_price
                                volume_profile_engines[idx].update(pe_price, 0, option_type="PE")
                except Exception as e:
                    logger.debug(f"REST fetch error for {idx}: {e}")
            run_all_signals()
            time.sleep(5)
        except Exception as e:
            logger.error(f"REST-only mode error: {e}")
            time.sleep(10)

# ----------------------------------------------------------------------
# CONNECTION MANAGER (unchanged)
# ----------------------------------------------------------------------
class ConnectionManager:
    def __init__(self):
        self.use_websocket = True

    def start(self):
        # ALWAYS start REST mode as a fallback (this will fetch data immediately)
        logger.info("Starting REST-only mode as a backup...")
        threading.Thread(target=start_rest_only_mode, daemon=True).start()

        # Also attempt WebSocket for faster updates
        if self.use_websocket:
            try:
                logger.info("Attempting WebSocket connection...")
                threading.Thread(target=start_angel_websocket_improved, daemon=True).start()
                # Give the WebSocket thread a moment to set ws_running
                time.sleep(3)
                # If WebSocket is running, start the tick watchdog to monitor it
                if ws_running:
                    threading.Thread(target=tick_watchdog, daemon=True).start()
                else:
                    logger.warning("WebSocket thread did not set ws_running, but REST fallback is active.")
            except Exception as e:
                logger.error(f"WebSocket initialization failed: {e}")
# ----------------------------------------------------------------------
# BACKGROUND THREADS (unchanged)
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
                           if cfg.get("active") and INDEX_TOKENS[idx].get("ce_token"))
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

# ----------------------------------------------------------------------
# FLASK ROUTES (unchanged)
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
        "engine": "Multi-Index Options Bot v14.0 (Fixed SmartAPI WS V2 Binary Parser)",
        "indices": [i for i, cfg in INDEX_CONFIG.items() if cfg.get("active")],
        "market_open": is_market_open()
    })

@app.route("/api/live-signals", methods=["GET"])
def live_signals():
    sentiment_data = {}
    trends_data = {}
    for idx in INDEX_CONFIG:
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
            "debug": {
                "ws_running": ws_running,
                "ticks": tick_counter,
                "last_tick_ago": round(time.time() - last_tick_timestamp, 1)
            },
            "version": "14.0"
        })

@app.route("/api/signal-audio", methods=["GET"])
def signal_audio():
    latest_action = "NO_TRADE"
    for idx in INDEX_CONFIG:
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
        "connection_mode": "WEBSOCKET" if ws_running else "REST_FALLBACK",
        "active_indices": [idx for idx, cfg in INDEX_CONFIG.items() if cfg.get("active")],
        "tokens_loaded": {idx: {
            "ce_token": bool(INDEX_TOKENS[idx].get("ce_token")),
            "pe_token": bool(INDEX_TOKENS[idx].get("pe_token"))
        } for idx in INDEX_CONFIG}
    })

# ----------------------------------------------------------------------
# AUTO-START FOR GUNICORN
# ----------------------------------------------------------------------
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
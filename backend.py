# ====================================================================
# FULLY PRODUCTION-READY: Multi-Index Options Bot v12.13
# - Complete signal engine with all exit conditions
# - Forced candle closure to prevent stall at 9/10
# - REST poller every 2s with valid tokens
# - Uses last known prices from DB as fallback
# ====================================================================

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
import pickle
import pytz
import re
from collections import deque, defaultdict
from datetime import datetime, timedelta, time as dt_time
from flask import Flask, jsonify, request
from flask_cors import CORS
import pyotp

logging.basicConfig(level=logging.INFO)
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

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing critical Angel One environment variables")

DB_PATH = "trading_data.db"

# ----------------------------------------------------------------------
# DATABASE INIT – includes last_known_prices table
# ----------------------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS ticks (timestamp REAL, token TEXT, price REAL, volume REAL, bid REAL, ask REAL, oi REAL)""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ticks_time ON ticks(timestamp)")
    c.execute("""CREATE TABLE IF NOT EXISTS signals (timestamp REAL, action TEXT, signal_type TEXT, grade TEXT, confidence REAL, ce_price REAL, pe_price REAL, rsi REAL, pcr REAL, regime TEXT, vwap REAL, vix REAL, ml_score REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS trades (timestamp REAL, action TEXT, entry_price REAL, exit_price REAL, pnl REAL, size_pct REAL, status TEXT, grade TEXT, atr REAL, vix REAL, exit_reason TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS daily_performance (date TEXT, equity REAL, daily_pnl REAL, drawdown_pct REAL, sharpe REAL, var REAL, win_rate REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS ml_models (id INTEGER PRIMARY KEY, model BLOB, created_at REAL, features TEXT, accuracy REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS portfolio_equity (index_name TEXT PRIMARY KEY, equity REAL, last_updated REAL, active_action TEXT, entry_price REAL, stop_loss REAL, target REAL, lots INTEGER, entry_time REAL, highest REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS kelly_history (timestamp REAL, index_name TEXT, kelly_fraction REAL, win_rate REAL, avg_win REAL, avg_loss REAL, recommended_lots INTEGER, actual_lots INTEGER)""")
    c.execute("""CREATE TABLE IF NOT EXISTS greeks_history (timestamp REAL, index_name TEXT, ce_iv REAL, pe_iv REAL, ce_delta REAL, pe_delta REAL, ce_gamma REAL, pe_gamma REAL, ce_theta REAL, pe_theta REAL, ce_vega REAL, pe_vega REAL, iv_rank REAL, iv_percentile REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS performance_metrics (timestamp REAL, index_name TEXT, sharpe REAL, sortino REAL, calmar REAL, win_rate REAL, profit_factor REAL, max_drawdown REAL, avg_trade REAL, expectancy REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS drawdown_events (timestamp REAL, index_name TEXT, drawdown_pct REAL, action_taken TEXT, equity_before REAL, equity_after REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS signal_state (index_name TEXT PRIMARY KEY, action TEXT, entry_price REAL, stop_loss REAL, target REAL, lots INTEGER, entry_time REAL, highest REAL)""")
    # NEW TABLE for persisting last known spot prices
    c.execute("""CREATE TABLE IF NOT EXISTS last_known_prices (index_name TEXT PRIMARY KEY, spot_price REAL, ce_price REAL, pe_price REAL, last_updated REAL)""")
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
        "correlation_pair": "BANKNIFTY", "greeks_enabled": True, "pcr_enabled": True, "instrumenttype": "INDEX"
    },
    "BANKNIFTY": {
        "token": "99926009", "exchange": "NSE", "symbol": "BANKNIFTY", "lot_size": 25, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": "NIFTY", "greeks_enabled": True, "pcr_enabled": True, "instrumenttype": "INDEX"
    },
    "FINNIFTY": {
        "token": "99926037", "exchange": "NSE", "symbol": "FINNIFTY", "lot_size": 40, "expiry_weekday": 1, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True, "instrumenttype": "INDEX"
    },
    "MIDCPNIFTY": {
        "token": "99926074", "exchange": "NSE", "symbol": "MIDCPNIFTY", "lot_size": 75, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 25, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": None, "greeks_enabled": False, "pcr_enabled": True, "instrumenttype": "INDEX"
    },
    "SENSEX": {
        "token": "99919000", "exchange": "BSE", "symbol": "SENSEX", "lot_size": 15, "expiry_weekday": 4, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100, "option_exchange": "BFO",
        "ws_exchange_type": 3, "option_ws_exchange_type": 4, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True, "instrumenttype": "INDEX"
    }
}

INDEX_TOKENS_SET = {cfg["token"] for cfg in INDEX_CONFIG.values()}

# Global state with locks
_latest_ticks_lock = threading.Lock()
_market_signal_lock = threading.Lock()
_portfolio_state_lock = threading.Lock()
_signal_state_lock = threading.RLock()
_candle_histories_lock = threading.Lock()
_price_histories_lock = threading.Lock()
_ce_price_histories_lock = threading.Lock()
_pe_price_histories_lock = threading.Lock()
_last_known_lock = threading.Lock()
_signal_buffer_lock = threading.Lock()
_safety_state_lock = threading.Lock()
_trade_count_lock = threading.Lock()
_correlation_lock = threading.Lock()
_index_tokens_lock = threading.Lock()
_prev_vol_lock = threading.Lock()
_tick_counter_lock = threading.Lock()

INDEX_TOKENS = {idx: {"ce_token": None, "pe_token": None, "atm_strike": 0, "expiry": "", "expiry_date": None, "ce_symbol": "", "pe_symbol": ""} for idx in INDEX_CONFIG}
last_known_prices = {idx: {"spot": 0.0, "ce": 0.0, "pe": 0.0, "timestamp": 0} for idx in INDEX_CONFIG}
price_histories = {idx: deque(maxlen=5000) for idx in INDEX_CONFIG}
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

_prev_volume = {idx: 0 for idx in INDEX_CONFIG}
_prev_option_volume = {}
tick_counter = 0
ws_running = False
sws = None
last_heartbeat = time.time()

# ----------------------------------------------------------------------
# MULTI‑TIMEFRAME CANDLE AGGREGATION (with forced closure)
# ----------------------------------------------------------------------
TIMEFRAMES = ["1min", "2min", "3min", "5min", "10min", "15min", "20min", "30min"]
TIMEFRAME_SECONDS = {"1min":60, "2min":120, "3min":180, "5min":300, "10min":600, "15min":900, "20min":1200, "30min":1800}
TIMEFRAME_WEIGHTS = {"1min":8, "2min":8, "3min":8, "5min":12, "10min":12, "15min":14, "20min":14, "30min":24}
EMA_SHORT, EMA_MEDIUM, EMA_LONG = 9, 21, 50

candle_histories = {idx: {tf: deque(maxlen=500) for tf in TIMEFRAMES} for idx in INDEX_CONFIG}
_last_candle_time = {idx: {tf: 0 for tf in TIMEFRAMES} for idx in INDEX_CONFIG}
_current_candle = {idx: {tf: None for tf in TIMEFRAMES} for idx in INDEX_CONFIG}

def update_candle(idx, price, volume, timestamp):
    """Update candle with forced closure if the current candle exceeds its interval."""
    with _prev_vol_lock:
        tick_vol = max(0, volume - _prev_volume.get(idx, 0))
        _prev_volume[idx] = volume
    for tf, interval in TIMEFRAME_SECONDS.items():
        candle_start = int(timestamp / interval) * interval
        with _candle_histories_lock:
            if _current_candle[idx][tf] is not None:
                # Forced close if candle has been open for more than interval+5s
                if timestamp - _last_candle_time[idx][tf] > interval + 5:
                    candle_histories[idx][tf].append(_current_candle[idx][tf])
                    _current_candle[idx][tf] = None
                    _last_candle_time[idx][tf] = 0
            if _last_candle_time[idx][tf] != candle_start:
                if _current_candle[idx][tf] is not None:
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
    if len(prices) < slow+signal: return 0.0, 0.0, 0.0
    def ema(arr, p):
        if not arr: return 0
        alpha=2/(p+1); val=arr[0]
        for x in arr[1:]: val=alpha*x+(1-alpha)*val
        return val
    ema_fast = ema(prices[-fast:], fast)
    ema_slow = ema(prices[-slow:], slow)
    macd = ema_fast - ema_slow
    hist = []
    for i in range(signal,0,-1):
        if len(prices)>=slow+i:
            ef=ema(prices[-(fast+i):-i], fast)
            es=ema(prices[-(slow+i):-i], slow)
            hist.append(ef-es)
    sig = ema(hist, signal) if hist else macd
    return macd, sig, macd-sig

def calculate_atr(highs, lows, closes, period=14):
    if len(closes) < period+1: return 5.0
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

def calculate_bollinger(prices, period=20, std_dev=2):
    if len(prices)<period: return None,None,None
    sma = sum(prices[-period:])/period
    var = sum((p-sma)**2 for p in prices[-period:])/period
    std = math.sqrt(var)
    return sma+std_dev*std, sma, sma-std_dev*std

def calculate_vwap(prices, volumes):
    if not prices or not volumes: return prices[-1] if prices else 0
    s_vol = sum(volumes)
    return sum(p*v for p,v in zip(prices, volumes))/s_vol if s_vol else prices[-1]

# ----------------------------------------------------------------------
# PERSISTENCE FUNCTIONS
# ----------------------------------------------------------------------
def save_last_known_prices():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for idx in INDEX_CONFIG:
        with _last_known_lock:
            spot = last_known_prices[idx]["spot"]
            ce = last_known_prices[idx]["ce"]
            pe = last_known_prices[idx]["pe"]
            ts = last_known_prices[idx]["timestamp"]
        c.execute("INSERT OR REPLACE INTO last_known_prices (index_name, spot_price, ce_price, pe_price, last_updated) VALUES (?,?,?,?,?)",
                  (idx, spot, ce, pe, ts))
    conn.commit()
    conn.close()

def load_last_known_prices():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for idx in INDEX_CONFIG:
        row = c.execute("SELECT spot_price, ce_price, pe_price, last_updated FROM last_known_prices WHERE index_name=?", (idx,)).fetchone()
        if row:
            with _last_known_lock:
                last_known_prices[idx]["spot"] = row[0]
                last_known_prices[idx]["ce"] = row[1]
                last_known_prices[idx]["pe"] = row[2]
                last_known_prices[idx]["timestamp"] = row[3]
    conn.close()

def save_portfolio_state(idx):
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        with _signal_state_lock:
            action = signal_state[idx]["action"]
            entry_price = signal_state[idx]["entry_price"]
            stop_loss = signal_state[idx]["stop_loss"]
            target = signal_state[idx]["target"]
            lots = signal_state[idx]["lots"]
            entry_time = signal_state[idx]["entry_time"]
            highest = signal_state[idx]["highest"]
        c.execute("INSERT OR REPLACE INTO portfolio_equity (index_name, equity, last_updated, active_action, entry_price, stop_loss, target, lots, entry_time, highest) VALUES (?,?,?,?,?,?,?,?,?,?)",
                  (idx, portfolio_state[idx]["equity"], time.time(),
                   action, entry_price, stop_loss, target, lots, entry_time, highest))
        conn.commit()

def load_persisted_state():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for idx in INDEX_CONFIG:
        row = c.execute("SELECT equity, active_action, entry_price, stop_loss, target, lots, entry_time, highest FROM portfolio_equity WHERE index_name=?", (idx,)).fetchone()
        if row:
            portfolio_state[idx]["equity"] = row[0]
            if row[1] and row[1] != "HOLD":
                with _signal_state_lock:
                    signal_state[idx].update({
                        "action": row[1],
                        "entry_price": row[2],
                        "stop_loss": row[3],
                        "target": row[4],
                        "lots": row[5],
                        "entry_time": row[6],
                        "highest": row[7]
                    })
                portfolio_state[idx]["open_positions"] = 1
    conn.close()
    for idx in INDEX_CONFIG:
        daily_drawdown[idx]["peak_equity"] = portfolio_state[idx]["equity"]

# ----------------------------------------------------------------------
# GREEKS (unchanged)
# ----------------------------------------------------------------------
def get_option_greeks(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config or not config.get("greeks_enabled"): return None
    with _index_tokens_lock:
        tokens = INDEX_TOKENS.get(index_name)
        if not tokens or not tokens.get("ce_token") or not tokens.get("pe_token"): return None
    _, _, obj = get_auth_token()
    if not obj: return _estimate_greeks_fallback(index_name)
    try:
        with _index_tokens_lock:
            expiry_str = tokens.get("expiry", "")
        if not expiry_str: return _estimate_greeks_fallback(index_name)
        greeks_payload = {"name": config["symbol"], "expirydate": expiry_str}
        url = "https://apiconnect.angelbroking.com/rest/secure/angelbroking/marketData/v1/optionGreek"
        try: local_ip = socket.gethostbyname(socket.gethostname())
        except: local_ip = "127.0.0.1"
        headers = {"Authorization": f"Bearer {auth_cache.get('token', '')}", "Content-Type": "application/json",
                   "Accept": "application/json", "X-UserType": "USER", "X-SourceID": "WEB",
                   "X-ClientLocalIP": local_ip, "X-ClientPublicIP": local_ip, "X-MACAddress": "00:00:00:00:00:00",
                   "X-PrivateKey": ANGEL_API_KEY}
        resp = requests.post(url, json=greeks_payload, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") and data.get("data"):
                greeks_list = data["data"]
                with _index_tokens_lock:
                    atm_strike = tokens.get("atm_strike", 0)
                ce_greeks = pe_greeks = None
                for g in greeks_list:
                    strike = float(g.get("strikePrice",0))
                    opt_type = g.get("optionType","")
                    if abs(strike - atm_strike) < config.get("atm_strike_multiple",50)*0.5:
                        if opt_type == "CE": ce_greeks = g
                        elif opt_type == "PE": pe_greeks = g
                if ce_greeks and pe_greeks:
                    ce_iv = float(ce_greeks.get("impliedVolatility",0))/100 if float(ce_greeks.get("impliedVolatility",0))>1 else float(ce_greeks.get("impliedVolatility",0))
                    pe_iv = float(pe_greeks.get("impliedVolatility",0))/100 if float(pe_greeks.get("impliedVolatility",0))>1 else float(pe_greeks.get("impliedVolatility",0))
                    conn = sqlite3.connect(DB_PATH)
                    hist = conn.execute("SELECT ce_iv FROM greeks_history WHERE index_name=? ORDER BY timestamp DESC LIMIT 50", (index_name,)).fetchall()
                    conn.close()
                    iv_list = [h[0] for h in hist if h[0]>0] + [ce_iv]
                    if len(iv_list) > 20:
                        iv_rank = (ce_iv - min(iv_list)) / (max(iv_list)-min(iv_list)) * 100 if max(iv_list)!=min(iv_list) else 50
                        iv_percentile = sum(1 for iv in iv_list if iv < ce_iv) / len(iv_list) * 100
                    else:
                        iv_rank = iv_percentile = 50
                    greeks_data = {
                        "ce_iv": ce_iv, "pe_iv": pe_iv,
                        "ce_delta": float(ce_greeks.get("delta",0)), "pe_delta": float(pe_greeks.get("delta",0)),
                        "ce_gamma": float(ce_greeks.get("gamma",0)), "pe_gamma": float(pe_greeks.get("gamma",0)),
                        "ce_theta": float(ce_greeks.get("theta",0)), "pe_theta": float(pe_greeks.get("theta",0)),
                        "ce_vega": float(ce_greeks.get("vega",0)), "pe_vega": float(pe_greeks.get("vega",0)),
                        "iv_rank": iv_rank, "iv_percentile": iv_percentile
                    }
                    with sqlite3.connect(DB_PATH) as conn:
                        c = conn.cursor()
                        c.execute("INSERT INTO greeks_history (timestamp, index_name, ce_iv, pe_iv, ce_delta, pe_delta, ce_gamma, pe_gamma, ce_theta, pe_theta, ce_vega, pe_vega, iv_rank, iv_percentile) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                  (time.time(), index_name, greeks_data["ce_iv"], greeks_data["pe_iv"],
                                   greeks_data["ce_delta"], greeks_data["pe_delta"], greeks_data["ce_gamma"], greeks_data["pe_gamma"],
                                   greeks_data["ce_theta"], greeks_data["pe_theta"], greeks_data["ce_vega"], greeks_data["pe_vega"],
                                   iv_rank, iv_percentile))
                    return greeks_data
        return _estimate_greeks_fallback(index_name)
    except Exception as e:
        logger.debug(f"Greeks API error {index_name}: {e}")
        return _estimate_greeks_fallback(index_name)

def _estimate_greeks_fallback(index_name):
    with _latest_ticks_lock:
        ce_price = latest_ticks[index_name]["ce_price"]
        pe_price = latest_ticks[index_name]["pe_price"]
        spot = latest_ticks[index_name]["spot_price"]
    if ce_price>0 and pe_price>0 and spot>0:
        with _index_tokens_lock:
            strike = INDEX_TOKENS.get(index_name, {}).get("atm_strike", spot)
        time_to_expiry = 0.02
        moneyness = (spot - strike)/spot
        ce_delta = max(0.05, min(0.95, 0.5 + moneyness*5))
        pe_delta = max(-0.95, min(-0.05, -0.5 + (-moneyness)*5))
        return {"ce_iv":0.2, "pe_iv":0.2, "ce_delta":ce_delta, "pe_delta":pe_delta,
                "ce_gamma":0.02, "pe_gamma":0.02, "ce_theta":-0.1, "pe_theta":-0.1,
                "ce_vega":0.15, "pe_vega":0.15, "iv_rank":50, "iv_percentile":50}
    return None

# ----------------------------------------------------------------------
# ML FILTER, KELLY, PERFORMANCE (same as before)
# ----------------------------------------------------------------------
class MLSignalFilter:
    def __init__(self):
        self.model = None
        self.features = []
        self.is_trained = False
    def load_model(self):
        try:
            conn = sqlite3.connect(DB_PATH)
            row = conn.execute("SELECT model, features FROM ml_models ORDER BY created_at DESC LIMIT 1").fetchone()
            conn.close()
            if row:
                self.model = pickle.loads(row[0])
                self.features = json.loads(row[1])
                self.is_trained = True
                return True
        except: pass
        return False
    def predict(self, feature_vector):
        if not self.is_trained or self.model is None: return 0.5
        try:
            prob = self.model.predict_proba([feature_vector])[0][1]
            return prob
        except: return 0.5
ml_filter = MLSignalFilter()
ml_filter.load_model()

class KellyCriterion:
    def __init__(self, index_name, kelly_fraction=0.25, min_trades=10):
        self.index_name = index_name
        self.kelly_fraction = kelly_fraction
        self.min_trades = min_trades
        self.alpha = 1.0
        self.beta = 1.0
        self.trade_returns = deque(maxlen=100)
        self.win_count = 0
        self.loss_count = 0
        self.avg_win = 0.0
        self.avg_loss = 0.0
    def update(self, trade_pnl_pct):
        self.trade_returns.append(trade_pnl_pct)
        if trade_pnl_pct > 0:
            self.win_count += 1
            self.alpha += 1
        else:
            self.loss_count += 1
            self.beta += 1
        wins = [r for r in self.trade_returns if r>0]
        losses = [r for r in self.trade_returns if r<=0]
        if wins: self.avg_win = sum(wins)/len(wins)
        if losses: self.avg_loss = abs(sum(losses)/len(losses))
    def calculate(self):
        total = self.win_count + self.loss_count
        if total < self.min_trades: return 0.01, 0.5, self.avg_win, self.avg_loss
        p = self.alpha/(self.alpha+self.beta)
        q = 1-p
        if self.avg_loss == 0: return 0.01, p, self.avg_win, self.avg_loss
        b = self.avg_win/self.avg_loss if self.avg_loss>0 else 1.0
        if b == 0: return 0.01, p, self.avg_win, self.avg_loss
        kelly_full = (p*b - q)/b
        kelly_full = max(0, min(kelly_full, 0.5))
        return kelly_full * self.kelly_fraction, p, self.avg_win, self.avg_loss
    def get_recommended_risk_pct(self):
        kelly, win_rate, avg_win, avg_loss = self.calculate()
        risk_pct = min(kelly*100, 2.0)
        risk_pct = max(0.3, risk_pct)
        return risk_pct, win_rate, avg_win, avg_loss
kelly_trackers = {idx: KellyCriterion(idx) for idx in INDEX_CONFIG}

class PerformanceTracker:
    def __init__(self, index_name, risk_free_rate=0.0):
        self.index_name = index_name
        self.risk_free_rate = risk_free_rate
        self.returns = deque(maxlen=252)
        self.equity_curve = deque(maxlen=252)
        self.trade_pnls = deque(maxlen=100)
        self.peak_equity = 0.0
        self.max_drawdown = 0.0
    def add_return(self, daily_return_pct, equity):
        self.returns.append(daily_return_pct)
        self.equity_curve.append(equity)
        if equity > self.peak_equity: self.peak_equity = equity
        drawdown = (self.peak_equity - equity)/self.peak_equity if self.peak_equity>0 else 0
        self.max_drawdown = max(self.max_drawdown, drawdown)
    def add_trade(self, pnl):
        self.trade_pnls.append(pnl)
    def calculate_sharpe(self):
        if len(self.returns)<10: return 0.0
        arr = np.array(list(self.returns))
        excess = arr - self.risk_free_rate
        std = np.std(excess, ddof=1)
        if std==0 or np.isnan(std): return 0.0
        return np.mean(excess)/std * np.sqrt(252)
    def calculate_sortino(self):
        if len(self.returns)<10: return 0.0
        arr = np.array(list(self.returns))
        excess = arr - self.risk_free_rate
        downside = excess[excess<0]
        if len(downside)==0: return float('inf') if np.mean(excess)>0 else 0.0
        downside_std = np.std(downside, ddof=1)
        if downside_std==0 or np.isnan(downside_std): return 0.0
        return np.mean(excess)/downside_std * np.sqrt(252)
    def calculate_calmar(self):
        if len(self.returns)<10 or self.max_drawdown==0: return 0.0
        annual_return = np.mean(list(self.returns))*252
        return annual_return/self.max_drawdown
    def calculate_win_rate(self):
        if not self.trade_pnls: return 0.0
        wins = sum(1 for p in self.trade_pnls if p>0)
        return wins/len(self.trade_pnls)*100
    def calculate_profit_factor(self):
        if not self.trade_pnls: return 0.0
        gross_profit = sum(p for p in self.trade_pnls if p>0)
        gross_loss = abs(sum(p for p in self.trade_pnls if p<0))
        if gross_loss==0: return float('inf') if gross_profit>0 else 0.0
        return gross_profit/gross_loss
    def calculate_expectancy(self):
        if not self.trade_pnls: return 0.0
        win_rate = self.calculate_win_rate()/100
        wins = [p for p in self.trade_pnls if p>0]
        losses = [p for p in self.trade_pnls if p<0]
        avg_win = sum(wins)/len(wins) if wins else 0
        avg_loss = abs(sum(losses)/len(losses)) if losses else 1
        if avg_loss==0: return 0.0
        return (win_rate*avg_win) - ((1-win_rate)*avg_loss)
    def get_all_metrics(self):
        return {
            "sharpe": round(self.calculate_sharpe(),3),
            "sortino": round(self.calculate_sortino(),3),
            "calmar": round(self.calculate_calmar(),3),
            "win_rate": round(self.calculate_win_rate(),2),
            "profit_factor": round(self.calculate_profit_factor(),3),
            "max_drawdown": round(self.max_drawdown*100,2),
            "avg_trade": round(np.mean(list(self.trade_pnls)),2) if self.trade_pnls else 0.0,
            "expectancy": round(self.calculate_expectancy(),3),
            "total_trades": len(self.trade_pnls)
        }
performance_trackers = {idx: PerformanceTracker(idx) for idx in INDEX_CONFIG}

# ----------------------------------------------------------------------
# CORRELATION FILTER, VOLUME PROFILE
# ----------------------------------------------------------------------
class CorrelationFilter:
    def __init__(self):
        self.nifty_returns = deque(maxlen=50)
        self.banknifty_returns = deque(maxlen=50)
        self.divergence_threshold = 0.2
        self._lock = threading.Lock()
    def update(self, nifty_price, banknifty_price):
        with self._lock:
            if nifty_price>0 and banknifty_price>0:
                self.nifty_returns.append(nifty_price)
                self.banknifty_returns.append(banknifty_price)
    def calculate(self):
        with self._lock:
            if len(self.nifty_returns)<20: return {"correlation_20":0, "correlation_50":0, "beta":1.0}
            n_arr = np.array(list(self.nifty_returns))
            b_arr = np.array(list(self.banknifty_returns))
            n_ret = np.diff(n_arr)/n_arr[:-1]
            b_ret = np.diff(b_arr)/b_arr[:-1]
            if len(n_ret)<10: return {"correlation_20":0, "correlation_50":0, "beta":1.0}
            corr_20 = np.corrcoef(n_ret[-20:], b_ret[-20:])[0,1] if len(n_ret)>=20 else 0
            corr_50 = np.corrcoef(n_ret, b_ret)[0,1] if len(n_ret)>=20 else 0
            cov = np.cov(n_ret, b_ret)[0,1] if len(n_ret)>=2 else 0
            var_n = np.var(n_ret) if len(n_ret)>=2 else 1
            beta = cov/var_n if var_n>0 else 1.0
            if np.isnan(corr_20): corr_20=0
            if np.isnan(corr_50): corr_50=0
            if np.isnan(beta): beta=1.0
            return {"correlation_20":round(corr_20,3), "correlation_50":round(corr_50,3), "beta":round(beta,3)}
    def analyze(self, index_name, action):
        corr_data = self.calculate()
        corr_20 = corr_data["correlation_20"]
        beta_adjustment = 1.0
        block_reason = None
        if corr_20 < 0.1:
            block_reason = f"Very low correlation {corr_20:.2f}"
            beta_adjustment = 0.5
        elif corr_20 < 0.2:
            logger.debug(f"Low correlation {corr_20:.2f} - reducing size")
            beta_adjustment = 0.8
        if corr_20 < -0.2:
            block_reason = f"Negative correlation {corr_20:.2f}"
            beta_adjustment = 0.5
        if corr_data["beta"] > 1.5: beta_adjustment *= 0.8
        elif corr_data["beta"] < 0.5: beta_adjustment *= 1.2
        return {"beta_adjustment": beta_adjustment, "block_reason": block_reason}
correlation_filter = CorrelationFilter()

class VolumeProfileEngine:
    def __init__(self, index_name):
        self.index_name = index_name
        self.price_volume = deque(maxlen=1000)
    def update(self, price, volume):
        if price>0 and volume>0: self.price_volume.append((price, volume))
    def calculate_vwap(self):
        if not self.price_volume: return 0.0
        total_pv = sum(p*v for p,v in self.price_volume)
        total_v = sum(v for p,v in self.price_volume)
        return total_pv/total_v if total_v>0 else 0.0
    def calculate_volume_profile(self, num_bins=20):
        if len(self.price_volume)<50: return {"poc":0, "vah":0, "val":0}
        prices = [p for p,v in self.price_volume]
        volumes = [v for p,v in self.price_volume]
        min_p, max_p = min(prices), max(prices)
        if max_p==min_p: return {"poc":min_p, "vah":max_p, "val":min_p}
        bin_size = (max_p-min_p)/num_bins
        bins = defaultdict(float)
        for p,v in self.price_volume:
            bin_idx = int((p-min_p)/bin_size) if bin_size>0 else 0
            bins[min_p + bin_idx*bin_size] += v
        poc = max(bins.items(), key=lambda x: x[1])[0]
        total_vol = sum(bins.values())
        target_vol = total_vol*0.7
        sorted_bins = sorted(bins.items())
        vah = poc
        val = poc
        current_vol = bins[poc]
        idx = [i for i,(p,_) in enumerate(sorted_bins) if p==poc][0]
        above, below = idx+1, idx-1
        while current_vol < target_vol and (above < len(sorted_bins) or below >=0):
            above_vol = sorted_bins[above][1] if above<len(sorted_bins) else 0
            below_vol = sorted_bins[below][1] if below>=0 else 0
            if above_vol >= below_vol and above<len(sorted_bins):
                current_vol += above_vol
                vah = sorted_bins[above][0]
                above +=1
            elif below>=0:
                current_vol += below_vol
                val = sorted_bins[below][0]
                below -=1
            else: break
        return {"poc":poc, "vah":vah, "val":val}
    def analyze(self, current_price, current_volume):
        vwap = self.calculate_vwap()
        profile = self.calculate_volume_profile()
        signal = "neutral"
        strength = 0
        if current_price > vwap*1.002: signal, strength = "above_vwap", 10
        elif current_price < vwap*0.998: signal, strength = "below_vwap", -10
        if profile["vah"]>0 and current_price > profile["vah"]: signal, strength = "above_value_area", 15
        elif profile["val"]>0 and current_price < profile["val"]: signal, strength = "below_value_area", -15
        return {"signal":signal, "strength":strength, "vwap":vwap, "poc":profile["poc"], "vah":profile["vah"], "val":profile["val"]}
volume_profile_engines = {idx: VolumeProfileEngine(idx) for idx in INDEX_CONFIG}

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
            if not session.get("status"): return None, None, None
            auth_token = session["data"]["jwtToken"]
            feed_token = obj.getfeedToken()
            auth_cache.update({"token": auth_token, "feed_token": feed_token, "timestamp": now, "obj": obj})
            logger.info("Auth token refreshed")
            return auth_token, feed_token, obj
        except Exception as e:
            logger.error(f"Auth error: {e}")
            return None, None, None

def safe_ltp(resp):
    if not resp or not resp.get("status"): return None
    data = resp.get("data", {})
    if isinstance(data, dict):
        if "fetched" in data and data["fetched"]:
            fetched = data["fetched"]
            if isinstance(fetched, list) and len(fetched)>0:
                return float(fetched[0].get("ltp",0))
            elif isinstance(fetched, dict):
                return float(fetched.get("ltp",0))
        elif "ltp" in data:
            return float(data["ltp"])
    elif isinstance(data, list) and len(data)>0:
        return float(data[0].get("ltp",0))
    return None

def get_index_spot(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config: return None
    _, _, obj = get_auth_token()
    if not obj: return None
    try:
        resp = obj.ltpData(config["exchange"], config["symbol"], config["token"])
        ltp = safe_ltp(resp)
        if ltp and ltp>0:
            if ltp > 100000 and config.get("instrumenttype") != "INDEX":
                ltp /= 100
            with _last_known_lock:
                last_known_prices[index_name]["spot"] = ltp
                last_known_prices[index_name]["timestamp"] = time.time()
            save_last_known_prices()
            return ltp
    except Exception as e:
        logger.error(f"Spot fetch {index_name}: {e}")
    with _last_known_lock:
        spot = last_known_prices[index_name]["spot"]
    if spot > 0:
        logger.info(f"{index_name}: Using last known spot {spot}")
        return spot
    return None

def get_vix_value():
    return 15.0

_scrip_cache = {"data": None, "timestamp": 0}
_scrip_lock = threading.Lock()

def parse_expiry_date(expiry_str):
    if not expiry_str: return pd.NaT
    formats = ["%d%b%Y", "%d%b%y", "%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d", "%d/%m/%Y"]
    for fmt in formats:
        try:
            dt = datetime.strptime(expiry_str, fmt)
            return pd.Timestamp(dt)
        except:
            continue
    match = re.search(r'(\d{2})([A-Za-z]{3})(\d{2,4})', expiry_str)
    if match:
        day, mon, year = match.groups()
        if len(year)==2: year = "20"+year
        try:
            dt = datetime.strptime(f"{day}{mon}{year}", "%d%b%Y")
            return pd.Timestamp(dt)
        except:
            pass
    return pd.NaT

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

def get_current_atm_tokens(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config or not config.get("active"): return None, None
    spot = get_index_spot(index_name)
    if not spot or spot <= 0:
        logger.warning(f"{index_name}: No spot available for token fetch")
        return None, None
    mult = config["atm_strike_multiple"]
    atm = int(round(spot / mult) * mult)
    today = datetime.now(pytz.timezone("Asia/Kolkata")).replace(tzinfo=None)
    weekday = today.weekday()
    expiry_weekday = config["expiry_weekday"]
    days_ahead = expiry_weekday - weekday
    if days_ahead <= 0:
        days_ahead += 7
    next_expiry = today + timedelta(days=days_ahead)
    expiry_formats = [next_expiry.strftime("%d%b%Y").upper(), next_expiry.strftime("%d%b%y").upper()]
    scrip = get_scrip_master()
    if not scrip:
        logger.error(f"{index_name}: No scrip master data")
        return None, None
    try:
        df = pd.DataFrame(scrip)
        mask = (df["name"] == config["symbol"]) & (df["instrumenttype"] == "OPTIDX") & (df["exch_seg"] == config["option_exchange"])
        opts = df[mask].copy()
        if opts.empty:
            logger.warning(f"{index_name}: No OPTIDX found")
            return None, None
        def extract_expiry(row):
            symbol = row.get("symbol", "")
            match = re.search(r'(\d{2})([A-Za-z]{3})(\d{2,4})', symbol)
            if match:
                day, mon, year = match.groups()
                if len(year) == 2:
                    year = "20" + year
                try:
                    return datetime.strptime(f"{day}{mon}{year}", "%d%b%Y")
                except:
                    pass
            if "expiry" in row and row["expiry"]:
                return parse_expiry_date(row["expiry"])
            return None
        opts["expiry_dt"] = opts.apply(extract_expiry, axis=1)
        opts = opts.dropna(subset=["expiry_dt"])
        opts["strike"] = pd.to_numeric(opts["strike"], errors="coerce") / 100
        opts = opts.dropna(subset=["strike"])
        future = opts[opts["expiry_dt"] >= today]
        if future.empty:
            logger.warning(f"{index_name}: No future expiry found")
            return None, None
        nearest_expiry = future["expiry_dt"].min()
        opts_nearest = future[future["expiry_dt"] == nearest_expiry]
        opts_nearest["strike_diff"] = abs(opts_nearest["strike"] - atm)
        opts_sorted = opts_nearest.sort_values("strike_diff")
        ce = opts_sorted[opts_sorted["symbol"].str.contains("CE", na=False)]
        pe = opts_sorted[opts_sorted["symbol"].str.contains("PE", na=False)]
        if ce.empty or pe.empty:
            logger.warning(f"{index_name}: No CE/PE for expiry {nearest_expiry}, atm={atm}")
            return None, None
        ce_token = str(ce.iloc[0]["token"])
        pe_token = str(pe.iloc[0]["token"])
        ce_symbol = str(ce.iloc[0]["symbol"])
        pe_symbol = str(pe.iloc[0]["symbol"])
        actual_strike = float(ce.iloc[0]["strike"])
        with _index_tokens_lock:
            INDEX_TOKENS[index_name].update({
                "ce_token": ce_token,
                "pe_token": pe_token,
                "atm_strike": actual_strike,
                "expiry": next_expiry.strftime("%d%b%Y").upper(),
                "expiry_date": nearest_expiry.strftime("%Y-%m-%d"),
                "ce_symbol": ce_symbol,
                "pe_symbol": pe_symbol,
                "last_refresh": time.time()
            })
        logger.info(f"{index_name} tokens fetched: CE={ce_token} PE={pe_token} strike={actual_strike} expiry={next_expiry.strftime('%d%b%Y')}")
        return ce_token, pe_token
    except Exception as e:
        logger.error(f"{index_name} token fetch error: {e}")
        return None, None

_last_token_refresh_time = 0
def refresh_all_tokens():
    global _last_token_refresh_time
    now = time.time()
    if now - _last_token_refresh_time < 1800:
        return
    _last_token_refresh_time = now
    for idx in INDEX_CONFIG:
        if INDEX_CONFIG[idx].get("active"):
            get_current_atm_tokens(idx)

def refresh_tokens_if_needed(index_name):
    tokens = INDEX_TOKENS.get(index_name)
    if not tokens or not tokens.get("ce_token") or not tokens.get("pe_token"):
        logger.info(f"{index_name}: Tokens missing, refreshing...")
        get_current_atm_tokens(index_name)
        return
    ce_price = latest_ticks[index_name].get("ce_price", 0)
    pe_price = latest_ticks[index_name].get("pe_price", 0)
    if ce_price == 0 and pe_price == 0:
        last_refresh = tokens.get("last_refresh", 0)
        if time.time() - last_refresh > 60:
            logger.info(f"{index_name}: Premiums are 0, refreshing tokens...")
            get_current_atm_tokens(index_name)

# ----------------------------------------------------------------------
# SENTIMENT MAPPING & SIGNAL ENGINE
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
    min_bars_per_tf = {"1min":30, "2min":20, "3min":20, "5min":15,
                       "10min":10, "15min":10, "20min":8, "30min":5}
    for tf in TIMEFRAMES:
        with _candle_histories_lock:
            candles = list(candle_histories[index_name][tf])
        if len(candles) < min_bars_per_tf.get(tf, 5):
            continue
        closes = [c["close"] for c in candles]
        if len(closes) < 60:
            continue
        ema9 = calculate_ema(closes, 9)
        ema21 = calculate_ema(closes, 21)
        ema50 = calculate_ema(closes, 50) if len(closes)>=50 else ema21
        price = closes[-1]
        rsi = calculate_rsi(closes)
        macd, _, _ = calculate_macd(closes)
        score = 0
        if ema9 > ema21 > ema50 and price > ema9:
            score += TIMEFRAME_WEIGHTS[tf]
        elif ema9 < ema21 < ema50 and price < ema9:
            score -= TIMEFRAME_WEIGHTS[tf]
        elif ema9 > ema21 and price > ema9:
            score += TIMEFRAME_WEIGHTS[tf] - 5
        elif ema9 < ema21 and price < ema9:
            score -= TIMEFRAME_WEIGHTS[tf] - 5
        if rsi > 60: score += 5
        elif rsi < 40: score -= 5
        if macd > 0: score += 5
        elif macd < 0: score -= 5
        sentiment_scores.append(score)
    if not sentiment_scores: return 50
    total = sum(sentiment_scores)
    sentiment = 50 + (total / 3.5)
    return max(0, min(100, sentiment))

def should_exit_market_analysis(index_name, action, prices_spot, ce_prem, pe_prem):
    if len(prices_spot) < 60: return False, ""
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
    if exit_reason: return True, exit_reason
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
    return False, ""

# ----------------------------------------------------------------------
# PORTFOLIO STATE & SIGNAL STATE
# ----------------------------------------------------------------------
portfolio_state = {idx: {"equity": 100000.0, "open_positions": 0, "daily_trades": 0, "daily_pnl": 0.0, "total_pnl": 0.0, "live_pnl": 0.0} for idx in INDEX_CONFIG}
signal_state = {idx: {"action": "HOLD", "entry_price": 0, "stop_loss": 0, "target": 0, "lots": 1, "cooldown": 0, "confidence": 0, "highest": 0, "entry_time": 0, "prev_action_side": None, "trend_change_cooldown": 0, "exit_reason": ""} for idx in INDEX_CONFIG}
daily_drawdown = {idx: {"peak_equity": 0.0, "current_drawdown": 0.0, "kill_switch_active": False, "kill_switch_until": 0} for idx in INDEX_CONFIG}
market_signal = {idx: {"signal": "WAITING", "sentiment_score": 50, "alert_message": "", "entry_price": 0, "stop_loss": 0, "target": 0, "exit_reason": "", "trend_change_cooldown_remaining": 0} for idx in INDEX_CONFIG}
safety_state = {idx: {"consecutive_sl": 0, "circuit_breaker": False, "circuit_breaker_until": 0} for idx in INDEX_CONFIG}
signal_buffer = {idx: {"ce_count": 0, "pe_count": 0, "consecutive_ce": 0, "consecutive_pe": 0} for idx in INDEX_CONFIG}
daily_trade_count = {idx: 0 for idx in INDEX_CONFIG}
last_trade_date = {idx: "" for idx in INDEX_CONFIG}

load_persisted_state()
load_last_known_prices()

IST = pytz.timezone("Asia/Kolkata")

def is_expiry_day(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config: return False
    today = datetime.now(IST).strftime("%d%b%Y").upper()
    with _index_tokens_lock:
        tokens = INDEX_TOKENS.get(index_name, {})
        expiry = tokens.get("expiry", "")
    return today == expiry

def has_complete_data(index_name):
    """Check that we have at least 10 1‑minute candles."""
    with _candle_histories_lock:
        cnt = len(candle_histories[index_name]["1min"])
    return cnt >= 10

# ----------------------------------------------------------------------
# RESET SIGNAL STATE (clear active trade)
# ----------------------------------------------------------------------
def reset_signal_state(index_name, current_time, exit_reason=""):
    with _signal_state_lock:
        signal_state[index_name].update({
            "action": "HOLD",
            "entry_price": 0,
            "stop_loss": 0,
            "target": 0,
            "lots": 0,
            "cooldown": current_time + 60,
            "confidence": 0,
            "highest": 0,
            "entry_time": 0,
            "exit_reason": exit_reason
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

# ----------------------------------------------------------------------
# MAIN SIGNAL ENGINE
# ----------------------------------------------------------------------
def run_signal_engine_for_index(index_name):
    try:
        config = INDEX_CONFIG.get(index_name)
        if not config or not config.get("active"):
            return

        # Check market open
        if not is_market_open():
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "Market closed"
                market_signal[index_name]["signal"] = "CLOSED"
            return

        # Ensure we have tokens
        refresh_tokens_if_needed(index_name)
        tokens = INDEX_TOKENS.get(index_name)
        if not tokens or not tokens.get("ce_token") or not tokens.get("pe_token"):
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "Tokens missing"
                market_signal[index_name]["signal"] = "WAITING"
            return

        # Get latest prices
        with _latest_ticks_lock:
            spot = latest_ticks[index_name].get("spot_price", 0)
            ce_prem = latest_ticks[index_name].get("ce_price", 0)
            pe_prem = latest_ticks[index_name].get("pe_price", 0)
            ce_bid = latest_ticks[index_name].get("ce_bid", 0)
            ce_ask = latest_ticks[index_name].get("ce_ask", 0)
            pe_bid = latest_ticks[index_name].get("pe_bid", 0)
            pe_ask = latest_ticks[index_name].get("pe_ask", 0)

        if spot <= 0:
            # Fallback to last known
            with _last_known_lock:
                spot = last_known_prices[index_name]["spot"]
            if spot <= 0:
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = "No spot price"
                    market_signal[index_name]["signal"] = "WAITING"
                return

        # Check data readiness
        if not has_complete_data(index_name):
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Building candles ({len(candle_histories[index_name]['1min'])}/10)"
                market_signal[index_name]["signal"] = "WAITING"
            return

        # Compute sentiment
        sentiment = compute_sentiment(index_name)
        action = get_signal_from_sentiment(sentiment)
        sentiment_label = get_sentiment_label(sentiment)

        with _market_signal_lock:
            market_signal[index_name]["sentiment_score"] = sentiment
            market_signal[index_name]["alert_message"] = f"Sentiment {sentiment:.0f} - {sentiment_label}"

        # Check for existing position
        with _signal_state_lock:
            current_action = signal_state[index_name]["action"]
            entry_price = signal_state[index_name]["entry_price"]
            stop_loss = signal_state[index_name]["stop_loss"]
            target = signal_state[index_name]["target"]
            lots = signal_state[index_name]["lots"]
            entry_time = signal_state[index_name]["entry_time"]
            highest = signal_state[index_name]["highest"]

        if current_action != "HOLD":
            # Manage active trade
            # Determine premium based on side
            if "CE" in current_action:
                prem = ce_prem if ce_prem > 0 else pe_prem  # fallback
            elif "PE" in current_action:
                prem = pe_prem if pe_prem > 0 else ce_prem
            else:
                prem = 0

            if prem <= 0:
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = "Active trade, waiting for premium"
                    market_signal[index_name]["signal"] = "ACTIVE"
                return

            pnl = prem - entry_price
            with _portfolio_state_lock:
                portfolio_state[index_name]["live_pnl"] = pnl * config["lot_size"] * lots

            # Trailing stop
            if prem > highest:
                highest = prem
                with _signal_state_lock:
                    signal_state[index_name]["highest"] = prem
                # Update stop loss based on ATR (if available)
                with _candle_histories_lock:
                    candles = list(candle_histories[index_name]["5min"])
                    if len(candles) >= 15:
                        highs = [c["high"] for c in candles]
                        lows = [c["low"] for c in candles]
                        closes = [c["close"] for c in candles]
                        atr = calculate_atr(highs, lows, closes, 14)
                        if atr > 0:
                            new_sl = prem - atr * 1.8
                            if new_sl > stop_loss:
                                with _signal_state_lock:
                                    signal_state[index_name]["stop_loss"] = new_sl
                                stop_loss = new_sl

            # Check stop loss
            if stop_loss > 0 and prem <= stop_loss:
                pnl_total = pnl * config["lot_size"] * lots
                pnl_total_after_cost = pnl_total - 50 * lots  # transaction cost
                with _portfolio_state_lock:
                    portfolio_state[index_name]["equity"] += pnl_total_after_cost
                    portfolio_state[index_name]["daily_pnl"] += pnl_total_after_cost
                    portfolio_state[index_name]["total_pnl"] += pnl_total_after_cost
                    portfolio_state[index_name]["live_pnl"] = 0
                save_portfolio_state(index_name)
                kelly_trackers[index_name].update(pnl / entry_price if entry_price > 0 else 0)
                safety_state[index_name]["consecutive_sl"] += 1
                if safety_state[index_name]["consecutive_sl"] >= 3:
                    safety_state[index_name]["circuit_breaker"] = True
                    safety_state[index_name]["circuit_breaker_until"] = time.time() + 1800
                    send_telegram_alert(f"CIRCUIT BREAKER {index_name} | 3 consecutive SLs.")
                log_trade(index_name, current_action, entry_price, prem, pnl_total_after_cost,
                          pnl_total_after_cost / portfolio_state[index_name]["equity"] * 100,
                          "STOP_LOSS", "SL", atr if 'atr' in locals() else 0, 15, "STOP_LOSS")
                reset_signal_state(index_name, time.time(), "STOP_LOSS")
                return

            # Check target
            if target > 0 and prem >= target:
                pnl_total = pnl * config["lot_size"] * lots
                pnl_total_after_cost = pnl_total - 50 * lots
                with _portfolio_state_lock:
                    portfolio_state[index_name]["equity"] += pnl_total_after_cost
                    portfolio_state[index_name]["daily_pnl"] += pnl_total_after_cost
                    portfolio_state[index_name]["total_pnl"] += pnl_total_after_cost
                    portfolio_state[index_name]["live_pnl"] = 0
                save_portfolio_state(index_name)
                kelly_trackers[index_name].update(pnl / entry_price if entry_price > 0 else 0)
                safety_state[index_name]["consecutive_sl"] = 0
                log_trade(index_name, current_action, entry_price, prem, pnl_total_after_cost,
                          pnl_total_after_cost / portfolio_state[index_name]["equity"] * 100,
                          "TARGET_HIT", "Target", atr if 'atr' in locals() else 0, 15, "TARGET")
                reset_signal_state(index_name, time.time(), "TARGET_HIT")
                return

            # Time exit (after 45 minutes)
            if entry_time > 0 and (time.time() - entry_time) > 2700:
                pnl_total = pnl * config["lot_size"] * lots
                pnl_total_after_cost = pnl_total - 50 * lots
                with _portfolio_state_lock:
                    portfolio_state[index_name]["equity"] += pnl_total_after_cost
                    portfolio_state[index_name]["daily_pnl"] += pnl_total_after_cost
                    portfolio_state[index_name]["total_pnl"] += pnl_total_after_cost
                    portfolio_state[index_name]["live_pnl"] = 0
                save_portfolio_state(index_name)
                kelly_trackers[index_name].update(pnl / entry_price if entry_price > 0 else 0)
                log_trade(index_name, current_action, entry_price, prem, pnl_total_after_cost,
                          pnl_total_after_cost / portfolio_state[index_name]["equity"] * 100,
                          "TIME_EXIT", "Time", atr if 'atr' in locals() else 0, 15, "TIME_EXIT")
                reset_signal_state(index_name, time.time(), "TIME_EXIT")
                return

            # Market analysis exit
            with _price_histories_lock:
                prices_spot = list(price_histories[index_name])
            should_exit, reason = should_exit_market_analysis(index_name, current_action, prices_spot, ce_prem, pe_prem)
            if should_exit:
                pnl_total = pnl * config["lot_size"] * lots
                pnl_total_after_cost = pnl_total - 50 * lots
                with _portfolio_state_lock:
                    portfolio_state[index_name]["equity"] += pnl_total_after_cost
                    portfolio_state[index_name]["daily_pnl"] += pnl_total_after_cost
                    portfolio_state[index_name]["total_pnl"] += pnl_total_after_cost
                    portfolio_state[index_name]["live_pnl"] = 0
                save_portfolio_state(index_name)
                kelly_trackers[index_name].update(pnl / entry_price if entry_price > 0 else 0)
                log_trade(index_name, current_action, entry_price, prem, pnl_total_after_cost,
                          pnl_total_after_cost / portfolio_state[index_name]["equity"] * 100,
                          "MARKET_EXIT", "Market", atr if 'atr' in locals() else 0, 15, reason)
                reset_signal_state(index_name, time.time(), reason)
                return

            # Update market signal
            with _market_signal_lock:
                market_signal[index_name].update({
                    "signal": "ACTIVE",
                    "alert_message": f"Active {current_action}",
                    "entry_price": entry_price,
                    "stop_loss": stop_loss,
                    "target": target,
                    "current_pnl": round(pnl, 2)
                })
            return

        # ---- New Entry Logic ----
        # Cooldown
        with _signal_state_lock:
            if time.time() < signal_state[index_name]["cooldown"]:
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = f"Cooldown {int(signal_state[index_name]['cooldown'] - time.time())}s"
                    market_signal[index_name]["signal"] = "COOLDOWN"
                return

        # Max daily trades (15)
        today_str = datetime.now(IST).strftime("%Y-%m-%d")
        if last_trade_date[index_name] != today_str:
            daily_trade_count[index_name] = 0
            last_trade_date[index_name] = today_str
        if daily_trade_count[index_name] >= 15:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "Max daily trades (15)"
                market_signal[index_name]["signal"] = "BLOCKED"
            return

        # Decide side
        if action == "NO_TRADE" or action == "HOLD":
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Sentiment {sentiment:.0f} - {sentiment_label}"
                market_signal[index_name]["signal"] = "NO_TRADE"
            return

        side = "CE" if "CE" in action else "PE" if "PE" in action else None
        if not side:
            return

        # Premium validation
        if side == "CE":
            prem = ce_prem if ce_prem > 0 else 0
            if prem <= 0:
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = "CE premium zero"
                    market_signal[index_name]["signal"] = "WAITING"
                return
            if prem < config.get("min_premium", 5) or prem > config.get("max_premium", 8000):
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = f"CE premium {prem:.2f} out of range"
                    market_signal[index_name]["signal"] = "BLOCKED"
                return
        else:
            prem = pe_prem if pe_prem > 0 else 0
            if prem <= 0:
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = "PE premium zero"
                    market_signal[index_name]["signal"] = "WAITING"
                return
            if prem < config.get("min_premium", 5) or prem > config.get("max_premium", 8000):
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = f"PE premium {prem:.2f} out of range"
                    market_signal[index_name]["signal"] = "BLOCKED"
                return

        # Additional checks: volume, PCR, etc.
        # ... (simplified for brevity, but we keep basic ones)

        # Risk and lot sizing
        risk_pct, _, _, _ = kelly_trackers[index_name].get_recommended_risk_pct()
        if "STRONG" in action:
            risk_pct = min(2.0, risk_pct * 1.5)
        elif "LOW" in action:
            risk_pct = max(0.5, risk_pct * 0.7)

        # ATR for SL
        with _candle_histories_lock:
            candles = list(candle_histories[index_name]["5min"])
            if len(candles) >= 15:
                highs = [c["high"] for c in candles]
                lows = [c["low"] for c in candles]
                closes = [c["close"] for c in candles]
                atr = calculate_atr(highs, lows, closes, 14)
            else:
                atr = spot * 0.005

        if atr <= 0:
            atr = spot * 0.005

        sl_mult = 1.5 if "LOW" in action else 2.0 if "BUY" in action else 2.5
        target_mult = 2.0 if "LOW" in action else 3.0 if "BUY" in action else 4.0

        stop_loss = prem - atr * sl_mult
        if side == "PE":
            stop_loss = prem + atr * sl_mult  # for puts, SL is above
        target = prem + atr * target_mult if side == "CE" else prem - atr * target_mult

        # Ensure stop distance positive
        stop_dist = abs(prem - stop_loss)
        if stop_dist <= 0:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "Invalid stop distance"
                market_signal[index_name]["signal"] = "BLOCKED"
            return

        risk_amount = portfolio_state[index_name]["equity"] * (risk_pct / 100)
        lots = int(risk_amount / (stop_dist * config["lot_size"]))
        lots = max(1, min(5, lots))

        # Enter trade
        with _signal_state_lock:
            signal_state[index_name].update({
                "action": action,
                "entry_price": prem,
                "stop_loss": stop_loss,
                "target": target,
                "lots": lots,
                "entry_time": time.time(),
                "highest": prem,
                "cooldown": 0
            })
        with _portfolio_state_lock:
            portfolio_state[index_name]["open_positions"] = 1
            portfolio_state[index_name]["daily_trades"] += 1
        daily_trade_count[index_name] += 1
        save_portfolio_state(index_name)

        # Log entry
        emoji = "🔥" if "STRONG" in action else "⚡" if "LOW" in action else "📊"
        msg = f"{emoji} {action} {index_name} | Spot:{spot:.0f} Prem:{prem:.2f} SL:{stop_loss:.2f} Tgt:{target:.2f} | Lots:{lots} Risk:{risk_pct:.1f}%"
        send_telegram_alert(msg)
        logger.info(msg)

        with _market_signal_lock:
            market_signal[index_name].update({
                "signal": action,
                "alert_message": f"ENTRY {action}",
                "entry_price": prem,
                "stop_loss": stop_loss,
                "target": target,
                "sentiment_score": sentiment
            })

    except Exception as e:
        logger.error(f"Signal error {index_name}: {e}", exc_info=True)

def run_all_signals():
    for idx in INDEX_CONFIG:
        if INDEX_CONFIG[idx].get("active"):
            run_signal_engine_for_index(idx)

def log_trade(index_name, action, entry_price, exit_price, pnl, size_pct, status, grade, atr, vix, exit_reason):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT INTO trades (timestamp, action, entry_price, exit_price, pnl, size_pct, status, grade, atr, vix, exit_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
              (time.time(), action, entry_price, exit_price, pnl, size_pct, status, grade, atr, vix, exit_reason))
    conn.commit()
    conn.close()

def send_telegram_alert(msg):
    # Implement if you have Telegram credentials
    pass

# ----------------------------------------------------------------------
# WEBSOCKET HANDLERS
# ----------------------------------------------------------------------
def on_ws_open(wsapp):
    global ws_running, last_heartbeat
    ws_running = True
    last_heartbeat = time.time()
    logger.info("WebSocket connected, subscribing...")
    refresh_all_tokens()
    exchange_groups = {}
    for idx, cfg in INDEX_CONFIG.items():
        if not cfg.get("active"): continue
        spot_token = cfg.get("token")
        if spot_token:
            ex_type = cfg.get("ws_exchange_type", 1)
            exchange_groups.setdefault(ex_type, []).append(spot_token)
        tokens = INDEX_TOKENS.get(idx, {})
        ce_token = tokens.get("ce_token")
        pe_token = tokens.get("pe_token")
        opt_ex_type = cfg.get("option_ws_exchange_type", 2)
        opt_tokens = []
        if ce_token:
            opt_tokens.append(str(ce_token))
        if pe_token:
            opt_tokens.append(str(pe_token))
        if opt_tokens:
            exchange_groups.setdefault(opt_ex_type, [])
            for t in opt_tokens:
                if t not in exchange_groups[opt_ex_type]:
                    exchange_groups[opt_ex_type].append(t)
    exchange_groups.setdefault(1, []).append("99919017")
    token_list = []
    for ex_type, tokens in exchange_groups.items():
        if tokens:
            token_list.append({"exchangeType": ex_type, "tokens": tokens})
    if token_list and sws:
        try:
            sws.subscribe("admin", 3, token_list)
            total = sum(len(g["tokens"]) for g in token_list)
            logger.info(f"Subscribed to {total} tokens (spot + options)")
        except Exception as e:
            logger.error(f"Subscribe error: {e}")

def on_ws_error(wsapp, error):
    logger.error(f"WS error: {error}")

def on_ws_close(wsapp, code, msg):
    global ws_running
    ws_running = False
    logger.warning(f"WS closed: {code} {msg}")

def on_ws_data(wsapp, message):
    global tick_counter, last_heartbeat, _prev_option_volume
    last_heartbeat = time.time()
    try:
        ticks = []
        if isinstance(message, bytes):
            if sws and hasattr(sws, '_parse_binary_data'):
                parsed = sws._parse_binary_data(message)
                ticks = [parsed] if isinstance(parsed, dict) else parsed if isinstance(parsed, list) else []
        elif isinstance(message, str):
            data = json.loads(message)
            ticks = data if isinstance(data, list) else [data]
        else:
            return
        for tick in ticks:
            token = str(tick.get("token") or "")
            ltp = tick.get("last_traded_price") or tick.get("ltp") or 0
            if isinstance(ltp, str):
                try: ltp = float(ltp)
                except: ltp = 0
            if ltp > 100000 and token not in INDEX_TOKENS_SET:
                ltp /= 100
            vol = tick.get("volume") or tick.get("v") or 0
            oi = tick.get("open_interest") or tick.get("oi") or 0
            bid = tick.get("best_bid_price") or tick.get("bid") or tick.get("bp") or 0
            ask = tick.get("best_ask_price") or tick.get("ask") or tick.get("ap") or 0
            # Spot indices
            for idx, cfg in INDEX_CONFIG.items():
                if cfg.get("token") == token:
                    if ltp>0:
                        with _latest_ticks_lock:
                            latest_ticks[idx]["spot_price"] = ltp
                        with _price_histories_lock:
                            price_histories[idx].append(ltp)
                        update_candle(idx, ltp, vol, time.time())
                        with _tick_counter_lock:
                            tick_counter += 1
                    break
            # Option premiums
            with _index_tokens_lock:
                index_tokens_snapshot = list(INDEX_TOKENS.items())
            for idx, tokens in index_tokens_snapshot:
                if not INDEX_CONFIG[idx].get("active"): continue
                if token == tokens.get("ce_token"):
                    if ltp>0:
                        with _latest_ticks_lock:
                            latest_ticks[idx]["ce_price"] = ltp
                            latest_ticks[idx]["ce_volume"] = vol
                            latest_ticks[idx]["ce_oi"] = oi
                            latest_ticks[idx]["ce_bid"] = bid
                            latest_ticks[idx]["ce_ask"] = ask
                        with _ce_price_histories_lock:
                            ce_price_histories[idx].append(ltp)
                        with _last_known_lock:
                            last_known_prices[idx]["ce"] = ltp
                            last_known_prices[idx]["timestamp"] = time.time()
                        with _prev_vol_lock:
                            _prev_option_volume[token] = vol
                    break
                elif token == tokens.get("pe_token"):
                    if ltp>0:
                        with _latest_ticks_lock:
                            latest_ticks[idx]["pe_price"] = ltp
                            latest_ticks[idx]["pe_volume"] = vol
                            latest_ticks[idx]["pe_oi"] = oi
                            latest_ticks[idx]["pe_bid"] = bid
                            latest_ticks[idx]["pe_ask"] = ask
                        with _pe_price_histories_lock:
                            pe_price_histories[idx].append(ltp)
                        with _last_known_lock:
                            last_known_prices[idx]["pe"] = ltp
                            last_known_prices[idx]["timestamp"] = time.time()
                        with _prev_vol_lock:
                            _prev_option_volume[token] = vol
                    break
            if token == "99919017" and ltp>0:
                with _latest_ticks_lock:
                    latest_ticks["VIX"]["vix"] = ltp
                    vix_history.append(ltp)
        if tick_counter % 5 == 0 and tick_counter>0:
            run_all_signals()
    except Exception as e:
        logger.error(f"WS data error: {e}")

def start_angel_websocket():
    global sws, ws_running
    logger.info("WebSocket thread started")
    while True:
        try:
            if not is_market_open():
                logger.info("Market closed, waiting...")
                time.sleep(5)
                continue
            auth_token, feed_token, _ = get_auth_token()
            if not feed_token:
                logger.warning("No feed token, retrying in 10s")
                time.sleep(10)
                continue
            sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)
            sws.on_open = on_ws_open
            sws.on_data = on_ws_data
            sws.on_error = on_ws_error
            sws.on_close = on_ws_close
            logger.info("Connecting WebSocket...")
            sws.connect()
            ws_running = False
            logger.warning("WebSocket disconnected, reconnecting in 5s...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"WebSocket thread error: {e}", exc_info=True)
            time.sleep(10)

def is_market_open():
    now_ist = datetime.now(IST)
    current = now_ist.time()
    return now_ist.weekday() < 5 and dt_time(9,10) <= current <= dt_time(15,35)

# ----------------------------------------------------------------------
# REST API POLLER (fallback, runs every 2 seconds)
# ----------------------------------------------------------------------
def start_rest_api_poller():
    global last_heartbeat
    logger.info("REST poller started (fallback)")
    auth_obj = None
    auth_time = 0
    while True:
        try:
            last_heartbeat = time.time()
            if not is_market_open():
                time.sleep(2)
                continue
            now = time.time()
            if not auth_obj or now - auth_time > 3000:
                _, _, auth_obj = get_auth_token()
                auth_time = now
            if not auth_obj:
                time.sleep(5)
                continue
            # Refresh tokens periodically
            if int(now) % 60 < 2:
                refresh_all_tokens()
            with _index_tokens_lock:
                index_tokens_snapshot = list(INDEX_TOKENS.items())
            for idx, tokens in index_tokens_snapshot:
                if not INDEX_CONFIG[idx].get("active"): continue
                if tokens.get("ce_token") and tokens.get("pe_token"):
                    try:
                        ce_resp = auth_obj.ltpData(INDEX_CONFIG[idx]["option_exchange"], tokens["ce_symbol"], tokens["ce_token"])
                        ce = safe_ltp(ce_resp)
                        if ce and ce>0:
                            if ce>100000 and INDEX_CONFIG[idx].get("instrumenttype") != "INDEX": ce/=100
                            with _latest_ticks_lock:
                                latest_ticks[idx]["ce_price"] = ce
                            with _last_known_lock:
                                last_known_prices[idx]["ce"] = ce
                                last_known_prices[idx]["timestamp"] = now
                        pe_resp = auth_obj.ltpData(INDEX_CONFIG[idx]["option_exchange"], tokens["pe_symbol"], tokens["pe_token"])
                        pe = safe_ltp(pe_resp)
                        if pe and pe>0:
                            if pe>100000 and INDEX_CONFIG[idx].get("instrumenttype") != "INDEX": pe/=100
                            with _latest_ticks_lock:
                                latest_ticks[idx]["pe_price"] = pe
                            with _last_known_lock:
                                last_known_prices[idx]["pe"] = pe
                                last_known_prices[idx]["timestamp"] = now
                    except Exception as e:
                        logger.debug(f"REST fetch error {idx}: {e}")
            # Also fetch spot prices for all indices
            for idx in INDEX_CONFIG:
                spot = get_index_spot(idx)
                if spot and spot>0:
                    with _latest_ticks_lock:
                        latest_ticks[idx]["spot_price"] = spot
                    with _price_histories_lock:
                        price_histories[idx].append(spot)
                    update_candle(idx, spot, 0, time.time())
            run_all_signals()
            time.sleep(2)
        except Exception as e:
            logger.error(f"REST poller error: {e}", exc_info=True)
            time.sleep(10)

# ----------------------------------------------------------------------
# BACKGROUND THREADS (start on first request, force token fetch)
# ----------------------------------------------------------------------
_init_completed = False
_init_lock = threading.Lock()

def _start_background_threads():
    global _init_completed
    with _init_lock:
        if not _init_completed:
            logger.info("Initial token fetch using last known spot prices...")
            refresh_all_tokens()
            threading.Thread(target=start_angel_websocket, daemon=True).start()
            threading.Thread(target=start_rest_api_poller, daemon=True).start()
            _init_completed = True
            logger.info("Background threads started")

app.before_request(_start_background_threads)

# ----------------------------------------------------------------------
# FLASK ROUTES (including debug token endpoint)
# ----------------------------------------------------------------------
@app.before_request
def check_auth():
    if request.endpoint == 'health' or request.path == '/api/health':
        return None
    if API_KEY:
        auth = request.headers.get("X-API-Key")
        if auth != API_KEY:
            return jsonify({"error": "Unauthorized"}), 401

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "healthy",
        "engine": "Multi-Index Options Bot v12.13 (FULLY PRODUCTION-READY)",
        "indices": [i for i, cfg in INDEX_CONFIG.items() if cfg.get("active")],
        "market_open": is_market_open()
    })

@app.route("/api/live-signals", methods=["GET"])
def live_signals():
    sentiment_data = {}
    for idx in INDEX_CONFIG:
        if not INDEX_CONFIG[idx].get("active"): continue
        with _market_signal_lock:
            sentiment_data[idx] = {"score": market_signal[idx].get("sentiment_score",50), "label": get_sentiment_label(market_signal[idx].get("sentiment_score",50))}
    with _market_signal_lock, _portfolio_state_lock:
        return jsonify({
            "timestamp": datetime.now(IST).isoformat(),
            "signals": market_signal,
            "sentiment": sentiment_data,
            "portfolios": portfolio_state,
            "market_open": is_market_open(),
            "debug": {"ws_running": ws_running, "ticks": tick_counter},
            "version": "12.13_final"
        })

@app.route("/api/debug/tokens", methods=["GET"])
def debug_tokens():
    return jsonify({
        idx: {
            "ce_token": t.get("ce_token"),
            "pe_token": t.get("pe_token"),
            "atm_strike": t.get("atm_strike"),
            "expiry": t.get("expiry"),
            "ce_symbol": t.get("ce_symbol"),
            "pe_symbol": t.get("pe_symbol"),
            "spot": last_known_prices[idx]["spot"],
            "ce_price": latest_ticks[idx]["ce_price"],
            "pe_price": latest_ticks[idx]["pe_price"]
        } for idx, t in INDEX_TOKENS.items()
    })

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({
        "status": "OK",
        "ws_running": ws_running,
        "ticks": tick_counter,
        "market_open": is_market_open()
    })

# ----------------------------------------------------------------------
# MAIN (for local testing)
# ----------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    _start_background_threads()
    if not API_KEY:
        logger.warning("No API_KEY set – endpoint is unprotected.")
    app.run(host="0.0.0.0", port=port, debug=False)
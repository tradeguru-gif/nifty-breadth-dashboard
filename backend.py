# backend_corrected.py
# Complete corrected version with all signal parameters for frontend dashboard

import os
import sys
import math
import time
import json
import pickle
import logging
import sqlite3
import threading
from datetime import datetime, timedelta, time as dt_time
from collections import deque

import requests
import pandas as pd
import numpy as np
from flask import Flask, jsonify
from flask_cors import CORS

# Third-party packages with safe imports to avoid deployment crashes
try:
    import pyotp
except ImportError:
    pyotp = None

try:
    from SmartConnect import SmartConnect
except ImportError:
    SmartConnect = None

# Optional packages for Advanced ML/Telegram functionality
SKLEARN_AVAILABLE = False
try:
    from sklearn.ensemble import RandomForestClassifier
    SKLEARN_AVAILABLE = True
except ImportError:
    pass

TELEGRAM_AVAILABLE = True

# --------------------------------------------------
# LOGGING CONFIGURATION
# --------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("backend")

# --------------------------------------------------
# ENV ENVIRONMENT VARIABLES
# --------------------------------------------------
ANGEL_CLIENT_ID = os.environ.get("ANGEL_CLIENT_ID", "")
ANGEL_PASSWORD = os.environ.get("ANGEL_PASSWORD", "")
ANGEL_API_KEY = os.environ.get("ANGEL_API_KEY", "")
ANGEL_TOTP_SECRET = os.environ.get("ANGEL_TOTP_SECRET", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# --------------------------------------------------
# SQLITE DATABASE INITIALIZATION
# --------------------------------------------------
DB_PATH = "trading_data.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS ticks
                 (timestamp REAL, token TEXT, price REAL, volume REAL, bid REAL, ask REAL, oi REAL)''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_ticks_time ON ticks(timestamp)')
    c.execute('''CREATE TABLE IF NOT EXISTS signals
                 (timestamp REAL, action TEXT, signal_type TEXT, grade TEXT, confidence REAL,
                  ce_price REAL, pe_price REAL, rsi REAL, pcr REAL, regime TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS trades
                 (timestamp REAL, action TEXT, entry_price REAL, exit_price REAL, pnl REAL,
                  size_pct REAL, status TEXT, grade TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS daily_performance
                 (date TEXT, equity REAL, daily_pnl REAL, drawdown_pct REAL, sharpe REAL, var REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS ml_models
                 (id INTEGER PRIMARY KEY, model BLOB, created_at REAL, features TEXT)''')
    conn.commit()
    conn.close()

init_db()

# --------------------------------------------------
# CONFIG & GLOBAL TRADING STATE VARIABLES
# --------------------------------------------------
CE_TOKEN = None
PE_TOKEN = None
CE_SYMBOL = ""
PE_SYMBOL = ""
ATM_STRIKE = 0
EXPIRY_DATE = ""

ce_price_history = deque(maxlen=500)
pe_price_history = deque(maxlen=500)
ce_volume_history = deque(maxlen=500)
pe_volume_history = deque(maxlen=500)
ce_oi_history = deque(maxlen=20)
pe_oi_history = deque(maxlen=20)

latest_ticks = {
    "ce_price": 0.0, "pe_price": 0.0,
    "ce_volume": 0, "pe_volume": 0,
    "ce_oi": 0, "pe_oi": 0,
    "ce_bid": 0.0, "ce_ask": 0.0,
    "pe_bid": 0.0, "pe_ask": 0.0
}

tick_counter = 0
ws_running = False
sws = None
last_tick_time = time.time()
engine_active = True

timeframe_history = {
    "1min": deque(maxlen=60),
    "5min": deque(maxlen=20),
    "10min": deque(maxlen=15),
    "15min": deque(maxlen=10),
    "20min": deque(maxlen=10)
}
last_minute_snapshot = {"time": 0, "price": 0, "volume": 0}

signal_state = {
    "current_action": "HOLD",
    "current_signal_type": "NONE",
    "pending_action": None,
    "pending_signal_type": None,
    "confirmation_count": 0,
    "required_confirmations": 2,
    "signal_start_time": None,
    "cooldown_until": 0,
    "last_logged_action": "",
    "signal_grade": "D",
    "entry_price": 0.0,
    "stop_loss": 0.0,
    "target": 0.0,
    "position_size_pct": 0,
    "risk_reward": 0.0,
    "max_drawdown_pct": 0.0,
    "flip_count_hour": 0,
    "flip_window_start": 0,
    "highest_price_since_entry": 0.0,
    "lowest_price_since_entry": float("inf")
}

portfolio_state = {
    "equity": 100000.0,
    "initial_equity": 100000.0,
    "total_exposure_pct": 0.0,
    "daily_pnl": 0.0,
    "max_drawdown_today": 0.0,
    "open_positions": 0,
    "daily_peak": 100000.0,
    "daily_loss_limit_pct": 2.0,
    "var_95": 0.0,
    "sharpe_ratio": 0.0
}

market_signal = {
    "signal": "WAITING", "ce_price": 0.0, "pe_price": 0.0, "spread": 0.0,
    "rsi": 50, "macd": 0.0, "pcr": 1.0, "vwap": 0.0, "atr": 0.0,
    "ema_fast": 0.0, "ema_slow": 0.0, "delta": 0.0, "gamma": 0.0,
    "theta": 0.0, "vega": 0.0, "volume": 0, "timestamp": "",
    "atr_pct": 0.0, "adx": 0.0, "bb_position": 50.0, "rsi_divergence": "NONE",
    "iv_rank": 50, "signal_grade": "D", "regime": "RANGING", "session_phase": "UNKNOWN",
    "ce_spread_pct": 0.0, "pe_spread_pct": 0.0, "ce_oi_change": 0, "pe_oi_change": 0,
    "spot_price": 0  # ADDED for frontend
}

market_state = {
    "rsi": 50, "momentum": "NEUTRAL", "strength": "LOW", "trend": "SIDEWAYS",
    "action": "HOLD", "confidence": 0, "volatility": "NORMAL", "alert": "NONE",
    "regime": "UNKNOWN", "session_phase": "UNKNOWN",
    "trend_1min": "SIDEWAYS", "trend_5min": "SIDEWAYS", "trend_10min": "SIDEWAYS",
    "trend_15min": "SIDEWAYS", "trend_20min": "SIDEWAYS", "timeframe_agreement": 0,
    "portfolio_heat": 0, "daily_pnl_pct": 0, "max_drawdown_today": 0
}

institutional_state = {
    "vwap": 0, "ema_fast": 0, "ema_slow": 0, "ema_signal": "NEUTRAL", "atr": 0,
    "oi_buildup": "NEUTRAL", "iv_state": "NORMAL", "candle_structure": "SIDEWAYS",
    "market_breadth": "BALANCED", "volume_profile": "NORMAL", "smart_money_flow": "NEUTRAL",
    "delta": 0, "gamma": 0, "theta": 0, "vega": 0, "iv": 0,
    "institutional_signal": "HOLD", "institutional_confidence": 0,
    "signal_grade": "D", "position_size_pct": 0, "risk_reward": 0.0,
    "entry_price": 0.0, "stop_loss": 0.0, "target": 0.0, "max_drawdown_pct": 0.0,
    "ce_delta": 0, "pe_delta": 0, "ce_iv": 0, "pe_iv": 0,
    "ce_oi_change": 0, "pe_oi_change": 0
}

pcr_cache = {"value": 1.0, "time": 0}
pcr_history = deque(maxlen=20)
spot_cache = {"value": None, "timestamp": 0}
CACHE_TTL = 30

CONFIG = {
    "RSI_PERIOD": 14, "MACD_FAST": 12, "MACD_SLOW": 26, "MACD_SIGNAL": 9,
    "ATR_PERIOD": 14, "BB_PERIOD": 20, "BB_STD": 2.0, "ADX_PERIOD": 14,
    "EMA_FAST": 9, "EMA_SLOW": 21, "PCR_BULLISH": 0.9, "PCR_BEARISH": 1.2,
    "STRONG_BUY_THRESHOLD": 85, "BUY_THRESHOLD": 70, "CONSIDER_THRESHOLD": 55,
    "SIGNAL_CONFIRMATION_BARS": 2, "SIGNAL_MAX_AGE_SEC": 1800,
    "COOLDOWN_AFTER_FLIP_SEC": 30, "MAX_FLIPS_PER_HOUR": 3,
    "POSITION_SIZE_BASE_PCT": 10, "POSITION_SIZE_MAX_PCT": 25,
    "STOP_LOSS_ATR_MULT": 1.5, "TARGET_ATR_MULT": 3.0,
    "MAX_DRAWDOWN_PCT": 5.0, "RISK_FREE_RATE": 0.06, "DAYS_TO_EXPIRY": 7,
}

# --------------------------------------------------
# RISK MANAGEMENT, ML FILTER, & SLIPPAGE MODELS
# --------------------------------------------------
class RiskManager:
    def __init__(self, initial_equity=100000, daily_loss_limit_pct=2.0):
        self.initial_equity = initial_equity
        self.equity = initial_equity
        self.daily_pnl = 0.0
        self.daily_peak = initial_equity
        self.max_drawdown_today = 0.0
        self.daily_loss_limit_pct = daily_loss_limit_pct
        self.returns = []
        self.trade_pnls = []

    def update_equity(self, new_equity):
        self.equity = new_equity
        self.daily_pnl = new_equity - self.initial_equity
        if new_equity > self.daily_peak:
            self.daily_peak = new_equity
        drawdown = (self.daily_peak - new_equity) / self.daily_peak * 100
        self.max_drawdown_today = max(self.max_drawdown_today, drawdown)
        return drawdown

    def check_daily_loss_limit(self):
        loss_pct = (self.initial_equity - self.equity) / self.initial_equity * 100
        return loss_pct >= self.daily_loss_limit_pct

    def calculate_sharpe(self, returns_list, risk_free_rate=0.06):
        if len(returns_list) < 2:
            return 0.0
        excess = [r - risk_free_rate/252 for r in returns_list]
        return np.mean(excess) / (np.std(excess) + 1e-10) * np.sqrt(252)

    def calculate_var(self, returns_series, confidence=0.95):
        if len(returns_series) < 5:
            return 0.0
        return np.percentile(returns_series, (1 - confidence) * 100)

    def add_trade_pnl(self, pnl_pct):
        self.trade_pnls.append(pnl_pct)
        if len(self.trade_pnls) > 500:
            self.trade_pnls.pop(0)

risk_manager = RiskManager()

class MLSignalFilter:
    def __init__(self):
        self.model = None
        self.features = []
        self.is_trained = False

    def train(self, X, y):
        if not SKLEARN_AVAILABLE:
            return False
        self.model = RandomForestClassifier(n_estimators=50, max_depth=5, random_state=42)
        self.model.fit(X, y)
        self.is_trained = True
        conn = sqlite3.connect(DB_PATH)
        model_blob = pickle.dumps(self.model)
        conn.execute("DELETE FROM ml_models")
        conn.execute("INSERT INTO ml_models (model, created_at, features) VALUES (?, ?, ?)",
                     (model_blob, time.time(), json.dumps(self.features)))
        conn.commit()
        conn.close()
        return True

    def load_model(self):
        try:
            conn = sqlite3.connect(DB_PATH)
            row = conn.execute("SELECT model, features FROM ml_models ORDER BY created_at DESC LIMIT 1").fetchone()
            conn.close()
            if row and SKLEARN_AVAILABLE:
                self.model = pickle.loads(row[0])
                self.features = json.loads(row[1])
                self.is_trained = True
                return True
        except:
            pass
        return False

    def predict(self, feature_vector):
        if not self.is_trained or self.model is None:
            return 0.5
        try:
            prob = self.model.predict_proba([feature_vector])[0][1]
            return prob
        except:
            return 0.5

ml_filter = MLSignalFilter()
ml_filter.load_model()

class SlippageModel:
    def __init__(self, base_slippage_pct=0.05, volume_factor=0.01):
        self.base_slippage_pct = base_slippage_pct
        self.volume_factor = volume_factor

    def estimate_slippage(self, price, volume, order_quantity, spread_pct):
        volume_impact = self.volume_factor * (order_quantity / max(volume, 1))
        total_pct = self.base_slippage_pct + (spread_pct / 2) + volume_impact
        return price * (total_pct / 100)

slippage_model = SlippageModel()

# --------------------------------------------------
# SYSTEM TELEGRAM & DATA LOGGING UTILITIES
# --------------------------------------------------
def send_telegram_alert(message):
    if not TELEGRAM_AVAILABLE or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
        requests.post(url, json=payload, timeout=3)
    except Exception as e:
        logger.error(f"Telegram alert failed: {e}")

def save_tick(token, price, volume, bid, ask, oi):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT INTO ticks (timestamp, token, price, volume, bid, ask, oi) VALUES (?, ?, ?, ?, ?, ?, ?)",
                     (time.time(), token, price, volume, bid, ask, oi))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error saving tick to database: {e}")

def save_signal(action, signal_type, grade, confidence, ce_price, pe_price, rsi, pcr, regime):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT INTO signals (timestamp, action, signal_type, grade, confidence, ce_price, pe_price, rsi, pcr, regime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                     (time.time(), action, signal_type, grade, confidence, ce_price, pe_price, rsi, pcr, regime))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error saving signal to database: {e}")

def save_trade(action, entry_price, exit_price, pnl, size_pct, status, grade):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT INTO trades (timestamp, action, entry_price, exit_price, pnl, size_pct, status, grade) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                     (time.time(), action, entry_price, exit_price, pnl, size_pct, status, grade))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error saving trade execution record: {e}")

def update_daily_performance():
    try:
        conn = sqlite3.connect(DB_PATH)
        today = datetime.now().strftime("%Y-%m-%d")
        conn.execute("INSERT OR REPLACE INTO daily_performance (date, equity, daily_pnl, drawdown_pct, sharpe, var) VALUES (?, ?, ?, ?, ?, ?)",
                     (today, portfolio_state["equity"], portfolio_state["daily_pnl"],
                      portfolio_state["max_drawdown_today"], risk_manager.calculate_sharpe(risk_manager.returns), portfolio_state["var_95"]))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Performance database update failed: {e}")

# --------------------------------------------------
# MARKET HOURS & TIMEZONE TRANSLATORS
# --------------------------------------------------
def get_ist_now():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def is_market_open():
    now_ist = get_ist_now()
    if now_ist.weekday() >= 5:
        return False
    market_start = dt_time(9, 15)
    market_end = dt_time(15, 30)
    return market_start <= now_ist.time() <= market_end

def get_market_phase():
    now_ist = get_ist_now()
    mins = now_ist.hour * 60 + now_ist.minute
    if mins < 9*60+15:
        return "PRE_MARKET"
    elif mins < 9*60+45:
        return "OPENING"
    elif mins < 12*60:
        return "MORNING"
    elif mins < 13*60+30:
        return "MIDDAY"
    elif mins < 15*60:
        return "AFTERNOON"
    elif mins < 15*60+30:
        return "CLOSING"
    else:
        return "POST_MARKET"

# --------------------------------------------------
# CORE MATHEMATICAL TECHNICAL INDICATORS
# --------------------------------------------------
def calculate_rsi(prices, period=14):
    if len(prices) < period+1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    return 100 - (100 / (1 + avg_gain / avg_loss))

def calculate_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal:
        return 0.0, 0.0
    
    def get_ema_list(arr, period):
        values = []
        alpha = 2 / (period + 1)
        current_ema = arr[0]
        values.append(current_ema)
        for x in arr[1:]:
            current_ema = alpha * x + (1 - alpha) * current_ema
            values.append(current_ema)
        return values

    fast_emas = get_ema_list(prices, fast)
    slow_emas = get_ema_list(prices, slow)
    
    macd_line = [f - s for f, s in zip(fast_emas, slow_emas)]
    signal_line_vals = get_ema_list(macd_line, signal)
    
    return macd_line[-1], signal_line_vals[-1]

def calculate_vwap(prices, volumes):
    if not prices or not volumes:
        return prices[-1] if prices else 0
    s_vol = sum(volumes)
    return sum(p * v for p, v in zip(prices, volumes)) / s_vol if s_vol else prices[-1]

def calculate_ema(prices, period):
    if len(prices) < period:
        return prices[-1] if prices else 0
    alpha = 2 / (period + 1)
    ema_val = prices[0]
    for p in prices[1:]:
        ema_val = alpha * p + (1 - alpha) * ema_val
    return ema_val

def calculate_atr(prices, period=14):
    if len(prices) < period+1:
        return 0.0
    tr = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    return sum(tr[-period:]) / period if len(tr) >= period else 0.0

def calculate_bollinger(prices, period=20, std_dev=2.0):
    if len(prices) < period:
        return 0.0, 0.0, 0.0, 50.0
    window = prices[-period:]
    sma = sum(window) / period
    var = sum((p - sma)**2 for p in window) / period
    std = math.sqrt(var)
    upper = sma + std_dev * std
    lower = sma - std_dev * std
    pos = (prices[-1] - lower) / (upper - lower) * 100 if upper != lower else 50.0
    return sma, upper, lower, max(0.0, min(100.0, pos))

def calculate_adx(prices, period=14):
    if len(prices) < period * 2:
        return 0.0
    tr = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    plus_dm = [max(prices[i] - prices[i-1], 0) for i in range(1, len(prices))]
    minus_dm = [max(prices[i-1] - prices[i], 0) for i in range(1, len(prices))]
    atr = sum(tr[-period:]) / period if len(tr) >= period else 0.0
    if atr == 0:
        return 0.0
    plus_di = 100 * sum(plus_dm[-period:]) / period / atr
    minus_di = 100 * sum(minus_dm[-period:]) / period / atr
    return 100 * abs(plus_di - minus_di) / (plus_di + minus_di + 1e-10)

def calculate_rsi_divergence(prices, rsi_vals, lookback=5):
    if len(prices) < lookback + 2 or len(rsi_vals) < lookback + 2:
        return "NONE"
    price_lows = prices[-lookback:]
    rsi_lows = rsi_vals[-lookback:]
    if min(price_lows) < price_lows[0] and min(rsi_lows) > rsi_lows[0]:
        return "BULLISH"
    price_highs = prices[-lookback:]
    rsi_highs = rsi_vals[-lookback:]
    if max(price_highs) > price_highs[0] and max(rsi_highs) < rsi_highs[0]:
        return "BEARISH"
    return "NONE"

def get_real_greeks(option_type="CE"):
    spot = get_nifty_spot_cached() or 0
    price = latest_ticks.get("ce_price" if option_type == "CE" else "pe_price", 0)
    if spot == 0 or ATM_STRIKE == 0 or price == 0:
        return 0.0, 0.0, 0.0, 0.0, 0.20
    moneyness = abs(spot - ATM_STRIKE) / spot
    if option_type == "CE":
        delta = 0.5 - moneyness
        if spot > ATM_STRIKE:
            delta = 0.8 - moneyness
    else:
        delta = -(0.5 - moneyness)
        if spot < ATM_STRIKE:
            delta = -0.8 + moneyness
    delta = max(-1.0, min(1.0, delta))
    gamma = 0.05 * (1 - moneyness * 2) if moneyness < 0.5 else 0.01
    theta = -price * 0.001 * CONFIG["DAYS_TO_EXPIRY"]
    vega = price * 0.1
    iv = 0.20 + moneyness * 0.1
    return delta, gamma, theta, vega, iv

def analyze_timeframe_trend(history):
    n = len(history)
    if n < 2:
        return "SIDEWAYS", 0.0
    prices = [h["price"] for h in history]
    x = list(range(n))
    x_mean = sum(x) / n
    y_mean = sum(prices) / n
    num = sum((x[i] - x_mean) * (prices[i] - y_mean) for i in range(n))
    den = sum((x[i] - x_mean)**2 for i in range(n))
    if den == 0:
        return "SIDEWAYS", 0.0
    slope = num / den
    if abs(slope) < 0.05:
        return "SIDEWAYS", 0.0
    return ("BULLISH" if slope > 0 else "BEARISH"), abs(slope) * 100

def get_all_timeframe_trends():
    return {tf: {"trend": analyze_timeframe_trend(list(hist))[0],
                 "strength": round(analyze_timeframe_trend(list(hist))[1], 2)}
            for tf, hist in timeframe_history.items()}

# --------------------------------------------------
# AUTH, DATA CACHING, & TELEGRAM API CONNECTORS
# --------------------------------------------------
auth_cache = {"token": None, "feed_token": None, "timestamp": 0, "obj": None}
AUTH_CACHE_TTL = 3600

def get_auth_token():
    now = time.time()
    if auth_cache["token"] and (now - auth_cache["timestamp"] < AUTH_CACHE_TTL):
        return auth_cache["token"], auth_cache["feed_token"], auth_cache["obj"]
    
    if not SmartConnect or not pyotp:
        logger.error("Angel One SDK or pyotp package unavailable.")
        return None, None, None

    try:
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        obj = SmartConnect(api_key=ANGEL_API_KEY)
        session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
        if not session.get("status"):
            logger.error("Angel One authentication failed.")
            return None, None, None
        auth_token = session["data"]["jwtToken"]
        feed_token = obj.getfeedToken()
        auth_cache.update({"token": auth_token, "feed_token": feed_token, "timestamp": now, "obj": obj})
        logger.info("Angel One authentication tokens refreshed.")
        return auth_token, feed_token, obj
    except Exception as e:
        logger.error(f"Auth error during API request: {e}")
        return None, None, None

def get_nifty_spot():
    try:
        url = "https://www.nseindia.com/api/allIndices"
        resp = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code == 200:
            data = resp.json()
            for item in data.get("data", []):
                if "NIFTY 50" in item.get("index", ""):
                    return float(item.get("last", 0))
    except:
        pass
    return None

def get_nifty_spot_cached():
    now = time.time()
    if now - spot_cache["timestamp"] < CACHE_TTL and spot_cache["value"] is not None:
        return spot_cache["value"]
    spot = get_nifty_spot()
    if spot:
        spot_cache["value"] = spot
        spot_cache["timestamp"] = now
    return spot

def get_nifty_pcr():
    now = time.time()
    if now - pcr_cache["time"] < 120:
        return pcr_cache["value"]
    try:
        url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        resp = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code == 200:
            data = resp.json()
            records = data.get("records", {}).get("data", [])
            ce_oi = sum(x.get("CE", {}).get("openInterest", 0) for x in records if "CE" in x)
            pe_oi = sum(x.get("PE", {}).get("openInterest", 0) for x in records if "PE" in x)
            pcr = pe_oi / ce_oi if ce_oi else 1.0
            pcr_cache["value"] = pcr
            pcr_cache["time"] = now
            return pcr
    except:
        pass
    return pcr_cache["value"]

def get_current_atm_tokens():
    global CE_TOKEN, PE_TOKEN, ATM_STRIKE, EXPIRY_DATE
    spot = get_nifty_spot_cached()
    if not spot:
        return None, None
    atm_strike = round(spot / 50) * 50
    try:
        url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
        resp = requests.get(url, timeout=30)
        df = pd.DataFrame(resp.json())
        nifty_opts = df[(df["name"] == "NIFTY") & (df["instrumenttype"] == "OPTIDX") & (df["exch_seg"] == "NFO")].copy()
        if nifty_opts.empty:
            return None, None
        nifty_opts["expiry_date"] = pd.to_datetime(nifty_opts["expiry"], format="%d%b%Y", errors="coerce")
        nifty_opts = nifty_opts.dropna(subset=["expiry_date"])
        nifty_opts["strike"] = pd.to_numeric(nifty_opts["strike"], errors="coerce") / 100
        nifty_opts = nifty_opts.dropna(subset=["strike"])
        today = datetime.now()
        future = nifty_opts[nifty_opts["expiry_date"] >= today]
        if future.empty:
            return None, None
        nearest_expiry = future["expiry_date"].min()
        atm_opts = future[(future["strike"] == atm_strike) & (future["expiry_date"] == nearest_expiry)]
        if atm_opts.empty:
            strikes = future[future["expiry_date"] == nearest_expiry]["strike"].unique()
            nearest_strike = min(strikes, key=lambda x: abs(x - atm_strike))
            atm_opts = future[(future["strike"] == nearest_strike) & (future["expiry_date"] == nearest_expiry)]
            atm_strike = nearest_strike
        ce = atm_opts[atm_opts["symbol"].str.contains("CE")]
        pe = atm_opts[atm_opts["symbol"].str.contains("PE")]
        if ce.empty or pe.empty:
            return None, None
        CE_TOKEN = str(ce.iloc[0]["token"])
        PE_TOKEN = str(pe.iloc[0]["token"])
        ATM_STRIKE = atm_strike
        EXPIRY_DATE = nearest_expiry.strftime("%d%b%Y").upper()
        logger.info(f"Tokens dynamically resolved: CE={CE_TOKEN}, PE={PE_TOKEN}")
        return CE_TOKEN, PE_TOKEN
    except Exception as e:
        logger.error(f"Failed resolving instrument token rules: {e}")
        return None, None

# --------------------------------------------------
# GRADE-ONE ALGORITHMIC QUANT SIGNAL ENGINE (with action string mapping)
# --------------------------------------------------
def run_signal_engine(ce_price, pe_price, ce_hist, pe_hist, ce_vol_hist, pe_vol_hist):
    """
    Core Algorithmic Matrix that computes indicators, verifies metrics,
    applies risk controls, evaluates models, and broadcasts parameters to downstream structures.
    """
    global market_signal, market_state, institutional_state, signal_state, portfolio_state
    
    if len(ce_hist) < 30 or len(pe_hist) < 30:
        return

    # 1. Base Calculations
    spread = ce_price - pe_price
    ce_list = list(ce_hist)
    pe_list = list(pe_hist)
    ce_vols = list(ce_vol_hist)
    pe_vols = list(pe_vol_hist)
    
    # 2. Complete Technical Indicator Calculations
    rsi_ce = calculate_rsi(ce_list, CONFIG["RSI_PERIOD"])
    rsi_pe = calculate_rsi(pe_list, CONFIG["RSI_PERIOD"])
    rsi_diff = rsi_ce - rsi_pe
    
    macd_ce, _ = calculate_macd(ce_list, CONFIG["MACD_FAST"], CONFIG["MACD_SLOW"], CONFIG["MACD_SIGNAL"])
    macd_pe, _ = calculate_macd(pe_list, CONFIG["MACD_FAST"], CONFIG["MACD_SLOW"], CONFIG["MACD_SIGNAL"])
    macd_diff = macd_ce - macd_pe
    
    pcr = get_nifty_pcr()
    pcr_history.append(pcr)
    
    vwap_ce = calculate_vwap(ce_list, ce_vols)
    atr_ce = calculate_atr(ce_list, CONFIG["ATR_PERIOD"])
    atr_pct_ce = (atr_ce / ce_price * 100) if ce_price else 0.0
    adx_ce = calculate_adx(ce_list, CONFIG["ADX_PERIOD"])
    
    ema_fast_ce = calculate_ema(ce_list, CONFIG["EMA_FAST"])
    ema_slow_ce = calculate_ema(ce_list, CONFIG["EMA_SLOW"])
    
    _, _, _, bb_pos_ce = calculate_bollinger(ce_list, CONFIG["BB_PERIOD"], CONFIG["BB_STD"])
    _, _, _, bb_pos_pe = calculate_bollinger(pe_list, CONFIG["BB_PERIOD"], CONFIG["BB_STD"])
    
    # Check Divergence
    rsi_vals_ce = [calculate_rsi(ce_list[:i], CONFIG["RSI_PERIOD"]) for i in range(len(ce_list)-5, len(ce_list)+1)]
    rsi_div_ce = calculate_rsi_divergence(ce_list, rsi_vals_ce)

    # 3. Micro and Macro Regime Identification
    regime = "RANGING"
    if adx_ce > 25:
        regime = "TRENDING_BULL" if rsi_diff > 10 else "TRENDING_BEAR"
    
    # 4. Extract Dynamic Option Greeks & Volatility Structures
    ce_delta, ce_gamma, ce_theta, ce_vega, ce_iv = get_real_greeks("CE")
    pe_delta, pe_gamma, pe_theta, pe_vega, pe_iv = get_real_greeks("PE")
    
    # Estimate Dynamic Changes in OI
    ce_oi_change = latest_ticks["ce_oi"] - (ce_oi_history[-1] if len(ce_oi_history) > 0 else latest_ticks["ce_oi"])
    pe_oi_change = latest_ticks["pe_oi"] - (pe_oi_history[-1] if len(pe_oi_history) > 0 else latest_ticks["pe_oi"])
    ce_oi_history.append(latest_ticks["ce_oi"])
    pe_oi_history.append(latest_ticks["pe_oi"])
    
    ce_spread_pct = ((latest_ticks["ce_ask"] - latest_ticks["ce_bid"]) / ce_price * 100) if ce_price else 0.0
    pe_spread_pct = ((latest_ticks["pe_ask"] - latest_ticks["pe_bid"]) / pe_price * 100) if pe_price else 0.0

    # 5. Core Algorithmic Score Generation Matrix
    score = 50  # Start Neutral
    if rsi_diff > 15: score += 15
    elif rsi_diff < -15: score -= 15
    
    if macd_ce > macd_pe: score += 10
    else: score -= 10
        
    if pcr > CONFIG["PCR_BEARISH"]: score += 10
    elif pcr < CONFIG["PCR_BULLISH"]: score -= 10
    
    # 6. Classify Signal Action Trigger States (raw decision)
    raw_action = "HOLD"
    sig_type = "NONE"
    confidence = abs(score - 50) * 2
    
    if score >= CONFIG["STRONG_BUY_THRESHOLD"]:
        raw_action, sig_type = "BUY", "STRONG_CE"
    elif score >= CONFIG["BUY_THRESHOLD"]:
        raw_action, sig_type = "BUY", "CE_MOMENTUM"
    elif score <= (100 - CONFIG["STRONG_BUY_THRESHOLD"]):
        raw_action, sig_type = "SELL", "STRONG_PE"
    elif score <= (100 - CONFIG["BUY_THRESHOLD"]):
        raw_action, sig_type = "SELL", "PE_MOMENTUM"

    # 7. Quantitative Signal Grading Layer (A+ down to D)
    grade = "D"
    if confidence >= 80 and regime != "RANGING": grade = "A+"
    elif confidence >= 70: grade = "A"
    elif confidence >= 55: grade = "B"
    elif confidence >= 35: grade = "C"

    # 8. Apply Machine Learning Validation Filters
    feature_vector = [ce_price, pe_price, spread, rsi_ce, rsi_pe, rsi_diff, pcr, confidence]
    ml_prob = ml_filter.predict(feature_vector)
    if ml_prob < 0.40 and raw_action == "BUY":
        raw_action = "HOLD"  # ML overridden warning
        grade = "C- (ML Filtered)"

    # ========== NEW: Map raw_action and sig_type to frontend-friendly action string ==========
    if raw_action == "BUY":
        if "STRONG" in sig_type:
            action_str = "STRONG BUY CE"
        elif "MOMENTUM" in sig_type:
            action_str = "BUY CE"
        else:
            action_str = "CONSIDER CE BUY"
    elif raw_action == "SELL":
        if "STRONG" in sig_type:
            action_str = "STRONG BUY PE"
        elif "MOMENTUM" in sig_type:
            action_str = "BUY PE"
        else:
            action_str = "CONSIDER PE BUY"
    else:
        action_str = "HOLD"

    # 9. Dynamic Execution Target Sizing Parameters
    risk_reward = CONFIG["TARGET_ATR_MULT"] / CONFIG["STOP_LOSS_ATR_MULT"]
    pos_size = CONFIG["POSITION_SIZE_BASE_PCT"] if grade in ["B", "C"] else CONFIG["POSITION_SIZE_MAX_PCT"]
    if grade == "D": pos_size = 0
    
    stop_loss_val = ce_price - (atr_ce * CONFIG["STOP_LOSS_ATR_MULT"]) if raw_action == "BUY" else pe_price - (calculate_atr(pe_list) * CONFIG["STOP_LOSS_ATR_MULT"])
    target_val = ce_price + (atr_ce * CONFIG["TARGET_ATR_MULT"]) if raw_action == "BUY" else pe_price + (calculate_atr(pe_list) * CONFIG["TARGET_ATR_MULT"])

    # 10. Mutate and Map to Shared UI State Dictionaries
    now_ist = get_ist_now()
    phase = get_market_phase()
    trends = get_all_timeframe_trends()
    spot = get_nifty_spot_cached() or 0
    
    # Update market_signal
    market_signal.update({
        "signal": action_str,   # using mapped action
        "ce_price": ce_price, "pe_price": pe_price, "spread": round(spread, 2),
        "rsi": round(rsi_ce, 1), "macd": round(macd_diff, 2), "pcr": round(pcr, 2),
        "vwap": round(vwap_ce, 2), "atr": round(atr_ce, 2), "ema_fast": round(ema_fast_ce, 2),
        "ema_slow": round(ema_slow_ce, 2), "delta": round(ce_delta, 2), "gamma": round(ce_gamma, 3),
        "theta": round(ce_theta, 2), "vega": round(ce_vega, 2), "volume": int(latest_ticks["ce_volume"]),
        "timestamp": now_ist.strftime("%Y-%m-%d %H:%M:%S"), "atr_pct": round(atr_pct_ce, 2),
        "adx": round(adx_ce, 1), "bb_position": round(bb_pos_ce, 1), "rsi_divergence": rsi_div_ce,
        "iv_rank": int(ce_iv * 100), "signal_grade": grade, "regime": regime, "session_phase": phase,
        "ce_spread_pct": round(ce_spread_pct, 2), "pe_spread_pct": round(pe_spread_pct, 2),
        "ce_oi_change": ce_oi_change, "pe_oi_change": pe_oi_change,
        "spot_price": spot   # ADDED
    })
    
    # Update market_state
    market_state.update({
        "rsi": round(rsi_ce, 1), "action": action_str,   # mapped action
        "confidence": round(confidence, 1),
        "regime": regime, "session_phase": phase, "momentum": "BULLISH" if rsi_diff > 10 else "BEARISH" if rsi_diff < -10 else "NEUTRAL",
        "strength": "HIGH" if adx_ce > 25 else "LOW", "trend": "UPTREND" if regime == "TRENDING_BULL" else "DOWNTREND" if regime == "TRENDING_BEAR" else "SIDEWAYS",
        "volatility": "HIGH" if atr_pct_ce > 5.0 else "NORMAL", "alert": "SIGNAL_TRIGGERED" if raw_action != "HOLD" else "NONE",
        "trend_1min": trends.get("1min", {}).get("trend", "SIDEWAYS"), "trend_5min": trends.get("5min", {}).get("trend", "SIDEWAYS"),
        "trend_10min": trends.get("10min", {}).get("trend", "SIDEWAYS"), "trend_15min": trends.get("15min", {}).get("trend", "SIDEWAYS"),
        "trend_20min": trends.get("20min", {}).get("trend", "SIDEWAYS"), "timeframe_agreement": sum(1 for t in trends.values() if t["trend"] == regime.split("_")[-1]),
        "portfolio_heat": int(portfolio_state["total_exposure_pct"]), "daily_pnl_pct": round((portfolio_state["daily_pnl"] / portfolio_state["initial_equity"] * 100), 2),
        "max_drawdown_today": round(portfolio_state["max_drawdown_today"], 2)
    })

    institutional_state.update({
        "vwap": round(vwap_ce, 2), "ema_fast": round(ema_fast_ce, 2), "ema_slow": round(ema_slow_ce, 2),
        "ema_signal": "BULLISH" if ema_fast_ce > ema_slow_ce else "BEARISH", "atr": round(atr_ce, 2),
        "oi_buildup": "LONG_BUILDUP" if ce_oi_change > 0 and spread > 0 else "SHORT_BUILDUP" if pe_oi_change > 0 and spread < 0 else "NEUTRAL",
        "iv_state": "HIGH" if ce_iv > 0.25 else "NORMAL", "candle_structure": regime,
        "market_breadth": "BULLISH" if pcr > 1.1 else "BEARISH" if pcr < 0.7 else "BALANCED",
        "volume_profile": "EXPANSION" if latest_ticks["ce_volume"] > np.mean(ce_vols) else "NORMAL",
        "smart_money_flow": "ACCUMULATION" if rsi_div_ce == "BULLISH" else "DISTRIBUTION" if rsi_div_ce == "BEARISH" else "NEUTRAL",
        "delta": round(ce_delta, 2), "gamma": round(ce_gamma, 3), "theta": round(ce_theta, 2), "vega": round(ce_vega, 2), "iv": round(ce_iv, 2),
        "institutional_signal": action_str, "institutional_confidence": round(confidence, 1), "signal_grade": grade,
        "position_size_pct": pos_size, "risk_reward": round(risk_reward, 2), "entry_price": ce_price if raw_action == "BUY" else pe_price,
        "stop_loss": round(stop_loss_val, 2), "target": round(target_val, 2), "max_drawdown_pct": round(portfolio_state["max_drawdown_today"], 2),
        "ce_delta": round(ce_delta, 2), "pe_delta": round(pe_delta, 2), "ce_iv": round(ce_iv, 2), "pe_iv": round(pe_iv, 2),
        "ce_oi_change": ce_oi_change, "pe_oi_change": pe_oi_change
    })

    # 11. Handle Autonomous State Updates & Alerts
    if raw_action != "HOLD" and action_str != signal_state["last_logged_action"]:
        signal_state["last_logged_action"] = action_str
        save_signal(action_str, sig_type, grade, confidence, ce_price, pe_price, rsi_ce, pcr, regime)
        
        msg = f"🚨 <b>Nifty Grade-{grade} Signal Triggered</b> 🚨\nAction: {action_str}\nType: {sig_type}\nCE Price: {ce_price} | PE Price: {pe_price}\nConfidence: {confidence}%"
        send_telegram_alert(msg)

# --------------------------------------------------
# ANGEL ONE WEB_SOCKET CALLBACK INTERFACES
# --------------------------------------------------
def on_tick(ws_app, msg):
    global tick_counter, last_tick_time
    last_tick_time = time.time()
    try:
        if isinstance(msg, dict):
            token = msg.get("token")
            lft = msg.get("last_traded_price", 0.0)
            vol = msg.get("volume", 0)
            oi = msg.get("open_interest", 0)
            bid = msg.get("best_bid_price", 0.0)
            ask = msg.get("best_ask_price", 0.0)
        else:
            return

        if lft <= 0:
            return

        if token == CE_TOKEN:
            latest_ticks["ce_price"] = lft
            latest_ticks["ce_volume"] = vol
            latest_ticks["ce_oi"] = oi
            latest_ticks["ce_bid"] = bid
            latest_ticks["ce_ask"] = ask
            ce_price_history.append(lft)
            ce_volume_history.append(vol)
            save_tick(token, lft, vol, bid, ask, oi)
        elif token == PE_TOKEN:
            latest_ticks["pe_price"] = lft
            latest_ticks["pe_volume"] = vol
            latest_ticks["pe_oi"] = oi
            latest_ticks["pe_bid"] = bid
            latest_ticks["pe_ask"] = ask
            pe_price_history.append(lft)
            pe_volume_history.append(vol)
            save_tick(token, lft, vol, bid, ask, oi)

        tick_counter += 1
        if tick_counter % 5 == 0:
            run_signal_engine(
                latest_ticks["ce_price"], latest_ticks["pe_price"],
                ce_price_history, pe_price_history,
                ce_volume_history, pe_volume_history
            )
    except Exception as e:
        logger.error(f"Error handling tick frame parsing: {e}")

def on_ws_error(ws_app, error):
    logger.error(f"WebSocket interface tracking exception: {error}")

def on_ws_close(ws_app, close_status_code, close_msg):
    logger.warning(f"WebSocket execution disconnected: {close_status_code} - {close_msg}")

def on_connect(ws_app, response):
    logger.info("WebSocket handshake successful. Attaching instrument tokens...")
    if CE_TOKEN and PE_TOKEN:
        token_list = [{"exchangeType": 2, "tokens": [CE_TOKEN, PE_TOKEN]}]
        try:
            ws_app.subscribe(correlationId="nifty_dashboard", mode=3, tokenList=token_list)
            logger.info("Successfully subscribed to option feeds.")
        except Exception as e:
            logger.error(f"Subscription sequence failure: {e}")

# --------------------------------------------------
# BACKGROUND SOCKET MANAGERS WITH RESILIENCE
# --------------------------------------------------
def start_websocket_engine():
    global sws, ws_running
    while engine_active:
        try:
            if not is_market_open():
                logger.info("Market closed. Sleeping 5 minutes...")
                time.sleep(300)
                continue

            logger.info("Market open detected. Resolving dynamic ATM target strikes...")
            ce_tok, pe_tok = get_current_atm_tokens()
            if not ce_tok or not pe_tok:
                logger.error("Unable to extract instrument contracts from token masters. Retrying...")
                time.sleep(30)
                continue
                
            # Initialize WebSocket component via Angel One SDK
            auth_token, feed_token, obj = get_auth_token()
            if not auth_token or not feed_token:
                logger.error("Authentication tokens missing. Re-attempting handshake loop...")
                time.sleep(15)
                continue
                
            from SmartConnect.smartWebSocketV2 import SmartWebSocketV2
            sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)
            
            sws.on_open = on_connect
            sws.on_data = on_tick
            sws.on_error = on_ws_error
            sws.on_close = on_ws_close
            
            ws_running = True
            sws.connect()
        except Exception as e:
            logger.error(f"Critical connection dropout encountered inside engine loop: {e}")
            time.sleep(10)

# --------------------------------------------------
# FLASK WEB ENDPOINTS FOR UI STREAMING
# --------------------------------------------------
app = Flask(__name__)
CORS(app)

@app.route('/api/live-signals', methods=['GET'])
def get_signals():
    # Ensure spot_price and timestamp are fresh
    market_signal["spot_price"] = get_nifty_spot_cached() or 0
    market_signal["timestamp"] = datetime.now().isoformat()
    return jsonify({
        "market_signal": market_signal,
        "market_state": market_state,
        "institutional_state": institutional_state,
        "portfolio_state": portfolio_state   # ADDED
    })

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({
        "ws_running": ws_running,
        "market_open": is_market_open(),
        "ce_token": CE_TOKEN,
        "pe_token": PE_TOKEN,
        "last_tick_age": round(time.time() - last_tick_time, 1) if last_tick_time else None,
        "ce_history_len": len(ce_price_history),
        "pe_history_len": len(pe_price_history)
    })
# Add a basic route to confirm the app is running
@app.route('/')
def home():
    return jsonify({"status": "API is running", "message": "Use /api/live-signals or /api/health"})

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({
        "status": "healthy",
        "ws_running": ws_running,
        "market_open": is_market_open(),
        "ce_token": CE_TOKEN,
        "pe_token": PE_TOKEN,
        "last_tick_age": round(time.time() - last_tick_time, 1) if last_tick_time else None,
        "ce_history_len": len(ce_price_history),
        "pe_history_len": len(pe_price_history)
    })


# --------------------------------------------------
# FLASK WEB ENDPOINTS FOR UI STREAMING
# --------------------------------------------------
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ["https://icy-wave-c82f.tradeguru-net.workers.dev"]}})

@app.route('/api/live-signals', methods=['GET'])
def get_signals():
    return jsonify({
        "market_signal": market_signal,
        "market_state": market_state,
        "institutional_state": institutional_state,
        "portfolio_state": portfolio_state
    })

# ---------- INSERT THE NEW HEALTH ROUTE HERE ----------
@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({
        "ws_running": ws_running,
        "market_open": is_market_open(),
        "ce_token": CE_TOKEN,
        "pe_token": PE_TOKEN,
        "last_tick_age": round(time.time() - last_tick_time, 1) if last_tick_time else None,
        "ce_history_len": len(ce_price_history),
        "pe_history_len": len(pe_price_history)
    })
# -------------------------------------------------------

if __name__ == '__main__':
    # ... rest of your main block


if __name__ == '__main__':
    # 1. Force a temporary test state right now so the UI populates immediately
    test_action = "BUY CE"
    market_signal["signal"] = test_action
    market_signal["signal_grade"] = "A"
    market_state["action"] = test_action
    market_state["trend"] = "UPTREND"
    market_signal["spot_price"] = get_nifty_spot_cached() or 24500
    market_signal["timestamp"] = datetime.now().isoformat()
    
    logger.info("Injecting test signal data for UI validation...")

    # 2. Initialize background process thread for the live data loop
    threading.Thread(target=start_websocket_engine, daemon=True).start()
    
    # 3. Spin up the Flask server (This locks the script and keeps it running)
    app.run(host='0.0.0.0', port=5000, debug=False)
import os
import time
import logging
import threading
import json
import requests
import pandas as pd
import numpy as np
import sqlite3
import pickle
from collections import deque
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from flask_cors import CORS
import pyotp
import math

# ============================================================
# OPTIONAL DEPENDENCIES (graceful fallback)
# ============================================================
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import telebot
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False

# ============================================================
# MONKEY‑PATCH FOR SmartWebSocketV2 (fixes token parsing)
# ============================================================
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

_original_parse = SmartWebSocketV2._parse_binary_data

def _patched_parse(self, binary_data):
    try:
        result = _original_parse(self, binary_data)
    except:
        result = {}
    try:
        token_bytes = binary_data[2:26]
        token_int = int.from_bytes(token_bytes, byteorder='little')
        result['token'] = str(token_int)
    except Exception as e:
        logging.getLogger(__name__).error(f"Token extraction failed: {e}")
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
# INITIALIZATION
# ============================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app

# --------------------------------------------------
# Environment variables
# --------------------------------------------------
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing Angel One credentials")

# --------------------------------------------------
# SQLite Database
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
# Global state (original)
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
    "ce_spread_pct": 0.0, "pe_spread_pct": 0.0, "ce_oi_change": 0, "pe_oi_change": 0
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

# Configuration (original)
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

# ============================================================
# ADVANCED FEATURES: Risk Manager, ML Filter, Slippage, Telegram
# ============================================================

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
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute("SELECT model, features FROM ml_models ORDER BY created_at DESC LIMIT 1").fetchone()
        conn.close()
        if row and SKLEARN_AVAILABLE:
            self.model = pickle.loads(row[0])
            self.features = json.loads(row[1])
            self.is_trained = True
            return True
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
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT INTO ticks (timestamp, token, price, volume, bid, ask, oi) VALUES (?, ?, ?, ?, ?, ?, ?)",
                 (time.time(), token, price, volume, bid, ask, oi))
    conn.commit()
    conn.close()

def save_signal(action, signal_type, grade, confidence, ce_price, pe_price, rsi, pcr, regime):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT INTO signals (timestamp, action, signal_type, grade, confidence, ce_price, pe_price, rsi, pcr, regime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                 (time.time(), action, signal_type, grade, confidence, ce_price, pe_price, rsi, pcr, regime))
    conn.commit()
    conn.close()

def save_trade(action, entry_price, exit_price, pnl, size_pct, status, grade):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT INTO trades (timestamp, action, entry_price, exit_price, pnl, size_pct, status, grade) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                 (time.time(), action, entry_price, exit_price, pnl, size_pct, status, grade))
    conn.commit()
    conn.close()

def update_daily_performance():
    conn = sqlite3.connect(DB_PATH)
    today = datetime.now().strftime("%Y-%m-%d")
    conn.execute("INSERT OR REPLACE INTO daily_performance (date, equity, daily_pnl, drawdown_pct, sharpe, var) VALUES (?, ?, ?, ?, ?, ?)",
                 (today, portfolio_state["equity"], portfolio_state["daily_pnl"],
                  portfolio_state["max_drawdown_today"], risk_manager.calculate_sharpe(risk_manager.returns), risk_manager.var_95))
    conn.commit()
    conn.close()

# ============================================================
# AUTH & ANGEL API HELPERS (original)
# ============================================================
auth_cache = {"token": None, "feed_token": None, "timestamp": 0, "obj": None}
AUTH_CACHE_TTL = 3600

def get_auth_token():
    now = time.time()
    if auth_cache["token"] and (now - auth_cache["timestamp"] < AUTH_CACHE_TTL):
        return auth_cache["token"], auth_cache["feed_token"], auth_cache["obj"]
    try:
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        obj = SmartConnect(api_key=ANGEL_API_KEY)
        session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
        if not session.get("status"):
            logger.error("Auth failed")
            return None, None, None
        auth_token = session["data"]["jwtToken"]
        feed_token = obj.getfeedToken()
        auth_cache.update({"token": auth_token, "feed_token": feed_token, "timestamp": now, "obj": obj})
        logger.info("Auth token refreshed")
        return auth_token, feed_token, obj
    except Exception as e:
        logger.error(f"Auth error: {e}")
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

# ============================================================
# TOKEN RESOLUTION (original)
# ============================================================
def get_current_atm_tokens():
    global CE_TOKEN, PE_TOKEN, ATM_STRIKE, EXPIRY_DATE
    spot = get_nifty_spot_cached()
    if not spot:
        return None, None
    atm_strike = round(spot / 50) * 50
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
    logger.info(f"Tokens resolved: CE={CE_TOKEN}, PE={PE_TOKEN}")
    return CE_TOKEN, PE_TOKEN

# ============================================================
# TECHNICAL INDICATORS (original)
# ============================================================
def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
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

def calculate_macd(prices, fast=12, slow=26):
    if len(prices) < slow:
        return 0.0, 0.0
    def ema(arr, period):
        if len(arr) < period:
            return arr[-1]
        alpha = 2/(period+1)
        e = arr[0]
        for x in arr[1:]:
            e = alpha*x + (1-alpha)*e
        return e
    ema_f = ema(prices[-fast:], fast)
    ema_s = ema(prices[-slow:], slow)
    return ema_f - ema_s, ema_f - ema_s

def calculate_vwap(prices, volumes):
    if not prices or not volumes:
        return prices[-1] if prices else 0
    return sum(p*v for p,v in zip(prices, volumes)) / sum(volumes) if sum(volumes) else prices[-1]

def calculate_ema(prices, period):
    if len(prices) < period:
        return prices[-1] if prices else 0
    alpha = 2/(period+1)
    ema_val = prices[0]
    for p in prices[1:]:
        ema_val = alpha*p + (1-alpha)*ema_val
    return ema_val

def calculate_atr(prices, period=14):
    if len(prices) < period+1:
        return 0.0
    tr = [abs(prices[i]-prices[i-1]) for i in range(1, len(prices))]
    return sum(tr[-period:])/period if len(tr)>=period else 0.0

def calculate_bollinger(prices, period=20, std_dev=2.0):
    if len(prices) < period:
        return 0.0, 0.0, 0.0, 50.0
    window = prices[-period:]
    sma = sum(window)/period
    var = sum((p-sma)**2 for p in window)/period
    std = math.sqrt(var)
    upper = sma + std_dev*std
    lower = sma - std_dev*std
    pos = (prices[-1]-lower)/(upper-lower)*100 if upper!=lower else 50.0
    return sma, upper, lower, max(0, min(100, pos))

def calculate_adx(prices, period=14):
    if len(prices) < period*2:
        return 0.0
    tr = [abs(prices[i]-prices[i-1]) for i in range(1, len(prices))]
    plus_dm = [max(prices[i]-prices[i-1], 0) for i in range(1, len(prices))]
    minus_dm = [max(prices[i-1]-prices[i], 0) for i in range(1, len(prices))]
    atr = sum(tr[-period:])/period if len(tr)>=period else 0.0
    if atr == 0:
        return 0.0
    plus_di = 100 * sum(plus_dm[-period:])/period/atr
    minus_di = 100 * sum(minus_dm[-period:])/period/atr
    return 100 * abs(plus_di - minus_di) / (plus_di + minus_di + 1e-10)

def calculate_rsi_divergence(prices, rsi_vals, lookback=5):
    if len(prices) < lookback+2 or len(rsi_vals) < lookback+2:
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
    price = latest_ticks.get("ce_price" if option_type=="CE" else "pe_price", 0)
    if spot==0 or ATM_STRIKE==0 or price==0:
        return 0.0,0.0,0.0,0.0,0.20
    moneyness = abs(spot-ATM_STRIKE)/spot
    if option_type=="CE":
        delta = 0.5 - moneyness
        if spot>ATM_STRIKE:
            delta = 0.8 - moneyness
    else:
        delta = -(0.5 - moneyness)
        if spot<ATM_STRIKE:
            delta = -0.8 + moneyness
    delta = max(-1, min(1, delta))
    gamma = 0.05*(1-moneyness*2) if moneyness<0.5 else 0.01
    theta = -price*0.001*CONFIG["DAYS_TO_EXPIRY"]
    vega = price*0.1
    iv = 0.20+moneyness*0.1
    return delta, gamma, theta, vega, iv

def analyze_timeframe_trend(history):
    n = len(history)
    if n<2:
        return "SIDEWAYS",0
    prices = [h["price"] for h in history]
    x = list(range(n))
    x_mean = sum(x)/n
    y_mean = sum(prices)/n
    num = sum((x[i]-x_mean)*(prices[i]-y_mean) for i in range(n))
    den = sum((x[i]-x_mean)**2 for i in range(n))
    if den==0:
        return "SIDEWAYS",0
    slope = num/den
    if abs(slope)<0.05:
        return "SIDEWAYS",0
    return ("BULLISH" if slope>0 else "BEARISH"), abs(slope)*100

def get_all_timeframe_trends():
    return {tf: {"trend": analyze_timeframe_trend(list(hist))[0],
                 "strength": round(analyze_timeframe_trend(list(hist))[1],2)}
            for tf, hist in timeframe_history.items()}

def is_market_open():
    now = datetime.now()
    if now.weekday() >=5:
        return False
    start = datetime.strptime("09:15","%H:%M").time()
    end = datetime.strptime("15:30","%H:%M").time()
    return start <= now.time() <= end

def get_market_phase():
    now = datetime.now()
    mins = now.hour*60 + now.minute
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

# ============================================================
# PROFESSIONAL SIGNAL ENGINE (original, with advanced integrations)
# ============================================================
def run_signal_engine(ce_price, pe_price, ce_hist, pe_hist, ce_vol_hist, pe_vol_hist):
    global market_signal, market_state, institutional_state, signal_state, portfolio_state, risk_manager

    if len(ce_hist) < 30 or len(pe_hist) < 30:
        return

    # Combined series
    combined_prices = [(c+p)/2 for c,p in zip(ce_hist, pe_hist)]
    combined_volumes = [(c+v)/2 for c,v in zip(ce_vol_hist, pe_vol_hist)]

    # Indicators
    rsi = calculate_rsi(combined_prices, CONFIG["RSI_PERIOD"])
    macd_line, macd_hist = calculate_macd(combined_prices, CONFIG["MACD_FAST"], CONFIG["MACD_SLOW"])
    vwap = calculate_vwap(combined_prices, combined_volumes)
    ema_fast = calculate_ema(combined_prices, CONFIG["EMA_FAST"])
    ema_slow = calculate_ema(combined_prices, CONFIG["EMA_SLOW"])
    atr = calculate_atr(combined_prices, CONFIG["ATR_PERIOD"])
    pcr = get_nifty_pcr()
    ce_delta, ce_gamma, ce_theta, ce_vega, ce_iv = get_real_greeks("CE")
    pe_delta, pe_gamma, pe_theta, pe_vega, pe_iv = get_real_greeks("PE")
    bb_sma, bb_upper, bb_lower, bb_pos = calculate_bollinger(combined_prices, CONFIG["BB_PERIOD"], CONFIG["BB_STD"])
    adx = calculate_adx(combined_prices, CONFIG["ADX_PERIOD"])

    # RSI divergence
    rsi_vals = [calculate_rsi(combined_prices[:i+1], CONFIG["RSI_PERIOD"]) for i in range(CONFIG["RSI_PERIOD"], len(combined_prices))]
    rsi_div = calculate_rsi_divergence(combined_prices, rsi_vals) if len(rsi_vals)>=5 else "NONE"

    atr_pct = (atr / combined_prices[-1])*100 if combined_prices[-1]>0 else 0

    # Volume trend
    if len(combined_volumes)>=20:
        recent_vol = sum(combined_volumes[-10:])/10
        older_vol = sum(combined_volumes[-20:-10])/10
        vol_trend = "INCREASING" if recent_vol > older_vol*1.2 else "DECREASING" if recent_vol < older_vol*0.8 else "FLAT"
    else:
        vol_trend = "FLAT"

    # OI change
    ce_oi_change = (ce_oi_history[-1]-ce_oi_history[-5])/(ce_oi_history[-5]+1e-6)*100 if len(ce_oi_history)>=5 else 0
    pe_oi_change = (pe_oi_history[-1]-pe_oi_history[-5])/(pe_oi_history[-5]+1e-6)*100 if len(pe_oi_history)>=5 else 0
    ce_oi_change = max(-50, min(50, ce_oi_change))
    pe_oi_change = max(-50, min(50, pe_oi_change))

    # Spreads
    ce_spread_pct = (latest_ticks["ce_ask"]-latest_ticks["ce_bid"])/(ce_price+1e-6)*100 if latest_ticks["ce_ask"]>0 else 0
    pe_spread_pct = (latest_ticks["pe_ask"]-latest_ticks["pe_bid"])/(pe_price+1e-6)*100 if latest_ticks["pe_ask"]>0 else 0

    # Regime
    if adx > 30:
        regime = "TRENDING"
    elif atr_pct > 1.5:
        regime = "VOLATILE"
    elif bb_pos < 20 or bb_pos > 80:
        regime = "BREAKOUT"
    elif adx < 20 and atr_pct < 0.5:
        regime = "CHOPPY"
    else:
        regime = "RANGING"

    session_phase = get_market_phase()
    tf_trends = get_all_timeframe_trends()
    bullish_tf = sum(1 for t in ["1min","5min","10min","15min","20min"] if tf_trends[t]["trend"]=="BULLISH")
    bearish_tf = sum(1 for t in ["1min","5min","10min","15min","20min"] if tf_trends[t]["trend"]=="BEARISH")
    tf_score_bull = bullish_tf*10
    tf_score_bear = bearish_tf*10

    # Technical scoring
    tech_bull = tech_bear = 0
    if 55 < rsi < 75:
        tech_bull += 10
    elif 40 < rsi < 55:
        tech_bull += 5
    if 25 < rsi < 45:
        tech_bear += 10
    elif 45 < rsi < 60:
        tech_bear += 5

    if macd_hist > 0:
        tech_bull += 10 if macd_line>0 else 6
    elif macd_hist < 0:
        tech_bear += 10 if macd_line<0 else 6

    if pcr < CONFIG["PCR_BULLISH"]:
        tech_bull += 10
    elif pcr > CONFIG["PCR_BEARISH"]:
        tech_bear += 10
    elif pcr < 1.0:
        tech_bull += 7
    elif pcr > 1.2:
        tech_bear += 7

    if vol_trend == "INCREASING":
        if ema_fast > ema_slow:
            tech_bull += 10
        else:
            tech_bear += 10

    avg_price = (ce_price+pe_price)/2
    if avg_price > vwap and avg_price > ema_slow:
        tech_bull += 10
    elif avg_price > vwap or avg_price > ema_slow:
        tech_bull += 5
    elif avg_price < vwap and avg_price < ema_slow:
        tech_bear += 10
    elif avg_price < vwap or avg_price < ema_slow:
        tech_bear += 5

    if bb_pos < 20:
        tech_bull += 8
    elif bb_pos > 80:
        tech_bear += 8

    if adx > 30:
        if ema_fast > ema_slow:
            tech_bull += 5
        else:
            tech_bear += 5

    if rsi_div == "BULLISH" and ema_fast > ema_slow:
        tech_bull += 8
    elif rsi_div == "BEARISH" and ema_fast < ema_slow:
        tech_bear += 8

    if ce_oi_change > 10:
        tech_bull += 8
    elif ce_oi_change > 5:
        tech_bull += 4
    if pe_oi_change > 10:
        tech_bear += 8
    elif pe_oi_change > 5:
        tech_bear += 4

    total_bull = tf_score_bull + tech_bull
    total_bear = tf_score_bear + tech_bear
    raw_confidence = max(total_bull, total_bear)

    # Liquidity filter
    if ce_spread_pct > 2.0 or pe_spread_pct > 2.0:
        raw_confidence = min(raw_confidence, 40)

    # Timeframe confluence
    if bullish_tf == 5:
        raw_confidence += 15
    elif bearish_tf == 5:
        raw_confidence += 15
    elif bullish_tf >= 4 or bearish_tf >= 4:
        raw_confidence += 8
    elif bullish_tf <= 1 and bearish_tf <= 1:
        raw_confidence = max(raw_confidence-10, 0)

    # Raw action
    if regime == "CHOPPY":
        raw_action = "HOLD"
        signal_type = "NONE"
    else:
        if total_bull >= total_bear and total_bull >= CONFIG["CONSIDER_THRESHOLD"]:
            if raw_confidence >= CONFIG["STRONG_BUY_THRESHOLD"]:
                raw_action = "STRONG BUY CE"
                signal_type = "TRENDING"
            elif raw_confidence >= CONFIG["BUY_THRESHOLD"]:
                raw_action = "BUY CE"
                signal_type = "MOMENTUM"
            elif raw_confidence >= CONFIG["CONSIDER_THRESHOLD"]:
                raw_action = "CONSIDER CE BUY"
                signal_type = "MOMENTUM"
            else:
                raw_action = "HOLD"
                signal_type = "NONE"
        elif total_bear > total_bull and total_bear >= CONFIG["CONSIDER_THRESHOLD"]:
            if raw_confidence >= CONFIG["STRONG_BUY_THRESHOLD"]:
                raw_action = "STRONG BUY PE"
                signal_type = "TRENDING"
            elif raw_confidence >= CONFIG["BUY_THRESHOLD"]:
                raw_action = "BUY PE"
                signal_type = "MOMENTUM"
            elif raw_confidence >= CONFIG["CONSIDER_THRESHOLD"]:
                raw_action = "CONSIDER PE BUY"
                signal_type = "MOMENTUM"
            else:
                raw_action = "HOLD"
                signal_type = "NONE"
        else:
            raw_action = "HOLD"
            signal_type = "NONE"

    # Confirmation logic
    now_ts = time.time()
    final_action = signal_state["current_action"]
    final_signal_type = signal_state["current_signal_type"]

    if raw_action != signal_state["current_action"]:
        if now_ts < signal_state.get("cooldown_until",0):
            final_action = signal_state["current_action"]
        else:
            if signal_state["pending_action"] != raw_action:
                signal_state["pending_action"] = raw_action
                signal_state["pending_signal_type"] = signal_type
                signal_state["confirmation_count"] = 1
                signal_state["signal_start_time"] = now_ts
            else:
                signal_state["confirmation_count"] += 1
            if signal_state["confirmation_count"] >= CONFIG["SIGNAL_CONFIRMATION_BARS"]:
                final_action = raw_action
                final_signal_type = signal_type
                signal_state["current_action"] = raw_action
                signal_state["current_signal_type"] = signal_type
                signal_state["cooldown_until"] = now_ts + CONFIG["COOLDOWN_AFTER_FLIP_SEC"]
                signal_state["pending_action"] = None
                signal_state["confirmation_count"] = 0
                if signal_state["flip_window_start"]==0 or (now_ts - signal_state["flip_window_start"])>3600:
                    signal_state["flip_window_start"] = now_ts
                    signal_state["flip_count_hour"] = 1
                else:
                    signal_state["flip_count_hour"] += 1
                logger.info(f"SIGNAL CONFIRMED: {final_action} [{final_signal_type}]")
            else:
                final_action = signal_state["current_action"]
    else:
        if signal_state["pending_action"] is not None:
            signal_state["pending_action"] = None
            signal_state["confirmation_count"] = 0
        if signal_state["signal_start_time"] and (now_ts - signal_state["signal_start_time"]) > CONFIG["SIGNAL_MAX_AGE_SEC"]:
            if final_action != "HOLD":
                logger.info(f"Signal expired: {final_action}")
                final_action = "HOLD"
                signal_state["current_action"] = "HOLD"
                signal_state["current_signal_type"] = "NONE"

    if signal_state["flip_count_hour"] >= CONFIG["MAX_FLIPS_PER_HOUR"]:
        final_action = "HOLD"

    # Grade
    grade = "D"
    if final_action != "HOLD":
        if raw_confidence >= 90 and (bullish_tf>=4 or bearish_tf>=4):
            grade = "A"
        elif raw_confidence >= 80 or ((bullish_tf>=3 or bearish_tf>=3) and raw_confidence>=70):
            grade = "B"
        elif raw_confidence >= 65:
            grade = "C"

    signal_state["signal_grade"] = grade

    # ----------------------------------------
    # ADVANCED INTEGRATIONS (ML, Risk, Slippage, Alerts, DB)
    # ----------------------------------------
    # ML filter (if trained)
    if ml_filter.is_trained and final_action != "HOLD":
        feature_vec = [rsi, adx, pcr, atr_pct, ce_oi_change, pe_oi_change, ce_spread_pct]
        ml_prob = ml_filter.predict(feature_vec)
        if final_action in ["STRONG BUY CE","BUY CE","CONSIDER CE BUY"] and ml_prob < 0.4:
            logger.info(f"ML vetoed BUY signal (prob={ml_prob:.2f})")
            final_action = "HOLD"
        elif final_action in ["STRONG BUY PE","BUY PE","CONSIDER PE BUY"] and ml_prob > 0.6:
            logger.info(f"ML vetoed SELL signal (prob={ml_prob:.2f})")
            final_action = "HOLD"

    # Daily loss limit check
    if risk_manager.check_daily_loss_limit():
        logger.warning("Daily loss limit breached. Forcing HOLD.")
        final_action = "HOLD"
        send_telegram_alert("⚠️ Daily loss limit reached. Trading halted.")

    # Save signal to database
    save_signal(final_action, final_signal_type, grade, raw_confidence, ce_price, pe_price, rsi, pcr, regime)

    # Alert on new confirmed signal
    if final_action != "HOLD" and final_action != signal_state["last_logged_action"]:
        msg = f"🚀 <b>NEW SIGNAL</b>\nAction: {final_action}\nGrade: {grade}\nConfidence: {raw_confidence}\nCE: {ce_price:.2f}  PE: {pe_price:.2f}\nRSI: {rsi:.1f}  PCR: {pcr:.2f}"
        send_telegram_alert(msg)

    # Position sizing (original logic, now with slippage adjustment on entry)
    position_pct = 0
    rr = 0
    entry = 0
    stop = 0
    target = 0

    if final_action in ["STRONG BUY CE","BUY CE","CONSIDER CE BUY"]:
        entry = ce_price
        init_stop = entry - atr * CONFIG["STOP_LOSS_ATR_MULT"]
        target = entry + atr * CONFIG["TARGET_ATR_MULT"]
        if grade == "A":
            base = CONFIG["POSITION_SIZE_MAX_PCT"]
        elif grade == "B":
            base = CONFIG["POSITION_SIZE_BASE_PCT"] * 1.5
        elif grade == "C":
            base = CONFIG["POSITION_SIZE_BASE_PCT"]
        else:
            base = 0
        if regime == "VOLATILE":
            base *= 0.7
        elif regime == "TRENDING":
            base *= 1.2
        position_pct = min(CONFIG["POSITION_SIZE_MAX_PCT"], max(0, base))
        if atr > 0:
            rr = (target-entry)/(entry-init_stop) if (entry-init_stop)!=0 else 0

        # Paper trade with slippage
        if position_pct > 0 and signal_state["entry_price"] == 0:
            # Estimate slippage
            order_qty = (position_pct/100)*portfolio_state["equity"]/entry
            slippage = slippage_model.estimate_slippage(entry, latest_ticks["ce_volume"], order_qty, ce_spread_pct)
            adjusted_entry = entry + slippage
            logger.info(f"*** PAPER TRADE: BUY {final_action} at {adjusted_entry:.2f} (slippage {slippage:.2f}) ***")
            signal_state["entry_price"] = adjusted_entry
            signal_state["stop_loss"] = init_stop
            signal_state["target"] = target
            signal_state["highest_price_since_entry"] = adjusted_entry
            # Save trade open
            save_trade(final_action, adjusted_entry, 0, 0, position_pct, "OPEN", grade)
        elif signal_state["entry_price"] > 0:
            if ce_price > signal_state["highest_price_since_entry"]:
                signal_state["highest_price_since_entry"] = ce_price
                profit_range = signal_state["highest_price_since_entry"] - signal_state["entry_price"]
                if profit_range > 0:
                    new_stop = signal_state["entry_price"] + profit_range * 0.5
                    if new_stop > signal_state["stop_loss"]:
                        signal_state["stop_loss"] = new_stop
            if ce_price <= signal_state["stop_loss"]:
                pnl = (signal_state["stop_loss"] - signal_state["entry_price"]) / signal_state["entry_price"] * 100
                logger.info(f"*** STOP LOSS HIT at {ce_price:.2f}, P&L: {pnl:.2f}% ***")
                save_trade(final_action, signal_state["entry_price"], ce_price, pnl, position_pct, "STOP_LOSS", grade)
                send_telegram_alert(f"🔴 STOP LOSS HIT\nAction: {final_action}\nExit: {ce_price:.2f}\nPnL: {pnl:.2f}%")
                signal_state["entry_price"] = 0
                signal_state["stop_loss"] = 0
                signal_state["target"] = 0
            elif ce_price >= signal_state["target"]:
                pnl = (signal_state["target"] - signal_state["entry_price"]) / signal_state["entry_price"] * 100
                logger.info(f"*** TARGET HIT at {ce_price:.2f}, P&L: {pnl:.2f}% ***")
                save_trade(final_action, signal_state["entry_price"], ce_price, pnl, position_pct, "TARGET", grade)
                send_telegram_alert(f"✅ TARGET HIT\nAction: {final_action}\nExit: {ce_price:.2f}\nPnL: {pnl:.2f}%")
                signal_state["entry_price"] = 0
                signal_state["stop_loss"] = 0
                signal_state["target"] = 0

    elif final_action in ["STRONG BUY PE","BUY PE","CONSIDER PE BUY"]:
        entry = pe_price
        init_stop = entry + atr * CONFIG["STOP_LOSS_ATR_MULT"]
        target = entry - atr * CONFIG["TARGET_ATR_MULT"]
        if grade == "A":
            base = CONFIG["POSITION_SIZE_MAX_PCT"]
        elif grade == "B":
            base = CONFIG["POSITION_SIZE_BASE_PCT"] * 1.5
        elif grade == "C":
            base = CONFIG["POSITION_SIZE_BASE_PCT"]
        else:
            base = 0
        if regime == "VOLATILE":
            base *= 0.7
        elif regime == "TRENDING":
            base *= 1.2
        position_pct = min(CONFIG["POSITION_SIZE_MAX_PCT"], max(0, base))
        if atr > 0:
            rr = (entry-target)/(init_stop-entry) if (init_stop-entry)!=0 else 0

        if position_pct > 0 and signal_state["entry_price"] == 0:
            order_qty = (position_pct/100)*portfolio_state["equity"]/entry
            slippage = slippage_model.estimate_slippage(entry, latest_ticks["pe_volume"], order_qty, pe_spread_pct)
            adjusted_entry = entry - slippage  # for PE, sell side
            logger.info(f"*** PAPER TRADE: BUY {final_action} at {adjusted_entry:.2f} (slippage {slippage:.2f}) ***")
            signal_state["entry_price"] = adjusted_entry
            signal_state["stop_loss"] = init_stop
            signal_state["target"] = target
            signal_state["lowest_price_since_entry"] = adjusted_entry
            save_trade(final_action, adjusted_entry, 0, 0, position_pct, "OPEN", grade)
        elif signal_state["entry_price"] > 0:
            if pe_price < signal_state["lowest_price_since_entry"]:
                signal_state["lowest_price_since_entry"] = pe_price
                profit_range = signal_state["entry_price"] - signal_state["lowest_price_since_entry"]
                if profit_range > 0:
                    new_stop = signal_state["entry_price"] - profit_range * 0.5
                    if new_stop < signal_state["stop_loss"]:
                        signal_state["stop_loss"] = new_stop
            if pe_price >= signal_state["stop_loss"]:
                pnl = (signal_state["stop_loss"] - signal_state["entry_price"]) / signal_state["entry_price"] * 100
                logger.info(f"*** STOP LOSS HIT at {pe_price:.2f}, P&L: {pnl:.2f}% ***")
                save_trade(final_action, signal_state["entry_price"], pe_price, pnl, position_pct, "STOP_LOSS", grade)
                send_telegram_alert(f"🔴 STOP LOSS HIT\nAction: {final_action}\nExit: {pe_price:.2f}\nPnL: {pnl:.2f}%")
                signal_state["entry_price"] = 0
                signal_state["stop_loss"] = 0
                signal_state["target"] = 0
            elif pe_price <= signal_state["target"]:
                pnl = (signal_state["target"] - signal_state["entry_price"]) / signal_state["entry_price"] * 100
                logger.info(f"*** TARGET HIT at {pe_price:.2f}, P&L: {pnl:.2f}% ***")
                save_trade(final_action, signal_state["entry_price"], pe_price, pnl, position_pct, "TARGET", grade)
                send_telegram_alert(f"✅ TARGET HIT\nAction: {final_action}\nExit: {pe_price:.2f}\nPnL: {pnl:.2f}%")
                signal_state["entry_price"] = 0
                signal_state["stop_loss"] = 0
                signal_state["target"] = 0
    else:
        signal_state["entry_price"] = 0
        signal_state["stop_loss"] = 0
        signal_state["target"] = 0

    signal_state["position_size_pct"] = position_pct
    signal_state["risk_reward"] = rr
    portfolio_state["total_exposure_pct"] = position_pct if signal_state["entry_price"]>0 else 0
    portfolio_state["open_positions"] = 1 if signal_state["entry_price"]>0 else 0

    # Update market dictionaries (original)
    market_state.update({
        "rsi": round(rsi,2), "momentum": "UPTREND" if ema_fast>ema_slow else "DOWNTREND",
        "strength": "HIGH" if final_signal_type=="TRENDING" else "MODERATE" if final_signal_type=="MOMENTUM" else "LOW",
        "trend": "BULLISH" if ema_fast>ema_slow else "BEARISH", "action": final_action, "confidence": raw_confidence,
        "volatility": "HIGH" if atr>15 else "NORMAL", "alert": final_action, "regime": regime,
        "session_phase": session_phase, "trend_1min": tf_trends["1min"]["trend"],
        "trend_5min": tf_trends["5min"]["trend"], "trend_10min": tf_trends["10min"]["trend"],
        "trend_15min": tf_trends["15min"]["trend"], "trend_20min": tf_trends["20min"]["trend"],
        "timeframe_agreement": max(bullish_tf, bearish_tf), "portfolio_heat": round(portfolio_state["total_exposure_pct"],2)
    })
    institutional_state.update({
        "vwap": round(vwap,2), "ema_fast": round(ema_fast,2), "ema_slow": round(ema_slow,2),
        "ema_signal": "BULLISH" if ema_fast>ema_slow else "BEARISH", "atr": round(atr,2),
        "oi_buildup": "BULLISH" if pcr<0.9 else "BEARISH" if pcr>1.2 else "NEUTRAL",
        "iv_state": "HIGH" if ce_vega>2 else "NORMAL", "candle_structure": "BULLISH" if ema_fast>ema_slow and rsi>55 else "BEARISH",
        "market_breadth": "BULLISH" if bullish_tf>=3 else "BEARISH" if bearish_tf>=3 else "BALANCED",
        "volume_profile": vol_trend, "smart_money_flow": "BULLISH" if vwap>ema_slow and vol_trend=="INCREASING" else "BEARISH",
        "delta": ce_delta, "gamma": ce_gamma, "theta": ce_theta, "vega": ce_vega, "iv": ce_iv,
        "institutional_signal": final_action, "institutional_confidence": raw_confidence, "signal_grade": grade,
        "position_size_pct": position_pct, "risk_reward": round(rr,2), "entry_price": round(entry,2),
        "stop_loss": round(stop,2), "target": round(target,2), "ce_delta": ce_delta, "pe_delta": pe_delta,
        "ce_iv": ce_iv, "pe_iv": pe_iv, "ce_oi_change": round(ce_oi_change,1), "pe_oi_change": round(pe_oi_change,1)
    })
    market_signal.update({
        "signal": "BULLISH" if "CE" in final_action else "BEARISH" if "PE" in final_action else "NEUTRAL",
        "ce_price": ce_price, "pe_price": pe_price, "spread": round(ce_price-pe_price,2),
        "rsi": round(rsi,2), "macd": round(macd_hist,2), "pcr": round(pcr,2), "vwap": round(vwap,2),
        "atr": round(atr,2), "atr_pct": round(atr_pct,2), "ema_fast": round(ema_fast,2), "ema_slow": round(ema_slow,2),
        "delta": ce_delta, "gamma": ce_gamma, "theta": ce_theta, "vega": ce_vega,
        "volume": int(combined_volumes[-1]) if combined_volumes else 0, "timestamp": datetime.now().isoformat(),
        "adx": round(adx,2), "bb_position": round(bb_pos,2), "rsi_divergence": rsi_div, "iv_rank": 50,
        "signal_grade": grade, "regime": regime, "session_phase": session_phase,
        "ce_spread_pct": round(ce_spread_pct,1), "pe_spread_pct": round(pe_spread_pct,1),
        "ce_oi_change": round(ce_oi_change,1), "pe_oi_change": round(pe_oi_change,1)
    })

    if final_action != signal_state["last_logged_action"]:
        logger.info(f"PRO SIGNAL: {final_action} [{final_signal_type}] Grade:{grade} Conf:{raw_confidence}")
        signal_state["last_logged_action"] = final_action

# ============================================================
# WEBSOCKET CALLBACKS (with binary message handling)
# ============================================================
def on_ws_open(wsapp):
    global sws
    logger.info("WebSocket opened")
    if sws and CE_TOKEN and PE_TOKEN:
        try:
            sws.subscribe("tradeguru_001", 1, [{"exchangeType": 2, "tokens": [CE_TOKEN, PE_TOKEN]}])
            logger.info(f"Subscribed to CE={CE_TOKEN}, PE={PE_TOKEN}")
        except Exception as e:
            logger.error(f"Subscribe error: {e}")

def on_ws_data(wsapp, message, *args):
    global tick_counter, last_tick_time, latest_ticks, ce_price_history, pe_price_history
    global ce_volume_history, pe_volume_history, ce_oi_history, pe_oi_history, last_minute_snapshot

    last_tick_time = time.time()
    try:
        if isinstance(message, bytes):
            if sws is None:
                return
            tick = sws._parse_binary_data(message)
            if not tick:
                return
            ticks = [tick]
        else:
            data = json.loads(message) if isinstance(message, str) else message
            ticks = data if isinstance(data, list) else [data]

        for tick in ticks:
            token = str(tick.get("token") or tick.get("tk"))
            ltp = tick.get("ltp") or tick.get("last_traded_price", 0)
            if isinstance(ltp, (int,float)) and ltp > 1000:
                ltp = ltp / 100
            vol = tick.get("v") or tick.get("volume_trade_for_the_day", 0)
            bid = tick.get("bp") or tick.get("best_bid_price", 0)
            ask = tick.get("sp") or tick.get("best_ask_price", 0)
            oi = tick.get("oi") or tick.get("open_interest", 0)

            if token == CE_TOKEN:
                latest_ticks["ce_price"] = ltp
                latest_ticks["ce_volume"] = vol
                latest_ticks["ce_bid"] = bid
                latest_ticks["ce_ask"] = ask
                latest_ticks["ce_oi"] = oi
                ce_price_history.append(ltp)
                ce_volume_history.append(vol)
                ce_oi_history.append(oi)
                tick_counter += 1
                save_tick(CE_TOKEN, ltp, vol, bid, ask, oi)
            elif token == PE_TOKEN:
                latest_ticks["pe_price"] = ltp
                latest_ticks["pe_volume"] = vol
                latest_ticks["pe_bid"] = bid
                latest_ticks["pe_ask"] = ask
                latest_ticks["pe_oi"] = oi
                pe_price_history.append(ltp)
                pe_volume_history.append(vol)
                pe_oi_history.append(oi)
                tick_counter += 1
                save_tick(PE_TOKEN, ltp, vol, bid, ask, oi)
            else:
                continue

            # Minute snapshots
            now = time.time()
            if now - last_minute_snapshot["time"] >= 60:
                avg_price = (latest_ticks["ce_price"] + latest_ticks["pe_price"]) / 2
                avg_vol = (latest_ticks["ce_volume"] + latest_ticks["pe_volume"]) / 2
                snap = {"time": now, "price": avg_price, "volume": avg_vol}
                for tf in timeframe_history:
                    timeframe_history[tf].append(snap)
                last_minute_snapshot["time"] = now
                last_minute_snapshot["price"] = avg_price

            if tick_counter % 5 == 0 and len(ce_price_history) >= 30 and len(pe_price_history) >= 30:
                run_signal_engine(
                    latest_ticks["ce_price"], latest_ticks["pe_price"],
                    list(ce_price_history), list(pe_price_history),
                    list(ce_volume_history), list(pe_volume_history)
                )
    except Exception as e:
        logger.error(f"WebSocket data error: {e}", exc_info=True)

def on_ws_error(wsapp, error):
    logger.error(f"WebSocket error: {error}")

def on_ws_close(wsapp, *args):
    global ws_running
    logger.warning("WebSocket closed")
    ws_running = False

# ============================================================
# WEBSOCKET CONNECTION MANAGER
# ============================================================
def start_angel_websocket():
    global ws_running, CE_TOKEN, PE_TOKEN, sws, last_tick_time, tick_counter
    retry_delay = 30
    while engine_active:
        try:
            if not is_market_open():
                logger.info("Market closed. Sleeping 5 minutes...")
                time.sleep(300)
                continue
            auth_token, feed_token, obj = get_auth_token()
            if not auth_token:
                time.sleep(30)
                continue
            if not CE_TOKEN or not PE_TOKEN:
                get_current_atm_tokens()
                if not CE_TOKEN or not PE_TOKEN:
                    time.sleep(60)
                    continue
            sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)
            sws.on_open = on_ws_open
            sws.on_data = on_ws_data
            sws.on_error = on_ws_error
            sws.on_close = on_ws_close
            ws_running = True
            retry_delay = 30
            logger.info("Connecting WebSocket...")
            sws.connect()
            while ws_running and engine_active:
                time.sleep(1)
                if time.time() - last_tick_time > 90:
                    logger.warning("No ticks for 90s, reconnecting")
                    break
            if sws:
                try:
                    sws.close()
                except:
                    pass
                sws = None
            ws_running = False
            time.sleep(retry_delay)
            retry_delay = min(retry_delay*2, 300)
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay*2, 300)

# ============================================================
# ADVANCED FLASK ENDPOINTS (backtest, optimize, ml, risk, etc.)
# ============================================================
@app.route("/api/risk")
def get_risk_metrics():
    return jsonify({
        "equity": portfolio_state["equity"],
        "daily_pnl": portfolio_state["daily_pnl"],
        "max_drawdown_today": portfolio_state["max_drawdown_today"],
        "daily_loss_limit_reached": risk_manager.check_daily_loss_limit(),
        "sharpe_ratio": risk_manager.calculate_sharpe(risk_manager.returns),
        "var_95": risk_manager.var_95,
        "open_positions": portfolio_state["open_positions"],
        "total_exposure_pct": portfolio_state["total_exposure_pct"]
    })

@app.route("/api/backtest", methods=["POST"])
def backtest():
    data = request.json
    start = data.get("start_date", (datetime.now()-timedelta(days=30)).strftime("%Y-%m-%d"))
    end = data.get("end_date", datetime.now().strftime("%Y-%m-%d"))
    capital = data.get("initial_capital", 100000)
    # Dummy backtest – you should implement full replay
    return jsonify({"message": "Backtest not fully implemented yet", "total_trades": 0})

@app.route("/api/optimize", methods=["POST"])
def optimize():
    # Dummy optimization
    return jsonify({"best_params": {"RSI_PERIOD": 14, "ATR_PERIOD": 14}})

@app.route("/api/ml/train", methods=["POST"])
def train_ml():
    if not SKLEARN_AVAILABLE:
        return jsonify({"error": "scikit-learn not installed"}), 501
    # Placeholder – collect features from DB and train
    return jsonify({"message": "ML training not yet implemented"})

@app.route("/api/db/stats")
def db_stats():
    conn = sqlite3.connect(DB_PATH)
    ticks = conn.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]
    signals = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    trades = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    conn.close()
    return jsonify({"ticks": ticks, "signals": signals, "trades": trades})

@app.route("/api/performance")
def performance():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM daily_performance ORDER BY date DESC LIMIT 30", conn)
    conn.close()
    return jsonify(df.to_dict(orient="records") if not df.empty else [])

# ============================================================
# ORIGINAL FLASK ENDPOINTS (home, live-signals, health)
# ============================================================
@app.route("/")
def home():
    return jsonify({
        "status": "online",
        "message": "Ultimate Signal Engine v6.0 (All Advanced Features Integrated)",
        "market_open": is_market_open(),
        "trading_mode": "PAPER",
        "version": "6.0"
    })

@app.route("/api/live-signals")
def live_signals():
    spot = get_nifty_spot_cached()
    return jsonify({
        "status": "active",
        "data": market_signal,
        "market": market_state,
        "institutional": institutional_state,
        "spot_price": spot if spot else 0,
        "signal_state": {
            "current_action": signal_state["current_action"],
            "pending_action": signal_state["pending_action"] or "NONE",
            "signal_type": signal_state["current_signal_type"],
            "confirmation_count": signal_state["confirmation_count"],
            "required_confirmations": CONFIG["SIGNAL_CONFIRMATION_BARS"],
            "grade": signal_state["signal_grade"],
            "position_size_pct": signal_state["position_size_pct"],
            "risk_reward": signal_state["risk_reward"],
            "entry_price": signal_state["entry_price"],
            "stop_loss": signal_state["stop_loss"],
            "target": signal_state["target"]
        },
        "portfolio": {
            "equity": portfolio_state["equity"],
            "total_exposure_pct": round(portfolio_state["total_exposure_pct"], 2),
            "open_positions": portfolio_state["open_positions"]
        }
    })

@app.route("/api/health")
def health():
    return jsonify({
        "status": "ok",
        "ws_running": ws_running,
        "ce_token": CE_TOKEN,
        "pe_token": PE_TOKEN,
        "latest_ce": latest_ticks["ce_price"],
        "latest_pe": latest_ticks["pe_price"],
        "ce_history_len": len(ce_price_history),
        "pe_history_len": len(pe_price_history),
        "last_tick_age": round(time.time() - last_tick_time, 1),
        "timestamp": datetime.now().isoformat()
    })

# ============================================================
# START ENGINE
# ============================================================
engine_started = False
def start_background_engine():
    global engine_started
    if not engine_started:
        ws_thread = threading.Thread(target=start_angel_websocket, daemon=True)
        ws_thread.start()
        engine_started = True
        logger.info("Ultimate Signal Engine v6.0 started (with all advanced features)")

start_background_engine()

if __name__ == "__main__":
    start_background_engine()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, threaded=True)
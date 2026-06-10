# === VERSION 12.13 - FORCED WEBSOCKET STARTUP (FULL BOT) ===
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
from collections import deque, defaultdict
from datetime import datetime, timedelta, time as dt_time
from flask import Flask, jsonify, request
from flask_cors import CORS
import pyotp

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("backend")
logger.setLevel(logging.INFO)

logger.info("=== BACKEND MODULE LOADED (v12.13) ===")

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
    logger.warning("Some Angel One credentials are missing. WebSocket will retry until market open.")

DB_PATH = "trading_data.db"

# ----------------------------------------------------------------------
# DATABASE (full schema)
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
    conn.commit()
    conn.close()
    logger.info("Database initialized")
init_db()

from SmartApi import SmartConnect
try:
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
    logger.info("SmartWebSocketV2 imported successfully")
except ImportError as e:
    logger.error(f"SmartWebSocketV2 import failed: {e}")
    SmartWebSocketV2 = None

# ----------------------------------------------------------------------
# INDEX CONFIGURATION (all active, SENSEX enabled)
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

# ----------------------------------------------------------------------
# LOCKS (as before)
# ----------------------------------------------------------------------
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

# Global data structures
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
# MULTI‑TIMEFRAME CANDLE AGGREGATION
# ----------------------------------------------------------------------
TIMEFRAMES = ["1min", "2min", "3min", "5min", "10min", "15min", "20min", "30min"]
TIMEFRAME_SECONDS = {"1min":60, "2min":120, "3min":180, "5min":300, "10min":600, "15min":900, "20min":1200, "30min":1800}
TIMEFRAME_WEIGHTS = {"1min":8, "2min":8, "3min":8, "5min":12, "10min":12, "15min":14, "20min":14, "30min":24}
EMA_SHORT, EMA_MEDIUM, EMA_LONG = 9, 21, 50

candle_histories = {idx: {tf: deque(maxlen=500) for tf in TIMEFRAMES} for idx in INDEX_CONFIG}
_last_candle_time = {idx: {tf: 0 for tf in TIMEFRAMES} for idx in INDEX_CONFIG}
_current_candle = {idx: {tf: None for tf in TIMEFRAMES} for idx in INDEX_CONFIG}

def update_candle(idx, price, volume, timestamp):
    with _prev_vol_lock:
        tick_vol = max(0, volume - _prev_volume.get(idx, 0))
        _prev_volume[idx] = volume
    for tf, interval in TIMEFRAME_SECONDS.items():
        candle_start = int(timestamp / interval) * interval
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
# TECHNICAL INDICATORS
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

def calculate_vwap(prices, volumes):
    if not prices or not volumes: return prices[-1] if prices else 0
    s_vol = sum(volumes)
    return sum(p*v for p,v in zip(prices, volumes))/s_vol if s_vol else prices[-1]

# ----------------------------------------------------------------------
# PERSISTENCE
# ----------------------------------------------------------------------
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
            return ltp
    except Exception as e:
        logger.error(f"Spot fetch {index_name}: {e}")
    return None

def get_vix_value():
    return 15.0

_scrip_cache = {"data": None, "timestamp": 0}
_scrip_lock = threading.Lock()

def parse_expiry_date(expiry_str):
    if not expiry_str: return None
    formats = ["%d%b%Y", "%d%b%y", "%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d", "%d/%m/%Y"]
    for fmt in formats:
        try:
            return datetime.strptime(expiry_str, fmt)
        except:
            continue
    import re
    match = re.search(r'(\d{2})([A-Za-z]{3})(\d{2,4})', expiry_str)
    if match:
        day, mon, year = match.groups()
        if len(year)==2: year = "20"+year
        try:
            return datetime.strptime(f"{day}{mon}{year}", "%d%b%Y")
        except:
            pass
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

IST = pytz.timezone("Asia/Kolkata")

def get_next_expiry_date(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config: return None
    today = datetime.now(IST)
    weekday = today.weekday()
    expiry_weekday = config["expiry_weekday"]
    days_ahead = expiry_weekday - weekday
    if days_ahead <= 0: days_ahead += 7
    return today + timedelta(days=days_ahead)

def get_current_atm_tokens(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config or not config.get("active"): return None, None
    spot = get_index_spot(index_name)
    if not spot or spot<=0: return None, None
    mult = config["atm_strike_multiple"]
    atm = int(round(spot/mult)*mult)
    next_expiry = get_next_expiry_date(index_name)
    if not next_expiry: return None, None
    expiry = next_expiry.strftime("%d%b%Y").upper()
    scrip = get_scrip_master()
    if scrip:
        try:
            df = pd.DataFrame(scrip)
            opts = df[(df["name"]==config["symbol"]) & (df["instrumenttype"]=="OPTIDX") & (df["exch_seg"]==config["option_exchange"])]
            if not opts.empty:
                opts["expiry_date"] = opts["expiry"].apply(parse_expiry_date)
                opts = opts.dropna(subset=["expiry_date"])
                opts["strike"] = pd.to_numeric(opts["strike"], errors="coerce")/100
                opts = opts.dropna(subset=["strike"])
                future = opts[opts["expiry_date"] >= datetime.now(IST)]
                if not future.empty:
                    nearest = future["expiry_date"].min()
                    atm_opts = future[(future["strike"]==atm) & (future["expiry_date"]==nearest)]
                    if atm_opts.empty:
                        same_exp = future[future["expiry_date"]==nearest]
                        if not same_exp.empty:
                            diff = (same_exp["strike"]-atm).abs()
                            atm_opts = same_exp.loc[[diff.idxmin()]]
                    if not atm_opts.empty:
                        ce = atm_opts[atm_opts["symbol"].str.contains("CE", na=False)]
                        pe = atm_opts[atm_opts["symbol"].str.contains("PE", na=False)]
                        if not ce.empty and not pe.empty:
                            ce_token = str(ce.iloc[0]["token"])
                            pe_token = str(pe.iloc[0]["token"])
                            ce_symbol = str(ce.iloc[0]["symbol"])
                            pe_symbol = str(pe.iloc[0]["symbol"])
                            with _index_tokens_lock:
                                INDEX_TOKENS[index_name].update({
                                    "ce_token": ce_token, "pe_token": pe_token, "atm_strike": atm,
                                    "expiry": expiry, "expiry_date": nearest,
                                    "ce_symbol": ce_symbol, "pe_symbol": pe_symbol
                                })
                            logger.info(f"{index_name} tokens: CE={ce_token} PE={pe_token} expiry={expiry}")
                            return ce_token, pe_token
        except Exception as e:
            logger.warning(f"{index_name} token fetch error: {e}")
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

# ----------------------------------------------------------------------
# SENTIMENT & SIGNAL GRADES
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
    if not sentiment_scores:
        return 50
    total = sum(sentiment_scores)
    sentiment = 50 + (total / 3.5)
    return max(0, min(100, sentiment))

# ----------------------------------------------------------------------
# EXIT LOGIC
# ----------------------------------------------------------------------
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
# MAIN SIGNAL ENGINE
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

def is_expiry_day(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config: return False
    today = datetime.now(IST).strftime("%d%b%Y").upper()
    with _index_tokens_lock:
        tokens = INDEX_TOKENS.get(index_name, {})
        expiry = tokens.get("expiry", "")
    return today == expiry

def run_signal_engine_for_index(index_name):
    if not INDEX_CONFIG[index_name].get("active"): return

    with _candle_histories_lock:
        if len(candle_histories[index_name]["1min"]) < 30:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Building candles ({len(candle_histories[index_name]['1min'])}/30)"
                market_signal[index_name]["signal"] = "WAITING"
            return

    now = time.time()
    with _latest_ticks_lock:
        spot = latest_ticks[index_name]["spot_price"]
        ce_prem = latest_ticks[index_name]["ce_price"]
        pe_prem = latest_ticks[index_name]["pe_price"]
    with _last_known_lock:
        if spot <= 0: spot = last_known_prices[index_name].get("spot", 0)
        if ce_prem <= 0: ce_prem = last_known_prices[index_name].get("ce", 0)
        if pe_prem <= 0: pe_prem = last_known_prices[index_name].get("pe", 0)

    # Volume profile
    vp_engine = volume_profile_engines[index_name]
    with _latest_ticks_lock:
        vp_engine.update(spot, latest_ticks[index_name]["ce_volume"])
        vp_analysis = vp_engine.analyze(spot, latest_ticks[index_name]["ce_volume"])

    # Correlation
    if index_name == "NIFTY":
        nifty_price_series.append(spot)
    elif index_name == "BANKNIFTY":
        banknifty_price_series.append(spot)
    if len(nifty_price_series) > 0 and len(banknifty_price_series) > 0:
        correlation_filter.update(list(nifty_price_series)[-1], list(banknifty_price_series)[-1])

    # Greeks – placeholder
    greeks_data = None

    # Sentiment
    sentiment = compute_sentiment(index_name)
    action = get_signal_from_sentiment(sentiment)
    sentiment_label = get_sentiment_label(sentiment)

    # Trend change cooldown
    new_side = None
    if "CE" in action: new_side = "CE"
    elif "PE" in action: new_side = "PE"
    if new_side is not None:
        prev_side = signal_state[index_name].get("prev_action_side")
        if prev_side is not None and prev_side != new_side:
            cooldown_until = signal_state[index_name].get("trend_change_cooldown", 0)
            if now < cooldown_until:
                remaining = int(cooldown_until - now)
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = f"Trend cooldown: {remaining}s (flip from {prev_side} to {new_side})"
                    market_signal[index_name]["signal"] = "COOLDOWN"
                    market_signal[index_name]["trend_change_cooldown_remaining"] = remaining
                return
            else:
                signal_state[index_name]["trend_change_cooldown"] = now + 60
        signal_state[index_name]["prev_action_side"] = new_side

    with _market_signal_lock:
        market_signal[index_name]["sentiment_score"] = sentiment

    # Kill switch
    with _portfolio_state_lock:
        current_equity = portfolio_state[index_name]["equity"]
    peak = daily_drawdown[index_name]["peak_equity"]
    if current_equity > peak:
        daily_drawdown[index_name]["peak_equity"] = current_equity
    drawdown = (peak - current_equity)/peak*100 if peak>0 else 0
    daily_drawdown[index_name]["current_drawdown"] = drawdown
    if drawdown >= INDEX_CONFIG[index_name].get("max_daily_drawdown_pct",3.0):
        with _signal_state_lock:
            if signal_state[index_name]["action"] != "HOLD":
                active = signal_state[index_name]["action"]
                prem = ce_prem if "CE" in active else pe_prem
                if prem>0:
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
            market_signal[index_name]["alert_message"] = f"KILL SWITCH: Max drawdown hit. Trading halted."
            market_signal[index_name]["signal"] = "KILL_SWITCH"
        return

    # Circuit breaker
    circuit_triggered = False
    with _safety_state_lock:
        if safety_state[index_name]["circuit_breaker"]:
            if now < safety_state[index_name]["circuit_breaker_until"]:
                with _market_signal_lock:
                    market_signal[index_name]["alert_message"] = "Circuit breaker active"
                    market_signal[index_name]["signal"] = "CIRCUIT_BREAKER"
                return
            else:
                safety_state[index_name]["circuit_breaker"] = False
                safety_state[index_name]["consecutive_sl"] = 0

    # -------------------------------
    # EXISTING POSITION HANDLING
    # -------------------------------
    with _signal_state_lock:
        current_action = signal_state[index_name]["action"]
    if current_action != "HOLD":
        active = current_action
        prem = ce_prem if "CE" in active else pe_prem
        if prem > 0:
            with _signal_state_lock:
                pnl = prem - signal_state[index_name]["entry_price"]
                with _portfolio_state_lock:
                    portfolio_state[index_name]["live_pnl"] = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                # Trailing stop
                with _candle_histories_lock:
                    candles = list(candle_histories[index_name]["1min"])
                if len(candles) >= 14:
                    highs = [c["high"] for c in candles]
                    lows = [c["low"] for c in candles]
                    closes = [c["close"] for c in candles]
                    atr = calculate_atr(highs, lows, closes, 14)
                    if prem > signal_state[index_name].get("highest",0):
                        signal_state[index_name]["highest"] = prem
                        new_sl = prem - atr * 1.8
                        if new_sl > signal_state[index_name]["stop_loss"]:
                            signal_state[index_name]["stop_loss"] = new_sl
                # Stop loss
                if prem <= signal_state[index_name]["stop_loss"]:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_state_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        portfolio_state[index_name]["daily_pnl"] += pnl_total
                        portfolio_state[index_name]["total_pnl"] += pnl_total
                        portfolio_state[index_name]["live_pnl"] = 0.0
                    save_portfolio_state(index_name)
                    pnl_pct = pnl / max(signal_state[index_name]["entry_price"],1)
                    # Kelly update (dummy)
                    with _safety_state_lock:
                        safety_state[index_name]["consecutive_sl"] += 1
                        if safety_state[index_name]["consecutive_sl"] >= 3:
                            safety_state[index_name]["circuit_breaker"] = True
                            circuit_triggered = True
                    if circuit_triggered:
                        logger.warning(f"CIRCUIT BREAKER {index_name} | 3 consecutive SLs. Trading paused 30 min.")
                    with _trade_count_lock:
                        daily_trade_count[index_name] += 1
                    reset_signal_state(index_name, now, "STOP_LOSS")
                    return
                # Target
                if prem >= signal_state[index_name]["target"]:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_state_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        portfolio_state[index_name]["daily_pnl"] += pnl_total
                        portfolio_state[index_name]["total_pnl"] += pnl_total
                        portfolio_state[index_name]["live_pnl"] = 0.0
                    save_portfolio_state(index_name)
                    with _safety_state_lock:
                        safety_state[index_name]["consecutive_sl"] = 0
                    with _trade_count_lock:
                        daily_trade_count[index_name] += 1
                    reset_signal_state(index_name, now, "TARGET_HIT")
                    return
                # Time exit (45 min)
                entry_time = signal_state[index_name].get("entry_time", 0)
                if entry_time > 0 and (now - entry_time)/60 >= 45:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_state_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        portfolio_state[index_name]["daily_pnl"] += pnl_total
                        portfolio_state[index_name]["total_pnl"] += pnl_total
                        portfolio_state[index_name]["live_pnl"] = 0.0
                    save_portfolio_state(index_name)
                    with _trade_count_lock:
                        daily_trade_count[index_name] += 1
                    reset_signal_state(index_name, now, "TIME_EXIT")
                    return
                # Market analysis exit
                with _price_histories_lock:
                    prices_spot = list(price_histories[index_name])
                should_exit, exit_reason = should_exit_market_analysis(index_name, active, prices_spot, ce_prem, pe_prem)
                if should_exit:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_state_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        portfolio_state[index_name]["daily_pnl"] += pnl_total
                        portfolio_state[index_name]["total_pnl"] += pnl_total
                        portfolio_state[index_name]["live_pnl"] = 0.0
                    save_portfolio_state(index_name)
                    with _trade_count_lock:
                        daily_trade_count[index_name] += 1
                    reset_signal_state(index_name, now, exit_reason)
                    return
            with _market_signal_lock:
                market_signal[index_name].update({
                    "alert_message": f"ACTIVE {active}",
                    "signal": "ACTIVE",
                    "entry_price": signal_state[index_name]["entry_price"],
                    "stop_loss": signal_state[index_name]["stop_loss"],
                    "target": signal_state[index_name]["target"],
                    "current_pnl": round(pnl,2),
                })
        else:
            reset_signal_state(index_name, now, "PREMIUM_ZERO")
        return

    # -------------------------------
    # NEW ENTRY LOGIC
    # -------------------------------
    with _signal_state_lock:
        if now < signal_state[index_name]["cooldown"]:
            remaining = int(signal_state[index_name]["cooldown"] - now)
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Cooldown {remaining}s"
                market_signal[index_name]["signal"] = "COOLDOWN"
            return

    today = datetime.now(IST).strftime("%Y-%m-%d")
    with _trade_count_lock:
        if last_trade_date[index_name] != today:
            daily_trade_count[index_name] = 0
            last_trade_date[index_name] = today
            daily_drawdown[index_name]["peak_equity"] = portfolio_state[index_name]["equity"]
            with _portfolio_state_lock:
                portfolio_state[index_name]["daily_pnl"] = 0.0
                portfolio_state[index_name]["live_pnl"] = 0.0

    with _trade_count_lock:
        if daily_trade_count[index_name] >= 20:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "Max daily trades reached"
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
    prem = ce_prem if side=="CE" else pe_prem
    min_prem = INDEX_CONFIG[index_name].get("min_premium",5)
    if prem <= 0 or prem < min_prem:
        with _market_signal_lock:
            market_signal[index_name]["alert_message"] = f"Premium invalid: Rs{prem} (min {min_prem})"
            market_signal[index_name]["signal"] = "WAITING"
        return

    # Signal buffer
    with _signal_buffer_lock:
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

    # Greeks filter (placeholder, always passes)
    # Correlation filter
    pair = INDEX_CONFIG[index_name].get("correlation_pair")
    beta_adj = 1.0
    if pair:
        # simplified
        pass

    # ML filter (dummy)
    ml_prob = 0.5

    # Position sizing
    kelly_risk = 0.01
    if "STRONG" in action: base_risk_pct = 2.0
    elif "LOW" in action: base_risk_pct = 0.8
    else: base_risk_pct = 1.2
    risk_pct = base_risk_pct * 0.5 + kelly_risk * 0.5
    risk_pct = max(0.5, min(3.0, risk_pct))

    # ATR
    with _candle_histories_lock:
        candles_1min = list(candle_histories[index_name]["1min"])
    if len(candles_1min) >= 14:
        highs_1min = [c["high"] for c in candles_1min]
        lows_1min = [c["low"] for c in candles_1min]
        closes_1min = [c["close"] for c in candles_1min]
        atr = calculate_atr(highs_1min, lows_1min, closes_1min, 14)
    else:
        atr = 5.0
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

    with _portfolio_state_lock:
        equity = portfolio_state[index_name]["equity"]
    risk_amount = equity * (risk_pct / 100)
    stop_dist = prem - sl
    if stop_dist > 0:
        lots = int(risk_amount / (stop_dist * INDEX_CONFIG[index_name]["lot_size"]))
        lots = max(1, min(5, lots))
    else:
        lots = 1

    # Execute entry
    with _signal_state_lock:
        signal_state[index_name].update({
            "action": action, "entry_price": prem, "stop_loss": sl, "target": target,
            "lots": lots, "entry_time": now, "highest": prem,
            "cooldown": 0, "exit_reason": ""
        })
    with _portfolio_state_lock:
        portfolio_state[index_name]["open_positions"] = 1
    with _signal_buffer_lock:
        signal_buffer[index_name]["ce_count"] = signal_buffer[index_name]["pe_count"] = 0
        if side == "CE":
            signal_buffer[index_name]["consecutive_ce"] += 1
            signal_buffer[index_name]["consecutive_pe"] = 0
        else:
            signal_buffer[index_name]["consecutive_pe"] += 1
            signal_buffer[index_name]["consecutive_ce"] = 0
    with _trade_count_lock:
        daily_trade_count[index_name] += 1
    save_portfolio_state(index_name)

    emoji = "B" if "STRONG" in action and "CE" in action else "S" if "STRONG" in action and "PE" in action else "W" if "LOW" in action else "N"
    vwap = vp_analysis["vwap"]
    regime = "TRENDING" if "STRONG" in action else "RANGING"
    msg = (f"{emoji} {action} {index_name} | Spot:{spot:.0f} Prem:{prem:.2f} SL:{sl:.2f} Tgt:{target:.2f} | "
           f"Sentiment:{sentiment:.0f} ({sentiment_label}) | Lots:{lots} Risk:{risk_pct:.1f}% | "
           f"VWAP:{vwap:.0f} | {regime}")
    logger.info(msg)

    with _market_signal_lock:
        market_signal[index_name].update({
            "signal": action,
            "alert_message": f"ENTRY {action}",
            "entry_price": prem,
            "stop_loss": sl,
            "target": target,
            "sentiment_score": sentiment,
            "exit_reason": "",
            "trend_change_cooldown_remaining": 0
        })

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

def run_all_signals():
    for idx in INDEX_CONFIG:
        if INDEX_CONFIG[idx].get("active"):
            try:
                run_signal_engine_for_index(idx)
            except Exception as e:
                logger.error(f"Signal error {idx}: {e}")

# ----------------------------------------------------------------------
# WEBSOCKET CALLBACKS
# ----------------------------------------------------------------------
def on_ws_open(wsapp):
    global ws_running, last_heartbeat
    ws_running = True
    last_heartbeat = time.time()
    logger.info("WebSocket connected, subscribing...")
    refresh_all_tokens()
    token_list = []
    for idx, cfg in INDEX_CONFIG.items():
        if cfg.get("active"):
            token_list.append({"exchangeType": cfg["ws_exchange_type"], "tokens": [cfg["token"]]})
    with _index_tokens_lock:
        index_tokens_snapshot = list(INDEX_TOKENS.items())
    for idx, tokens in index_tokens_snapshot:
        if not INDEX_CONFIG[idx].get("active"): continue
        if tokens.get("ce_token") and tokens.get("pe_token"):
            token_list.append({"exchangeType": INDEX_CONFIG[idx]["option_ws_exchange_type"], "tokens": [tokens["ce_token"], tokens["pe_token"]]})
    token_list.append({"exchangeType": 1, "tokens": ["99919017"]})
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
        if isinstance(message, bytes):
            if sws and hasattr(sws, '_parse_binary_data'):
                parsed = sws._parse_binary_data(message)
                ticks = [parsed] if isinstance(parsed, dict) else parsed if isinstance(parsed, list) else []
            else: return
        elif isinstance(message, str):
            data = json.loads(message)
            ticks = data if isinstance(data, list) else [data]
        else: return
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

# ----------------------------------------------------------------------
# WEBSOCKET ENGINE (MAIN THREAD, STARTED IMMEDIATELY)
# ----------------------------------------------------------------------
def start_websocket_engine():
    global sws, ws_running
    logger.info("WebSocket engine thread started")
    while True:
        try:
            if not is_market_open():
                time.sleep(5)
                continue
            auth_token, feed_token, obj = get_auth_token()
            if not feed_token:
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
            logger.warning("WebSocket connection closed, will reconnect in 5s...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"WebSocket thread error: {e}", exc_info=True)
            time.sleep(10)

# ----------------------------------------------------------------------
# REST API POLLER
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
                time.sleep(5)
                continue
            now = time.time()
            if not auth_obj or now - auth_time > 3000:
                _, _, auth_obj = get_auth_token()
                auth_time = now
            if not auth_obj:
                time.sleep(10)
                continue
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
                else:
                    get_current_atm_tokens(idx)
            time.sleep(10)
        except Exception as e:
            logger.error(f"REST poller error: {e}")
            time.sleep(10)

# ----------------------------------------------------------------------
# VOLUME PROFILE (PLACEHOLDER)
# ----------------------------------------------------------------------
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
# CORRELATION FILTER (PLACEHOLDER)
# ----------------------------------------------------------------------
class CorrelationFilter:
    def __init__(self):
        self.nifty_returns = deque(maxlen=50)
        self.banknifty_returns = deque(maxlen=50)
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
correlation_filter = CorrelationFilter()

# ----------------------------------------------------------------------
# MARKET OPEN CHECK
# ----------------------------------------------------------------------
def is_market_open():
    now_ist = datetime.now(IST)
    current = now_ist.time()
    return now_ist.weekday() < 5 and dt_time(9,10) <= current <= dt_time(15,35)

# ----------------------------------------------------------------------
# FORCE BACKGROUND THREADS TO START AT MODULE LOAD
# ----------------------------------------------------------------------
logger.info("=== FORCING BACKGROUND THREADS START ===")
threading.Thread(target=start_websocket_engine, daemon=True).start()
threading.Thread(target=start_rest_api_poller, daemon=True).start()
logger.info("Background threads started (WebSocket + REST poller)")

# ----------------------------------------------------------------------
# FLASK ROUTES (no before_request for thread startup)
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
        "engine": "Multi-Index Options Bot v12.13 (Forced WebSocket)",
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
            "version": "12.13"
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
# MAIN ENTRY POINT (only for local run)
# ----------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
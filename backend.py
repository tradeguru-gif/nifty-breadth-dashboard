# === VERSION 11.0 - COMPLETE UPDATE: 8 Timeframes + Market Analysis Exit + Trend Change Cooldown + Full SL Visibility ===
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
from collections import deque
from datetime import datetime, timedelta, time as dt_time
from flask import Flask, jsonify, request
from flask_cors import CORS
import pyotp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info(f"Python version: {sys.version}")

for pkg in ['numpy', 'pandas', 'flask', 'requests']:
    try:
        __import__(pkg)
        logger.info(f"{pkg} loaded")
    except Exception as e:
        logger.error(f"{pkg} import failed: {e}")

app = Flask(__name__)
CORS(app)
application = app

try:
    from sklearn.ensemble import RandomForestClassifier
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import telebot
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False

ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing critical Angel One environment variables")

DB_PATH = "trading_data.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS ticks (timestamp REAL, token TEXT, price REAL, volume REAL, bid REAL, ask REAL, oi REAL)""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ticks_time ON ticks(timestamp)")
    c.execute("""CREATE TABLE IF NOT EXISTS signals (timestamp REAL, action TEXT, signal_type TEXT, grade TEXT, confidence REAL, ce_price REAL, pe_price REAL, rsi REAL, pcr REAL, regime TEXT, vwap REAL, vix REAL, ml_score REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS trades (timestamp REAL, action TEXT, entry_price REAL, exit_price REAL, pnl REAL, size_pct REAL, status TEXT, grade TEXT, atr REAL, vix REAL, exit_reason TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS daily_performance (date TEXT, equity REAL, daily_pnl REAL, drawdown_pct REAL, sharpe REAL, var REAL, win_rate REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS ml_models (id INTEGER PRIMARY KEY, model BLOB, created_at REAL, features TEXT, accuracy REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS backtest_results (id INTEGER PRIMARY KEY, date TEXT, strategy TEXT, trades INTEGER, win_rate REAL, profit_factor REAL, max_drawdown REAL, sharpe REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS greeks (timestamp REAL, delta REAL, gamma REAL, theta REAL, vega REAL, iv REAL, ce_delta REAL, pe_delta REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS market_profile (timestamp REAL, poc REAL, value_area_high REAL, value_area_low REAL, vwap REAL, volume_profile TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS portfolio_equity (index_name TEXT PRIMARY KEY, equity REAL, last_updated REAL)""")
    conn.commit()
    conn.close()

init_db()

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

INDEX_CONFIG = {
    "NIFTY": {
        "token": "99926000", "exchange": "NSE", "symbol": "NIFTY",
        "lot_size": 50, "expiry_weekday": 4, "active": True,
        "min_premium": 10, "max_premium": 5000, "atm_strike_multiple": 50,
        "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2
    },
    "BANKNIFTY": {
        "token": "99926009", "exchange": "NSE", "symbol": "BANKNIFTY",
        "lot_size": 25, "expiry_weekday": 4, "active": True,
        "min_premium": 10, "max_premium": 5000, "atm_strike_multiple": 100,
        "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2
    },
    "FINNIFTY": {
        "token": "99926037", "exchange": "NSE", "symbol": "FINNIFTY",
        "lot_size": 40, "expiry_weekday": 2, "active": True,
        "min_premium": 10, "max_premium": 5000, "atm_strike_multiple": 50,
        "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2
    },
    "MIDCPNIFTY": {
        "token": "99926074", "exchange": "NSE", "symbol": "MIDCPNIFTY",
        "lot_size": 75, "expiry_weekday": 4, "active": True,
        "min_premium": 10, "max_premium": 5000, "atm_strike_multiple": 25,
        "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2
    },
    "SENSEX": {
        "token": None,
        "exchange": "BSE", "symbol": "SENSEX",
        "lot_size": 15, "expiry_weekday": 4, "active": False,
        "min_premium": 10, "max_premium": 5000, "atm_strike_multiple": 100,
        "option_exchange": "BFO", "ws_exchange_type": 3, "option_ws_exchange_type": 4
    }
}

ACTIVE_INDEX = "NIFTY"

INDEX_TOKENS = {idx: {"ce_token": None, "pe_token": None, "atm_strike": 0, "expiry": "", "ce_symbol": "", "pe_symbol": ""} for idx in INDEX_CONFIG}

price_histories = {idx: deque(maxlen=5000) for idx in INDEX_CONFIG}
ce_price_histories = {idx: deque(maxlen=2000) for idx in INDEX_CONFIG}
pe_price_histories = {idx: deque(maxlen=2000) for idx in INDEX_CONFIG}
ce_volume_histories = {idx: deque(maxlen=1000) for idx in INDEX_CONFIG}
pe_volume_histories = {idx: deque(maxlen=1000) for idx in INDEX_CONFIG}
ce_oi_histories = {idx: deque(maxlen=200) for idx in INDEX_CONFIG}
pe_oi_histories = {idx: deque(maxlen=200) for idx in INDEX_CONFIG}

vix_history = deque(maxlen=200)
banknifty_history = deque(maxlen=1000)

latest_ticks = {idx: {"spot_price": 0.0, "ce_price": 0.0, "pe_price": 0.0,
                      "ce_volume": 0, "pe_volume": 0, "ce_oi": 0, "pe_oi": 0}
                for idx in INDEX_CONFIG}
latest_ticks["VIX"] = {"vix": 15.0}

tick_counter = 0
ws_running = False
sws = None
last_tick_time = time.time()
startup_complete = False
last_heartbeat = time.time()

_signal_lock = threading.Lock()
_portfolio_lock = threading.Lock()

_api_last_call = 0
_api_min_interval = 0.5
_api_lock = threading.Lock()

def rate_limited_api_call(func, *args, **kwargs):
    global _api_last_call
    with _api_lock:
        elapsed = time.time() - _api_last_call
        if elapsed < _api_min_interval:
            time.sleep(_api_min_interval - elapsed)
        try:
            result = func(*args, **kwargs)
            _api_last_call = time.time()
            return result
        except Exception as e:
            _api_last_call = time.time()
            raise e

# V11.0: 8 TIMEFRAMES
TIMEFRAMES = ["1min", "2min", "3min", "5min", "10min", "15min", "20min", "30min"]
TIMEFRAME_WEIGHTS = {
    "1min": 8, "2min": 8, "3min": 8, "5min": 12,
    "10min": 12, "15min": 14, "20min": 14, "30min": 24
}
EMA_SHORT, EMA_MEDIUM, EMA_LONG = 9, 21, 50

SENTIMENT_SCORES = {
    "STRONG_BULLISH": (85, 100, "STRONG BULLISH", "STRONG_BUY_CE"),
    "BULLISH": (70, 84, "BULLISH", "BUY_CE"),
    "SLOW_BULLISH": (55, 69, "SLOW BULLISH", "LOW_BUY_CE"),
    "NEUTRAL": (45, 54, "NEUTRAL", "NO_TRADE"),
    "SLOW_BEARISH": (30, 44, "SLOW BEARISH", "LOW_BUY_PE"),
    "BEARISH": (15, 29, "BEARISH", "BUY_PE"),
    "STRONG_BEARISH": (0, 14, "STRONG BEARISH", "STRONG_BUY_PE")
}

market_sentiment = {idx: {
    "score": 50, "label": "NEUTRAL",
    "trend_1min": "NEUTRAL", "trend_2min": "NEUTRAL",
    "trend_3min": "NEUTRAL", "trend_5min": "NEUTRAL",
    "trend_10min": "NEUTRAL", "trend_15min": "NEUTRAL",
    "trend_20min": "NEUTRAL", "trend_30min": "NEUTRAL"
} for idx in INDEX_CONFIG}

signal_state = {idx: {
    "action": "HOLD", "entry_price": 0, "stop_loss": 0, "target": 0,
    "lots": 1, "cooldown": 0, "confidence": 0,
    "highest": 0, "entry_time": 0,
    "prev_action_side": None,
    "trend_change_cooldown": 0,
    "exit_reason": ""
} for idx in INDEX_CONFIG}

def load_portfolio_equity():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT index_name, equity FROM portfolio_equity")
        rows = c.fetchall()
        conn.close()
        return {row[0]: {"equity": row[1], "open_positions": 0, "daily_trades": 0} for row in rows}
    except Exception as e:
        logger.error(f"Failed to load portfolio equity: {e}")
        return {}

persisted = load_portfolio_equity()
portfolio_state = {idx: {"equity": persisted.get(idx, {}).get("equity", 100000.0), "open_positions": 0, "daily_trades": 0} for idx in INDEX_CONFIG}

market_signal = {idx: {
    "signal": "WAITING", "sentiment_score": 50, "alert_message": "",
    "entry_price": 0.0, "stop_loss": 0.0, "target": 0.0,
    "exit_reason": "",
    "trend_change_cooldown_remaining": 0
} for idx in INDEX_CONFIG}

safety_state = {idx: {"consecutive_sl": 0, "circuit_breaker": False, "circuit_breaker_until": 0} for idx in INDEX_CONFIG}
signal_buffer = {idx: {"ce_count": 0, "pe_count": 0, "consecutive_ce": 0, "consecutive_pe": 0} for idx in INDEX_CONFIG}
daily_trade_count = {idx: 0 for idx in INDEX_CONFIG}
last_trade_date = {idx: "" for idx in INDEX_CONFIG}

def is_valid_option_premium(premium, spot_price, side):
    if premium <= 0 or spot_price <= 0:
        return False
    premium_pct = premium / spot_price
    return 0.001 < premium_pct < 0.15

def calculate_rsi(prices, period=14, smoothing=3):
    if len(prices) < period + 1: return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    if len(gains) < period or len(losses) < period:
        return 50.0
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0: return 100.0
    rsi_raw = 100 - (100 / (1 + avg_gain / avg_loss))
    if smoothing > 1 and len(prices) >= period + smoothing and len(gains) >= period + smoothing:
        rsi_vals = []
        for j in range(smoothing):
            sub_gains = gains[-(period+j):-j if j > 0 else None]
            sub_losses = losses[-(period+j):-j if j > 0 else None]
            if len(sub_gains) == period and len(sub_losses) == period:
                ag = sum(sub_gains) / period
                al = sum(sub_losses) / period
                rsi_vals.append(100 - (100 / (1 + ag / al)) if al > 0 else 100.0)
        if rsi_vals:
            alpha = 2 / (smoothing + 1)
            rsi_smooth = rsi_vals[0]
            for rv in rsi_vals[1:]:
                rsi_smooth = alpha * rv + (1 - alpha) * rsi_smooth
            return rsi_smooth
    return rsi_raw

def calculate_ema(prices, period):
    if not prices:
        return 0
    if len(prices) < period: 
        return sum(prices) / len(prices) if prices else 0
    alpha = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = alpha * p + (1 - alpha) * ema
    return ema

def calculate_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal: return 0.0, 0.0, 0.0
    def ema(arr, p):
        if not arr:
            return 0
        alpha = 2 / (p + 1)
        val = arr[0]
        for x in arr[1:]: 
            val = alpha * x + (1 - alpha) * val
        return val
    ema_fast = ema(prices[-fast:], fast)
    ema_slow = ema(prices[-slow:], slow)
    macd_line = ema_fast - ema_slow
    hist = []
    for i in range(signal, 0, -1):
        if len(prices) >= slow + i:
            ef = ema(prices[-(fast+i):-i], fast)
            es = ema(prices[-(slow+i):-i], slow)
            hist.append(ef - es)
    sig_line = ema(hist, signal) if hist else macd_line
    return macd_line, sig_line, macd_line - sig_line

def calculate_atr(prices, period=14):
    if len(prices) < period + 1: return 5.0
    tr = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    if not tr:
        return 5.0
    return sum(tr[-period:]) / period

def calculate_adx(prices, period=14):
    if len(prices) < period * 2 + 1: return 20.0
    plus_dm, minus_dm, tr = [], [], []
    for i in range(1, len(prices)):
        up = prices[i] - prices[i-1]
        down = prices[i-1] - prices[i]
        plus_dm.append(max(up, 0) if up > down else 0)
        minus_dm.append(max(down, 0) if down > up else 0)
        tr.append(abs(prices[i] - prices[i-1]))
    if not tr or not plus_dm or not minus_dm:
        return 20.0
    atr = sum(tr[-period:]) / period
    plus_di = 100 * sum(plus_dm[-period:]) / period / atr if atr > 0 else 0
    minus_di = 100 * sum(minus_dm[-period:]) / period / atr if atr > 0 else 0
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di) if (plus_di + minus_di) > 0 else 0
    return dx

def calculate_bollinger(prices, period=20, std_dev=2):
    if len(prices) < period:
        return None, None, None
    sma = sum(prices[-period:]) / period
    variance = sum((p - sma) ** 2 for p in prices[-period:]) / period
    std = math.sqrt(variance)
    upper = sma + (std_dev * std)
    lower = sma - (std_dev * std)
    return upper, sma, lower

def get_min_bars_for_timeframe(tf_name):
    mapping = {
        "1min": 60, "2min": 60, "3min": 60, "5min": 60,
        "10min": 60, "15min": 60, "20min": 80, "30min": 100
    }
    return mapping.get(tf_name, 60)

def is_sideways(prices, tf_name):
    if len(prices) < 30:
        return False, 0
    upper, sma, lower = calculate_bollinger(prices, period=20)
    if upper and lower and sma > 0:
        band_width = (upper - lower) / sma
        if band_width < 0.008:
            return True, band_width
    adx = calculate_adx(prices, period=14)
    if adx < 20:
        return True, adx
    recent = prices[-30:]
    price_range = (max(recent) - min(recent)) / sum(recent) * len(recent)
    if price_range < 0.005:
        return True, price_range
    return False, 0

def get_trend_score(prices, tf_name):
    min_bars = get_min_bars_for_timeframe(tf_name)
    if len(prices) < min_bars:
        return "NEUTRAL", 0
    w = TIMEFRAME_WEIGHTS[tf_name]
    ema9 = calculate_ema(prices, EMA_SHORT)
    ema21 = calculate_ema(prices, EMA_MEDIUM)
    ema50 = calculate_ema(prices, EMA_LONG)
    price = prices[-1] if prices else 0
    sideways, sideways_metric = is_sideways(prices, tf_name)
    if sideways:
        return "SIDEWAYS", 0
    if tf_name in ["1min", "2min", "3min"]:
        if ema9 > ema21 and price > ema9:
            return "BULLISH", w
        if ema9 < ema21 and price < ema9:
            return "BEARISH", -w
        return "NEUTRAL", 0
    elif tf_name in ["5min", "10min"]:
        if ema9 > ema21 > ema50 and price > ema9:
            return "BULLISH", w
        if ema9 < ema21 < ema50 and price < ema9:
            return "BEARISH", -w
        if ema9 > ema21 and price > ema9:
            return "BULLISH", w - 5
        if ema9 < ema21 and price < ema9:
            return "BEARISH", -(w - 5)
        return "NEUTRAL", 0
    else:
        if len(prices) >= 20:
            highs = []
            lows = []
            step = 5 if tf_name == "15min" else 7 if tf_name == "20min" else 10
            for i in range(0, min(30, len(prices) - step), step):
                slice_prices = prices[-i-step:-i] if i > 0 else prices[-step:]
                if slice_prices:
                    highs.append(max(slice_prices))
                    lows.append(min(slice_prices))
            if len(highs) >= 2 and len(lows) >= 2:
                if highs[0] > highs[1] and lows[0] > lows[1] and ema9 > ema21:
                    return "BULLISH", w
                if highs[0] < highs[1] and lows[0] < lows[1] and ema9 < ema21:
                    return "BEARISH", -w
                if highs[0] > highs[1]:
                    return "BULLISH", w - 8
                if highs[0] < highs[1]:
                    return "BEARISH", -(w - 8)
        if ema9 > ema21 > ema50:
            return "BULLISH", w - 5
        if ema9 < ema21 < ema50:
            return "BEARISH", -(w - 5)
        return "NEUTRAL", 0

def compute_sentiment(index_name):
    prices = list(price_histories[index_name])
    if len(prices) < 60:
        market_sentiment[index_name]["score"] = 50
        return 50
    total = 0
    for tf in TIMEFRAMES:
        trend, score = get_trend_score(prices, tf)
        market_sentiment[index_name][f"trend_{tf}"] = trend
        total += score
    sentiment = 50 + (total / 3.5)
    sentiment = max(0, min(100, sentiment))
    market_sentiment[index_name]["score"] = sentiment
    for k, (low, high, label, action) in SENTIMENT_SCORES.items():
        if low <= sentiment <= high:
            market_sentiment[index_name]["label"] = label
            break
    return sentiment

def get_signal_from_sentiment(index_name, sentiment):
    for k, (low, high, label, action) in SENTIMENT_SCORES.items():
        if low <= sentiment <= high:
            trend_30m = market_sentiment[index_name].get("trend_30min", "NEUTRAL")
            trend_20m = market_sentiment[index_name].get("trend_20min", "NEUTRAL")
            if "LOW" in action:
                if "CE" in action and trend_30m == "BEARISH" and trend_20m == "BEARISH":
                    return "NO_TRADE", label, sentiment
                if "PE" in action and trend_30m == "BULLISH" and trend_20m == "BULLISH":
                    return "NO_TRADE", label, sentiment
            return action, label, sentiment
    return "NO_TRADE", "UNKNOWN", sentiment

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
            logger.info("Auth token refreshed successfully")
            return auth_token, feed_token, obj
        except Exception as e:
            logger.error(f"Auth loop fail: {e}")
            return None, None, None

def safe_ltp(resp):
    if not resp or not resp.get("status"):
        return None
    data = resp.get("data", {})
    if isinstance(data, dict):
        if "fetched" in data and data["fetched"]:
            return float(data["fetched"][0].get("ltp", 0))
        elif "ltp" in data:
            return float(data["ltp"])
    return None

def get_index_spot(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config: return None
    _, _, obj = get_auth_token()
    if not obj: return None
    if index_name == "SENSEX":
        try:
            resp = rate_limited_api_call(obj.searchScrip, "BSE", "SENSEX")
            if resp and resp.get("status") and resp.get("data") and len(resp["data"]) > 0:
                token = str(resp["data"][0].get("symboltoken"))
                ltp_resp = rate_limited_api_call(obj.ltpData, "BSE", "SENSEX", token)
                ltp = safe_ltp(ltp_resp)
                if ltp is not None:
                    if ltp > 100000: ltp /= 100
                    return ltp
        except Exception as e:
            logger.error(f"SENSEX spot error: {e}")
        return None
    try:
        resp = rate_limited_api_call(obj.ltpData, config["exchange"], config["symbol"], config["token"])
        ltp = safe_ltp(resp)
        if ltp is not None:
            if ltp > 100000: ltp /= 100
            if index_name == "MIDCPNIFTY" and (ltp < 5000 or ltp > 25000):
                search = rate_limited_api_call(obj.searchScrip, "NSE", "MIDCPNIFTY")
                if search and search.get("status") and search.get("data") and len(search["data"]) > 0:
                    token = str(search["data"][0].get("symboltoken"))
                    ltp2 = safe_ltp(rate_limited_api_call(obj.ltpData, "NSE", "MIDCPNIFTY", token))
                    if ltp2 is not None:
                        if ltp2 > 100000: ltp2 /= 100
                        ltp = ltp2
            return ltp
    except Exception as e:
        logger.error(f"Spot fetch {index_name}: {e}")
    return None

def get_vix_value():
    _, _, obj = get_auth_token()
    if not obj: return 15.0
    try:
        vix_tokens = ["99919017", "99919011"]
        for token in vix_tokens:
            try:
                resp = rate_limited_api_call(obj.ltpData, "NSE", "INDIAVIX", token)
                ltp = safe_ltp(resp)
                if ltp is not None:
                    return ltp
            except Exception:
                continue
        try:
            search = rate_limited_api_call(obj.searchScrip, "NSE", "INDIAVIX")
            if search and search.get("status") and search.get("data") and len(search["data"]) > 0:
                token = str(search["data"][0].get("symboltoken"))
                resp = rate_limited_api_call(obj.ltpData, "NSE", "INDIAVIX", token)
                ltp = safe_ltp(resp)
                if ltp is not None:
                    return ltp
        except Exception:
            pass
    except Exception:
        pass
    return 15.0

_scrip_cache = {"data": None, "timestamp": 0}
_scrip_lock = threading.Lock()

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
            logger.info("Scrip Master refreshed")
            return data
        except Exception as e:
            logger.error(f"Scrip Master failed: {e}")
            return _scrip_cache["data"] or []

def is_expiry_day(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config: return False
    today = datetime.now()
    weekday = today.weekday()
    return weekday == config["expiry_weekday"]

def get_current_atm_tokens(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config or not config.get("active"): 
        return None, None
    spot = get_index_spot(index_name)
    if not spot or spot <= 0:
        logger.warning(f"{index_name} spot unavailable")
        return None, None
    mult = config["atm_strike_multiple"]
    atm = int(round(spot / mult) * mult)
    today = datetime.now()
    days = (config["expiry_weekday"] - today.weekday()) % 7
    if days == 0: days = 7
    expiry = (today + timedelta(days=days)).strftime("%d%b%Y").upper()
    scrip = get_scrip_master()
    if scrip:
        try:
            df = pd.DataFrame(scrip)
            if df.empty:
                logger.warning(f"{index_name}: Scrip master DataFrame is empty")
                raise ValueError("Empty scrip master")
            opts = df[(df["name"] == config["symbol"]) &
                      (df["instrumenttype"] == "OPTIDX") &
                      (df["exch_seg"] == config["option_exchange"])].copy()
            if opts.empty:
                logger.warning(f"{index_name}: No options found in scrip master")
                raise ValueError("No options in scrip master")
            opts["expiry_date"] = pd.to_datetime(opts["expiry"], format="%d%b%Y", errors="coerce")
            opts = opts.dropna(subset=["expiry_date"])
            if opts.empty:
                logger.warning(f"{index_name}: No valid expiry dates found")
                raise ValueError("No valid expiry dates")
            opts["strike"] = pd.to_numeric(opts["strike"], errors="coerce") / 100
            opts = opts.dropna(subset=["strike"])
            if opts.empty:
                logger.warning(f"{index_name}: No valid strikes found")
                raise ValueError("No valid strikes")
            future = opts[opts["expiry_date"] >= today]
            if future.empty:
                logger.warning(f"{index_name}: No future expiry options found")
                raise ValueError("No future options")
            nearest = future["expiry_date"].min()
            atm_opts = future[(future["strike"] == atm) & (future["expiry_date"] == nearest)]
            if atm_opts.empty:
                same_exp = future[future["expiry_date"] == nearest]
                if same_exp.empty:
                    logger.warning(f"{index_name}: No options for nearest expiry {nearest}")
                    raise ValueError("No nearest expiry options")
                strike_diffs = (same_exp["strike"] - atm).abs()
                min_idx = strike_diffs.idxmin()
                if pd.isna(min_idx):
                    logger.warning(f"{index_name}: Could not find nearest strike")
                    raise ValueError("No nearest strike found")
                atm_opts = same_exp.loc[[min_idx]]
            ce = atm_opts[atm_opts["symbol"].str.contains("CE", na=False)]
            pe = atm_opts[atm_opts["symbol"].str.contains("PE", na=False)]
            if not ce.empty and not pe.empty:
                ce_token = str(ce.iloc[0]["token"])
                pe_token = str(pe.iloc[0]["token"])
                INDEX_TOKENS[index_name].update({
                    "ce_token": ce_token, "pe_token": pe_token,
                    "atm_strike": atm, "expiry": expiry,
                    "ce_symbol": ce.iloc[0]["symbol"], "pe_symbol": pe.iloc[0]["symbol"]
                })
                logger.info(f"{index_name} tokens: CE={ce_token} PE={pe_token}")
                return ce_token, pe_token
            else:
                logger.warning(f"{index_name}: CE or PE empty after filtering")
                raise ValueError("CE/PE filtering failed")
        except Exception as e:
            logger.warning(f"{index_name} scrip master path failed: {e}, trying API fallback")
    _, _, obj = get_auth_token()
    if obj:
        ce_token = pe_token = None
        try:
            ce_resp = rate_limited_api_call(obj.searchScrip, config["option_exchange"], f"{config['symbol']}{atm}CE")
            if ce_resp and ce_resp.get("status") and ce_resp.get("data") and len(ce_resp["data"]) > 0:
                ce_token = str(ce_resp["data"][0].get("symboltoken"))
            pe_resp = rate_limited_api_call(obj.searchScrip, config["option_exchange"], f"{config['symbol']}{atm}PE")
            if pe_resp and pe_resp.get("status") and pe_resp.get("data") and len(pe_resp["data"]) > 0:
                pe_token = str(pe_resp["data"][0].get("symboltoken"))
            if ce_token and pe_token:
                INDEX_TOKENS[index_name].update({
                    "ce_token": ce_token, "pe_token": pe_token,
                    "atm_strike": atm, "expiry": expiry
                })
                logger.info(f"{index_name} tokens (search): CE={ce_token} PE={pe_token}")
                return ce_token, pe_token
        except Exception as e:
            logger.error(f"Search fallback error {index_name}: {e}")
    return None, None

def refresh_all_tokens():
    for idx in INDEX_CONFIG:
        if INDEX_CONFIG[idx].get("active"):
            get_current_atm_tokens(idx)

def calculate_position_size(index_name, signal_strength, atr, vix):
    config = INDEX_CONFIG[index_name]
    base_risk = 1.0
    if "STRONG" in signal_strength:
        risk_pct = min(2.0, base_risk * 1.3)
    elif "LOW" in signal_strength:
        risk_pct = max(0.5, base_risk * 0.7)
    else:
        risk_pct = base_risk
    if vix > 25: risk_pct *= 0.7
    elif vix > 20: risk_pct *= 0.85
    if is_expiry_day(index_name):
        risk_pct *= 0.5
        logger.info(f"{index_name}: Expiry day - position size halved")
    risk_amount = portfolio_state[index_name]["equity"] * (risk_pct / 100)
    stop_dist = atr * 1.5
    if stop_dist > 0:
        lots = int(risk_amount / (stop_dist * config["lot_size"]))
        lots = max(1, min(5, lots))
    else:
        lots = 1
    return lots, risk_pct

def reset_signal_state(index_name, current_time, exit_reason=""):
    signal_state[index_name].update({
        "action": "HOLD", "entry_price": 0, "stop_loss": 0, "target": 0,
        "lots": 1, "cooldown": current_time + 60, "confidence": 0,
        "highest": 0, "entry_time": 0,
        "exit_reason": exit_reason
    })
    portfolio_state[index_name]["open_positions"] = 0

def save_portfolio_equity(index_name):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""INSERT OR REPLACE INTO portfolio_equity (index_name, equity, last_updated)
                       VALUES (?, ?, ?)""",
                  (index_name, portfolio_state[index_name]["equity"], time.time()))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to save equity for {index_name}: {e}")

def send_telegram_alert(msg):
    if not TELEGRAM_AVAILABLE or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}, timeout=3)
    except Exception as e:
        logger.error(f"Telegram error: {e}")

# V11.0: MARKET ANALYSIS EXIT ENGINE
def should_exit_market_analysis(index_name, action, prices, ce_prem, pe_prem):
    if len(prices) < 60:
        return False, ""
    exit_reason = ""
    trends = {tf: market_sentiment[index_name].get(f"trend_{tf}", "NEUTRAL") for tf in TIMEFRAMES}
    if "CE" in action:
        bearish_count = sum(1 for t in trends.values() if t in ["BEARISH", "SIDEWAYS"])
        if bearish_count >= 5:
            exit_reason = f"Trend Reversal: {bearish_count}/8 timeframes bearish/sideways"
            return True, exit_reason
    elif "PE" in action:
        bullish_count = sum(1 for t in trends.values() if t in ["BULLISH", "SIDEWAYS"])
        if bullish_count >= 5:
            exit_reason = f"Trend Reversal: {bullish_count}/8 timeframes bullish/sideways"
            return True, exit_reason
    rsi = calculate_rsi(prices)
    if len(prices) >= 20:
        price_trend = prices[-1] - prices[-10]
        rsi_trend = calculate_rsi(prices[-10:]) - calculate_rsi(prices[-20:-10])
        if "CE" in action and price_trend > 0 and rsi_trend < 0 and rsi > 70:
            exit_reason = f"Bearish Divergence: Price up + RSI down (RSI={rsi:.1f})"
            return True, exit_reason
        if "PE" in action and price_trend < 0 and rsi_trend > 0 and rsi < 30:
            exit_reason = f"Bullish Divergence: Price down + RSI up (RSI={rsi:.1f})"
            return True, exit_reason
    vix = latest_ticks["VIX"]["vix"]
    if len(vix_history) >= 10:
        vix_sma = sum(list(vix_history)[-10:]) / 10
        if vix > vix_sma * 1.25:
            if "CE" in action:
                exit_reason = f"VIX Spike: {vix:.1f} vs SMA {vix_sma:.1f} (volatility crush risk)"
                return True, exit_reason
            elif "PE" in action:
                exit_reason = f"VIX Spike: {vix:.1f} vs SMA {vix_sma:.1f} (volatility crush risk)"
                return True, exit_reason
    adx = calculate_adx(prices)
    if adx < 15 and len(prices) >= 30:
        current_prem = ce_prem if "CE" in action else pe_prem
        entry = signal_state[index_name].get("entry_price", 0)
        if entry > 0 and current_prem > 0:
            profit_pct = (current_prem - entry) / entry
            if profit_pct > 0.15:
                exit_reason = f"Trend Weakness: ADX {adx:.1f} with {profit_pct*100:.1f}% profit - secure gains"
                return True, exit_reason
    upper, sma, lower = calculate_bollinger(prices)
    if upper and lower and sma > 0:
        if "CE" in action and prices[-1] > upper * 0.998:
            exit_reason = f"Overbought: Price {prices[-1]:.2f} near upper Bollinger {upper:.2f}"
            return True, exit_reason
        if "PE" in action and prices[-1] < lower * 1.002:
            exit_reason = f"Oversold: Price {prices[-1]:.2f} near lower Bollinger {lower:.2f}"
            return True, exit_reason
    return False, ""

# V11.0: TREND CHANGE COOLDOWN
def get_action_side(action):
    if "CE" in action:
        return "CE"
    elif "PE" in action:
        return "PE"
    return None

def check_trend_change_cooldown(index_name, new_action, current_time):
    current_side = get_action_side(signal_state[index_name]["action"])
    new_side = get_action_side(new_action)
    if current_side is not None and signal_state[index_name]["action"] != "HOLD":
        return True, 0
    prev_side = signal_state[index_name].get("prev_action_side")
    if prev_side is not None and new_side is not None and prev_side != new_side:
        cooldown_until = current_time + 60
        signal_state[index_name]["trend_change_cooldown"] = cooldown_until
        signal_state[index_name]["prev_action_side"] = new_side
        logger.info(f"{index_name}: Trend change cooldown activated {prev_side} -> {new_side}")
        return False, 60
    cooldown_until = signal_state[index_name].get("trend_change_cooldown", 0)
    if current_time < cooldown_until:
        remaining = int(cooldown_until - current_time)
        return False, remaining
    if new_side is not None:
        signal_state[index_name]["prev_action_side"] = new_side
    return True, 0

# V11.0: MAIN SIGNAL ENGINE
def run_signal_engine_for_index(index_name):
    if not INDEX_CONFIG[index_name].get("active"): 
        return
    with _signal_lock:
        prices = list(price_histories[index_name])
        if len(prices) < 30:
            market_signal[index_name]["alert_message"] = f"Collecting data ({len(prices)}/30)"
            market_signal[index_name]["signal"] = "WAITING"
            return
        now = time.time()
        spot = prices[-1] if prices else 0
        sentiment = compute_sentiment(index_name)
        action, label, conf = get_signal_from_sentiment(index_name, sentiment)
        rsi = calculate_rsi(prices)
        _, _, macd_hist = calculate_macd(prices)
        atr = calculate_atr(prices)
        vix = latest_ticks["VIX"]["vix"]
        ce_prem = latest_ticks[index_name]["ce_price"]
        pe_prem = latest_ticks[index_name]["pe_price"]
        if safety_state[index_name]["circuit_breaker"]:
            if now < safety_state[index_name]["circuit_breaker_until"]:
                market_signal[index_name]["alert_message"] = "Circuit breaker active"
                market_signal[index_name]["signal"] = "CIRCUIT_BREAKER"
                return
            else:
                safety_state[index_name]["circuit_breaker"] = False
                safety_state[index_name]["consecutive_sl"] = 0
                logger.info(f"{index_name}: Circuit breaker released")
        if signal_state[index_name]["action"] != "HOLD":
            active = signal_state[index_name]["action"]
            prem = ce_prem if "CE" in active else pe_prem
            if prem > 0:
                pnl = prem - signal_state[index_name]["entry_price"]
                if prem > signal_state[index_name].get("highest", 0):
                    signal_state[index_name]["highest"] = prem
                    new_sl = prem - (atr * 1.8)
                    if new_sl > signal_state[index_name]["stop_loss"]:
                        signal_state[index_name]["stop_loss"] = new_sl
                if prem <= signal_state[index_name]["stop_loss"]:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        save_portfolio_equity(index_name)
                    safety_state[index_name]["consecutive_sl"] += 1
                    if safety_state[index_name]["consecutive_sl"] >= 3:
                        safety_state[index_name]["circuit_breaker"] = True
                        safety_state[index_name]["circuit_breaker_until"] = now + 1800
                        send_telegram_alert(f"CIRCUIT BREAKER {index_name} | 3 consecutive SLs. Trading paused 30 min.")
                    daily_trade_count[index_name] += 1
                    send_telegram_alert(f"EXIT {index_name} | SL | PnL: {pnl:.2f} pts")
                    reset_signal_state(index_name, now, "STOP_LOSS")
                    return
                if prem >= signal_state[index_name]["target"]:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        save_portfolio_equity(index_name)
                    safety_state[index_name]["consecutive_sl"] = 0
                    daily_trade_count[index_name] += 1
                    send_telegram_alert(f"TARGET {index_name} | PnL: {pnl:.2f} pts")
                    reset_signal_state(index_name, now, "TARGET_HIT")
                    return
                entry_time = signal_state[index_name].get("entry_time", 0)
                if entry_time > 0 and (now - entry_time) / 60 >= 45:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        save_portfolio_equity(index_name)
                    daily_trade_count[index_name] += 1
                    send_telegram_alert(f"TIME EXIT {index_name} | PnL: {pnl:.2f} pts")
                    reset_signal_state(index_name, now, "TIME_EXIT")
                    return
                should_exit, exit_reason = should_exit_market_analysis(index_name, active, prices, ce_prem, pe_prem)
                if should_exit:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        save_portfolio_equity(index_name)
                    safety_state[index_name]["consecutive_sl"] = 0
                    daily_trade_count[index_name] += 1
                    send_telegram_alert(f"MARKET EXIT {index_name} | {exit_reason} | PnL: {pnl:.2f} pts")
                    reset_signal_state(index_name, now, exit_reason)
                    return
            market_signal[index_name].update({
                "alert_message": f"ACTIVE {active} {index_name}",
                "signal": "ACTIVE",
                "entry_price": signal_state[index_name]["entry_price"],
                "stop_loss": signal_state[index_name]["stop_loss"],
                "target": signal_state[index_name]["target"],
                "exit_reason": "",
                "trend_change_cooldown_remaining": 0
            })
        else:
            can_trade, cooldown_remaining = check_trend_change_cooldown(index_name, action, now)
            if not can_trade:
                market_signal[index_name]["alert_message"] = f"Trend change cooldown: {cooldown_remaining}s"
                market_signal[index_name]["signal"] = "COOLDOWN"
                market_signal[index_name]["trend_change_cooldown_remaining"] = cooldown_remaining
                return
            if now < signal_state[index_name]["cooldown"]:
                remaining = int(signal_state[index_name]["cooldown"] - now)
                market_signal[index_name]["alert_message"] = f"Cooldown {remaining}s"
                market_signal[index_name]["signal"] = "COOLDOWN"
                market_signal[index_name]["trend_change_cooldown_remaining"] = 0
                return
            today = datetime.now().strftime("%Y-%m-%d")
            if last_trade_date[index_name] != today:
                daily_trade_count[index_name] = 0
                last_trade_date[index_name] = today
            if daily_trade_count[index_name] >= 20:
                market_signal[index_name]["alert_message"] = "Max daily trades reached"
                market_signal[index_name]["signal"] = "BLOCKED"
                return
            prem = ce_prem if "CE" in action else pe_prem if "PE" in action else 0
            min_prem = INDEX_CONFIG[index_name].get("min_premium", 10)
            if prem <= 0 or prem < min_prem:
                market_signal[index_name]["alert_message"] = f"Premium invalid Rs{prem}"
                market_signal[index_name]["signal"] = "WAITING"
                return
            buf = signal_buffer[index_name]
            if "CE" in action:
                buf["ce_count"] += 1
                buf["pe_count"] = 0
                if buf["ce_count"] < 2:
                    market_signal[index_name]["alert_message"] = f"Building CE ({buf['ce_count']}/2)"
                    market_signal[index_name]["signal"] = "BUILDING"
                    return
            elif "PE" in action:
                buf["pe_count"] += 1
                buf["ce_count"] = 0
                if buf["pe_count"] < 2:
                    market_signal[index_name]["alert_message"] = f"Building PE ({buf['pe_count']}/2)"
                    market_signal[index_name]["signal"] = "BUILDING"
                    return
            else:
                buf["ce_count"] = buf["pe_count"] = 0
            lots, risk = calculate_position_size(index_name, action, atr, vix)
            sl_pct = 0.3 if "LOW" in action else 0.45
            sl = max(prem * (1 - sl_pct), prem - (atr * 1.5))
            target = prem + (atr * 3.5)
            if "LOW" in action:
                target = prem + (atr * 2.5)
            signal_state[index_name].update({
                "action": action, "entry_price": prem, "stop_loss": sl, "target": target,
                "lots": lots, "entry_time": now, "highest": prem, "confidence": conf,
                "exit_reason": ""
            })
            portfolio_state[index_name]["open_positions"] = 1
            buf["ce_count"] = 0
            buf["pe_count"] = 0
            if "CE" in action:
                buf["consecutive_ce"] += 1
                buf["consecutive_pe"] = 0
            else:
                buf["consecutive_pe"] += 1
                buf["consecutive_ce"] = 0
            daily_trade_count[index_name] += 1
            emoji = "B" if "STRONG" in action and "CE" in action else "S" if "STRONG" in action and "PE" in action else "W" if "LOW" in action else "N"
            msg = f"{emoji} {action} {index_name} | Spot: {spot:.2f} | Prem: Rs{prem:.2f} | SL: {sl:.2f} Tgt: {target:.2f} | Sentiment: {sentiment:.1f} ({label}) | Lots: {lots}"
            send_telegram_alert(msg)
            logger.info(f"ENTRY {index_name} {action}")
        market_signal[index_name].update({
            "spot_price": spot,
            "ce_price": ce_prem,
            "pe_price": pe_prem,
            "sentiment_score": sentiment,
            "sentiment": label,
            "signal": signal_state[index_name]["action"],
            "confidence": conf,
            "trend_1min": market_sentiment[index_name]["trend_1min"],
            "trend_2min": market_sentiment[index_name]["trend_2min"],
            "trend_3min": market_sentiment[index_name]["trend_3min"],
            "trend_5min": market_sentiment[index_name]["trend_5min"],
            "trend_10min": market_sentiment[index_name]["trend_10min"],
            "trend_15min": market_sentiment[index_name]["trend_15min"],
            "trend_20min": market_sentiment[index_name]["trend_20min"],
            "trend_30min": market_sentiment[index_name]["trend_30min"],
            "timestamp": datetime.now().isoformat(),
            "entry_price": signal_state[index_name]["entry_price"] if signal_state[index_name]["action"] != "HOLD" else 0.0,
            "stop_loss": signal_state[index_name]["stop_loss"] if signal_state[index_name]["action"] != "HOLD" else 0.0,
            "target": signal_state[index_name]["target"] if signal_state[index_name]["action"] != "HOLD" else 0.0,
            "exit_reason": signal_state[index_name].get("exit_reason", ""),
            "trend_change_cooldown_remaining": max(0, int(signal_state[index_name].get("trend_change_cooldown", 0) - now))
        })

def run_all_signals():
    for idx in INDEX_CONFIG:
        if INDEX_CONFIG[idx].get("active"):
            try:
                run_signal_engine_for_index(idx)
            except Exception as e:
                logger.error(f"Signal engine error for {idx}: {e}")

# WEBSOCKET HANDLERS
def on_ws_open(wsapp):
    global sws
    logger.info("WebSocket opened successfully")
    tokens_by_exchange = {1: [], 2: [], 3: [], 4: []}
    for idx, cfg in INDEX_CONFIG.items():
        if cfg.get("active"):
            idx_ws_type = cfg.get("ws_exchange_type", 1)
            opt_ws_type = cfg.get("option_ws_exchange_type", 2)
            if cfg.get("token"):
                tokens_by_exchange[idx_ws_type].append(cfg["token"])
            if INDEX_TOKENS[idx].get("ce_token"):
                tokens_by_exchange[opt_ws_type].append(INDEX_TOKENS[idx]["ce_token"])
            if INDEX_TOKENS[idx].get("pe_token"):
                tokens_by_exchange[opt_ws_type].append(INDEX_TOKENS[idx]["pe_token"])
    if sws:
        total_subs = 0
        for etype, toks in tokens_by_exchange.items():
            toks = list(set([t for t in toks if t]))
            if toks:
                try:
                    sws.subscribe("tradeguru_001", 1, [{"exchangeType": etype, "tokens": toks}])
                    logger.info(f"Subscribed {len(toks)} tokens on exchangeType {etype}")
                    total_subs += len(toks)
                except Exception as e:
                    logger.error(f"Subscription failed for exchangeType {etype}: {e}")
        logger.info(f"Total subscriptions: {total_subs} tokens")

def on_ws_error(wsapp, error):
    logger.error(f"WebSocket Error: {error}")

def on_ws_close(wsapp, close_status_code=None, close_msg=None):
    global ws_running
    ws_running = False
    logger.warning(f"WebSocket closed: {close_status_code} {close_msg}")

def on_ws_data(wsapp, message):
    global tick_counter, last_tick_time, last_heartbeat
    last_tick_time = time.time()
    last_heartbeat = time.time()
    try:
        ticks = []
        if isinstance(message, bytes):
            if sws is not None and hasattr(sws, '_parse_binary_data'):
                try:
                    parsed = sws._parse_binary_data(message)
                    if parsed and isinstance(parsed, dict):
                        ticks = [parsed]
                except Exception as e:
                    logger.debug(f"Binary parse failed: {e}")
                    return
            else:
                return
        elif isinstance(message, str):
            try:
                data = json.loads(message)
                ticks = data if isinstance(data, list) else [data]
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse JSON: {message[:100]}")
                return
        elif isinstance(message, dict):
            ticks = [message]
        elif isinstance(message, list):
            ticks = message
        else:
            logger.debug(f"Unexpected message type: {type(message)}")
            return
        for tick in ticks:
            if not isinstance(tick, dict): 
                continue
            token = str(tick.get("token") or tick.get("tk") or "")
            ltp = tick.get("last_traded_price") or tick.get("ltp") or tick.get("lp") or 0
            if isinstance(ltp, str):
                try: 
                    ltp = float(ltp)
                except: 
                    ltp = 0
            if isinstance(ltp, (int, float)) and ltp > 100000:
                ltp /= 100
            vol = tick.get("volume") or tick.get("v") or 0
            oi = tick.get("open_interest") or tick.get("oi") or 0
            idx = None
            for i, cfg in INDEX_CONFIG.items():
                if cfg.get("token") == token:
                    idx = i
                    break
            if not idx:
                for i, t in INDEX_TOKENS.items():
                    if t.get("ce_token") == token or t.get("pe_token") == token:
                        idx = i
                        break
            if idx:
                if token == INDEX_CONFIG[idx]["token"]:
                    if ltp > 0:
                        latest_ticks[idx]["spot_price"] = ltp
                        price_histories[idx].append(ltp)
                        tick_counter += 1
                elif token == INDEX_TOKENS[idx].get("ce_token"):
                    pass
                elif token == INDEX_TOKENS[idx].get("pe_token"):
                    pass
            elif token == "99919017":
                if ltp > 0:
                    latest_ticks["VIX"]["vix"] = ltp
                    vix_history.append(ltp)
        if tick_counter % 3 == 0 and tick_counter > 0:
            try:
                run_all_signals()
            except Exception as e:
                logger.error(f"Signal engine error in WS callback: {e}")
    except Exception as e:
        logger.error(f"WS data error: {e}")

def start_angel_websocket():
    global sws, ws_running, last_heartbeat
    logger.info("="*60)
    logger.info("WEBSOCKET THREAD STARTED")
    logger.info(f"Current IST time: {(datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Market open check: {is_market_open()}")
    logger.info("="*60)
    refresh_all_tokens()
    while True:
        try:
            if not is_market_open():
                time.sleep(5)
                last_heartbeat = time.time()
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
            ws_running = True
            sws.connect()
        except Exception as e:
            logger.error(f"WebSocket thread error: {e}")
            ws_running = False
            sws = None
            time.sleep(10)

# REST API POLLER
def is_market_open():
    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    current_time = now_ist.time()
    is_weekday = now_ist.weekday() < 5
    market_open = dt_time(9, 10)
    market_close = dt_time(15, 35)
    is_trading_hours = market_open <= current_time <= market_close
    return is_weekday and is_trading_hours

def start_rest_api_poller():
    global last_heartbeat
    logger.info("REST API poller started - PRIMARY DATA SOURCE")
    auth_obj = None
    auth_time = 0
    spot_cache = {idx: {"price": 0, "time": 0} for idx in INDEX_CONFIG}
    spot_cache_ttl = 5
    poll_count = 0
    while True:
        try:
            last_heartbeat = time.time()
            if not is_market_open():
                time.sleep(5)
                poll_count += 1
                if poll_count % 60 == 0:
                    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
                    logger.info(f"Market CLOSED | IST: {now_ist.strftime('%H:%M')} | Waiting for 09:15 IST...")
                continue
            poll_count = 0
            now = time.time()
            if not auth_obj or (now - auth_time > 3000):
                _, _, auth_obj = get_auth_token()
                auth_time = now
            if not auth_obj:
                time.sleep(10)
                continue
            for idx in INDEX_CONFIG:
                if now - spot_cache[idx]["time"] > spot_cache_ttl:
                    spot = get_index_spot(idx)
                    if spot and spot > 0:
                        valid_ranges = {
                            "NIFTY": (15000, 30000),
                            "BANKNIFTY": (30000, 70000),
                            "FINNIFTY": (15000, 30000),
                            "MIDCPNIFTY": (8000, 18000),
                            "SENSEX": (50000, 100000)
                        }
                        min_val, max_val = valid_ranges.get(idx, (0, 999999))
                        if not (min_val < spot < max_val):
                            logger.warning(f"REST POLLER: Spot price {spot} out of valid range for {idx}, skipping")
                            continue
                        price_histories[idx].append(spot)
                        latest_ticks[idx]["spot_price"] = spot
                        spot_cache[idx] = {"price": spot, "time": now}
                        logger.info(f"REST POLLER: {idx} spot fetched: {spot}")
            if int(now) % 30 < 10:
                vix = get_vix_value()
                if vix:
                    latest_ticks["VIX"]["vix"] = vix
                    vix_history.append(vix)
            for idx, tokens in INDEX_TOKENS.items():
                if tokens.get("ce_token") and tokens.get("pe_token") and tokens.get("ce_symbol") and tokens.get("pe_symbol"):
                    try:
                        spot = latest_ticks[idx]["spot_price"]
                        ce_resp = auth_obj.ltpData(INDEX_CONFIG[idx]["option_exchange"], tokens["ce_symbol"], tokens["ce_token"])
                        ce = safe_ltp(ce_resp)
                        if ce is not None:
                            if ce > 100000: ce /= 100
                            if ce > 0 and ce < 10000:
                                if is_valid_option_premium(ce, spot, "CE"):
                                    ce_price_histories[idx].append(ce)
                                    latest_ticks[idx]["ce_price"] = ce
                                    logger.info(f"REST POLLER: {idx} CE fetched: {ce}")
                                else:
                                    logger.warning(f"REST POLLER: {idx} CE price {ce} invalid for spot {spot}")
                            else:
                                logger.warning(f"REST POLLER: {idx} CE price {ce} out of range")
                        pe_resp = auth_obj.ltpData(INDEX_CONFIG[idx]["option_exchange"], tokens["pe_symbol"], tokens["pe_token"])
                        pe = safe_ltp(pe_resp)
                        if pe is not None:
                            if pe > 100000: pe /= 100
                            if pe > 0 and pe < 10000:
                                if is_valid_option_premium(pe, spot, "PE"):
                                    pe_price_histories[idx].append(pe)
                                    latest_ticks[idx]["pe_price"] = pe
                                    logger.info(f"REST POLLER: {idx} PE fetched: {pe}")
                                else:
                                    logger.warning(f"REST POLLER: {idx} PE price {pe} invalid for spot {spot}")
                            else:
                                logger.warning(f"REST POLLER: {idx} PE price {pe} out of range")
                    except Exception as e:
                        logger.debug(f"REST {idx} option fetch error: {e}")
            try:
                run_all_signals()
            except Exception as e:
                logger.error(f"REST poller signal engine error: {e}")
            time.sleep(10)
        except Exception as e:
            logger.error(f"REST poller error: {e}")
            auth_obj = None
            time.sleep(10)

# BACKGROUND THREADS
_init_completed = False
_init_lock = threading.Lock()

def _start_background_threads():
    global _init_completed
    with _init_lock:
        if not _init_completed:
            threading.Thread(target=start_angel_websocket, daemon=True).start()
            threading.Thread(target=start_rest_api_poller, daemon=True).start()
            _init_completed = True
            logger.info("Background threads started")

@app.before_request
def start_backgrounds():
    _start_background_threads()

# FLASK ROUTES - V11.0
@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "healthy",
        "engine": "Multi-Index Options Bot v11.0 (8-Timeframes + Market Analysis Exit + Trend Cooldown)",
        "indices": list(INDEX_CONFIG.keys()),
        "market_open": is_market_open(),
        "timestamp": time.time()
    })

@app.route("/api/live-signals", methods=["GET"])
def live_signals():
    sentiment_data = {}
    for idx in INDEX_CONFIG:
        sentiment_data[idx] = {
            "score": market_sentiment[idx]["score"],
            "label": market_sentiment[idx]["label"],
            "trend_1min": market_sentiment[idx]["trend_1min"],
            "trend_2min": market_sentiment[idx]["trend_2min"],
            "trend_3min": market_sentiment[idx]["trend_3min"],
            "trend_5min": market_sentiment[idx]["trend_5min"],
            "trend_10min": market_sentiment[idx]["trend_10min"],
            "trend_15min": market_sentiment[idx]["trend_15min"],
            "trend_20min": market_sentiment[idx]["trend_20min"],
            "trend_30min": market_sentiment[idx]["trend_30min"]
        }
    return jsonify({
        "timestamp": datetime.now().isoformat(),
        "signals": market_signal,
        "sentiment": sentiment_data,
        "portfolios": portfolio_state,
        "safety": safety_state,
        "tokens": INDEX_TOKENS,
        "market_open": is_market_open(),
        "debug": {"ws_running": ws_running, "ticks": tick_counter},
        "version": "11.0"
    })

@app.route("/api/health")
def health():
    now = time.time()
    heartbeat_age = now - last_heartbeat
    is_healthy = heartbeat_age < 60
    status_code = 200 if is_healthy else 503
    return jsonify({
        "status": "OK" if is_healthy else "STALE",
        "ws_running": ws_running,
        "ticks_received": tick_counter,
        "market_open": is_market_open(),
        "last_heartbeat_age_sec": round(heartbeat_age, 1),
        "threads_alive": _init_completed,
        "version": "11.0"
    }), status_code

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    _start_background_threads()
    app.run(host="0.0.0.0", port=port, debug=False)
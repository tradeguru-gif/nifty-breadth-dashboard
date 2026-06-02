# === VERSION 9.1 - STABLE MULTI-INDEX BOT (NIFTY/BANKNIFTY/FINNIFTY ONLY) ===
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
    c.execute("""CREATE TABLE IF NOT EXISTS trades (timestamp REAL, action TEXT, entry_price REAL, exit_price REAL, pnl REAL, size_pct REAL, status TEXT, grade TEXT, atr REAL, vix REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS daily_performance (date TEXT, equity REAL, daily_pnl REAL, drawdown_pct REAL, sharpe REAL, var REAL, win_rate REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS ml_models (id INTEGER PRIMARY KEY, model BLOB, created_at REAL, features TEXT, accuracy REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS backtest_results (id INTEGER PRIMARY KEY, date TEXT, strategy TEXT, trades INTEGER, win_rate REAL, profit_factor REAL, max_drawdown REAL, sharpe REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS greeks (timestamp REAL, delta REAL, gamma REAL, theta REAL, vega REAL, iv REAL, ce_delta REAL, pe_delta REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS market_profile (timestamp REAL, poc REAL, value_area_high REAL, value_area_low REAL, vwap REAL, volume_profile TEXT)""")
    conn.commit()
    conn.close()

init_db()

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ================== INDEX CONFIGURATION ==================
# Only indices that resolved successfully are active
INDEX_CONFIG = {
    "NIFTY": {
        "token": "99926000", "exchange": "NSE", "symbol": "NIFTY",
        "lot_size": 50, "expiry_weekday": 4, "active": True,
        "min_premium": 10, "max_premium": 5000, "atm_strike_multiple": 50,
        "option_exchange": "NFO"
    },
    "BANKNIFTY": {
        "token": "99926009", "exchange": "NSE", "symbol": "BANKNIFTY",
        "lot_size": 25, "expiry_weekday": 4, "active": True,
        "min_premium": 10, "max_premium": 5000, "atm_strike_multiple": 100,
        "option_exchange": "NFO"
    },
    "FINNIFTY": {
        "token": "99926037", "exchange": "NSE", "symbol": "FINNIFTY",
        "lot_size": 40, "expiry_weekday": 2, "active": True,
        "min_premium": 10, "max_premium": 5000, "atm_strike_multiple": 50,
        "option_exchange": "NFO"
    },
    # MIDCPNIFTY and SENSEX are disabled until symbol format is confirmed
    "MIDCPNIFTY": {
        "token": "99926031", "exchange": "NSE", "symbol": "MIDCPNIFTY",
        "lot_size": 75, "expiry_weekday": 4, "active": False,
        "min_premium": 10, "max_premium": 5000, "atm_strike_multiple": 50,
        "option_exchange": "NFO"
    },
    "SENSEX": {
        "token": None, "exchange": "BSE", "symbol": "SENSEX",
        "lot_size": 15, "expiry_weekday": 4, "active": False,
        "min_premium": 10, "max_premium": 5000, "atm_strike_multiple": 100,
        "option_exchange": "BFO"
    }
}

ACTIVE_INDEX = "NIFTY"

# Global state (per active index)
INDEX_TOKENS = {idx: {"ce_token": None, "pe_token": None, "atm_strike": 0, "expiry": "", "ce_symbol": "", "pe_symbol": ""} for idx in INDEX_CONFIG}
price_histories = {idx: deque(maxlen=2000) for idx in INDEX_CONFIG}
ce_price_histories = {idx: deque(maxlen=2000) for idx in INDEX_CONFIG}
pe_price_histories = {idx: deque(maxlen=2000) for idx in INDEX_CONFIG}
ce_volume_histories = {idx: deque(maxlen=1000) for idx in INDEX_CONFIG}
pe_volume_histories = {idx: deque(maxlen=1000) for idx in INDEX_CONFIG}
ce_oi_histories = {idx: deque(maxlen=200) for idx in INDEX_CONFIG}
pe_oi_histories = {idx: deque(maxlen=200) for idx in INDEX_CONFIG}

vix_history = deque(maxlen=200)
banknifty_history = deque(maxlen=1000)

latest_ticks = {idx: {"spot_price": 0.0, "ce_price": 0.0, "pe_price": 0.0,
                      "ce_volume": 0, "pe_volume": 0, "ce_oi": 0, "pe_oi": 0} for idx in INDEX_CONFIG}
latest_ticks["VIX"] = {"vix": 15.0}

tick_counter = 0
ws_running = False
sws = None
last_tick_time = time.time()

# ================== SENTIMENT ENGINE ==================
TIMEFRAMES = ["1min", "5min", "15min"]
TIMEFRAME_WEIGHTS = {"1min": 30, "5min": 40, "15min": 30}
EMA_SHORT, EMA_MEDIUM, EMA_LONG = 9, 21, 50

SENTIMENT_SCORES = {
    "STRONG_BULLISH": (85, 100, "🟢 STRONG BULLISH", "STRONG_BUY_CE"),
    "BULLISH": (70, 84, "🟢 BULLISH", "BUY_CE"),
    "SLOW_BULLISH": (55, 69, "🟡 SLOW BULLISH", "LOW_BUY_CE"),
    "NEUTRAL": (45, 54, "⚪ NEUTRAL", "NO_TRADE"),
    "SLOW_BEARISH": (30, 44, "🟠 SLOW BEARISH", "LOW_BUY_PE"),
    "BEARISH": (15, 29, "🔴 BEARISH", "BUY_PE"),
    "STRONG_BEARISH": (0, 14, "🔴 STRONG BEARISH", "STRONG_BUY_PE")
}

market_sentiment = {idx: {"score": 50, "label": "NEUTRAL", "trend_1m": "NEUTRAL", "trend_5m": "NEUTRAL", "trend_15m": "NEUTRAL"} for idx in INDEX_CONFIG}
signal_state = {idx: {"action": "HOLD", "entry_price": 0, "stop_loss": 0, "target": 0, "lots": 1, "cooldown": 0, "confidence": 0, "entry_time": 0, "highest": 0} for idx in INDEX_CONFIG}
portfolio_state = {idx: {"equity": 100000.0, "open_positions": 0, "daily_trades": 0} for idx in INDEX_CONFIG}
market_signal = {idx: {"signal": "WAITING", "sentiment_score": 50, "alert_message": ""} for idx in INDEX_CONFIG}
safety_state = {idx: {"consecutive_sl": 0, "circuit_breaker": False} for idx in INDEX_CONFIG}
signal_buffer = {idx: {"ce_count": 0, "pe_count": 0, "consecutive_ce": 0, "consecutive_pe": 0} for idx in INDEX_CONFIG}
daily_trade_count = {idx: 0 for idx in INDEX_CONFIG}
last_trade_date = {idx: "" for idx in INDEX_CONFIG}

def calculate_rsi(prices, period=14, smoothing=3):
    if len(prices) < period + 1: return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0: return 100.0
    rsi_raw = 100 - (100 / (1 + avg_gain / avg_loss))
    if smoothing > 1 and len(prices) >= period + smoothing:
        rsi_vals = []
        for j in range(smoothing):
            sub_gains = gains[-(period+j):-j if j > 0 else None]
            sub_losses = losses[-(period+j):-j if j > 0 else None]
            if len(sub_gains) == period:
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
    if len(prices) < period: return sum(prices) / len(prices) if prices else 0
    alpha = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = alpha * p + (1 - alpha) * ema
    return ema

def calculate_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal: return 0.0, 0.0, 0.0
    def ema(arr, p):
        alpha = 2 / (p + 1)
        val = arr[0]
        for x in arr[1:]: val = alpha * x + (1 - alpha) * val
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
    return sum(tr[-period:]) / period

def get_trend_score(prices, tf_name):
    if len(prices) < 60: return "NEUTRAL", 0
    w = TIMEFRAME_WEIGHTS[tf_name]
    ema9 = calculate_ema(prices, EMA_SHORT)
    ema21 = calculate_ema(prices, EMA_MEDIUM)
    ema50 = calculate_ema(prices, EMA_LONG)
    price = prices[-1]
    if tf_name == "1min":
        if ema9 > ema21 and price > ema9: return "BULLISH", w
        if ema9 < ema21 and price < ema9: return "BEARISH", -w
        return "NEUTRAL", 0
    elif tf_name == "5min":
        if ema9 > ema21 > ema50: return "BULLISH", w
        if ema9 < ema21 < ema50: return "BEARISH", -w
        if ema9 > ema21 and price > ema9: return "BULLISH", w - 10
        if ema9 < ema21 and price < ema9: return "BEARISH", -(w - 10)
        return "NEUTRAL", 0
    else:
        if len(prices) >= 20:
            highs = [max(prices[-i-5:-i]) for i in range(0, 15, 5) if i+5 <= len(prices)]
            lows  = [min(prices[-i-5:-i]) for i in range(0, 15, 5) if i+5 <= len(prices)]
            if len(highs) >= 2 and len(lows) >= 2:
                if highs[0] > highs[1] and lows[0] > lows[1]: return "BULLISH", w
                if highs[0] < highs[1] and lows[0] < lows[1]: return "BEARISH", -w
                if highs[0] > highs[1]: return "BULLISH", w - 10
                if highs[0] < highs[1]: return "BEARISH", -(w - 10)
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
    sentiment = 50 + (total / 2)
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
            if "LOW" in action and market_sentiment[index_name]["trend_15m"] in ["BULLISH" if "PE" in action else "BEARISH"]:
                return "NO_TRADE", label, sentiment
            return action, label, sentiment
    return "NO_TRADE", "UNKNOWN", sentiment

# ================== AUTH & TOKEN (with rate limit protection) ==================
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
            logger.error(f"Auth fail: {e}")
            return None, None, None

def get_index_spot(index_name):
    if not INDEX_CONFIG[index_name].get("active"):
        return None
    config = INDEX_CONFIG[index_name]
    _, _, obj = get_auth_token()
    if not obj: return None
    try:
        resp = obj.ltpData(config["exchange"], config["symbol"], config["token"])
        if resp.get("status") and resp.get("data"):
            ltp = float(resp["data"]["ltp"])
            if ltp > 100000: ltp /= 100
            return ltp
    except Exception as e:
        logger.error(f"Spot fetch {index_name}: {e}")
    return None

def get_vix_value():
    _, _, obj = get_auth_token()
    if not obj: return 15.0
    try:
        resp = obj.ltpData("NSE", "INDIAVIX", "99919017")
        if resp.get("status") and resp.get("data"):
            return float(resp["data"]["ltp"])
    except Exception:
        pass
    return 15.0

_scrip_cache = {"data": None, "timestamp": 0}
def get_scrip_master():
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

def get_current_atm_tokens(index_name):
    if not INDEX_CONFIG[index_name].get("active"):
        return None, None
    config = INDEX_CONFIG[index_name]
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

    # Try scrip master
    scrip = get_scrip_master()
    if scrip:
        df = pd.DataFrame(scrip)
        opts = df[(df["name"] == config["symbol"]) &
                  (df["instrumenttype"] == "OPTIDX") &
                  (df["exch_seg"] == config["option_exchange"])].copy()
        if not opts.empty:
            opts["expiry_date"] = pd.to_datetime(opts["expiry"], format="%d%b%Y", errors="coerce")
            opts = opts.dropna(subset=["expiry_date"])
            opts["strike"] = pd.to_numeric(opts["strike"], errors="coerce") / 100
            future = opts[opts["expiry_date"] >= today]
            if not future.empty:
                nearest = future["expiry_date"].min()
                atm_opts = future[(future["strike"] == atm) & (future["expiry_date"] == nearest)]
                if atm_opts.empty:
                    same_exp = future[future["expiry_date"] == nearest]
                    if not same_exp.empty:
                        idx = (same_exp["strike"] - atm).abs().idxmin()
                        atm_opts = same_exp.loc[[idx]]
                ce = atm_opts[atm_opts["symbol"].str.contains("CE")]
                pe = atm_opts[atm_opts["symbol"].str.contains("PE")]
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
    # Fallback search (only once per index, avoid rate limit)
    _, _, obj = get_auth_token()
    if obj:
        try:
            ce_resp = obj.searchScrip(config["option_exchange"], f"{config['symbol']}{atm}CE")
            pe_resp = obj.searchScrip(config["option_exchange"], f"{config['symbol']}{atm}PE")
            if ce_resp and ce_resp.get("status") and ce_resp.get("data"):
                ce_token = str(ce_resp["data"][0].get("symboltoken"))
            if pe_resp and pe_resp.get("status") and pe_resp.get("data"):
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

# ================== RISK & EXITS ==================
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
    risk_amount = portfolio_state[index_name]["equity"] * (risk_pct / 100)
    stop_dist = atr * 1.5
    if stop_dist > 0:
        lots = int(risk_amount / (stop_dist * config["lot_size"]))
        lots = max(1, min(5, lots))
    else:
        lots = 1
    return lots, risk_pct

def reset_signal_state(index_name, current_time):
    signal_state[index_name].update({
        "action": "HOLD", "entry_price": 0, "stop_loss": 0, "target": 0,
        "lots": 1, "cooldown": current_time + 60, "confidence": 0, "entry_time": 0, "highest": 0
    })
    portfolio_state[index_name]["open_positions"] = 0

def send_telegram_alert(msg):
    if not TELEGRAM_AVAILABLE or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}, timeout=3)
    except Exception as e:
        logger.error(f"Telegram error: {e}")

# ================== SIGNAL ENGINE (per index) ==================
def run_signal_engine_for_index(index_name):
    if not INDEX_CONFIG[index_name].get("active"): return
    prices = list(price_histories[index_name])
    if len(prices) < 30:
        market_signal[index_name]["alert_message"] = f"Collecting ({len(prices)}/30)"
        return
    now = time.time()
    spot = prices[-1]
    sentiment = compute_sentiment(index_name)
    action, label, conf = get_signal_from_sentiment(index_name, sentiment)

    rsi = calculate_rsi(prices)
    _, _, macd_hist = calculate_macd(prices)
    atr = calculate_atr(prices)
    vix = latest_ticks["VIX"]["vix"]

    # --- EXIT CHECKS ---
    if signal_state[index_name]["action"] != "HOLD":
        active = signal_state[index_name]["action"]
        prem = latest_ticks[index_name]["ce_price"] if "CE" in active else latest_ticks[index_name]["pe_price"]
        if prem > 0:
            pnl = prem - signal_state[index_name]["entry_price"]
            if prem > signal_state[index_name].get("highest", 0):
                signal_state[index_name]["highest"] = prem
                new_sl = prem - (atr * 1.8)
                if new_sl > signal_state[index_name]["stop_loss"]:
                    signal_state[index_name]["stop_loss"] = new_sl
            if prem <= signal_state[index_name]["stop_loss"]:
                pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                portfolio_state[index_name]["equity"] += pnl_total
                daily_trade_count[index_name] += 1
                send_telegram_alert(f"⚠️ EXIT {index_name} | SL | PnL: {pnl:.2f} pts")
                reset_signal_state(index_name, now)
                return
            if prem >= signal_state[index_name]["target"]:
                pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                portfolio_state[index_name]["equity"] += pnl_total
                daily_trade_count[index_name] += 1
                send_telegram_alert(f"🎯 TARGET {index_name} | PnL: {pnl:.2f} pts")
                reset_signal_state(index_name, now)
                return
            if (now - signal_state[index_name]["entry_time"]) / 60 >= 45:
                pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                portfolio_state[index_name]["equity"] += pnl_total
                daily_trade_count[index_name] += 1
                send_telegram_alert(f"⏰ TIME EXIT {index_name} | PnL: {pnl:.2f} pts")
                reset_signal_state(index_name, now)
                return
        market_signal[index_name]["alert_message"] = f"ACTIVE {active} {index_name}"
        market_signal[index_name]["signal"] = "ACTIVE"
    else:
        if now < signal_state[index_name]["cooldown"]:
            market_signal[index_name]["alert_message"] = f"Cooldown {int(signal_state[index_name]['cooldown']-now)}s"
            return
        today = datetime.now().strftime("%Y-%m-%d")
        if last_trade_date[index_name] != today:
            daily_trade_count[index_name] = 0
            last_trade_date[index_name] = today
        if daily_trade_count[index_name] >= 10:
            market_signal[index_name]["alert_message"] = "Max daily trades"
            return
        prem = latest_ticks[index_name]["ce_price"] if "CE" in action else latest_ticks[index_name]["pe_price"] if "PE" in action else 0
        min_prem = INDEX_CONFIG[index_name].get("min_premium", 10)
        if prem <= 0 or prem < min_prem:
            market_signal[index_name]["alert_message"] = f"Premium invalid ₹{prem}"
            return
        buf = signal_buffer[index_name]
        if "CE" in action:
            buf["ce_count"] += 1
            buf["pe_count"] = 0
            if buf["ce_count"] < 2:
                market_signal[index_name]["alert_message"] = f"Building CE ({buf['ce_count']}/2)"
                return
        elif "PE" in action:
            buf["pe_count"] += 1
            buf["ce_count"] = 0
            if buf["pe_count"] < 2:
                market_signal[index_name]["alert_message"] = f"Building PE ({buf['pe_count']}/2)"
                return
        else:
            buf["ce_count"] = buf["pe_count"] = 0

        lots, risk = calculate_position_size(index_name, action, atr, vix)
        sl = prem - (atr * 1.5) if "CE" in action else prem - (atr * 1.5)
        target = prem + (atr * 3.5) if "CE" in action else prem + (atr * 3.5)
        if "LOW" in action:
            sl = prem - (atr * 1.0)
            target = prem + (atr * 2.5)
        signal_state[index_name].update({
            "action": action, "entry_price": prem, "stop_loss": sl, "target": target,
            "lots": lots, "entry_time": now, "highest": prem, "confidence": conf
        })
        portfolio_state[index_name]["open_positions"] = 1
        if "CE" in action:
            buf["consecutive_ce"] += 1
            buf["consecutive_pe"] = 0
        else:
            buf["consecutive_pe"] += 1
            buf["consecutive_ce"] = 0
        daily_trade_count[index_name] += 1
        emoji = "🟢" if "STRONG" in action and "CE" in action else "🔴" if "STRONG" in action and "PE" in action else "🟡" if "LOW" in action else "⚪"
        msg = f"{emoji} {action} {index_name}\nSpot: {spot:.2f} | Prem: ₹{prem:.2f}\nSL: {sl:.2f} Tgt: {target:.2f}\nSentiment: {sentiment:.1f} ({label})\nLots: {lots}"
        send_telegram_alert(msg)
        logger.info(f"ENTRY {index_name} {action}")

    market_signal[index_name].update({
        "spot_price": spot, "ce_price": latest_ticks[index_name]["ce_price"], "pe_price": latest_ticks[index_name]["pe_price"],
        "sentiment_score": sentiment, "sentiment": label, "signal": signal_state[index_name]["action"],
        "confidence": conf, "trend_1m": market_sentiment[index_name]["trend_1m"],
        "trend_5m": market_sentiment[index_name]["trend_5m"], "trend_15m": market_sentiment[index_name]["trend_15m"],
        "timestamp": datetime.now().isoformat()
    })

def run_all_signals():
    for idx in INDEX_CONFIG:
        if INDEX_CONFIG[idx].get("active"):
            run_signal_engine_for_index(idx)

# ================== WEBSOCKET (ORIGINAL STYLE) ==================
def on_ws_open(wsapp):
    global sws
    logger.info("WebSocket opened")
    tokens = []
    for idx, cfg in INDEX_CONFIG.items():
        if cfg.get("active"):
            tokens.append(cfg["token"])
            if INDEX_TOKENS[idx].get("ce_token"):
                tokens.append(INDEX_TOKENS[idx]["ce_token"])
            if INDEX_TOKENS[idx].get("pe_token"):
                tokens.append(INDEX_TOKENS[idx]["pe_token"])
    tokens.append("99919017")
    tokens = list(set([t for t in tokens if t]))
    if sws and tokens:
        try:
            sws.subscribe("tradeguru_001", 1, [{"exchangeType": 1, "tokens": tokens}])
            logger.info(f"Subscribed to {len(tokens)} tokens")
        except Exception as e:
            logger.error(f"Subscribe failed: {e}")

def on_ws_error(wsapp, error):
    logger.error(f"WebSocket Error: {error}")

def on_ws_close(wsapp, close_status_code=None, close_msg=None):
    global ws_running
    ws_running = False
    logger.warning(f"WebSocket closed: {close_status_code} {close_msg}")

def on_ws_data(wsapp, message):
    global tick_counter, last_tick_time
    last_tick_time = time.time()
    try:
        ticks = []
        if isinstance(message, bytes):
            if sws and hasattr(sws, '_parse_binary_data'):
                parsed = sws._parse_binary_data(message)
                if parsed and isinstance(parsed, dict):
                    ticks = [parsed]
        elif isinstance(message, str):
            data = json.loads(message)
            ticks = data if isinstance(data, list) else [data]
        elif isinstance(message, dict):
            ticks = [message]
        elif isinstance(message, list):
            ticks = message

        for tick in ticks:
            if not isinstance(tick, dict): continue
            token = str(tick.get("token") or tick.get("tk") or "")
            ltp = tick.get("ltp") or tick.get("last_traded_price") or tick.get("lp") or 0
            if isinstance(ltp, str):
                try: ltp = float(ltp)
                except: ltp = 0
            if isinstance(ltp, (int, float)) and ltp > 100000:
                ltp /= 100
            vol = tick.get("v") or tick.get("volume") or 0
            oi = tick.get("oi") or tick.get("open_interest") or 0

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
                    if ltp > 0:
                        latest_ticks[idx]["ce_price"] = ltp
                        ce_price_histories[idx].append(ltp)
                        ce_volume_histories[idx].append(vol)
                        ce_oi_histories[idx].append(oi)
                elif token == INDEX_TOKENS[idx].get("pe_token"):
                    if ltp > 0:
                        latest_ticks[idx]["pe_price"] = ltp
                        pe_price_histories[idx].append(ltp)
                        pe_volume_histories[idx].append(vol)
                        pe_oi_histories[idx].append(oi)
            elif token == "99919017":
                if ltp > 0:
                    latest_ticks["VIX"]["vix"] = ltp
                    vix_history.append(ltp)
    except Exception as e:
        logger.error(f"WS data error: {e}")

def start_angel_websocket():
    global sws, ws_running
    logger.info("WebSocket thread started (multi-index)")
    refresh_all_tokens()
    while True:
        try:
            if not is_market_open():
                time.sleep(30)
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

# ================== REST API POLLER (BACKUP) ==================
def is_market_open():
    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    return now_ist.weekday() < 5 and dt_time(9,15) <= now_ist.time() <= dt_time(15,30)

def start_rest_api_poller():
    logger.info("REST API poller started (backup data source)")
    while True:
        try:
            if not is_market_open():
                time.sleep(30)
                continue
            for idx in INDEX_CONFIG:
                if not INDEX_CONFIG[idx].get("active"): continue
                spot = get_index_spot(idx)
                if spot and spot > 0:
                    price_histories[idx].append(spot)
                    latest_ticks[idx]["spot_price"] = spot
            vix = get_vix_value()
            if vix:
                latest_ticks["VIX"]["vix"] = vix
                vix_history.append(vix)
            for idx, tokens in INDEX_TOKENS.items():
                if not INDEX_CONFIG[idx].get("active"): continue
                if tokens["ce_token"] and tokens["pe_token"]:
                    _, _, obj = get_auth_token()
                    if obj:
                        try:
                            ce_resp = obj.ltpData(INDEX_CONFIG[idx]["option_exchange"], tokens["ce_symbol"], tokens["ce_token"])
                            if ce_resp.get("status") and ce_resp.get("data"):
                                ce = float(ce_resp["data"]["ltp"])
                                if ce > 100000: ce /= 100
                                latest_ticks[idx]["ce_price"] = ce
                                ce_price_histories[idx].append(ce)
                            pe_resp = obj.ltpData(INDEX_CONFIG[idx]["option_exchange"], tokens["pe_symbol"], tokens["pe_token"])
                            if pe_resp.get("status") and pe_resp.get("data"):
                                pe = float(pe_resp["data"]["ltp"])
                                if pe > 100000: pe /= 100
                                latest_ticks[idx]["pe_price"] = pe
                                pe_price_histories[idx].append(pe)
                        except Exception as e:
                            logger.debug(f"REST {idx} option fetch error: {e}")
            run_all_signals()
            time.sleep(10)
        except Exception as e:
            logger.error(f"REST poller error: {e}")
            time.sleep(10)

# ================== BACKGROUND THREADS ==================
_init_completed = False

@app.before_request
def start_backgrounds():
    global _init_completed
    if not _init_completed:
        threading.Thread(target=start_angel_websocket, daemon=True).start()
        threading.Thread(target=start_rest_api_poller, daemon=True).start()
        _init_completed = True

# ================== FLASK ROUTES ==================
@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "healthy",
        "engine": "Multi-Index Bot (NIFTY/BANKNIFTY/FINNIFTY Active)",
        "indices": [idx for idx, cfg in INDEX_CONFIG.items() if cfg.get("active")],
        "market_open": is_market_open(),
        "timestamp": time.time()
    })

@app.route("/api/live-signals", methods=["GET"])
def live_signals():
    return jsonify({
        "timestamp": datetime.now().isoformat(),
        "signals": market_signal,
        "sentiment": {idx: {"score": market_sentiment[idx]["score"], "label": market_sentiment[idx]["label"],
                            "trend_1m": market_sentiment[idx]["trend_1m"], "trend_5m": market_sentiment[idx]["trend_5m"],
                            "trend_15m": market_sentiment[idx]["trend_15m"]} for idx in INDEX_CONFIG},
        "portfolios": portfolio_state,
        "tokens": INDEX_TOKENS,
        "debug": {"ws_running": ws_running, "ticks": tick_counter}
    })

@app.route("/api/health")
def health():
    return jsonify({"status": "OK", "ws_running": ws_running, "ticks_received": tick_counter})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
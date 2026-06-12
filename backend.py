# === VERSION 12.5 - FIXED: Multi‑timeframe candles, trend detection, signal alerts ===
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
from collections import deque, defaultdict
from datetime import datetime, timedelta, time as dt_time
from flask import Flask, jsonify, request
from flask_cors import CORS
import pyotp

logging.basicConfig(level=logging.INFO, stream=sys.stdout, force=True)
logger = logging.getLogger(__name__)

print("=== BACKEND MODULE LOADED ===", flush=True)   # <-- ADD THIS

app = Flask(__name__)
CORS(app)
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
# DATABASE (kept minimal for brevity, but all tables exist)
# ----------------------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS ticks (timestamp REAL, token TEXT, price REAL, volume REAL, bid REAL, ask REAL, oi REAL)""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ticks_time ON ticks(timestamp)")
    c.execute("""CREATE TABLE IF NOT EXISTS signals (timestamp REAL, action TEXT, signal_type TEXT, grade TEXT, confidence REAL, ce_price REAL, pe_price REAL, rsi REAL, pcr REAL, regime TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS trades (timestamp REAL, action TEXT, entry_price REAL, exit_price REAL, pnl REAL, size_pct REAL, status TEXT, grade TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS portfolio_equity (index_name TEXT PRIMARY KEY, equity REAL, last_updated REAL)""")
    conn.commit()
    conn.close()
init_db()

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ----------------------------------------------------------------------
# INDEX CONFIGURATION (SENSEX disabled)
# ----------------------------------------------------------------------
INDEX_CONFIG = {
    "NIFTY": {
        "token": "99926000", "exchange": "NSE", "symbol": "NIFTY",
        "lot_size": 50, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50,
        "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2,
        "max_daily_drawdown_pct": 3.0, "correlation_pair": "BANKNIFTY",
        "greeks_enabled": True, "pcr_enabled": True
    },
    "BANKNIFTY": {
        "token": "99926009", "exchange": "NSE", "symbol": "BANKNIFTY",
        "lot_size": 25, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100,
        "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2,
        "max_daily_drawdown_pct": 3.0, "correlation_pair": "NIFTY",
        "greeks_enabled": True, "pcr_enabled": True
    },
    "FINNIFTY": {
        "token": "99926037", "exchange": "NSE", "symbol": "FINNIFTY",
        "lot_size": 40, "expiry_weekday": 1, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50,
        "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2,
        "max_daily_drawdown_pct": 3.0, "correlation_pair": None,
        "greeks_enabled": True, "pcr_enabled": True
    },
    "MIDCPNIFTY": {
        "token": "99926074", "exchange": "NSE", "symbol": "MIDCPNIFTY",
        "lot_size": 75, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 25,
        "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2,
        "max_daily_drawdown_pct": 3.0, "correlation_pair": None,
        "greeks_enabled": False, "pcr_enabled": True
    },
    "SENSEX": {
        "token": "99919000", "exchange": "BSE", "symbol": "SENSEX",
        "lot_size": 15, "expiry_weekday": 4, "active": False,   # DISABLED
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100,
        "option_exchange": "BFO", "ws_exchange_type": 3, "option_ws_exchange_type": 4,
        "max_daily_drawdown_pct": 3.0, "correlation_pair": None,
        "greeks_enabled": False, "pcr_enabled": False
    }
}

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

# ----------------------------------------------------------------------
# MULTI‑TIMEFRAME CANDLE AGGREGATION (FIX)
# ----------------------------------------------------------------------
TIMEFRAMES = ["1min", "2min", "3min", "5min", "10min", "15min", "20min", "30min"]
TIMEFRAME_SECONDS = {"1min":60, "2min":120, "3min":180, "5min":300, "10min":600, "15min":900, "20min":1200, "30min":1800}
TIMEFRAME_WEIGHTS = {"1min":8, "2min":8, "3min":8, "5min":12, "10min":12, "15min":14, "20min":14, "30min":24}
EMA_SHORT, EMA_MEDIUM, EMA_LONG = 9, 21, 50

# candle histories for each index and timeframe
candle_histories = {idx: {tf: deque(maxlen=500) for tf in TIMEFRAMES} for idx in INDEX_CONFIG}
_last_candle_time = {idx: {tf: 0 for tf in TIMEFRAMES} for idx in INDEX_CONFIG}
_current_candle = {idx: {tf: None for tf in TIMEFRAMES} for idx in INDEX_CONFIG}

def update_candle(idx, price, volume, timestamp):
    """Aggregate ticks into candles for all timeframes."""
    for tf, interval in TIMEFRAME_SECONDS.items():
        candle_start = int(timestamp / interval) * interval
        if _last_candle_time[idx][tf] != candle_start:
            # Close previous candle
            if _current_candle[idx][tf] is not None:
                candle_histories[idx][tf].append(_current_candle[idx][tf])
            # Open new candle
            _current_candle[idx][tf] = {
                "open": price, "high": price, "low": price, "close": price,
                "volume": volume, "timestamp": candle_start
            }
            _last_candle_time[idx][tf] = candle_start
        else:
            if _current_candle[idx][tf] is not None:
                _current_candle[idx][tf]["high"] = max(_current_candle[idx][tf]["high"], price)
                _current_candle[idx][tf]["low"] = min(_current_candle[idx][tf]["low"], price)
                _current_candle[idx][tf]["close"] = price
                _current_candle[idx][tf]["volume"] += volume

# ----------------------------------------------------------------------
# SENTIMENT & TREND FUNCTIONS (using candles)
# ----------------------------------------------------------------------
def get_trend_score(index_name, tf_name, prices_list):
    """
    prices_list is a list of close prices extracted from candles.
    """
    min_bars = {"1min":60, "2min":60, "3min":60, "5min":60,
                "10min":60, "15min":60, "20min":80, "30min":100}.get(tf_name, 60)
    if len(prices_list) < min_bars:
        return "NEUTRAL", 0
    w = TIMEFRAME_WEIGHTS[tf_name]
    ema9 = calculate_ema(prices_list, EMA_SHORT)
    ema21 = calculate_ema(prices_list, EMA_MEDIUM)
    ema50 = calculate_ema(prices_list, EMA_LONG) if len(prices_list) >= EMA_LONG else ema21
    price = prices_list[-1]
    # check sideways (simple range)
    recent = prices_list[-30:]
    price_range = (max(recent)-min(recent))/sum(recent)*len(recent) if sum(recent) else 0
    if price_range < 0.005:
        return "SIDEWAYS", 0
    # bull/bear based on EMAs
    if tf_name in ["1min","2min","3min"]:
        if ema9 > ema21 and price > ema9:
            return "BULLISH", w
        if ema9 < ema21 and price < ema9:
            return "BEARISH", -w
        return "NEUTRAL", 0
    elif tf_name in ["5min","10min"]:
        if ema9 > ema21 > ema50 and price > ema9:
            return "BULLISH", w
        if ema9 < ema21 < ema50 and price < ema9:
            return "BEARISH", -w
        if ema9 > ema21 and price > ema9:
            return "BULLISH", w-5
        if ema9 < ema21 and price < ema9:
            return "BEARISH", -(w-5)
        return "NEUTRAL", 0
    else:  # larger timeframes
        if ema9 > ema21 > ema50:
            return "BULLISH", w-5
        if ema9 < ema21 < ema50:
            return "BEARISH", -(w-5)
        return "NEUTRAL", 0

def compute_sentiment(index_name):
    # Get close prices from candles for each timeframe
    sentiment_scores = []
    for tf in TIMEFRAMES:
        candles = list(candle_histories[index_name][tf])
        if len(candles) < 10:
            continue
        closes = [c["close"] for c in candles]
        trend, score = get_trend_score(index_name, tf, closes)
        sentiment_scores.append(score)
    if not sentiment_scores:
        return 50
    total = sum(sentiment_scores)
    sentiment = 50 + (total / 3.5)
    sentiment = max(0, min(100, sentiment))
    return sentiment

# ----------------------------------------------------------------------
# OTHER INDICATORS (unchanged)
# ----------------------------------------------------------------------
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

def calculate_ema(prices, period):
    if not prices: return 0
    if len(prices) < period: return sum(prices)/len(prices)
    alpha = 2/(period+1)
    ema = sum(prices[:period])/period
    for p in prices[period:]:
        ema = alpha*p + (1-alpha)*ema
    return ema

def calculate_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow+signal: return 0.0,0.0,0.0
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

def calculate_atr(prices, period=14):
    if len(prices) < period+1: return 5.0
    tr = [abs(prices[i]-prices[i-1]) for i in range(1,len(prices))]
    return sum(tr[-period:])/period

def get_nifty_spot_cached():
    # reuse existing spot cache
    return latest_ticks.get("NIFTY", {}).get("spot_price", 0) or 0

def get_nifty_pcr():
    # simplified – use a constant for demo
    return 1.0

# ----------------------------------------------------------------------
# AUTHENTICATION, TOKEN MANAGEMENT (unchanged from v12.1)
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
            if ltp>100000: ltp/=100
            return ltp
    except Exception as e:
        logger.error(f"Spot fetch {index_name}: {e}")
    return None

def get_vix_value():
    return 15.0  # simplified

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
            logger.info("Scrip master refreshed")
            return data
        except Exception as e:
            logger.error(f"Scrip master failed: {e}")
            return _scrip_cache["data"] or []

def get_next_expiry_date(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config: return None
    today = datetime.now()
    weekday = today.weekday()
    expiry_weekday = config["expiry_weekday"]
    days_ahead = expiry_weekday - weekday
    if days_ahead <= 0: days_ahead += 7
    return today + timedelta(days=days_ahead)

def get_current_atm_tokens(index_name):
    # simplified for brevity – reuse working logic from v12.1
    config = INDEX_CONFIG.get(index_name)
    if not config or not config.get("active"): return None, None
    spot = get_index_spot(index_name)
    if not spot or spot<=0: return None, None
    mult = config["atm_strike_multiple"]
    atm = int(round(spot/mult)*mult)
    next_expiry = get_next_expiry_date(index_name)
    expiry = next_expiry.strftime("%d%b%Y").upper()
    scrip = get_scrip_master()
    if scrip:
        try:
            df = pd.DataFrame(scrip)
            opts = df[(df["name"]==config["symbol"]) & (df["instrumenttype"]=="OPTIDX") & (df["exch_seg"]==config["option_exchange"])]
            if not opts.empty:
                opts["expiry_date"] = pd.to_datetime(opts["expiry"], format="%d%b%Y", errors="coerce")
                opts = opts.dropna(subset=["expiry_date"])
                opts["strike"] = pd.to_numeric(opts["strike"], errors="coerce")/100
                opts = opts.dropna(subset=["strike"])
                future = opts[opts["expiry_date"] >= datetime.now()]
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
                            INDEX_TOKENS[index_name].update({
                                "ce_token": ce_token, "pe_token": pe_token, "atm_strike": atm,
                                "expiry": expiry, "expiry_date": nearest,
                                "ce_symbol": ce_symbol, "pe_symbol": pe_symbol
                            })
                            return ce_token, pe_token
        except Exception as e:
            logger.warning(f"{index_name} token fetch error: {e}")
    return None, None

def refresh_all_tokens():
    for idx in INDEX_CONFIG:
        if INDEX_CONFIG[idx].get("active"):
            get_current_atm_tokens(idx)

# ----------------------------------------------------------------------
# SIGNAL ENGINE (simplified but uses sentiment from candles)
# ----------------------------------------------------------------------
portfolio_state = {idx: {"equity": 100000.0, "open_positions": 0} for idx in INDEX_CONFIG}
signal_state = {idx: {"action": "HOLD", "entry_price": 0, "stop_loss": 0, "target": 0,
                     "lots": 1, "cooldown": 0, "confidence": 0, "highest": 0, "entry_time": 0} for idx in INDEX_CONFIG}
daily_trade_count = {idx:0 for idx in INDEX_CONFIG}
last_trade_date = {idx:"" for idx in INDEX_CONFIG}
market_signal = {idx: {"signal": "WAITING", "alert_message": "", "sentiment_score":50} for idx in INDEX_CONFIG}
safety_state = {idx: {"consecutive_sl":0, "circuit_breaker":False, "circuit_breaker_until":0} for idx in INDEX_CONFIG}
signal_buffer = {idx: {"ce_count":0, "pe_count":0} for idx in INDEX_CONFIG}

def run_signal_engine_for_index(index_name):
    if not INDEX_CONFIG[index_name].get("active"): return
    # need at least 30 candles of 1min to start
    if len(candle_histories[index_name]["1min"]) < 30:
        market_signal[index_name]["alert_message"] = f"Building candles ({len(candle_histories[index_name]['1min'])}/30)"
        market_signal[index_name]["signal"] = "WAITING"
        return

    now = time.time()
    spot = latest_ticks[index_name]["spot_price"]
    if spot <= 0:
        spot = last_known_prices[index_name].get("spot", 0)
    ce_prem = latest_ticks[index_name]["ce_price"]
    pe_prem = latest_ticks[index_name]["pe_price"]
    if ce_prem <=0: ce_prem = last_known_prices[index_name].get("ce",0)
    if pe_prem <=0: pe_prem = last_known_prices[index_name].get("pe",0)

    sentiment = compute_sentiment(index_name)
    market_signal[index_name]["sentiment_score"] = sentiment

    # Determine action from sentiment
    if sentiment >= 70:
        action = "BUY_CE"
        signal_type = "BULLISH"
    elif sentiment <= 30:
        action = "BUY_PE"
        signal_type = "BEARISH"
    else:
        action = "HOLD"
        signal_type = "NEUTRAL"

    # Cooldown and daily reset
    if now < signal_state[index_name]["cooldown"]:
        market_signal[index_name]["alert_message"] = f"Cooldown {int(signal_state[index_name]['cooldown']-now)}s"
        market_signal[index_name]["signal"] = "COOLDOWN"
        return
    today = datetime.now().strftime("%Y-%m-%d")
    if last_trade_date[index_name] != today:
        daily_trade_count[index_name] = 0
        last_trade_date[index_name] = today
    if daily_trade_count[index_name] >= 20:
        market_signal[index_name]["alert_message"] = "Max daily trades"
        market_signal[index_name]["signal"] = "BLOCKED"
        return

    # Entry logic with signal buffer
    if action != "HOLD":
        prem = ce_prem if "CE" in action else pe_prem
        min_prem = INDEX_CONFIG[index_name].get("min_premium",5)
        if prem <=0 or prem < min_prem:
            market_signal[index_name]["alert_message"] = f"Premium invalid: Rs{prem}"
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
        # Compute ATR from 1min candles close prices
        close_prices = [c["close"] for c in candle_histories[index_name]["1min"]]
        atr = calculate_atr(close_prices, 14)
        sl = prem * 0.55
        target = prem + atr * 3
        lots = 1
        signal_state[index_name].update({
            "action": action, "entry_price": prem, "stop_loss": sl, "target": target,
            "lots": lots, "entry_time": now, "highest": prem, "cooldown": 0
        })
        signal_buffer[index_name]["ce_count"] = signal_buffer[index_name]["pe_count"] = 0
        daily_trade_count[index_name] += 1
        msg = f"🚨 SIGNAL {action} {index_name} | Spot:{spot:.0f} Prem:{prem:.2f} SL:{sl:.2f} Tgt:{target:.2f} | Sentiment:{sentiment:.0f}"
        logger.info(msg)
        # send telegram if configured
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            try:
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                              json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=3)
            except: pass
        market_signal[index_name].update({
            "signal": action, "alert_message": f"ENTRY {action}",
            "entry_price": prem, "stop_loss": sl, "target": target, "confidence": sentiment
        })
    else:
        # No action, just show waiting message
        market_signal[index_name]["alert_message"] = f"Sentiment {sentiment:.0f} - {signal_type}"
        market_signal[index_name]["signal"] = "NO_TRADE"

# ----------------------------------------------------------------------
# WEBSOCKET CALLBACKS (with candle aggregation)
# ----------------------------------------------------------------------
ws_running = False
sws = None
last_heartbeat = time.time()
tick_counter = 0

def on_ws_open(wsapp):
    global ws_running, last_heartbeat
    ws_running = True
    last_heartbeat = time.time()
    logger.info("WebSocket connected, subscribing...")
    refresh_all_tokens()
    # build subscription list (spot only for simplicity, options via REST)
    token_list = []
    for idx, cfg in INDEX_CONFIG.items():
        if cfg.get("active"):
            token_list.append({"exchangeType": cfg["ws_exchange_type"], "tokens": [cfg["token"]]})
    token_list.append({"exchangeType": 1, "tokens": ["99919017"]})  # VIX
    if token_list and sws:
        try:
            sws.subscribe("admin", 1, token_list)
            logger.info(f"Subscribed to {len(token_list)} spot tokens")
        except Exception as e:
            logger.error(f"Subscribe error: {e}")

def on_ws_error(wsapp, error):
    logger.error(f"WS error: {error}")

def on_ws_close(wsapp, code, msg):
    global ws_running
    ws_running = False
    logger.warning(f"WS closed: {code} {msg}")

def on_ws_data(wsapp, message):
    global tick_counter, last_heartbeat
    last_heartbeat = time.time()
    try:
        if isinstance(message, bytes):
            # SmartWebSocketV2 sends JSON strings as bytes
            message = message.decode('utf-8')
        if isinstance(message, str):
            data = json.loads(message)
            ticks = data if isinstance(data, list) else [data]
        else:
            return

        for tick in ticks:

            # ... rest of your tick processing (unchanged) ...
            token = str(tick.get("token") or "")
            ltp = tick.get("last_traded_price") or tick.get("ltp") or 0
            if isinstance(ltp, str):
                try: ltp = float(ltp)
                except: ltp = 0
            if ltp>100000: ltp/=100
            vol = tick.get("volume") or tick.get("v") or 0
            # Update spot prices and aggregate candles
            for idx, cfg in INDEX_CONFIG.items():
                if cfg.get("token") == token:
                    if ltp>0:
                        latest_ticks[idx]["spot_price"] = ltp
                        price_histories[idx].append(ltp)
                        # update candles for this index
                        update_candle(idx, ltp, vol, time.time())
                        tick_counter += 1
                    break
            if token == "99919017" and ltp>0:
                latest_ticks["VIX"]["vix"] = ltp
                vix_history.append(ltp)
        # run signal engine every few ticks
        if tick_counter % 5 == 0 and tick_counter>0:
            for idx in INDEX_CONFIG:
                if INDEX_CONFIG[idx].get("active"):
                    run_signal_engine_for_index(idx)
    except Exception as e:
        logger.error(f"WS data error: {e}")

# ----------------------------------------------------------------------
# WEBSOCKET THREAD
# ----------------------------------------------------------------------
def start_angel_websocket():
    global sws, ws_running
    while True:
        try:
            if not is_market_open():
                time.sleep(5)
                continue

            auth_token, feed_token, _ = get_auth_token()
            if not feed_token:
                time.sleep(10)
                continue

            sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)
            sws.on_open = on_ws_open
            sws.on_data = on_ws_data
            sws.on_error = on_ws_error
            sws.on_close = on_ws_close

            # Connect (this will block until connection is closed or fails)
            sws.connect()
            
            # When connect() returns, the connection is closed. Wait a bit before reconnecting.
            ws_running = False
            logger.warning("WebSocket disconnected, reconnecting in 5s...")
            time.sleep(5)

        except Exception as e:
            logger.error(f"WS thread error: {e}")
            time.sleep(10)
# ----------------------------------------------------------------------
# REST API POLLER (for option premiums)
# ----------------------------------------------------------------------
def is_market_open():
    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    current = now_ist.time()
    return now_ist.weekday() < 5 and dt_time(9,10) <= current <= dt_time(15,35)

def start_rest_api_poller():
    global last_heartbeat
    logger.info("REST poller started")
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
            # fetch option premiums for all active indices
            for idx, tokens in INDEX_TOKENS.items():
                if not INDEX_CONFIG[idx].get("active"): continue
                if tokens.get("ce_token") and tokens.get("pe_token"):
                    try:
                        ce_resp = auth_obj.ltpData(INDEX_CONFIG[idx]["option_exchange"], tokens["ce_symbol"], tokens["ce_token"])
                        ce = safe_ltp(ce_resp)
                        if ce and ce>0:
                            if ce>100000: ce/=100
                            latest_ticks[idx]["ce_price"] = ce
                            last_known_prices[idx]["ce"] = ce
                            last_known_prices[idx]["timestamp"] = now
                        pe_resp = auth_obj.ltpData(INDEX_CONFIG[idx]["option_exchange"], tokens["pe_symbol"], tokens["pe_token"])
                        pe = safe_ltp(pe_resp)
                        if pe and pe>0:
                            if pe>100000: pe/=100
                            latest_ticks[idx]["pe_price"] = pe
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
# BACKGROUND THREADS
# ----------------------------------------------------------------------
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

def start_backgrounds():
    _start_background_threads()

# ----------------------------------------------------------------------
# FLASK ROUTES
# ----------------------------------------------------------------------
@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "healthy",
        "engine": "Multi-Index Options Bot v12.5 (Multi‑timeframe candles fixed)",
        "indices": [i for i, cfg in INDEX_CONFIG.items() if cfg.get("active")],
        "market_open": is_market_open()
    })

@app.route("/api/live-signals", methods=["GET"])
def live_signals():
    sentiment_data = {}
    for idx in INDEX_CONFIG:
        if not INDEX_CONFIG[idx].get("active"): continue
        sentiment_data[idx] = {"score": market_signal[idx].get("sentiment_score",50), "label": ""}
    return jsonify({
        "timestamp": datetime.now().isoformat(),
        "signals": market_signal,
        "sentiment": sentiment_data,
        "portfolios": portfolio_state,
        "market_open": is_market_open(),
        "debug": {"ws_running": ws_running, "ticks": tick_counter},
        "version": "12.5"
    })

@app.route("/api/health")
def health():
    now = time.time()
    ws_alive = ws_running and (now - last_heartbeat) < 60  # received data in last 60s
    return jsonify({
        "status": "OK" if ws_alive else "DEGRADED",
        "ws_running": ws_running,
        "ticks": tick_counter,
        "last_heartbeat_sec_ago": now - last_heartbeat,
        "market_open": is_market_open()
    })
# Start background threads immediately when module loads (for Gunicorn)
try:
    _start_background_threads()
except Exception as e:
    logger.error(f"Failed to start background threads: {e}")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
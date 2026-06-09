# === VERSION 12.8 - FINAL: Hardcoded working tokens, premium fix ===
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
from collections import deque, defaultdict
from datetime import datetime, timedelta, time as dt_time
from flask import Flask, jsonify, request
from flask_cors import CORS
import pyotp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app

# ============================================================================
# ENVIRONMENT
# ============================================================================
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing Angel One credentials")

DB_PATH = "trading_data.db"

# ============================================================================
# DATABASE
# ============================================================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS portfolio_equity (index_name TEXT PRIMARY KEY, equity REAL, last_updated REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS signals (timestamp REAL, action TEXT, confidence REAL, ce_price REAL, pe_price REAL, rsi REAL, pcr REAL, regime TEXT, vix REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS trades (timestamp REAL, action TEXT, entry_price REAL, exit_price REAL, pnl REAL, exit_reason TEXT)""")
    conn.commit()
    conn.close()
init_db()

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ============================================================================
# INDEX CONFIGURATION + HARDCODED TOKENS (from working run)
# ============================================================================
INDEX_CONFIG = {
    "NIFTY": {
        "token": "99926000", "exchange": "NSE", "symbol": "NIFTY",
        "lot_size": 50, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "atm_strike_multiple": 50,
        "option_exchange": "NFO", "ws_exchange_type": 1,
        "max_daily_drawdown_pct": 3.0, "correlation_pair": "BANKNIFTY",
        "ce_token": "50565", "pe_token": "50566", "ce_symbol": "NIFTY23200CE", "pe_symbol": "NIFTY23200PE"
    },
    "BANKNIFTY": {
        "token": "99926009", "exchange": "NSE", "symbol": "BANKNIFTY",
        "lot_size": 25, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "atm_strike_multiple": 100,
        "option_exchange": "NFO", "ws_exchange_type": 1,
        "max_daily_drawdown_pct": 3.0, "correlation_pair": "NIFTY",
        "ce_token": "75600", "pe_token": "75601", "ce_symbol": "BANKNIFTY55100CE", "pe_symbol": "BANKNIFTY55100PE"
    },
    "FINNIFTY": {
        "token": "99926037", "exchange": "NSE", "symbol": "FINNIFTY",
        "lot_size": 40, "expiry_weekday": 1, "active": True,
        "min_premium": 5, "atm_strike_multiple": 50,
        "option_exchange": "NFO", "ws_exchange_type": 1,
        "max_daily_drawdown_pct": 3.0, "correlation_pair": None,
        "ce_token": "77498", "pe_token": "77499", "ce_symbol": "FINNIFTY25100CE", "pe_symbol": "FINNIFTY25100PE"
    },
    "MIDCPNIFTY": {
        "token": "99926074", "exchange": "NSE", "symbol": "MIDCPNIFTY",
        "lot_size": 75, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "atm_strike_multiple": 25,
        "option_exchange": "NFO", "ws_exchange_type": 1,
        "max_daily_drawdown_pct": 3.0, "correlation_pair": None,
        "ce_token": "78658", "pe_token": "78659", "ce_symbol": "MIDCPNIFTY14125CE", "pe_symbol": "MIDCPNIFTY14125PE"
    },
    "SENSEX": {
        "token": "99919000", "exchange": "BSE", "symbol": "SENSEX",
        "lot_size": 15, "expiry_weekday": 4, "active": True,
        "min_premium": 5, "atm_strike_multiple": 100,
        "option_exchange": "BFO", "ws_exchange_type": 3,
        "max_daily_drawdown_pct": 3.0, "correlation_pair": None,
        "ce_token": "1132676", "pe_token": "1132838", "ce_symbol": "SENSEX73800CE", "pe_symbol": "SENSEX73800PE"
    }
}

# Fill INDEX_TOKENS from config
INDEX_TOKENS = {}
for idx, cfg in INDEX_CONFIG.items():
    INDEX_TOKENS[idx] = {
        "ce_token": cfg.get("ce_token"),
        "pe_token": cfg.get("pe_token"),
        "ce_symbol": cfg.get("ce_symbol"),
        "pe_symbol": cfg.get("pe_symbol"),
        "atm_strike": 0,
        "expiry": "",
        "last_refresh": time.time()
    }

# Price caches
last_known_prices = {idx: {"spot": 0.0, "ce": 0.0, "pe": 0.0, "timestamp": 0} for idx in INDEX_CONFIG}
price_histories = {idx: deque(maxlen=5000) for idx in INDEX_CONFIG}
ce_price_histories = {idx: deque(maxlen=2000) for idx in INDEX_CONFIG}
pe_price_histories = {idx: deque(maxlen=2000) for idx in INDEX_CONFIG}
latest_ticks = {idx: {"spot_price": 0.0, "ce_price": 0.0, "pe_price": 0.0} for idx in INDEX_CONFIG}
latest_ticks["VIX"] = {"vix": 15.0}
vix_history = deque(maxlen=200)
nifty_price_series = deque(maxlen=200)
banknifty_price_series = deque(maxlen=200)

tick_counter = 0
ws_running = False
sws = None
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
        result = func(*args, **kwargs)
        _api_last_call = time.time()
        return result

# ============================================================================
# SENTIMENT & SIGNAL CORE (same as v12.1)
# ============================================================================
TIMEFRAMES = ["1min","2min","3min","5min","10min","15min","20min","30min"]
TIMEFRAME_WEIGHTS = {"1min":8,"2min":8,"3min":8,"5min":12,"10min":12,"15min":14,"20min":14,"30min":24}
EMA_SHORT, EMA_MEDIUM, EMA_LONG = 9, 21, 50

SENTIMENT_SCORES = {
    "STRONG_BULLISH": (85,100,"STRONG BULLISH","STRONG_BUY_CE"),
    "BULLISH": (70,84,"BULLISH","BUY_CE"),
    "SLOW_BULLISH": (55,69,"SLOW BULLISH","LOW_BUY_CE"),
    "NEUTRAL": (45,54,"NEUTRAL","NO_TRADE"),
    "SLOW_BEARISH": (30,44,"SLOW BEARISH","LOW_BUY_PE"),
    "BEARISH": (15,29,"BEARISH","BUY_PE"),
    "STRONG_BEARISH": (0,14,"STRONG BEARISH","STRONG_BUY_PE")
}

market_sentiment = {idx: {"score":50,"label":"NEUTRAL"} for idx in INDEX_CONFIG}
for tf in TIMEFRAMES:
    for idx in INDEX_CONFIG:
        market_sentiment[idx][f"trend_{tf}"] = "NEUTRAL"

signal_state = {idx: {"action":"HOLD","entry_price":0,"stop_loss":0,"target":0,"lots":1,"cooldown":0,"highest":0,"entry_time":0,"prev_action_side":None,"trend_change_cooldown":0} for idx in INDEX_CONFIG}
portfolio_state = {idx: {"equity":100000.0,"open_positions":0,"daily_trades":0} for idx in INDEX_CONFIG}
daily_drawdown = {idx: {"peak_equity":100000.0} for idx in INDEX_CONFIG}
market_signal = {idx: {"signal":"WAITING","sentiment_score":50,"alert_message":"","entry_price":0,"stop_loss":0,"target":0,"exit_reason":"","trend_change_cooldown_remaining":0} for idx in INDEX_CONFIG}
safety_state = {idx: {"consecutive_sl":0,"circuit_breaker":False,"circuit_breaker_until":0} for idx in INDEX_CONFIG}
signal_buffer = {idx: {"ce_count":0,"pe_count":0,"consecutive_ce":0,"consecutive_pe":0} for idx in INDEX_CONFIG}
daily_trade_count = {idx:0 for idx in INDEX_CONFIG}
last_trade_date = {idx:"" for idx in INDEX_CONFIG}

# ============================================================================
# UTILITIES
# ============================================================================
def is_valid_option_premium(premium, spot_price, side):
    if premium <= 0: return False
    if spot_price <= 0: return premium < 10000
    return premium < 2 * spot_price   # options are always worth less than underlying

def calculate_rsi(prices, period=14):
    if len(prices) < period+1: return 50.0
    gains, losses = [], []
    for i in range(1,len(prices)):
        diff = prices[i]-prices[i-1]
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

def calculate_adx(prices, period=14):
    if len(prices) < period*2+1: return 20.0
    plus_dm, minus_dm, tr = [], [], []
    for i in range(1,len(prices)):
        up = prices[i]-prices[i-1]
        down = prices[i-1]-prices[i]
        plus_dm.append(max(up,0) if up>down else 0)
        minus_dm.append(max(down,0) if down>up else 0)
        tr.append(abs(prices[i]-prices[i-1]))
    atr = sum(tr[-period:])/period
    plus_di = 100*sum(plus_dm[-period:])/period/atr if atr>0 else 0
    minus_di = 100*sum(minus_dm[-period:])/period/atr if atr>0 else 0
    dx = 100*abs(plus_di-minus_di)/(plus_di+minus_di) if (plus_di+minus_di)>0 else 0
    return dx

def calculate_bollinger(prices, period=20):
    if len(prices)<period: return None,None,None
    sma = sum(prices[-period:])/period
    var = sum((p-sma)**2 for p in prices[-period:])/period
    std = math.sqrt(var)
    return sma+2*std, sma, sma-2*std

def is_sideways(prices):
    if len(prices)<30: return False,0
    upper,sma,lower = calculate_bollinger(prices)
    if upper and sma>0:
        band_width = (upper-lower)/sma
        if band_width < 0.008: return True, band_width
    adx = calculate_adx(prices)
    if adx < 20: return True, adx
    recent = prices[-30:]
    price_range = (max(recent)-min(recent))/sum(recent)*len(recent)
    if price_range < 0.005: return True, price_range
    return False,0

def get_trend_score(prices, tf_name):
    if len(prices) < 60: return "NEUTRAL",0
    w = TIMEFRAME_WEIGHTS[tf_name]
    ema9 = calculate_ema(prices,9)
    ema21 = calculate_ema(prices,21)
    ema50 = calculate_ema(prices,50)
    price = prices[-1]
    sideways,_ = is_sideways(prices)
    if sideways: return "SIDEWAYS",0
    if tf_name in ["1min","2min","3min"]:
        if ema9>ema21 and price>ema9: return "BULLISH",w
        if ema9<ema21 and price<ema9: return "BEARISH",-w
        return "NEUTRAL",0
    elif tf_name in ["5min","10min"]:
        if ema9>ema21>ema50 and price>ema9: return "BULLISH",w
        if ema9<ema21<ema50 and price<ema9: return "BEARISH",-w
        if ema9>ema21 and price>ema9: return "BULLISH",w-5
        if ema9<ema21 and price<ema9: return "BEARISH",-(w-5)
        return "NEUTRAL",0
    else:
        if ema9>ema21>ema50: return "BULLISH",w-5
        if ema9<ema21<ema50: return "BEARISH",-(w-5)
        return "NEUTRAL",0

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
    sentiment = max(0, min(100, 50 + total/3.5))
    market_sentiment[index_name]["score"] = sentiment
    for k,(low,high,label,action) in SENTIMENT_SCORES.items():
        if low<=sentiment<=high:
            market_sentiment[index_name]["label"] = label
            break
    return sentiment

def get_signal_from_sentiment(index_name, sentiment):
    for k,(low,high,label,action) in SENTIMENT_SCORES.items():
        if low<=sentiment<=high:
            trend_30m = market_sentiment[index_name].get("trend_30min","NEUTRAL")
            trend_20m = market_sentiment[index_name].get("trend_20min","NEUTRAL")
            if "LOW" in action:
                if "CE" in action and trend_30m=="BEARISH" and trend_20m=="BEARISH":
                    return "NO_TRADE",label,sentiment
                if "PE" in action and trend_30m=="BULLISH" and trend_20m=="BULLISH":
                    return "NO_TRADE",label,sentiment
            return action,label,sentiment
    return "NO_TRADE","UNKNOWN",sentiment

# ============================================================================
# AUTH & DATA
# ============================================================================
auth_cache = {"token":None,"feed_token":None,"timestamp":0,"obj":None}
_auth_lock = threading.Lock()

def get_auth_token():
    with _auth_lock:
        now = time.time()
        if auth_cache["token"] and now - auth_cache["timestamp"] < 3300:
            return auth_cache["token"], auth_cache["feed_token"], auth_cache["obj"]
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        obj = SmartConnect(api_key=ANGEL_API_KEY)
        session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
        if not session.get("status"): return None,None,None
        auth_token = session["data"]["jwtToken"]
        feed_token = obj.getfeedToken()
        auth_cache.update({"token":auth_token,"feed_token":feed_token,"timestamp":now,"obj":obj})
        logger.info("Auth token refreshed")
        return auth_token, feed_token, obj

def safe_ltp(resp):
    if not resp or not resp.get("status"): return None
    data = resp.get("data",{})
    if isinstance(data,dict):
        if "fetched" in data and data["fetched"]:
            fetched = data["fetched"]
            if isinstance(fetched,list) and len(fetched)>0:
                return float(fetched[0].get("ltp",0))
            elif isinstance(fetched,dict):
                return float(fetched.get("ltp",0))
        elif "ltp" in data:
            return float(data["ltp"])
    elif isinstance(data,list) and len(data)>0:
        return float(data[0].get("ltp",0))
    return None

def get_index_spot(index_name):
    config = INDEX_CONFIG[index_name]
    _,_,obj = get_auth_token()
    if not obj: return None
    try:
        resp = rate_limited_api_call(obj.ltpData, config["exchange"], config["symbol"], config["token"])
        ltp = safe_ltp(resp)
        if ltp and ltp>0:
            if ltp>100000: ltp/=100
            last_known_prices[index_name]["spot"] = ltp
            last_known_prices[index_name]["timestamp"] = time.time()
            return ltp
    except Exception as e:
        logger.error(f"Spot fetch {index_name}: {e}")
    return last_known_prices[index_name].get("spot",0) or None

def get_vix_value():
    _,_,obj = get_auth_token()
    if not obj: return 15.0
    try:
        # Try with known VIX token
        resp = rate_limited_api_call(obj.ltpData, "NSE", "INDIAVIX", "99919017")
        ltp = safe_ltp(resp)
        if ltp and ltp>0: return ltp
    except: pass
    return 15.0

# ============================================================================
# SIGNAL ENGINE (same as v12.1, but simplified)
# ============================================================================
def run_signal_engine_for_index(index_name):
    if not INDEX_CONFIG[index_name].get("active"): return
    with _signal_lock:
        prices = list(price_histories[index_name])
        if len(prices) < 30:
            market_signal[index_name]["alert_message"] = f"Collecting data ({len(prices)}/30)"
            market_signal[index_name]["signal"] = "WAITING"
            return

        now = time.time()
        spot = prices[-1] if prices else last_known_prices[index_name].get("spot",0)
        if spot>0:
            last_known_prices[index_name]["spot"] = spot
        ce_prem = latest_ticks[index_name]["ce_price"]
        pe_prem = latest_ticks[index_name]["pe_price"]
        if ce_prem<=0: ce_prem = last_known_prices[index_name].get("ce",0)
        if pe_prem<=0: pe_prem = last_known_prices[index_name].get("pe",0)

        sentiment = compute_sentiment(index_name)
        action, label, conf = get_signal_from_sentiment(index_name, sentiment)
        rsi = calculate_rsi(prices)
        atr = calculate_atr(prices)
        vix = latest_ticks["VIX"]["vix"]

        # kill switch
        daily_peak = daily_drawdown[index_name]["peak_equity"]
        current_equity = portfolio_state[index_name]["equity"]
        if current_equity > daily_peak:
            daily_drawdown[index_name]["peak_equity"] = current_equity
        drawdown = (daily_peak - current_equity)/daily_peak*100 if daily_peak>0 else 0
        if drawdown >= INDEX_CONFIG[index_name].get("max_daily_drawdown_pct",3.0):
            if signal_state[index_name]["action"] != "HOLD":
                active = signal_state[index_name]["action"]
                prem = ce_prem if "CE" in active else pe_prem
                if prem>0:
                    pnl = prem - signal_state[index_name]["entry_price"]
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    portfolio_state[index_name]["equity"] += pnl_total
                    save_portfolio_equity(index_name)
                    reset_signal_state(index_name, now, "KILL_SWITCH")
            market_signal[index_name]["alert_message"] = "KILL SWITCH ACTIVE"
            market_signal[index_name]["signal"] = "KILL_SWITCH"
            return

        # circuit breaker
        if safety_state[index_name]["circuit_breaker"]:
            if now < safety_state[index_name]["circuit_breaker_until"]:
                market_signal[index_name]["alert_message"] = "Circuit breaker active"
                market_signal[index_name]["signal"] = "CIRCUIT_BREAKER"
                return
            else:
                safety_state[index_name]["circuit_breaker"] = False
                safety_state[index_name]["consecutive_sl"] = 0

        # active position handling
        if signal_state[index_name]["action"] != "HOLD":
            active = signal_state[index_name]["action"]
            prem = ce_prem if "CE" in active else pe_prem
            if prem>0:
                pnl = prem - signal_state[index_name]["entry_price"]
                # trailing stop
                if prem > signal_state[index_name].get("highest",0):
                    signal_state[index_name]["highest"] = prem
                    new_sl = prem - (atr*1.8)
                    if new_sl > signal_state[index_name]["stop_loss"]:
                        signal_state[index_name]["stop_loss"] = new_sl
                # stop loss
                if prem <= signal_state[index_name]["stop_loss"]:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    portfolio_state[index_name]["equity"] += pnl_total
                    save_portfolio_equity(index_name)
                    safety_state[index_name]["consecutive_sl"] += 1
                    if safety_state[index_name]["consecutive_sl"] >=3:
                        safety_state[index_name]["circuit_breaker"] = True
                        safety_state[index_name]["circuit_breaker_until"] = now+1800
                    daily_trade_count[index_name] += 1
                    send_telegram_alert(f"EXIT {index_name} SL | PnL: {pnl:.2f}")
                    reset_signal_state(index_name, now, "STOP_LOSS")
                    return
                # target
                if prem >= signal_state[index_name]["target"]:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    portfolio_state[index_name]["equity"] += pnl_total
                    save_portfolio_equity(index_name)
                    safety_state[index_name]["consecutive_sl"] = 0
                    daily_trade_count[index_name] += 1
                    send_telegram_alert(f"EXIT {index_name} TARGET | PnL: {pnl:.2f}")
                    reset_signal_state(index_name, now, "TARGET_HIT")
                    return
                # time exit (45 min)
                entry_time = signal_state[index_name].get("entry_time",0)
                if entry_time>0 and (now-entry_time)/60 >=45:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    portfolio_state[index_name]["equity"] += pnl_total
                    save_portfolio_equity(index_name)
                    daily_trade_count[index_name] += 1
                    send_telegram_alert(f"EXIT {index_name} TIME | PnL: {pnl:.2f}")
                    reset_signal_state(index_name, now, "TIME_EXIT")
                    return
                # update market signal
                market_signal[index_name].update({
                    "alert_message": f"ACTIVE {active}",
                    "signal": "ACTIVE",
                    "entry_price": signal_state[index_name]["entry_price"],
                    "stop_loss": signal_state[index_name]["stop_loss"],
                    "target": signal_state[index_name]["target"],
                    "current_pnl": round(pnl,2)
                })
            else:
                reset_signal_state(index_name, now, "PREMIUM_ZERO")
            return

        # no active position: check entry
        if now < signal_state[index_name]["cooldown"]:
            market_signal[index_name]["alert_message"] = f"Cooldown {int(signal_state[index_name]['cooldown']-now)}s"
            market_signal[index_name]["signal"] = "COOLDOWN"
            return
        today = datetime.now().strftime("%Y-%m-%d")
        if last_trade_date[index_name] != today:
            daily_trade_count[index_name] = 0
            last_trade_date[index_name] = today
            daily_drawdown[index_name]["peak_equity"] = portfolio_state[index_name]["equity"]
        if daily_trade_count[index_name] >= 20:
            market_signal[index_name]["alert_message"] = "Max daily trades"
            market_signal[index_name]["signal"] = "BLOCKED"
            return

        prem = ce_prem if "CE" in action else pe_prem if "PE" in action else 0
        min_prem = INDEX_CONFIG[index_name].get("min_premium",5)
        if prem <= 0 or prem < min_prem:
            market_signal[index_name]["alert_message"] = f"Premium invalid: Rs{prem}"
            market_signal[index_name]["signal"] = "WAITING"
            return

        # signal buffer (2 consecutive)
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
            market_signal[index_name]["alert_message"] = "No signal"
            market_signal[index_name]["signal"] = "NO_TRADE"
            return

        # position sizing
        risk_pct = 1.0
        if "STRONG" in action: risk_pct = 1.3
        elif "LOW" in action: risk_pct = 0.7
        risk_amount = portfolio_state[index_name]["equity"] * (risk_pct/100)
        stop_dist = atr * 1.5
        lots = max(1, int(risk_amount / (stop_dist * INDEX_CONFIG[index_name]["lot_size"])))
        lots = min(5, lots)

        # entry
        sl = prem * (0.7 if "LOW" in action else 0.55)
        target = prem + (atr*3.5)
        if "LOW" in action: target = prem + (atr*2.5)

        signal_state[index_name].update({
            "action": action, "entry_price": prem, "stop_loss": sl, "target": target,
            "lots": lots, "entry_time": now, "highest": prem, "confidence": conf
        })
        portfolio_state[index_name]["open_positions"] = 1
        buf["ce_count"] = buf["pe_count"] = 0
        daily_trade_count[index_name] += 1

        emoji = "B" if "STRONG" in action and "CE" in action else "S" if "STRONG" in action and "PE" in action else "W" if "LOW" in action else "N"
        msg = (f"{emoji} {action} {index_name} | Spot:{spot:.0f} Prem:{prem:.2f} SL:{sl:.2f} Tgt:{target:.2f} | "
               f"Sentiment:{sentiment:.0f} ({label}) Lots:{lots}")
        send_telegram_alert(msg)
        logger.info(f"ENTRY {index_name} {action} prem={prem}")

        market_signal[index_name].update({
            "spot_price": spot,
            "ce_price": ce_prem,
            "pe_price": pe_prem,
            "sentiment_score": sentiment,
            "sentiment": label,
            "signal": action,
            "entry_price": prem,
            "stop_loss": sl,
            "target": target,
            "confidence": conf,
            "alert_message": f"ENTRY {action}"
        })

def reset_signal_state(index_name, current_time, exit_reason=""):
    signal_state[index_name].update({
        "action":"HOLD","entry_price":0,"stop_loss":0,"target":0,"lots":1,
        "cooldown":current_time+60,"highest":0,"entry_time":0,
        "exit_reason":exit_reason
    })
    portfolio_state[index_name]["open_positions"] = 0

def save_portfolio_equity(index_name):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("INSERT OR REPLACE INTO portfolio_equity (index_name, equity, last_updated) VALUES (?,?,?)",
                      (index_name, portfolio_state[index_name]["equity"], time.time()))
            conn.commit()
    except: pass

def send_telegram_alert(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=3)
    except: pass

def run_all_signals():
    for idx in INDEX_CONFIG:
        if INDEX_CONFIG[idx].get("active"):
            try:
                run_signal_engine_for_index(idx)
            except Exception as e:
                logger.error(f"Signal error {idx}: {e}")

# ============================================================================
# WEBSOCKET (spot only, no option price updates)
# ============================================================================
def on_ws_open(wsapp):
    global ws_running, last_heartbeat
    ws_running = True
    last_heartbeat = time.time()
    logger.info("WebSocket connected")
    # subscribe only to spot indices and VIX
    token_list = []
    for idx, cfg in INDEX_CONFIG.items():
        if cfg.get("active"):
            token_list.append({"exchangeType": cfg["ws_exchange_type"], "tokens": [cfg["token"]]})
    token_list.append({"exchangeType": 1, "tokens": ["99919017"]})  # VIX
    if token_list and sws:
        try:
            sws.subscribe("admin", 1, token_list)
            logger.info("Subscribed to spot tokens")
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
            if sws and hasattr(sws, '_parse_binary_data'):
                parsed = sws._parse_binary_data(message)
                if isinstance(parsed, dict): ticks = [parsed]
                elif isinstance(parsed, list): ticks = parsed
                else: return
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
            if ltp>100000: ltp/=100
            # only update spot prices and VIX
            for idx, cfg in INDEX_CONFIG.items():
                if cfg.get("token") == token:
                    if ltp>0:
                        latest_ticks[idx]["spot_price"] = ltp
                        price_histories[idx].append(ltp)
                        tick_counter += 1
                    break
            if token == "99919017" and ltp>0:
                latest_ticks["VIX"]["vix"] = ltp
                vix_history.append(ltp)
        if tick_counter % 3 == 0 and tick_counter>0:
            run_all_signals()
    except Exception as e:
        logger.error(f"WS data error: {e}")

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
            sws.connect()
            ws_running = False
            time.sleep(5)
        except Exception as e:
            logger.error(f"WS thread error: {e}")
            time.sleep(10)

# ============================================================================
# REST API POLLER (fetches option premiums using hardcoded tokens)
# ============================================================================
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
            # fetch spot prices for all indices
            for idx in INDEX_CONFIG:
                spot = get_index_spot(idx)
                if spot and spot>0:
                    latest_ticks[idx]["spot_price"] = spot
                    last_known_prices[idx]["spot"] = spot
                    last_known_prices[idx]["timestamp"] = now
            # fetch option premiums using hardcoded tokens
            for idx, cfg in INDEX_CONFIG.items():
                if not cfg.get("active"): continue
                token_info = INDEX_TOKENS[idx]
                if not token_info.get("ce_token") or not token_info.get("pe_token"):
                    logger.warning(f"{idx}: Missing tokens, skipping")
                    continue
                try:
                    ce_resp = auth_obj.ltpData(cfg["option_exchange"], token_info["ce_symbol"], token_info["ce_token"])
                    ce = safe_ltp(ce_resp)
                    if ce and ce>0:
                        if ce>100000: ce/=100
                        latest_ticks[idx]["ce_price"] = ce
                        last_known_prices[idx]["ce"] = ce
                        last_known_prices[idx]["timestamp"] = now
                        logger.debug(f"REST CE {idx}: {ce}")
                    pe_resp = auth_obj.ltpData(cfg["option_exchange"], token_info["pe_symbol"], token_info["pe_token"])
                    pe = safe_ltp(pe_resp)
                    if pe and pe>0:
                        if pe>100000: pe/=100
                        latest_ticks[idx]["pe_price"] = pe
                        last_known_prices[idx]["pe"] = pe
                        last_known_prices[idx]["timestamp"] = now
                        logger.debug(f"REST PE {idx}: {pe}")
                except Exception as e:
                    logger.debug(f"REST fetch error {idx}: {e}")
            # fetch VIX
            vix = get_vix_value()
            if vix:
                latest_ticks["VIX"]["vix"] = vix
            run_all_signals()
            time.sleep(10)
        except Exception as e:
            logger.error(f"REST poller error: {e}")
            time.sleep(10)

# ============================================================================
# STARTUP
# ============================================================================
_init_completed = False
_init_lock = threading.Lock()
_bot_startup_time = time.time()

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

# ============================================================================
# FLASK ROUTES
# ============================================================================
@app.route("/", methods=["GET"])
def home():
    return jsonify({"status":"healthy","engine":"v12.8 - Hardcoded tokens","indices":list(INDEX_CONFIG.keys()),"market_open":is_market_open()})

@app.route("/api/live-signals", methods=["GET"])
def live_signals():
    return jsonify({
        "timestamp": datetime.now().isoformat(),
        "signals": market_signal,
        "portfolios": portfolio_state,
        "tokens": INDEX_TOKENS,
        "market_open": is_market_open(),
        "debug": {"ws_running": ws_running, "ticks": tick_counter},
        "version": "12.8"
    })

@app.route("/api/health")
def health():
    return jsonify({"status":"OK","ws_running":ws_running,"ticks":tick_counter,"market_open":is_market_open()})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    _start_background_threads()
    app.run(host="0.0.0.0", port=port, debug=False)
# backend.py – Professional Nifty Options Signal Engine
# Enhanced with volume/spread/volatility filters, consecutive minute bar confirmation,
# expiry day signal freeze, and spot price in API response.

import os
import sys
import time
import json
import queue
import logging
import threading
import signal
from collections import deque, defaultdict
from datetime import datetime, timedelta

import requests
import pandas as pd
import pyotp
from flask import Flask, jsonify
from flask_cors import CORS
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ------------------------------------------------------------
# Configuration & Environment Variables
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app

# Required credentials
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")
if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing Angel One credentials")

# Optional thresholds (can be overridden by env vars)
SPREAD_THRESHOLD = float(os.getenv("SPREAD_THRESHOLD", "3.0"))
STRONG_BUY_THRESHOLD = float(os.getenv("STRONG_BUY_THRESHOLD", "85"))
BUY_THRESHOLD = float(os.getenv("BUY_THRESHOLD", "70"))
CONSIDER_THRESHOLD = float(os.getenv("CONSIDER_THRESHOLD", "55"))
UPDATE_INTERVAL_TICKS = int(os.getenv("UPDATE_INTERVAL_TICKS", "5"))
PRICE_HISTORY_LEN = int(os.getenv("PRICE_HISTORY_LEN", "500"))
WATCHDOG_TIMEOUT_SEC = int(os.getenv("WATCHDOG_TIMEOUT_SEC", "90"))
REST_FALLBACK_DELAY_SEC = int(os.getenv("REST_FALLBACK_DELAY_SEC", "10"))
MARKET_HOURS_START = os.getenv("MARKET_HOURS_START", "09:15")
MARKET_HOURS_END = os.getenv("MARKET_HOURS_END", "15:30")
TOKEN_REFRESH_HOURS = int(os.getenv("TOKEN_REFRESH_HOURS", "20"))

# ------------------------------------------------------------
# Global State
# ------------------------------------------------------------
CE_TOKEN = None
PE_TOKEN = None
latest_ticks = {"ce_price": 0.0, "pe_price": 0.0, "ce_volume": 0, "pe_volume": 0}
price_history = deque(maxlen=PRICE_HISTORY_LEN)
volume_history = deque(maxlen=PRICE_HISTORY_LEN)
tick_counter = 0
ws_running = False
sws = None
last_tick_time = time.time()
last_contract_update = 0
last_auth_time = 0
engine_active = True

# Timeframe snapshots (price & volume)
timeframe_history = {
    "1min": deque(maxlen=60),
    "5min": deque(maxlen=20),
    "10min": deque(maxlen=15),
    "15min": deque(maxlen=10),
    "20min": deque(maxlen=10)
}
last_minute_snapshot = {"time": 0, "price": 0, "volume": 0}

# Threading: queue for tick processing
tick_queue = queue.Queue(maxsize=1000)

# Signal memory & anti‑flip
signal_memory = {
    "current_action": "HOLD",
    "current_signal_type": "NONE",
    "signal_start_time": None,
    "last_confirmed_action": "HOLD",
    "confirmation_count": 0,
    "required_confirmations": 2,
    "min_signal_duration_seconds": 180,
    "max_sideways_duration": 600,
    "cooldown_until": 0,
    "last_logged_action": ""
}

# State dictionaries
market_signal = {
    "signal": "WAITING",
    "ce_price": 0.0,
    "pe_price": 0.0,
    "spread": 0.0,
    "rsi": 50,
    "macd": 0.0,
    "pcr": 1.0,
    "vwap": 0.0,
    "atr": 0.0,
    "ema_fast": 0.0,
    "ema_slow": 0.0,
    "delta": 0.0,
    "gamma": 0.0,
    "theta": 0.0,
    "vega": 0.0,
    "volume": 0,
    "volume_avg": 0,
    "oi_signal": "NEUTRAL",
    "timestamp": ""
}
market_state = {
    "rsi": 50, "momentum": "NEUTRAL", "strength": "LOW", "trend": "SIDEWAYS",
    "action": "HOLD", "confidence": 0, "volatility": "NORMAL", "alert": "NONE",
    "signal_duration_minutes": 0, "trend_1min": "SIDEWAYS", "trend_5min": "SIDEWAYS",
    "trend_10min": "SIDEWAYS", "trend_15min": "SIDEWAYS", "trend_20min": "SIDEWAYS",
    "timeframe_agreement": 0
}
institutional_state = {
    "vwap": 0, "ema_fast": 0, "ema_slow": 0, "ema_signal": "NEUTRAL", "atr": 0,
    "oi_buildup": "NEUTRAL", "iv_state": "NORMAL", "candle_structure": "SIDEWAYS",
    "market_breadth": "BALANCED", "volume_profile": "NORMAL", "smart_money_flow": "NEUTRAL",
    "volume_trend": "FLAT", "delta": 0, "gamma": 0, "theta": 0, "vega": 0,
    "institutional_signal": "HOLD", "institutional_confidence": 0, "consecutive_confirmations": 0
}

# ------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------
def is_market_open():
    now = datetime.now()
    if now.weekday() >= 5:
        return False  # weekend
    # On expiry Thursday, freeze signals during last 30 minutes
    if now.weekday() == 3:
        minutes_since_open = (now.hour - 9) * 60 + (now.minute - 15)
        if minutes_since_open >= 6 * 60 + 15:  # after 3:30 PM
            return False
    market_start = datetime.strptime(MARKET_HOURS_START, "%H:%M").time()
    market_end = datetime.strptime(MARKET_HOURS_END, "%H:%M").time()
    return market_start <= now.time() <= market_end

def get_nifty_spot():
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        headers = {"User-Agent": "Mozilla/5.0"}
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        time.sleep(0.5)
        resp = session.get(url, headers=headers, timeout=10)
        data = resp.json()
        spot = data["data"][0]["lastPrice"]
        logger.info(f"NIFTY spot = {spot}")
        return float(spot)
    except Exception as e:
        logger.error(f"Spot fetch error: {e}")
        return None

def get_nifty_spot_cached():
    now = time.time()
    if now - spot_cache.get("timestamp", 0) < 15 and spot_cache.get("value"):
        return spot_cache["value"]
    spot = get_nifty_spot()
    if spot:
        spot_cache["value"] = spot
        spot_cache["timestamp"] = now
    return spot

def get_current_atm_tokens():
    """Fetch current ATM CE/PE tokens with fallback for missing strike."""
    spot = get_nifty_spot()
    if not spot:
        return None, None
    atm_strike = round(spot / 50) * 50
    logger.info(f"ATM strike = {atm_strike}")
    url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        df = pd.DataFrame(resp.json())
    except Exception as e:
        logger.error(f"Failed to load instrument master: {e}")
        return None, None

    # Filter NIFTY OPTIDX NFO
    nifty_opts = df[
        (df["name"].astype(str) == "NIFTY") & 
        (df["instrumenttype"].astype(str) == "OPTIDX") &
        (df["exch_seg"].astype(str) == "NFO")
    ].copy()
    if nifty_opts.empty:
        # fallback by symbol pattern
        nifty_opts = df[df["symbol"].astype(str).str.match(r'^NIFTY\d{2}[A-Z]{3}\d{2}', na=False)].copy()
    if nifty_opts.empty:
        logger.error("No NIFTY options found")
        return None, None

    # Parse expiry
    for fmt in ["%d%b%Y", "%d-%b-%Y", "%Y-%m-%d", "%d%m%Y"]:
        nifty_opts["expiry_date"] = pd.to_datetime(nifty_opts["expiry"], format=fmt, errors="coerce")
        if nifty_opts["expiry_date"].notna().sum() > 0:
            break
    nifty_opts = nifty_opts.dropna(subset=["expiry_date"])
    nifty_opts["strike"] = pd.to_numeric(nifty_opts["strike"], errors="coerce") / 100
    nifty_opts = nifty_opts.dropna(subset=["strike"])
    today = datetime.now()
    future = nifty_opts[nifty_opts["expiry_date"] > today]
    if future.empty:
        logger.error("No future expiry found")
        return None, None
    nearest = future["expiry_date"].min()
    atm_opts = nifty_opts[(nifty_opts["strike"] == atm_strike) & (nifty_opts["expiry_date"] == nearest)]
    if atm_opts.empty:
        # nearest strike
        strikes = sorted(nifty_opts[nifty_opts["expiry_date"] == nearest]["strike"].unique())
        nearest_strike = min(strikes, key=lambda x: abs(x - atm_strike))
        atm_opts = nifty_opts[(nifty_opts["strike"] == nearest_strike) & (nifty_opts["expiry_date"] == nearest)]
    ce = atm_opts[atm_opts["symbol"].str.upper().str.contains("CE", na=False)]
    pe = atm_opts[atm_opts["symbol"].str.upper().str.contains("PE", na=False)]
    if ce.empty or pe.empty:
        logger.error("CE/PE not found")
        return None, None
    return str(ce.iloc[0]["token"]), str(pe.iloc[0]["token"])

def get_ltp_rest(token):
    """REST fallback for LTP."""
    totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
    obj = SmartConnect(api_key=ANGEL_API_KEY)
    session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
    if not session.get("status"):
        logger.error("REST auth failed")
        return 0
    auth_token = session["data"]["jwtToken"]
    url = "https://apiconnect.angelbroking.com/rest/secure/angelbroking/ltp/v1"
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json",
        "X-PrivateKey": ANGEL_API_KEY
    }
    payload = {"symbols": [{"symboltoken": token, "exchange": "NFO"}]}
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=2)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("data"):
                return float(data["data"][0]["ltp"])
    except Exception as e:
        logger.error(f"REST LTP error: {e}")
    return 0

# ------------------------------------------------------------
# Technical Indicators
# ------------------------------------------------------------
def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calculate_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow:
        return 0.0, 0.0
    def ema(data, period):
        alpha = 2/(period+1)
        val = data[0]
        for p in data[1:]:
            val = alpha * p + (1-alpha) * val
        return val
    macd_line = ema(prices, fast) - ema(prices, slow)
    signal_line = ema(prices[-signal:], signal) if len(prices) >= signal else ema(prices, signal)
    return macd_line, macd_line - signal_line

def calculate_ema(prices, period):
    if len(prices) < period:
        return prices[-1] if prices else 0
    alpha = 2/(period+1)
    ema = prices[0]
    for p in prices[1:]:
        ema = alpha * p + (1-alpha) * ema
    return ema

def calculate_atr(prices, period=14):
    if len(prices) < period+1:
        return 0
    trs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    return sum(trs[-period:]) / period

def calculate_vwap(prices, volumes=None):
    if not prices:
        return 0
    if volumes is None:
        volumes = [100] * len(prices)
    pv = sum(p*v for p,v in zip(prices, volumes))
    tv = sum(volumes)
    return pv/tv if tv else 0

def estimate_greeks(ce, pe):
    delta = round((ce - pe) / 100, 2)
    gamma = round(abs(delta) / 10, 2)
    theta = round(-(ce+pe) / 1000, 2)
    vega = round((ce+pe) / 500, 2)
    return delta, gamma, theta, vega

# ------------------------------------------------------------
# PCR with fallback and smoothing
# ------------------------------------------------------------
pcr_cache = {"value": 1.0, "time": 0, "source": "default"}
pcr_history = deque(maxlen=5)

def get_nifty_pcr():
    now = time.time()
    if now - pcr_cache["time"] < 120:
        return pcr_cache["value"], pcr_cache["source"]
    # Try NSE OI
    try:
        url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        headers = {"User-Agent": "Mozilla/5.0"}
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        time.sleep(1)
        resp = session.get(url, headers=headers, timeout=10)
        data = resp.json()
        records = data["records"]["data"]
        ce_oi = sum(x.get("CE", {}).get("openInterest", 0) for x in records if "CE" in x)
        pe_oi = sum(x.get("PE", {}).get("openInterest", 0) for x in records if "PE" in x)
        pcr = pe_oi / ce_oi if ce_oi else 1.0
        pcr_cache.update({"value": pcr, "time": now, "source": "nse_oi"})
        pcr_history.append(pcr)
        return pcr, "nse_oi"
    except Exception as e:
        logger.warning(f"NSE PCR failed: {e}")
    # Fallback to price-based PCR with EMA smoothing
    ce = latest_ticks.get("ce_price", 0)
    pe = latest_ticks.get("pe_price", 0)
    if ce > 0 and pe > 0:
        raw = pe / ce
        pcr_history.append(raw)
        if len(pcr_history) >= 3:
            ema = raw
            alpha = 2/6
            for val in list(pcr_history)[-5:]:
                ema = alpha * val + (1-alpha) * ema
            pcr = round(max(0.5, min(2.0, ema)), 2)
        else:
            pcr = round(max(0.5, min(2.0, raw)), 2)
        pcr_cache.update({"value": pcr, "time": now, "source": "price_ema"})
        return pcr, "price_ema"
    # final fallback
    return pcr_cache["value"], "cached"

# ------------------------------------------------------------
# Timeframe Trend Analysis
# ------------------------------------------------------------
def analyze_timeframe_trend(history):
    n = len(history)
    if n < 2:
        return "SIDEWAYS", 0, 0
    prices = [h["price"] for h in history]
    x = list(range(n))
    x_mean = sum(x)/n
    y_mean = sum(prices)/n
    num = sum((x[i]-x_mean)*(prices[i]-y_mean) for i in range(n))
    den = sum((x[i]-x_mean)**2 for i in range(n))
    if den == 0:
        return "SIDEWAYS", 0, 0
    slope = num/den
    ss_res = sum((prices[i] - (y_mean + slope*(x[i]-x_mean)))**2 for i in range(n))
    ss_tot = sum((prices[i]-y_mean)**2 for i in range(n))
    r2 = 1 - (ss_res/ss_tot) if ss_tot else 0
    if abs(slope) < 0.05 or r2 < 0.3:
        return "SIDEWAYS", abs(slope)*r2*100, r2
    return ("BULLISH" if slope>0 else "BEARISH"), abs(slope)*r2*100, r2

def get_all_timeframe_trends():
    return {tf: {"trend": analyze_timeframe_trend(list(hist))[0],
                 "strength": round(analyze_timeframe_trend(list(hist))[1],2)}
            for tf, hist in timeframe_history.items()}

# ------------------------------------------------------------
# Core Signal Engine (professional scoring) – Enhanced with filters
# ------------------------------------------------------------
def run_signal_engine(ce_price, pe_price, price_list, vol_list):
    global market_signal, market_state, institutional_state, signal_memory
    if len(price_list) < 30:
        return
    spread = ce_price - pe_price
    rsi = calculate_rsi(price_list)
    macd_line, macd_hist = calculate_macd(price_list)
    vwap = calculate_vwap(price_list, vol_list)
    ema_fast = calculate_ema(price_list, 9)
    ema_slow = calculate_ema(price_list, 21)
    atr = calculate_atr(price_list)
    pcr, pcr_src = get_nifty_pcr()
    delta, gamma, theta, vega = estimate_greeks(ce_price, pe_price)

    # ---- NEW: Volume filter ----
    if len(vol_list) >= 20:
        avg_vol = sum(vol_list[-20:]) / 20
        if vol_list[-1] < avg_vol * 0.6:
            logger.debug("Volume too low – skipping signal")
            return

    # ---- NEW: Spread sanity ----
    if atr > 0 and abs(spread) > atr * 1.5:
        logger.debug("Spread too wide – skipping signal")
        return

    # ---- NEW: Volatility filter ----
    atr_pct = (atr / price_list[-1]) * 100 if price_list[-1] > 0 else 0
    if atr_pct < 0.3:
        logger.debug("ATR% too low – skipping signal")
        return

    # Volume trend from actual volumes
    if len(vol_list) >= 20:
        recent_vol = sum(vol_list[-10:])/10
        older_vol = sum(vol_list[-20:-10])/10
        vol_trend = "INCREASING" if recent_vol > older_vol*1.2 else "DECREASING" if recent_vol < older_vol*0.8 else "FLAT"
    else:
        vol_trend = "FLAT"

    # Multi‑timeframe analysis
    tf_trends = get_all_timeframe_trends()
    trend_1min = tf_trends["1min"]["trend"]
    trend_5min = tf_trends["5min"]["trend"]
    trend_10min = tf_trends["10min"]["trend"]
    trend_15min = tf_trends["15min"]["trend"]
    trend_20min = tf_trends["20min"]["trend"]
    bullish_tf = sum(1 for t in [trend_1min, trend_5min, trend_10min, trend_15min, trend_20min] if t == "BULLISH")
    bearish_tf = sum(1 for t in [trend_1min, trend_5min, trend_10min, trend_15min, trend_20min] if t == "BEARISH")
    tf_score_bull = bullish_tf * 10
    tf_score_bear = bearish_tf * 10

    # Technical scores
    tech_bull = 0
    tech_bear = 0
    # RSI
    if 55 < rsi < 75:
        tech_bull += 10
    elif 40 < rsi < 55:
        tech_bull += 5
    if 25 < rsi < 45:
        tech_bear += 10
    elif 45 < rsi < 60:
        tech_bear += 5
    # MACD
    if macd_hist > 0 and macd_line > 0:
        tech_bull += 10
    elif macd_hist > 0:
        tech_bull += 6
    elif macd_hist < 0 and macd_line < 0:
        tech_bear += 10
    elif macd_hist < 0:
        tech_bear += 6
    # PCR
    if pcr < 0.9:
        tech_bull += 10
    elif pcr < 1.0:
        tech_bull += 7
    elif pcr > 1.3:
        tech_bear += 10
    elif pcr > 1.2:
        tech_bear += 7
    # Volume trend
    if vol_trend == "INCREASING":
        if bullish_tf > bearish_tf:
            tech_bull += 10
        elif bearish_tf > bullish_tf:
            tech_bear += 10
    # Price vs VWAP/EMA
    avg_price = (ce_price+pe_price)/2
    if avg_price > vwap and avg_price > ema_slow:
        tech_bull += 10
    elif avg_price > vwap or avg_price > ema_slow:
        tech_bull += 5
    elif avg_price < vwap and avg_price < ema_slow:
        tech_bear += 10
    elif avg_price < vwap or avg_price < ema_slow:
        tech_bear += 5

    total_bull = tf_score_bull + tech_bull
    total_bear = tf_score_bear + tech_bear
    raw_confidence = max(total_bull, total_bear)

    # Determine raw action
    if total_bull >= total_bear and total_bull >= CONSIDER_THRESHOLD:
        if bullish_tf >= 4 and raw_confidence >= STRONG_BUY_THRESHOLD:
            raw_action = "TRENDING: CE"
            signal_type = "TRENDING"
        elif raw_confidence >= STRONG_BUY_THRESHOLD:
            raw_action = "STRONG BUY CE"
            signal_type = "MOMENTUM"
        elif raw_confidence >= BUY_THRESHOLD:
            raw_action = "BUY CE"
            signal_type = "MOMENTUM"
        elif raw_confidence >= CONSIDER_THRESHOLD:
            raw_action = "CONSIDER CE"
            signal_type = "MOMENTUM"
        else:
            raw_action = "HOLD"
            signal_type = "NONE"
    elif total_bear > total_bull and total_bear >= CONSIDER_THRESHOLD:
        if bearish_tf >= 4 and raw_confidence >= STRONG_BUY_THRESHOLD:
            raw_action = "TRENDING: PE"
            signal_type = "TRENDING"
        elif raw_confidence >= STRONG_BUY_THRESHOLD:
            raw_action = "STRONG BUY PE"
            signal_type = "MOMENTUM"
        elif raw_confidence >= BUY_THRESHOLD:
            raw_action = "BUY PE"
            signal_type = "MOMENTUM"
        elif raw_confidence >= CONSIDER_THRESHOLD:
            raw_action = "CONSIDER PE"
            signal_type = "MOMENTUM"
        else:
            raw_action = "HOLD"
            signal_type = "NONE"
    else:
        raw_action = "HOLD"
        signal_type = "NONE"
        raw_confidence = max(total_bull, total_bear)

    # ---- NEW: consecutive minute bar confirmation ----
    if len(timeframe_history["1min"]) >= 2:
        last_two = list(timeframe_history["1min"])[-2:]
        dir1 = 1 if last_two[0]["ce"] > last_two[0]["pe"] else -1
        dir2 = 1 if last_two[1]["ce"] > last_two[1]["pe"] else -1
        if dir1 != dir2 and raw_action != "HOLD":
            raw_action = "HOLD"
            raw_confidence = 0
            signal_type = "NONE"

    # Anti‑flip & persistence logic
    now_ts = time.time()
    if raw_action != signal_memory["current_action"]:
        # cooldown check
        if now_ts < signal_memory.get("cooldown_until", 0):
            final_action = signal_memory["current_action"]
            final_signal_type = signal_memory["current_signal_type"]
        else:
            signal_memory["confirmation_count"] = 1 if raw_action != "HOLD" else 0
            if signal_memory["confirmation_count"] >= signal_memory["required_confirmations"]:
                final_action = raw_action
                final_signal_type = signal_type
                signal_memory["signal_start_time"] = now_ts
                signal_memory["cooldown_until"] = now_ts + 30  # 30 sec cooldown
            else:
                final_action = signal_memory["current_action"]
                final_signal_type = signal_memory["current_signal_type"]
    else:
        if raw_action != "HOLD":
            signal_memory["confirmation_count"] += 1
        final_action = raw_action
        final_signal_type = signal_type

    # ---- NEW: signal expiry (time‑based invalidation) ----
    if signal_memory["signal_start_time"]:
        elapsed = now_ts - signal_memory["signal_start_time"]
        if elapsed > 1800:  # 30 minutes
            final_action = "HOLD"
            final_signal_type = "NONE"
            signal_memory["signal_start_time"] = None
            logger.info("Signal expired due to time limit")

    # Update memory
    signal_memory["current_action"] = final_action
    signal_memory["current_signal_type"] = final_signal_type

    # Duration for display
    signal_duration = 0
    if signal_memory["signal_start_time"]:
        signal_duration = int((now_ts - signal_memory["signal_start_time"]) / 60)

    # Update state objects
    market_state.update({
        "rsi": round(rsi,2),
        "momentum": "UPTREND" if bullish_tf>bearish_tf else "DOWNTREND" if bearish_tf>bullish_tf else "NEUTRAL",
        "strength": "HIGH" if final_signal_type=="TRENDING" else "MODERATE" if final_signal_type=="MOMENTUM" else "LOW",
        "trend": "BULLISH" if bullish_tf>bearish_tf else "BEARISH" if bearish_tf>bullish_tf else "SIDEWAYS",
        "action": final_action,
        "confidence": raw_confidence,
        "volatility": "HIGH" if atr>15 else "NORMAL" if atr>5 else "LOW",
        "alert": final_action,
        "signal_duration_minutes": signal_duration,
        "trend_1min": trend_1min,
        "trend_5min": trend_5min,
        "trend_10min": trend_10min,
        "trend_15min": trend_15min,
        "trend_20min": trend_20min,
        "timeframe_agreement": max(bullish_tf, bearish_tf)
    })
    institutional_state.update({
        "vwap": round(vwap,2),
        "ema_fast": round(ema_fast,2),
        "ema_slow": round(ema_slow,2),
        "ema_signal": "BULLISH" if ema_fast>ema_slow else "BEARISH",
        "atr": round(atr,2),
        "oi_buildup": "BULLISH" if pcr<0.9 else "BEARISH" if pcr>1.2 else "NEUTRAL",
        "iv_state": "HIGH" if vega>2 else "NORMAL",
        "candle_structure": "BULLISH" if trend_5min=="BULLISH" and rsi>55 else "BEARISH" if trend_5min=="BEARISH" and rsi<45 else "SIDEWAYS",
        "market_breadth": "BULLISH" if bullish_tf>=3 else "BEARISH" if bearish_tf>=3 else "BALANCED",
        "volume_profile": vol_trend,
        "smart_money_flow": "BULLISH" if vwap>ema_slow and vol_trend=="INCREASING" else "BEARISH" if vwap<ema_slow and vol_trend=="INCREASING" else "NEUTRAL",
        "volume_trend": vol_trend,
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "institutional_signal": final_action,
        "institutional_confidence": raw_confidence,
        "consecutive_confirmations": signal_memory["confirmation_count"]
    })
    market_signal.update({
        "signal": "BULLISH" if final_action in ["BUY CE","STRONG BUY CE","TRENDING: CE","CONSIDER CE"] else "BEARISH" if final_action in ["BUY PE","STRONG BUY PE","TRENDING: PE","CONSIDER PE"] else "NEUTRAL",
        "ce_price": ce_price,
        "pe_price": pe_price,
        "spread": round(spread,2),
        "rsi": round(rsi,2),
        "macd": round(macd_hist,2),
        "pcr": round(pcr,2),
        "vwap": round(vwap,2),
        "atr": round(atr,2),
        "ema_fast": round(ema_fast,2),
        "ema_slow": round(ema_slow,2),
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "volume": int(vol_list[-1]) if vol_list else 0,
        "volume_avg": 100,
        "oi_signal": institutional_state["oi_buildup"],
        "timestamp": datetime.now().isoformat()
    })
    # Log only on change or every 5 minutes
    if final_action != signal_memory.get("last_logged_action", ""):
        logger.info(f"PRO SIGNAL: {final_action} [{final_signal_type}] Dur:{signal_duration}m Conf:{raw_confidence} TF:{bullish_tf}B/{bearish_tf}Be PCR:{pcr:.2f} RSI:{rsi:.1f}")
        signal_memory["last_logged_action"] = final_action

# ------------------------------------------------------------
# WebSocket Callbacks with Queue Processing
# ------------------------------------------------------------
def ws_on_open(wsapp):
    logger.info("WebSocket opened")
    if sws and CE_TOKEN and PE_TOKEN:
        sws.subscribe("tradeguru", 2, [{"exchangeType": 2, "tokens": [CE_TOKEN, PE_TOKEN]}])

def ws_on_data(wsapp, message, *args):
    global last_tick_time
    last_tick_time = time.time()
    tick_queue.put(message)   # non‑blocking enqueue

def ws_on_error(wsapp, error):
    logger.error(f"WebSocket error: {error}")

def ws_on_close(wsapp, *args):
    logger.warning("WebSocket closed")
    global ws_running
    ws_running = False

def process_tick_worker():
    """Background worker that processes ticks from the queue."""
    global latest_ticks, price_history, volume_history, tick_counter, last_minute_snapshot
    while engine_active:
        try:
            message = tick_queue.get(timeout=1)
            data = json.loads(message) if isinstance(message, str) else message
            ticks = data if isinstance(data, list) else [data]
            for tick in ticks:
                token = str(tick.get("tk") or tick.get("token") or "")
                if token not in (CE_TOKEN, PE_TOKEN):
                    continue
                ltp = tick.get("ltp", 0)
                if isinstance(ltp, (int,float)):
                    ltp = ltp / 100 if ltp > 1000 else ltp
                vol = tick.get("v", 0) or tick.get("volume", 0)
                if token == CE_TOKEN:
                    latest_ticks["ce_price"] = ltp
                    latest_ticks["ce_volume"] = vol
                    price_history.append(ltp)
                    volume_history.append(vol)
                    tick_counter += 1
                elif token == PE_TOKEN:
                    latest_ticks["pe_price"] = ltp
                    latest_ticks["pe_volume"] = vol
                # Minute snapshots
                now = time.time()
                if now - last_minute_snapshot["time"] >= 60:
                    avg_price = (latest_ticks["ce_price"] + latest_ticks["pe_price"]) / 2
                    avg_vol = (latest_ticks["ce_volume"] + latest_ticks["pe_volume"]) / 2
                    snap = {"time": now, "price": avg_price, "volume": avg_vol,
                            "ce": latest_ticks["ce_price"], "pe": latest_ticks["pe_price"]}
                    for tf in timeframe_history:
                        timeframe_history[tf].append(snap)
                    last_minute_snapshot["time"] = now
                    last_minute_snapshot["price"] = avg_price
                # Run signal engine periodically
                ce = latest_ticks["ce_price"]
                pe = latest_ticks["pe_price"]
                if ce > 0 and pe > 0 and len(price_history) >= 30 and tick_counter % UPDATE_INTERVAL_TICKS == 0:
                    run_signal_engine(ce, pe, list(price_history), list(volume_history))
        except queue.Empty:
            continue
        except Exception as e:
            logger.error(f"Tick worker error: {e}")

# ------------------------------------------------------------
# Watchdog for missing ticks
# ------------------------------------------------------------
def watchdog_thread():
    while engine_active:
        time.sleep(10)
        if not ws_running:
            continue
        age = time.time() - last_tick_time
        if age > WATCHDOG_TIMEOUT_SEC:
            logger.warning(f"No ticks for {age:.0f}s, forcing reconnect")
            global sws
            if sws:
                try:
                    sws.close()
                except:
                    pass
            ws_running = False

# ------------------------------------------------------------
# REST fallback thread
# ------------------------------------------------------------
def rest_fallback_thread():
    while engine_active:
        time.sleep(REST_FALLBACK_DELAY_SEC)
        if not ws_running or (time.time() - last_tick_time) < 15:
            continue
        if CE_TOKEN and PE_TOKEN:
            ce_rest = get_ltp_rest(CE_TOKEN)
            pe_rest = get_ltp_rest(PE_TOKEN)
            if ce_rest > 0 and pe_rest > 0:
                latest_ticks["ce_price"] = ce_rest
                latest_ticks["pe_price"] = pe_rest
                price_history.append(ce_rest)
                volume_history.append(100)
                logger.info(f"REST fallback: CE={ce_rest}, PE={pe_rest}")

# ------------------------------------------------------------
# Auto token refresh timer
# ------------------------------------------------------------
def token_refresh_thread():
    while engine_active:
        time.sleep(TOKEN_REFRESH_HOURS * 3600)
        logger.info("Periodic token refresh triggered")
        global ws_running, sws
        if sws:
            try:
                sws.close()
            except:
                pass
        ws_running = False
        time.sleep(5)   # let reconnection happen

# ------------------------------------------------------------
# Contract rollover logic (freeze old during expiry day 3:00-3:30 PM)
# ------------------------------------------------------------
def contract_rollover_thread():
    global CE_TOKEN, PE_TOKEN, last_contract_update
    while engine_active:
        time.sleep(60)
        now = datetime.now()
        # Freeze 30 minutes before expiry close on Thursday
        if now.weekday() == 3 and now.hour == 15 and now.minute < 30:
            continue
        # After 4:00 PM, allow update
        if now.weekday() == 3 and now.hour >= 16:
            pass
        # Otherwise, refresh only if old contract has zero volume for 5 minutes
        ce_vol = latest_ticks.get("ce_volume", 0)
        pe_vol = latest_ticks.get("pe_volume", 0)
        if ce_vol == 0 and pe_vol == 0 and (time.time() - last_tick_time) > 300:
            new_ce, new_pe = get_current_atm_tokens()
            if new_ce and new_pe and (new_ce != CE_TOKEN or new_pe != PE_TOKEN):
                CE_TOKEN, PE_TOKEN = new_ce, new_pe
                last_contract_update = time.time()
                logger.info(f"Contract rollover: CE={CE_TOKEN}, PE={PE_TOKEN}")
                ws_running = False   # force reconnect

# ------------------------------------------------------------
# Main WebSocket connection with auto‑reconnect
# ------------------------------------------------------------
def start_angel_websocket():
    global ws_running, CE_TOKEN, PE_TOKEN, sws, last_auth_time
    retry_delay = 30
    while engine_active:
        try:
            if not is_market_open():
                logger.info("Market closed, waiting...")
                time.sleep(300)
                continue
            # Force token refresh if needed (every 20 hours)
            now_ts = time.time()
            if not CE_TOKEN or not PE_TOKEN or (now_ts - last_auth_time) > TOKEN_REFRESH_HOURS*3600:
                totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
                obj = SmartConnect(api_key=ANGEL_API_KEY)
                session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
                if not session.get("status"):
                    logger.error(f"Auth failed: {session}")
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay*2, 300)
                    continue
                auth_token = session["data"]["jwtToken"]
                feed_token = obj.getfeedToken()
                last_auth_time = now_ts
                if not CE_TOKEN or not PE_TOKEN:
                    CE_TOKEN, PE_TOKEN = get_current_atm_tokens()
                logger.info("Authenticated, tokens ready")
            # Establish WebSocket
            sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)
            sws.on_open = ws_on_open
            sws.on_data = ws_on_data
            sws.on_error = ws_on_error
            sws.on_close = ws_on_close
            ws_running = True
            sws.connect()
            while ws_running:
                time.sleep(0.1)
        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay*2, 300)

# ------------------------------------------------------------
# Flask Endpoints
# ------------------------------------------------------------
@app.route("/")
def home():
    return jsonify({"status": "online", "message": "Professional Nifty Signal Engine"})

@app.route("/api/live-signals")
def live_signals():
    spot = get_nifty_spot_cached()
    return jsonify({
        "status": "active",
        "data": market_signal,
        "market": market_state,
        "institutional": institutional_state,
        "timeframes": get_all_timeframe_trends(),
        "spot_price": spot if spot else 0,
        "signal_memory": {
            "current_action": signal_memory["current_action"],
            "signal_type": signal_memory["current_signal_type"],
            "confirmations": signal_memory["confirmation_count"]
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
        "tick_queue_size": tick_queue.qsize(),
        "last_tick_age": round(time.time() - last_tick_time, 1),
        "timestamp": datetime.now().isoformat()
    }), 200

@app.route("/metrics")
def metrics():
    return jsonify({
        "ws_running": ws_running,
        "last_tick_seconds_ago": round(time.time() - last_tick_time, 1),
        "price_history_len": len(price_history),
        "current_action": signal_memory["current_action"],
        "signal_confidence": market_state["confidence"],
        "ce_price": latest_ticks["ce_price"],
        "pe_price": latest_ticks["pe_price"],
        "spread": market_signal["spread"]
    })

# ------------------------------------------------------------
# Graceful shutdown
# ------------------------------------------------------------
def shutdown_handler(signum, frame):
    global engine_active
    logger.info("Shutdown signal received, cleaning up...")
    engine_active = False
    if sws:
        try:
            sws.close()
        except:
            pass
    sys.exit(0)
signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)

# ------------------------------------------------------------
# Background Threads
# ------------------------------------------------------------
def start_background_engine():
    threading.Thread(target=start_angel_websocket, daemon=True).start()
    threading.Thread(target=process_tick_worker, daemon=True).start()
    threading.Thread(target=watchdog_thread, daemon=True).start()
    threading.Thread(target=rest_fallback_thread, daemon=True).start()
    threading.Thread(target=token_refresh_thread, daemon=True).start()
    threading.Thread(target=contract_rollover_thread, daemon=True).start()
    logger.info("All background engines started")

if __name__ == "__main__":
    start_background_engine()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
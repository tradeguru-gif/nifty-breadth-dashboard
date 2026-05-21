"""
backend.py — Institutional‑Grade Nifty Options Signal Engine v4.4
FIXED: WebSocket subscription (exchangeType=5, mode=1), token refresh logic,
       Thursday expiry rollover, debug endpoints, robust tick reception.
"""

import os
import sys
import time
import json
import queue
import logging
import threading
import signal
import math
import statistics
import fcntl
from collections import deque
from datetime import datetime, timedelta

import requests
import pandas as pd
import pyotp
from flask import Flask, jsonify
from flask_cors import CORS
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app

# ------------------------------------------------------------
# Environment
# ------------------------------------------------------------
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing Angel One credentials")

# ------------------------------------------------------------
# Global State
# ------------------------------------------------------------
spot_cache = {"value": None, "timestamp": 0}
CE_TOKEN = None
PE_TOKEN = None
latest_ticks = {"ce_price": 0.0, "pe_price": 0.0, "ce_volume": 0, "pe_volume": 0}
price_history = deque(maxlen=500)
volume_history = deque(maxlen=500)
tick_counter = 0
ws_running = False
sws = None
last_tick_time = time.time()
engine_active = True
_last_ce_pe_zero_start = None   # for detecting stale tokens

# Gunicorn worker coordination
_LOCK_FILE = "/tmp/nifty_signal_engine.lock"
_is_primary_worker = False
_worker_lock_fd = None

# Reconnection state
_reconnecting = False
_reconnect_lock = threading.Lock()
_last_429_time = 0

# Multi‑timeframe storage
timeframe_history = {
    "1min": deque(maxlen=60),
    "5min": deque(maxlen=20),
    "10min": deque(maxlen=15),
    "15min": deque(maxlen=10),
    "20min": deque(maxlen=10)
}
last_minute_snapshot = {"time": 0, "price": 0, "volume": 0}

# Signal memory
signal_memory = {
    "current_action": "HOLD",
    "current_signal_type": "NONE",
    "signal_start_time": None,
    "last_confirmed_action": "HOLD",
    "confirmation_count": 0,
    "required_confirmations": 2,
    "cooldown_until": 0,
    "last_logged_action": "",
    "signal_grade": "D",
    "entry_price": 0.0,
    "stop_loss": 0.0,
    "target": 0.0,
    "position_size_pct": 0,
    "risk_reward": 0.0,
    "max_drawdown_pct": 0.0
}

market_signal = {
    "signal": "WAITING", "ce_price": 0.0, "pe_price": 0.0, "spread": 0.0,
    "rsi": 50, "macd": 0.0, "pcr": 1.0, "vwap": 0.0, "atr": 0.0,
    "ema_fast": 0.0, "ema_slow": 0.0, "delta": 0.0, "gamma": 0.0,
    "theta": 0.0, "vega": 0.0, "volume": 0, "timestamp": "",
    "atr_pct": 0.0, "adx": 0.0, "bb_position": 50.0, "rsi_divergence": "NONE",
    "iv_rank": 50, "signal_grade": "D"
}
market_state = {
    "rsi": 50, "momentum": "NEUTRAL", "strength": "LOW", "trend": "SIDEWAYS",
    "action": "HOLD", "confidence": 0, "volatility": "NORMAL", "alert": "NONE",
    "regime": "UNKNOWN", "session_phase": "UNKNOWN",
    "trend_1min": "SIDEWAYS", "trend_5min": "SIDEWAYS", "trend_10min": "SIDEWAYS",
    "trend_15min": "SIDEWAYS", "trend_20min": "SIDEWAYS", "timeframe_agreement": 0
}
institutional_state = {
    "vwap": 0, "ema_fast": 0, "ema_slow": 0, "ema_signal": "NEUTRAL", "atr": 0,
    "oi_buildup": "NEUTRAL", "iv_state": "NORMAL", "candle_structure": "SIDEWAYS",
    "market_breadth": "BALANCED", "volume_profile": "NORMAL", "smart_money_flow": "NEUTRAL",
    "delta": 0, "gamma": 0, "theta": 0, "vega": 0,
    "institutional_signal": "HOLD", "institutional_confidence": 0,
    "signal_grade": "D", "position_size_pct": 0, "risk_reward": 0.0,
    "entry_price": 0.0, "stop_loss": 0.0, "target": 0.0, "max_drawdown_pct": 0.0
}

CONFIG = {
    "RSI_PERIOD": 14,
    "MACD_FAST": 12,
    "MACD_SLOW": 26,
    "MACD_SIGNAL": 9,
    "ATR_PERIOD": 14,
    "BB_PERIOD": 20,
    "BB_STD": 2.0,
    "ADX_PERIOD": 14,
    "VWAP_WINDOW": 50,
    "PCR_EMA_PERIOD": 10,
    "PCR_BULLISH": 0.9,
    "PCR_BEARISH": 1.2,
    "STRONG_BUY_THRESHOLD": 85,
    "BUY_THRESHOLD": 70,
    "CONSIDER_THRESHOLD": 55,
    "MAX_FLIPS_PER_HOUR": 3,
    "VOLUME_FILTER_RATIO": 0.6,
    "SPREAD_ATR_MULTIPLIER": 1.5,
    "MIN_VOLATILITY_PCT": 0.3,
    "SIGNAL_CONFIRMATION_BARS": 2,
    "SIGNAL_MIN_DURATION_SEC": 180,
    "SIGNAL_MAX_AGE_SEC": 1800,
    "COOLDOWN_AFTER_FLIP_SEC": 30,
    "POSITION_SIZE_BASE_PCT": 10,
    "POSITION_SIZE_MAX_PCT": 25,
    "STOP_LOSS_ATR_MULT": 1.5,
    "TARGET_ATR_MULT": 3.0,
    "MAX_DRAWDOWN_PCT": 5.0,
    "RISK_FREE_RATE": 0.06,
    "DAYS_TO_EXPIRY": 7
}

# ------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------
def is_market_open():
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    # On Thursday, disable last 30 minutes (avoid expiry chaos)
    if now.weekday() == 3 and now.hour == 15 and now.minute >= 30:
        return False
    start = datetime.strptime("09:15", "%H:%M").time()
    end = datetime.strptime("15:30", "%H:%M").time()
    return start <= now.time() <= end

def get_nifty_spot():
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        time.sleep(0.5)
        resp = session.get(url, headers=headers, timeout=10)
        data = resp.json()
        spot = float(data["data"][0]["lastPrice"])
        logger.info(f"NIFTY spot = {spot}")
        return spot
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
    """Return (ce_token, pe_token) for the most liquid ATM strike (nearest expiry)."""
    spot = get_nifty_spot_cached()
    if not spot:
        logger.error("Cannot get ATM tokens: spot price unavailable")
        return None, None

    atm_strike = round(spot / 50) * 50
    logger.info(f"ATM strike = {atm_strike} (spot={spot})")

    try:
        url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        df = pd.DataFrame(resp.json())
    except Exception as e:
        logger.error(f"Instrument master error: {e}")
        return None, None

    nifty_opts = df[
        (df["name"].astype(str) == "NIFTY") &
        (df["instrumenttype"].astype(str) == "OPTIDX") &
        (df["exch_seg"].astype(str) == "NFO")
    ].copy()
    if nifty_opts.empty:
        nifty_opts = df[df["symbol"].astype(str).str.match(r'^NIFTY\d{2}[A-Z]{3}\d{2}', na=False)].copy()
    if nifty_opts.empty:
        logger.error("No NIFTY options found")
        return None, None

    # Parse expiry dates
    for fmt in ["%d%b%Y", "%d-%b-%Y", "%Y-%m-%d", "%d%m%Y"]:
        nifty_opts["expiry_date"] = pd.to_datetime(nifty_opts["expiry"], format=fmt, errors="coerce")
        if nifty_opts["expiry_date"].notna().sum() > 0:
            break
    nifty_opts = nifty_opts.dropna(subset=["expiry_date"])
    nifty_opts["strike"] = pd.to_numeric(nifty_opts["strike"], errors="coerce") / 100
    nifty_opts = nifty_opts.dropna(subset=["strike"])

    today = datetime.now()
    # Determine which expiry to use: if Thursday after 3:30 PM, skip to next week
    if today.weekday() == 3 and today.hour >= 15 and today.minute >= 30:
        # Use expiry after today (next week)
        future_expiries = nifty_opts[nifty_opts["expiry_date"] > today]
    else:
        # Use the first expiry after today (nearest)
        future_expiries = nifty_opts[nifty_opts["expiry_date"] >= today]
    if future_expiries.empty:
        logger.error("No future expiry found")
        return None, None
    nearest_expiry = future_expiries["expiry_date"].min()
    logger.info(f"Using expiry: {nearest_expiry.date()}")

    atm_opts = nifty_opts[(nifty_opts["strike"] == atm_strike) & (nifty_opts["expiry_date"] == nearest_expiry)]
    if atm_opts.empty:
        strikes = sorted(nifty_opts[nifty_opts["expiry_date"] == nearest_expiry]["strike"].unique())
        nearest_strike = min(strikes, key=lambda x: abs(x - atm_strike))
        logger.info(f"ATM strike not found, using nearest: {nearest_strike}")
        atm_opts = nifty_opts[(nifty_opts["strike"] == nearest_strike) & (nifty_opts["expiry_date"] == nearest_expiry)]

    ce = atm_opts[atm_opts["symbol"].str.upper().str.contains("CE", na=False)]
    pe = atm_opts[atm_opts["symbol"].str.upper().str.contains("PE", na=False)]
    if ce.empty or pe.empty:
        logger.error(f"CE/PE not found. CE count={len(ce)}, PE count={len(pe)}")
        return None, None

    ce_token = str(ce.iloc[0]["token"])
    pe_token = str(pe.iloc[0]["token"])
    logger.info(f"Tokens resolved: CE={ce_token}, PE={pe_token}")
    return ce_token, pe_token

# ------------------------------------------------------------
# Technical Indicators (unchanged)
# ------------------------------------------------------------
def calculate_ema_series(prices, period):
    if len(prices) < period:
        return [prices[-1]] * len(prices) if prices else [0]
    alpha = 2/(period+1)
    ema = [prices[0]]
    for p in prices[1:]:
        ema.append(alpha * p + (1-alpha) * ema[-1])
    return ema

def calculate_bollinger(prices, period=20, std_dev=2.0):
    if len(prices) < period:
        return 0.0, 0.0, 0.0, 50.0
    window = prices[-period:]
    sma = sum(window)/period
    variance = sum((p-sma)**2 for p in window)/period
    std = math.sqrt(variance)
    upper = sma + std_dev * std
    lower = sma - std_dev * std
    if upper == lower:
        pos = 50.0
    else:
        pos = (prices[-1] - lower) / (upper - lower) * 100
    return sma, upper, lower, max(0, min(100, pos))

def calculate_adx(prices, period=14):
    if len(prices) < period*2:
        return 0.0
    trs = [abs(prices[i]-prices[i-1]) for i in range(1, len(prices))]
    plus_dm = []
    minus_dm = []
    for i in range(1, len(prices)):
        move = prices[i] - prices[i-1]
        plus_dm.append(max(move, 0))
        minus_dm.append(max(-move, 0))
    if len(trs) < period:
        return 0.0
    atr = sum(trs[-period:])/period
    if atr == 0:
        return 0.0
    plus_di = 100 * sum(plus_dm[-period:])/period / atr
    minus_di = 100 * sum(minus_dm[-period:])/period / atr
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di + 1e-10)
    return dx

def calculate_rsi_divergence(prices, rsi_values, lookback=5):
    if len(prices) < lookback+2 or len(rsi_values) < lookback+2:
        return "NONE"
    price_lows = prices[-lookback:]
    rsi_lows = rsi_values[-lookback:]
    if min(price_lows) < price_lows[0] and min(rsi_lows) > rsi_lows[0]:
        return "BULLISH"
    price_highs = prices[-lookback:]
    rsi_highs = rsi_values[-lookback:]
    if max(price_highs) > price_highs[0] and max(rsi_highs) < rsi_highs[0]:
        return "BEARISH"
    return "NONE"

def estimate_iv_rank(ce_price, history, period=20):
    if len(history) < period:
        return 50
    iv_min = min(history[-period:])
    iv_max = max(history[-period:])
    if iv_max == iv_min:
        return 50
    rank = (ce_price - iv_min) / (iv_max - iv_min) * 100
    return max(0, min(100, rank))

def get_session_phase():
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
    elif mins < 15*60+0:
        return "AFTERNOON"
    elif mins < 15*60+30:
        return "CLOSING"
    else:
        return "POST_MARKET"

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

pcr_cache = {"value": 1.0, "time": 0, "source": "default"}
pcr_history = deque(maxlen=5)

def get_nifty_pcr():
    now = time.time()
    if now - pcr_cache["time"] < 120:
        return pcr_cache["value"], pcr_cache["source"]
    try:
        url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/option-chain"
        }
        session = requests.Session()
        home_resp = session.get("https://www.nseindia.com", headers=headers, timeout=15)
        time.sleep(2)
        resp = session.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if "records" in data and "data" in data["records"]:
                records = data["records"]["data"]
                ce_oi = sum(x.get("CE", {}).get("openInterest", 0) for x in records if "CE" in x)
                pe_oi = sum(x.get("PE", {}).get("openInterest", 0) for x in records if "PE" in x)
                pcr = pe_oi / ce_oi if ce_oi else 1.0
                pcr_cache.update({"value": pcr, "time": now, "source": "nse_oi"})
                pcr_history.append(pcr)
                logger.info(f"NSE PCR fetched: {pcr:.2f}")
                return pcr, "nse_oi"
            else:
                logger.warning(f"NSE PCR response missing records: {list(data.keys())}")
        else:
            logger.warning(f"NSE PCR HTTP {resp.status_code}")
    except Exception as e:
        logger.warning(f"NSE PCR failed: {e}")

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
    return pcr_cache["value"], "cached"

def calculate_pcr_ema():
    if len(pcr_history) < 3:
        return 1.0
    alpha = 2/(CONFIG["PCR_EMA_PERIOD"]+1)
    ema = pcr_history[0]
    for val in list(pcr_history)[1:]:
        ema = alpha * val + (1-alpha) * ema
    return ema

# ------------------------------------------------------------
# Core Calculations
# ------------------------------------------------------------
def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    gains = []
    losses = []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calculate_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow:
        return 0.0, 0.0
    ema_fast = calculate_ema_series(prices, fast)[-1]
    ema_slow = calculate_ema_series(prices, slow)[-1]
    macd_line = ema_fast - ema_slow
    macd_hist = macd_line
    return macd_line, macd_hist

def calculate_vwap(prices, volumes):
    if not prices or not volumes or len(prices) != len(volumes):
        return prices[-1] if prices else 0
    cum_pv = sum(p * v for p, v in zip(prices, volumes))
    cum_vol = sum(volumes)
    return cum_pv / cum_vol if cum_vol > 0 else prices[-1]

def calculate_ema(prices, period):
    if len(prices) < period:
        return prices[-1] if prices else 0
    alpha = 2 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = alpha * p + (1 - alpha) * ema
    return ema

def calculate_atr(prices, period=14):
    if len(prices) < period + 1:
        return 0.0
    trs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    return sum(trs[-period:]) / period if len(trs) >= period else 0.0

def estimate_greeks(ce_price, pe_price):
    spot = get_nifty_spot_cached() or 0
    atm_strike = round(spot / 50) * 50 if spot else 0
    moneyness = abs(spot - atm_strike) / spot if spot > 0 else 0
    delta = 0.5 - moneyness if ce_price > pe_price else -(0.5 - moneyness)
    delta = max(-1, min(1, delta))
    gamma = 0.05 * (1 - moneyness * 2) if moneyness < 0.5 else 0.01
    theta = -ce_price * 0.001 * CONFIG["DAYS_TO_EXPIRY"]
    vega = ce_price * 0.1
    return round(delta, 4), round(gamma, 4), round(theta, 4), round(vega, 4)

def run_signal_engine(ce_price, pe_price, price_list, vol_list):
    global market_signal, market_state, institutional_state, signal_memory
    if len(price_list) < 30:
        return

    spread = ce_price - pe_price
    rsi = calculate_rsi(price_list, CONFIG["RSI_PERIOD"])
    macd_line, macd_hist = calculate_macd(price_list, CONFIG["MACD_FAST"], CONFIG["MACD_SLOW"], CONFIG["MACD_SIGNAL"])
    vwap = calculate_vwap(price_list, vol_list)
    ema_fast = calculate_ema(price_list, 9)
    ema_slow = calculate_ema(price_list, 21)
    atr = calculate_atr(price_list, CONFIG["ATR_PERIOD"])
    pcr, pcr_src = get_nifty_pcr()
    pcr_ema = calculate_pcr_ema()
    delta, gamma, theta, vega = estimate_greeks(ce_price, pe_price)

    bb_sma, bb_upper, bb_lower, bb_pos = calculate_bollinger(price_list, CONFIG["BB_PERIOD"], CONFIG["BB_STD"])
    adx = calculate_adx(price_list, CONFIG["ADX_PERIOD"])
    rsi_values = [calculate_rsi(price_list[:i+1], CONFIG["RSI_PERIOD"]) for i in range(CONFIG["RSI_PERIOD"], len(price_list))]
    rsi_div = calculate_rsi_divergence(price_list, rsi_values) if len(rsi_values) >= 5 else "NONE"
    atr_pct = (atr / price_list[-1]) * 100 if price_list[-1] > 0 else 0
    iv_rank = estimate_iv_rank(ce_price, price_list[-min(20, len(price_list)):], 20)

    if len(vol_list) >= 20:
        recent_vol = sum(vol_list[-10:])/10
        older_vol = sum(vol_list[-20:-10])/10
        vol_trend = "INCREASING" if recent_vol > older_vol*1.2 else "DECREASING" if recent_vol < older_vol*0.8 else "FLAT"
    else:
        vol_trend = "FLAT"

    if adx > CONFIG["ADX_PERIOD"]:
        regime = "TRENDING"
    elif atr_pct > 1.5:
        regime = "VOLATILE"
    elif bb_pos < 20 or bb_pos > 80:
        regime = "BREAKOUT"
    else:
        regime = "RANGING"
    session_phase = get_session_phase()
    market_state["regime"] = regime
    market_state["session_phase"] = session_phase

    tf_trends = get_all_timeframe_trends()
    bullish_tf = sum(1 for t in ["1min","5min","10min","15min","20min"] if tf_trends[t]["trend"]=="BULLISH")
    bearish_tf = sum(1 for t in ["1min","5min","10min","15min","20min"] if tf_trends[t]["trend"]=="BEARISH")
    tf_score_bull = bullish_tf * 10
    tf_score_bear = bearish_tf * 10

    tech_bull = 0
    tech_bear = 0

    if 55 < rsi < 75:
        tech_bull += 10
    elif 40 < rsi < 55:
        tech_bull += 5
    if 25 < rsi < 45:
        tech_bear += 10
    elif 45 < rsi < 60:
        tech_bear += 5

    if macd_hist > 0 and macd_line > 0:
        tech_bull += 10
    elif macd_hist > 0:
        tech_bull += 6
    elif macd_hist < 0 and macd_line < 0:
        tech_bear += 10
    elif macd_hist < 0:
        tech_bear += 6

    if pcr < CONFIG["PCR_BULLISH"]:
        tech_bull += 10
    elif pcr < 1.0:
        tech_bull += 7
    elif pcr > CONFIG["PCR_BEARISH"]:
        tech_bear += 10
    elif pcr > 1.2:
        tech_bear += 7

    if vol_trend == "INCREASING":
        if ema_fast > ema_slow:
            tech_bull += 10
        elif ema_fast < ema_slow:
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

    if iv_rank > 70:
        tech_bull -= 5
        tech_bear += 3
    elif iv_rank < 30:
        tech_bull += 3

    total_bull = tf_score_bull + tech_bull
    total_bear = tf_score_bear + tech_bear
    raw_confidence = max(total_bull, total_bear)

    if total_bull >= total_bear and total_bull >= CONFIG["CONSIDER_THRESHOLD"]:
        if raw_confidence >= CONFIG["STRONG_BUY_THRESHOLD"]:
            raw_action = "STRONG BUY CE"
            signal_type = "TRENDING" if bullish_tf >= 4 else "MOMENTUM"
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
            signal_type = "TRENDING" if bearish_tf >= 4 else "MOMENTUM"
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
        raw_confidence = max(total_bull, total_bear)

    now_ts = time.time()
    if raw_action != signal_memory["current_action"]:
        if now_ts < signal_memory.get("cooldown_until", 0):
            final_action = signal_memory["current_action"]
            final_signal_type = signal_memory["current_signal_type"]
        else:
            signal_memory["confirmation_count"] = 1 if raw_action != "HOLD" else 0
            if signal_memory["confirmation_count"] >= signal_memory["required_confirmations"]:
                final_action = raw_action
                final_signal_type = signal_type
                signal_memory["signal_start_time"] = now_ts
                signal_memory["cooldown_until"] = now_ts + CONFIG["COOLDOWN_AFTER_FLIP_SEC"]
            else:
                final_action = signal_memory["current_action"]
                final_signal_type = signal_memory["current_signal_type"]
    else:
        if raw_action != "HOLD":
            signal_memory["confirmation_count"] += 1
        final_action = raw_action
        final_signal_type = signal_type

    if signal_memory["signal_start_time"] and (now_ts - signal_memory["signal_start_time"]) > CONFIG["SIGNAL_MAX_AGE_SEC"]:
        final_action = "HOLD"
        final_signal_type = "NONE"
        signal_memory["signal_start_time"] = None

    signal_memory["current_action"] = final_action
    signal_memory["current_signal_type"] = final_signal_type
    signal_duration = int((now_ts - signal_memory["signal_start_time"]) / 60) if signal_memory["signal_start_time"] else 0

    grade = "D"
    if final_action != "HOLD" and signal_type != "NONE":
        if raw_confidence >= 90 and bullish_tf >= 4:
            grade = "A"
        elif raw_confidence >= 80 or (bullish_tf >= 3 and raw_confidence >= 70):
            grade = "B"
        elif raw_confidence >= 65:
            grade = "C"
        else:
            grade = "D"
    signal_memory["signal_grade"] = grade

    position_pct = 0
    rr = 0
    entry = 0
    stop = 0
    target = 0
    max_dd = 0

    if final_action in ["STRONG BUY CE", "BUY CE", "CONSIDER CE BUY"]:
        entry = ce_price
        stop = entry - atr * CONFIG["STOP_LOSS_ATR_MULT"]
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
            risk = entry - stop
            reward = target - entry
            rr = reward / risk if risk > 0 else 0
        else:
            rr = 0
        if signal_memory["entry_price"] > 0:
            dd = ((signal_memory["entry_price"] - ce_price) / signal_memory["entry_price"]) * 100
            max_dd = max(max_dd, dd)
            if ce_price <= stop or dd >= CONFIG["MAX_DRAWDOWN_PCT"]:
                final_action = "HOLD"
                grade = "D"
                logger.warning("CE stop loss or max drawdown hit")
        else:
            signal_memory["entry_price"] = entry
            signal_memory["stop_loss"] = stop
            signal_memory["target"] = target

    elif final_action in ["STRONG BUY PE", "BUY PE", "CONSIDER PE BUY"]:
        entry = pe_price
        stop = entry + atr * CONFIG["STOP_LOSS_ATR_MULT"]
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
            risk = stop - entry
            reward = entry - target
            rr = reward / risk if risk > 0 else 0
        else:
            rr = 0
        if signal_memory["entry_price"] > 0:
            dd = ((signal_memory["entry_price"] - pe_price) / signal_memory["entry_price"]) * 100
            max_dd = max(max_dd, dd)
            if pe_price >= stop or dd >= CONFIG["MAX_DRAWDOWN_PCT"]:
                final_action = "HOLD"
                grade = "D"
                logger.warning("PE stop loss or max drawdown hit")
        else:
            signal_memory["entry_price"] = entry
            signal_memory["stop_loss"] = stop
            signal_memory["target"] = target

    else:
        signal_memory["entry_price"] = 0
        signal_memory["stop_loss"] = 0
        signal_memory["target"] = 0
        position_pct = 0
        rr = 0
        max_dd = 0

    signal_memory["position_size_pct"] = position_pct
    signal_memory["risk_reward"] = rr
    signal_memory["max_drawdown_pct"] = max_dd
    signal_memory["entry_price"] = entry if final_action not in ["HOLD","NONE"] else 0

    market_state.update({
        "rsi": round(rsi,2),
        "momentum": "UPTREND" if ema_fast>ema_slow else "DOWNTREND" if ema_fast<ema_slow else "NEUTRAL",
        "strength": "HIGH" if final_signal_type=="TRENDING" else "MODERATE" if final_signal_type=="MOMENTUM" else "LOW",
        "trend": "BULLISH" if ema_fast>ema_slow else "BEARISH" if ema_fast<ema_slow else "SIDEWAYS",
        "action": final_action,
        "confidence": raw_confidence,
        "volatility": "HIGH" if atr>15 else "NORMAL" if atr>5 else "LOW",
        "alert": final_action,
        "trend_1min": tf_trends["1min"]["trend"],
        "trend_5min": tf_trends["5min"]["trend"],
        "trend_10min": tf_trends["10min"]["trend"],
        "trend_15min": tf_trends["15min"]["trend"],
        "trend_20min": tf_trends["20min"]["trend"],
        "timeframe_agreement": max(bullish_tf, bearish_tf),
        "regime": regime,
        "session_phase": session_phase
    })

    institutional_state.update({
        "vwap": round(vwap,2),
        "ema_fast": round(ema_fast,2),
        "ema_slow": round(ema_slow,2),
        "ema_signal": "BULLISH" if ema_fast>ema_slow else "BEARISH",
        "atr": round(atr,2),
        "oi_buildup": "BULLISH" if pcr<0.9 else "BEARISH" if pcr>1.2 else "NEUTRAL",
        "iv_state": "HIGH" if vega>2 else "NORMAL",
        "candle_structure": "BULLISH" if ema_fast>ema_slow and rsi>55 else "BEARISH" if ema_fast<ema_slow and rsi<45 else "SIDEWAYS",
        "market_breadth": "BULLISH" if bullish_tf>=3 else "BEARISH" if bearish_tf>=3 else "BALANCED",
        "volume_profile": vol_trend,
        "smart_money_flow": "BULLISH" if vwap>ema_slow and vol_trend=="INCREASING" else "BEARISH" if vwap<ema_slow and vol_trend=="INCREASING" else "NEUTRAL",
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "institutional_signal": final_action,
        "institutional_confidence": raw_confidence,
        "signal_grade": grade,
        "position_size_pct": position_pct,
        "risk_reward": round(rr,2),
        "entry_price": round(entry,2) if entry else 0,
        "stop_loss": round(stop,2) if stop else 0,
        "target": round(target,2) if target else 0,
        "max_drawdown_pct": round(max_dd,2)
    })

    market_signal.update({
        "signal": "BULLISH" if final_action in ["STRONG BUY CE","BUY CE","CONSIDER CE BUY"] else "BEARISH" if final_action in ["STRONG BUY PE","BUY PE","CONSIDER PE BUY"] else "NEUTRAL",
        "ce_price": ce_price,
        "pe_price": pe_price,
        "spread": round(spread,2),
        "rsi": round(rsi,2),
        "macd": round(macd_hist,2),
        "pcr": round(pcr,2),
        "vwap": round(vwap,2),
        "atr": round(atr,2),
        "atr_pct": round(atr_pct,2),
        "ema_fast": round(ema_fast,2),
        "ema_slow": round(ema_slow,2),
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "volume": int(vol_list[-1]) if vol_list else 0,
        "timestamp": datetime.now().isoformat(),
        "adx": round(adx,2),
        "bb_position": round(bb_pos,2),
        "rsi_divergence": rsi_div,
        "iv_rank": round(iv_rank,2),
        "signal_grade": grade
    })

    if final_action != signal_memory["last_logged_action"]:
        logger.info(f"PRO SIGNAL: {final_action} [{final_signal_type}] Grade:{grade} Conf:{raw_confidence} "
                    f"BullTF:{bullish_tf} BearTF:{bearish_tf} RSI:{rsi:.1f} ADX:{adx:.1f} PCR:{pcr:.2f} "
                    f"PosSize:{position_pct}% RR:{rr:.1f}")
        signal_memory["last_logged_action"] = final_action

# ------------------------------------------------------------
# WebSocket Callbacks & Connection
# ------------------------------------------------------------
def _acquire_primary_lock():
    global _is_primary_worker, _worker_lock_fd
    try:
        fd = os.open(_LOCK_FILE, os.O_CREAT | os.O_RDWR)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _worker_lock_fd = fd
        _is_primary_worker = True
        logger.info("[WORKER] Acquired primary lock — this worker runs WebSocket")
        return True
    except (IOError, OSError):
        logger.info("[WORKER] Primary lock held by another worker — REST-only mode")
        _is_primary_worker = False
        return False

def patch_smartwebsocket(sws_instance):
    import websocket, ssl
    def fixed_connect():
        headers = {
            "Authorization": sws_instance.auth_token,
            "x-api-key": sws_instance.api_key,
            "x-client-code": sws_instance.client_code,
            "x-feed-token": sws_instance.feed_token
        }
        try:
            sws_instance.wsapp = websocket.WebSocketApp(
                sws_instance.ROOT_URI,
                header=headers,
                on_open=sws_instance._on_open,
                on_message=sws_instance._on_message,  # <-- will call our on_data
                on_error=sws_instance._on_error,
                on_close=sws_instance._on_close,
                on_ping=sws_instance._on_ping,
                on_pong=sws_instance._on_pong
            )
            sws_instance.wsapp.run_forever(
                sslopt={"cert_reqs": ssl.CERT_NONE},
                ping_interval=sws_instance.HEART_BEAT_INTERVAL
            )
        except Exception as e:
            logger.error(f"WebSocket connect error: {e}")
            raise
    sws_instance.connect = fixed_connect

    def fixed_on_close(wsapp, close_status_code=None, close_msg=None):
        logger.warning(f"WebSocket closed: code={close_status_code}, msg={close_msg}")
        if hasattr(sws_instance, 'on_close') and sws_instance.on_close:
            try:
                sws_instance.on_close(wsapp, close_status_code, close_msg)
            except:
                sws_instance.on_close(wsapp)
    sws_instance._on_close = fixed_on_close

    sws_instance.MAX_RETRY_ATTEMPT = 0
    sws_instance.retry_strategy = 0
    return sws_instance

def on_open(wsapp):
    logger.info("WebSocket OPENED")
    global _reconnecting
    with _reconnect_lock:
        _reconnecting = False
    if sws and CE_TOKEN and PE_TOKEN:
        try:
            # mode=1 (ticker), exchangeType=5 (NFO)
            sws.subscribe("nifty_signal", 1, [{"exchangeType": 5, "tokens": [CE_TOKEN, PE_TOKEN]}])
            logger.info(f"Subscribed to CE={CE_TOKEN}, PE={PE_TOKEN} (exchange=5, mode=1)")
        except Exception as e:
            logger.error(f"Subscribe error: {e}")

def on_data(wsapp, message):
    """Callback when a tick arrives."""
    global tick_counter, last_tick_time, _last_ce_pe_zero_start
    last_tick_time = time.time()
    try:
        # message may be string or bytes; Angel One sends binary data (protobuf)
        # For simplicity, we rely on the library's parsing; it should call on_data with a dict.
        # But we also log raw if needed.
        if isinstance(message, bytes):
            # Try to decode as string (fallback)
            try:
                msg_str = message.decode('utf-8', errors='ignore')
            except:
                msg_str = str(message)
            logger.debug(f"Raw binary message length: {len(message)}")
            # Not parsing binary directly; library's internal parser should have converted it.
            return
        if isinstance(message, str):
            try:
                data = json.loads(message)
            except:
                logger.error(f"Failed to parse JSON: {message[:200]}")
                return
        else:
            data = message

        # Angel One tick structure may be a list or dict.
        ticks = data if isinstance(data, list) else [data]

        for tick in ticks:
            token = str(tick.get("tk") or tick.get("token") or "")
            if not token:
                continue
            ltp = tick.get("ltp", 0)
            if isinstance(ltp, (int, float)) and ltp > 1000:
                ltp = ltp / 100
            vol = tick.get("v", 0) or tick.get("volume", 0)

            if token == CE_TOKEN:
                latest_ticks["ce_price"] = ltp
                latest_ticks["ce_volume"] = vol
                price_history.append(ltp)
                volume_history.append(vol)
                tick_counter += 1
                logger.debug(f"CE TICK: {ltp} vol={vol}")
            elif token == PE_TOKEN:
                latest_ticks["pe_price"] = ltp
                latest_ticks["pe_volume"] = vol
                tick_counter += 1
                logger.debug(f"PE TICK: {ltp} vol={vol}")

            # Minute snapshot
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
        if ce > 0 and pe > 0 and len(price_history) >= 30 and tick_counter % 5 == 0:
            run_signal_engine(ce, pe, list(price_history), list(volume_history))

        # Check for zero prices to refresh tokens
        if ce == 0 or pe == 0:
            if _last_ce_pe_zero_start is None:
                _last_ce_pe_zero_start = time.time()
            elif time.time() - _last_ce_pe_zero_start > 300:
                logger.warning("Both CE/PE prices zero for 5 minutes. Forcing token refresh.")
                global CE_TOKEN, PE_TOKEN
                CE_TOKEN, PE_TOKEN = None, None
                _last_ce_pe_zero_start = None
        else:
            _last_ce_pe_zero_start = None

    except Exception as e:
        logger.error(f"Data error: {e}")

def on_error(wsapp, error):
    logger.error(f"WebSocket error: {error}")

def on_close(wsapp, close_status_code=None, close_msg=None):
    logger.warning(f"WebSocket CLOSE: code={close_status_code}, msg={close_msg}")
    global ws_running
    ws_running = False

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

def start_websocket():
    global ws_running, CE_TOKEN, PE_TOKEN, sws, last_tick_time, tick_counter, _reconnecting, _last_429_time

    if not _is_primary_worker:
        logger.info("[WORKER] Not primary — WebSocket thread exiting")
        return

    retry_delay = 5
    consecutive_failures = 0

    while engine_active:
        ws_running = False

        if not is_market_open():
            logger.info("Market closed. Sleeping 5 minutes...")
            CE_TOKEN = None
            PE_TOKEN = None
            time.sleep(300)
            continue

        now = time.time()
        if now - _last_429_time < 300:
            remaining = int(300 - (now - _last_429_time))
            logger.warning(f"429 cooldown active. Waiting {remaining}s...")
            time.sleep(min(remaining, 60))
            continue

        with _reconnect_lock:
            if _reconnecting:
                time.sleep(2)
                continue
            _reconnecting = True

        try:
            if not CE_TOKEN or not PE_TOKEN:
                CE_TOKEN, PE_TOKEN = get_current_atm_tokens()
                if not CE_TOKEN or not PE_TOKEN:
                    logger.warning("No tokens, retrying in 60s...")
                    with _reconnect_lock:
                        _reconnecting = False
                    time.sleep(60)
                    continue

            auth_token, feed_token, obj = get_auth_token()
            if not auth_token:
                consecutive_failures += 1
                wait = min(retry_delay * (2 ** min(consecutive_failures, 6)), 300)
                logger.warning(f"Auth failed (#{consecutive_failures}), waiting {wait}s...")
                with _reconnect_lock:
                    _reconnecting = False
                time.sleep(wait)
                continue

            consecutive_failures = 0

            sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)
            sws = patch_smartwebsocket(sws)

            sws.on_open = on_open
            sws.on_data = on_data
            sws.on_error = on_error
            sws.on_close = on_close

            ws_running = True
            last_tick_time = time.time()
            tick_counter = 0
            logger.info("Connecting WebSocket...")

            ws_thread = threading.Thread(target=sws.connect, daemon=True)
            ws_thread.start()
            time.sleep(5)

            if not ws_running:
                logger.warning("WebSocket failed to connect, retrying...")
                with _reconnect_lock:
                    _reconnecting = False
                time.sleep(10)
                continue

            no_tick_count = 0
            while ws_running and engine_active:
                time.sleep(5)

                with _reconnect_lock:
                    if _reconnecting:
                        no_tick_count = 0
                        continue

                age = time.time() - last_tick_time
                if not ws_running:
                    no_tick_count = 0
                    continue

                if age > 90:
                    no_tick_count += 1
                    logger.warning(f"No ticks for {age:.0f}s (strike {no_tick_count}/3)")
                    if no_tick_count >= 3:
                        logger.error("Watchdog: Max strikes reached, forcing reconnect")
                        ws_running = False
                        break
                else:
                    if no_tick_count > 0:
                        logger.info("Ticks resumed")
                    no_tick_count = 0

            logger.warning("WebSocket loop ended, cleaning up...")
            try:
                if sws and hasattr(sws, 'wsapp') and sws.wsapp:
                    sws.wsapp.close()
            except:
                pass
            sws = None

            with _reconnect_lock:
                _reconnecting = False

            time.sleep(10)

        except Exception as e:
            error_str = str(e)
            logger.error(f"WebSocket fatal error: {e}", exc_info=True)

            if '429' in error_str or 'Connection Limit Exceeded' in error_str or 'Too Many Requests' in error_str:
                logger.error("RATE LIMIT 429 DETECTED. Entering 5-minute cooldown.")
                _last_429_time = time.time()
                with _reconnect_lock:
                    _reconnecting = False
                time.sleep(300)
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                wait = min(retry_delay * (2 ** min(consecutive_failures, 6)), 300)
                logger.info(f"Waiting {wait}s before reconnect...")
                with _reconnect_lock:
                    _reconnecting = False
                time.sleep(wait)

def rest_fallback():
    global CE_TOKEN, PE_TOKEN   # <--- MUST BE FIRST LINE
    while engine_active:
        time.sleep(60)

        if not is_market_open():
            continue

        if ws_running and (time.time() - last_tick_time) < 45:
            continue

        if not CE_TOKEN or not PE_TOKEN:
            logger.debug("REST fallback: No tokens available")
            continue

        auth_token, _, obj = get_auth_token()
        if not auth_token or not obj:
            logger.warning("REST fallback: Auth failed")
            continue

        try:
            ce_data = obj.ltpData("NFO", "NIFTY", CE_TOKEN)
            if ce_data and ce_data.get("data") and ce_data["data"].get("ltp"):
                ltp = float(ce_data["data"]["ltp"])
                latest_ticks["ce_price"] = ltp
                price_history.append(ltp)
                volume_history.append(100)
                logger.info(f"[REST FALLBACK] CE LTP: {ltp}")
            else:
                logger.warning(f"REST fallback CE invalid response: {ce_data}")

            pe_data = obj.ltpData("NFO", "NIFTY", PE_TOKEN)
            if pe_data and pe_data.get("data") and pe_data["data"].get("ltp"):
                ltp = float(pe_data["data"]["ltp"])
                latest_ticks["pe_price"] = ltp
                logger.info(f"[REST FALLBACK] PE LTP: {ltp}")
            else:
                logger.warning(f"REST fallback PE invalid response: {pe_data}")

            ce = latest_ticks["ce_price"]
            pe = latest_ticks["pe_price"]
            if ce > 0 and pe > 0 and len(price_history) >= 30:
                run_signal_engine(ce, pe, list(price_history), list(volume_history))
                logger.info(f"[REST FALLBACK] Signal engine triggered: CE={ce}, PE={pe}")
            else:
                logger.debug(f"REST fallback: Not enough data ({len(price_history)}/30)")

        except Exception as e:
            logger.error(f"REST fallback error: {e}")
# ------------------------------------------------------------
# Flask Endpoints
# ------------------------------------------------------------
@app.route("/")
def home():
    return jsonify({
        "status": "online", 
        "message": "Nifty Signal Engine v4.4 Professional",
        "worker_type": "PRIMARY (WebSocket)" if _is_primary_worker else "SECONDARY (REST-only)",
        "market_open": is_market_open()
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
        "signal_memory": {
            "current_action": signal_memory["current_action"],
            "signal_type": signal_memory["current_signal_type"],
            "confirmations": signal_memory["confirmation_count"],
            "grade": signal_memory["signal_grade"],
            "position_size_pct": signal_memory["position_size_pct"],
            "risk_reward": signal_memory["risk_reward"],
            "entry_price": signal_memory["entry_price"],
            "stop_loss": signal_memory["stop_loss"],
            "target": signal_memory["target"]
        }
    })

@app.route("/api/health")
def health():
    return jsonify({
        "status": "ok",
        "ws_running": ws_running,
        "reconnecting": _reconnecting,
        "is_primary_worker": _is_primary_worker,
        "ce_token": CE_TOKEN,
        "pe_token": PE_TOKEN,
        "latest_ce": latest_ticks["ce_price"],
        "latest_pe": latest_ticks["pe_price"],
        "price_history_len": len(price_history),
        "last_tick_age": round(time.time() - last_tick_time, 1),
        "timestamp": datetime.now().isoformat()
    })

@app.route("/api/debug/tokens")
def debug_tokens():
    """Check current token LTP via REST."""
    auth_token, _, obj = get_auth_token()
    if not obj:
        return jsonify({"error": "Auth failed"}), 500
    ce_ltp = obj.ltpData("NFO", "NIFTY", CE_TOKEN) if CE_TOKEN else None
    pe_ltp = obj.ltpData("NFO", "NIFTY", PE_TOKEN) if PE_TOKEN else None
    return jsonify({
        "CE_TOKEN": CE_TOKEN,
        "PE_TOKEN": PE_TOKEN,
        "CE_LTP": ce_ltp,
        "PE_LTP": pe_ltp,
        "timestamp": datetime.now().isoformat()
    })

# ------------------------------------------------------------
# Graceful shutdown
# ------------------------------------------------------------
def shutdown_handler(signum, frame):
    global engine_active
    logger.info("Shutdown signal received")
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
# Startup
# ------------------------------------------------------------
def start_engine():
    _acquire_primary_lock()
    threading.Thread(target=start_websocket, daemon=True, name="WS-Main").start()
    threading.Thread(target=rest_fallback, daemon=True, name="REST-Fallback").start()
    logger.info("=" * 50)
    logger.info("Nifty Signal Engine v4.4 (Institutional Grade) Started")
    logger.info(f"Worker type: {'PRIMARY (WebSocket)' if _is_primary_worker else 'SECONDARY (REST-only)'}")
    logger.info("FIXED: WebSocket exchangeType=5, mode=1, Thursday expiry rollover")
    logger.info("=" * 50)

start_engine()

if __name__ == "__main__":
    start_engine()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
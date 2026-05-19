import os
import time
import logging
import threading
import json
import requests
import pandas as pd
from collections import deque, defaultdict
from datetime import datetime, timedelta
from flask import Flask, jsonify
from flask_cors import CORS
import pyotp
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

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

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing Angel One credentials")

# --------------------------------------------------
# Global state
# --------------------------------------------------
CE_TOKEN = None
PE_TOKEN = None
latest_ticks = {"ce_price": 0.0, "pe_price": 0.0}
price_history = deque(maxlen=500)  # Increased for multi-timeframe analysis
tick_counter = 0
UPDATE_INTERVAL = 5  # Faster updates for 1min trend
ws_running = False
sws = None

# --------------------------------------------------
# Professional Signal State
# --------------------------------------------------
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
    "rsi": 50,
    "momentum": "NEUTRAL",
    "strength": "LOW",
    "trend": "SIDEWAYS",
    "action": "HOLD",
    "confidence": 0,
    "volatility": "NORMAL",
    "alert": "NONE",
    "signal_duration_minutes": 0,
    "trend_1min": "SIDEWAYS",
    "trend_5min": "SIDEWAYS",
    "trend_10min": "SIDEWAYS",
    "trend_15min": "SIDEWAYS",
    "trend_20min": "SIDEWAYS",
    "timeframe_agreement": 0  # How many timeframes agree (0-5)
}

institutional_state = {
    "vwap": 0,
    "ema_fast": 0,
    "ema_slow": 0,
    "ema_signal": "NEUTRAL",
    "atr": 0,
    "oi_buildup": "NEUTRAL",
    "iv_state": "NORMAL",
    "candle_structure": "SIDEWAYS",
    "market_breadth": "BALANCED",
    "volume_profile": "NORMAL",
    "smart_money_flow": "NEUTRAL",
    "volume_trend": "FLAT",
    "delta": 0,
    "gamma": 0,
    "theta": 0,
    "vega": 0,
    "institutional_signal": "HOLD",
    "institutional_confidence": 0,
    "consecutive_confirmations": 0
}

# --------------------------------------------------
# Signal Persistence & Anti-Flip System
# --------------------------------------------------
signal_memory = {
    "current_action": "HOLD",
    "current_signal_type": "NONE",  # NONE, TRENDING, MOMENTUM
    "signal_start_time": None,
    "last_confirmed_action": "HOLD",
    "confirmation_count": 0,
    "required_confirmations": 2,
    "min_signal_duration_seconds": 180,  # 3 minutes minimum
    "max_sideways_duration": 600  # 10 minutes before forcing re-evaluation
}

# Timeframe history storage (price snapshots per minute)
timeframe_history = {
    "1min": deque(maxlen=60),
    "5min": deque(maxlen=20),
    "10min": deque(maxlen=15),
    "15min": deque(maxlen=10),
    "20min": deque(maxlen=10)
}

last_minute_snapshot = {"time": 0, "price": 0}

# Thresholds
SPREAD_THRESHOLD = 3.0
STRONG_BUY_THRESHOLD = 85
BUY_THRESHOLD = 70
CONSIDER_THRESHOLD = 55
HOLD_THRESHOLD = 45

# --------------------------------------------------
# Helper: Nifty spot
# --------------------------------------------------
spot_cache = {"value": None, "timestamp": 0}
CACHE_TTL = 30

def get_nifty_spot():
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        headers = {"User-Agent": "Mozilla/5.0"}
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        time.sleep(0.5)
        response = session.get(url, headers=headers, timeout=10)
        data = response.json()
        spot = data["data"][0]["lastPrice"]
        logger.info(f"NIFTY spot = {spot}")
        return float(spot)
    except Exception as e:
        logger.error(f"Spot fetch error: {e}")
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

# --------------------------------------------------
# Fetch current ATM option tokens
# --------------------------------------------------
def get_current_atm_tokens():
    spot = get_nifty_spot_cached()
    if not spot:
        return None, None
    atm_strike = round(spot / 50) * 50
    logger.info(f"ATM strike = {atm_strike}")
    url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        df = pd.DataFrame(resp.json())
        logger.info(f"Total instruments loaded: {len(df)}")
    except Exception as e:
        logger.error(f"Failed to load instrument master: {e}")
        return None, None

    nifty_opts = df[
        (df["name"].astype(str) == "NIFTY") & 
        (df["instrumenttype"].astype(str) == "OPTIDX") &
        (df["exch_seg"].astype(str) == "NFO")
    ].copy()

    logger.info(f"NIFTY OPTIDX NFO symbols found: {len(nifty_opts)}")

    if nifty_opts.empty:
        nifty_opts = df[df["symbol"].astype(str).str.match(r'^NIFTY\d{2}[A-Z]{3}\d{2}', na=False)].copy()
        logger.info(f"Fallback NIFTY symbols found: {len(nifty_opts)}")

    if nifty_opts.empty:
        logger.error("No NIFTY symbols found")
        return None, None

    for fmt in ["%d%b%Y", "%d-%b-%Y", "%Y-%m-%d", "%d%m%Y"]:
        nifty_opts["expiry_date"] = pd.to_datetime(nifty_opts["expiry"], format=fmt, errors="coerce")
        valid_count = nifty_opts["expiry_date"].notna().sum()
        if valid_count > 0:
            logger.info(f"Parsed expiry with format {fmt}: {valid_count} valid")
            break
    else:
        logger.error("Could not parse any expiry dates")
        return None, None

    nifty_opts = nifty_opts.dropna(subset=["expiry_date"])
    nifty_opts["strike"] = pd.to_numeric(nifty_opts["strike"], errors="coerce") / 100
    nifty_opts = nifty_opts.dropna(subset=["strike"])
    logger.info(f"NIFTY with valid expiry and strike: {len(nifty_opts)}")

    today = datetime.now()
    future_expiries = nifty_opts[nifty_opts["expiry_date"] > today]
    logger.info(f"Future expiries: {len(future_expiries)}")

    if future_expiries.empty:
        logger.error("No future expiries found")
        return None, None

    nearest_expiry = future_expiries["expiry_date"].min()
    logger.info(f"Nearest expiry: {nearest_expiry}")

    nearest_opts = nifty_opts[nifty_opts["expiry_date"] == nearest_expiry]
    logger.info(f"Options for nearest expiry: {len(nearest_opts)}")

    available_strikes = sorted(nearest_opts["strike"].unique())
    logger.info(f"Available strikes (first 20): {available_strikes[:20]}")
    logger.info(f"Looking for strike: {atm_strike}")

    atm_opts = nifty_opts[(nifty_opts["strike"] == atm_strike) & (nifty_opts["expiry_date"] == nearest_expiry)]
    logger.info(f"ATM options found: {len(atm_opts)}")

    ce_row = atm_opts[atm_opts["symbol"].str.upper().str.contains("CE", na=False)]
    pe_row = atm_opts[atm_opts["symbol"].str.upper().str.contains("PE", na=False)]

    logger.info(f"CE matches: {len(ce_row)}, PE matches: {len(pe_row)}")

    if ce_row.empty or pe_row.empty:
        logger.error(f"CE or PE not found for strike {atm_strike}, expiry {nearest_expiry}")
        if available_strikes:
            nearest_strike = min(available_strikes, key=lambda x: abs(x - atm_strike))
            logger.info(f"Trying nearest strike: {nearest_strike}")
            atm_opts = nifty_opts[(nifty_opts["strike"] == nearest_strike) & (nifty_opts["expiry_date"] == nearest_expiry)]
            ce_row = atm_opts[atm_opts["symbol"].str.upper().str.contains("CE", na=False)]
            pe_row = atm_opts[atm_opts["symbol"].str.upper().str.contains("PE", na=False)]
            if not ce_row.empty and not pe_row.empty:
                ce_token = str(ce_row.iloc[0]["token"])
                pe_token = str(pe_row.iloc[0]["token"])
                logger.info(f"Fallback CE token = {ce_token}, PE token = {pe_token}")
                return ce_token, pe_token
        return None, None

    ce_token = str(ce_row.iloc[0]["token"])
    pe_token = str(pe_row.iloc[0]["token"])
    logger.info(f"CE token = {ce_token}, PE token = {pe_token}")
    return ce_token, pe_token

# --------------------------------------------------
# WebSocket Callbacks
# --------------------------------------------------
def on_ws_open(wsapp):
    global sws
    logger.info("Angel One WebSocket opened")
    if sws is not None:
        try:
            correlation_id = "tradeguru_001"
            mode = 2
            token_list = [
                {"exchangeType": 2, "tokens": [CE_TOKEN, PE_TOKEN]}
            ]
            sws.subscribe(correlation_id, mode, token_list)
            logger.info(f"Subscribed to tokens: {CE_TOKEN}, {PE_TOKEN}")
        except Exception as e:
            logger.error(f"Subscribe error: {e}")

def on_ws_data(wsapp, message, *args):
    global latest_ticks, price_history, tick_counter, last_minute_snapshot, timeframe_history
    try:
        if isinstance(message, str):
            data = json.loads(message)
        else:
            data = message

        ticks = data if isinstance(data, list) else [data]

        for tick in ticks:
            token = None
            for key in ["tk", "token", "symbolToken", "instrument_token"]:
                if key in tick:
                    token = str(tick.get(key))
                    break

            if not token:
                continue

            ltp = None
            for key in ["ltp", "last_traded_price", "lp", "price"]:
                if key in tick:
                    val = tick.get(key)
                    if isinstance(val, (int, float)):
                        ltp = val / 100 if val > 1000 else val
                    break

            if ltp is None:
                continue

            # Update prices
            if token == CE_TOKEN:
                latest_ticks["ce_price"] = ltp
                latest_ticks["ce_timestamp"] = datetime.now().isoformat()
                price_history.append(ltp)
                tick_counter += 1
            elif token == PE_TOKEN:
                latest_ticks["pe_price"] = ltp
                latest_ticks["pe_timestamp"] = datetime.now().isoformat()

            # Store minute snapshots for timeframe analysis
            now = time.time()
            if now - last_minute_snapshot["time"] >= 60:
                avg_price = (latest_ticks["ce_price"] + latest_ticks["pe_price"]) / 2
                last_minute_snapshot["time"] = now
                last_minute_snapshot["price"] = avg_price

                # Add to all timeframes
                snapshot = {"time": now, "price": avg_price, "ce": latest_ticks["ce_price"], "pe": latest_ticks["pe_price"]}
                for tf in timeframe_history:
                    timeframe_history[tf].append(snapshot)

            # Run signal engine
            ce = latest_ticks["ce_price"]
            pe = latest_ticks["pe_price"]
            if ce > 0 and pe > 0 and len(price_history) >= 20 and tick_counter % UPDATE_INTERVAL == 0:
                run_signal_engine(ce, pe, list(price_history))

    except Exception as e:
        logger.error(f"WebSocket data error: {e}")

def on_ws_error(wsapp, error):
    logger.error(f"WebSocket error: {error}")

def on_ws_close(wsapp, *args):
    logger.warning(f"WebSocket closed, args: {args}")
    global ws_running
    ws_running = False

# --------------------------------------------------
# Start WebSocket connection
# --------------------------------------------------
def start_angel_websocket():
    global ws_running, CE_TOKEN, PE_TOKEN, sws
    retry_delay = 30
    while True:
        try:
            totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
            obj = SmartConnect(api_key=ANGEL_API_KEY)
            session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
            if not session.get("status"):
                logger.error(f"Login failed: {session}")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 300)
                continue
            auth_token = session["data"]["jwtToken"]
            feed_token = obj.getfeedToken()
            logger.info("Authenticated, feed token obtained")

            CE_TOKEN, PE_TOKEN = get_current_atm_tokens()
            if not CE_TOKEN or not PE_TOKEN:
                logger.error("Could not fetch ATM tokens. Retrying...")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 300)
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

            while ws_running:
                time.sleep(0.1)

            logger.warning("WebSocket loop ended, reconnecting...")

        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 300)

# --------------------------------------------------
# Professional Technical Indicators
# --------------------------------------------------
def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
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
        alpha = 2 / (period + 1)
        val = data[0]
        for p in data[1:]:
            val = alpha * p + (1 - alpha) * val
        return val
    macd_line = ema(prices, fast) - ema(prices, slow)
    signal_line = ema(prices[-signal:], signal) if len(prices) >= signal else ema(prices, signal)
    histogram = macd_line - signal_line
    return macd_line, histogram

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
        return 0
    trs = [abs(prices[i] - prices[i - 1]) for i in range(1, len(prices))]
    return sum(trs[-period:]) / period

def calculate_vwap(prices, volumes=None):
    if not prices:
        return 0
    if volumes is None:
        volumes = [100] * len(prices)
    pv = sum(p * v for p, v in zip(prices, volumes))
    tv = sum(volumes)
    return pv / tv if tv else 0

def calculate_volume_trend(prices):
    """Estimate volume trend from price movement intensity"""
    if len(prices) < 20:
        return "FLAT", 0
    recent = prices[-10:]
    older = prices[-20:-10]
    recent_volatility = sum(abs(recent[i] - recent[i-1]) for i in range(1, len(recent)))
    older_volatility = sum(abs(older[i] - older[i-1]) for i in range(1, len(older)))
    if older_volatility == 0:
        return "FLAT", 0
    ratio = recent_volatility / older_volatility
    if ratio > 1.5:
        return "INCREASING", ratio
    elif ratio < 0.7:
        return "DECREASING", ratio
    return "FLAT", ratio

def estimate_greeks(ce, pe):
    delta = round((ce - pe) / 100, 2)
    gamma = round(abs(delta) / 10, 2)
    theta = round(-(ce + pe) / 1000, 2)
    vega = round((ce + pe) / 500, 2)
    return delta, gamma, theta, vega

# --------------------------------------------------
# Timeframe Trend Analysis
# --------------------------------------------------
def analyze_timeframe_trend(tf_name, history_deque):
    """Analyze trend for a specific timeframe"""
    history = list(history_deque)
    if len(history) < 2:
        return "SIDEWAYS", 0, 0

    # Calculate slope using linear regression on minute snapshots
    n = len(history)
    if n < 2:
        return "SIDEWAYS", 0, 0

    prices = [h["price"] for h in history]
    x = list(range(n))
    x_mean = sum(x) / n
    y_mean = sum(prices) / n

    numerator = sum((x[i] - x_mean) * (prices[i] - y_mean) for i in range(n))
    denominator = sum((x[i] - x_mean) ** 2 for i in range(n))

    if denominator == 0:
        return "SIDEWAYS", 0, 0

    slope = numerator / denominator

    # Calculate R-squared (trend strength)
    ss_res = sum((prices[i] - (y_mean + slope * (x[i] - x_mean))) ** 2 for i in range(n))
    ss_tot = sum((prices[i] - y_mean) ** 2 for i in range(n))
    r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0

    # Determine trend direction and strength
    slope_threshold = 0.05  # Minimum slope to be considered trending
    strength = abs(slope) * r_squared * 100  # 0-100 scale

    if abs(slope) < slope_threshold or r_squared < 0.3:
        return "SIDEWAYS", strength, r_squared
    elif slope > 0:
        return "BULLISH", strength, r_squared
    else:
        return "BEARISH", strength, r_squared

def get_all_timeframe_trends():
    """Get trends for all timeframes"""
    trends = {}
    for tf_name, tf_deque in timeframe_history.items():
        trend, strength, r2 = analyze_timeframe_trend(tf_name, tf_deque)
        trends[tf_name] = {
            "trend": trend,
            "strength": round(strength, 2),
            "confidence": round(r2, 2)
        }
    return trends

# --------------------------------------------------
# PCR with Multiple Fallbacks
# --------------------------------------------------
pcr_cache = {"value": 1.0, "time": 0, "source": "default"}
PCR_TTL = 120  # 2 minutes

def get_nifty_pcr():
    now = time.time()
    if now - pcr_cache["time"] < PCR_TTL:
        return pcr_cache["value"], pcr_cache["source"]

    # Try NSE Option Chain
    try:
        url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9"
        }
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        time.sleep(1)
        resp = session.get(url, headers=headers, timeout=10)
        data = resp.json()

        if "records" in data and "data" in data["records"]:
            records = data["records"]["data"]
            ce_oi = sum(x.get("CE", {}).get("openInterest", 0) for x in records if "CE" in x)
            pe_oi = sum(x.get("PE", {}).get("openInterest", 0) for x in records if "PE" in x)
            pcr = pe_oi / ce_oi if ce_oi else 1.0
            pcr_cache.update({"value": pcr, "time": now, "source": "nse_oi"})
            logger.info(f"PCR from NSE OI: {pcr:.2f}")
            return pcr, "nse_oi"
    except Exception as e:
        logger.warning(f"NSE PCR fetch failed: {e}")

    # Fallback 1: Price-based PCR
    try:
        ce = latest_ticks.get("ce_price", 0)
        pe = latest_ticks.get("pe_price", 0)
        if ce > 0 and pe > 0:
            price_pcr = pe / ce
            pcr = round(max(0.5, min(2.0, price_pcr)), 2)
            pcr_cache.update({"value": pcr, "time": now, "source": "price_based"})
            logger.info(f"PCR from price ratio: {pcr:.2f}")
            return pcr, "price_based"
    except Exception as e:
        logger.warning(f"Price PCR fallback failed: {e}")

    # Fallback 2: Historical average
    pcr_cache["time"] = now
    return pcr_cache["value"], "cached"

# --------------------------------------------------
# Professional Signal Engine
# --------------------------------------------------
def run_signal_engine(ce_price, pe_price, price_list):
    global market_signal, market_state, institutional_state, signal_memory

    if len(price_list) < 30:
        return

    # Calculate all indicators
    spread = ce_price - pe_price
    rsi = calculate_rsi(price_list)
    macd_line, macd_hist = calculate_macd(price_list)
    vwap = calculate_vwap(price_list)
    ema_fast = calculate_ema(price_list, 9)
    ema_slow = calculate_ema(price_list, 21)
    atr = calculate_atr(price_list)
    pcr, pcr_source = get_nifty_pcr()
    volume_trend, volume_ratio = calculate_volume_trend(price_list)
    delta, gamma, theta, vega = estimate_greeks(ce_price, pe_price)

    # Get timeframe trends
    tf_trends = get_all_timeframe_trends()

    # Extract individual timeframe directions
    trend_1min = tf_trends.get("1min", {}).get("trend", "SIDEWAYS")
    trend_5min = tf_trends.get("5min", {}).get("trend", "SIDEWAYS")
    trend_10min = tf_trends.get("10min", {}).get("trend", "SIDEWAYS")
    trend_15min = tf_trends.get("15min", {}).get("trend", "SIDEWAYS")
    trend_20min = tf_trends.get("20min", {}).get("trend", "SIDEWAYS")

    # Count timeframe agreement
    bullish_count = sum(1 for t in [trend_1min, trend_5min, trend_10min, trend_15min, trend_20min] if t == "BULLISH")
    bearish_count = sum(1 for t in [trend_1min, trend_5min, trend_10min, trend_15min, trend_20min] if t == "BEARISH")
    sideways_count = 5 - bullish_count - bearish_count

    timeframe_agreement = max(bullish_count, bearish_count)

    # ============================================================
    # PROFESSIONAL SIGNAL SCORING SYSTEM
    # ============================================================

    # Base score from timeframes (0-50 points)
    timeframe_score = 0
    if trend_1min == "BULLISH":
        timeframe_score += 10
    if trend_5min == "BULLISH":
        timeframe_score += 10
    if trend_10min == "BULLISH":
        timeframe_score += 10
    if trend_15min == "BULLISH":
        timeframe_score += 10
    if trend_20min == "BULLISH":
        timeframe_score += 10

    bearish_timeframe_score = 0
    if trend_1min == "BEARISH":
        bearish_timeframe_score += 10
    if trend_5min == "BEARISH":
        bearish_timeframe_score += 10
    if trend_10min == "BEARISH":
        bearish_timeframe_score += 10
    if trend_15min == "BEARISH":
        bearish_timeframe_score += 10
    if trend_20min == "BEARISH":
        bearish_timeframe_score += 10

    # Technical indicator scores (0-50 points)
    tech_bullish = 0
    tech_bearish = 0

    # RSI (0-10 points)
    if 55 < rsi < 75:  # Bullish zone, not overbought
        tech_bullish += 10
    elif rsi > 75:  # Overbought - reduce score
        tech_bullish += 3
    elif 40 < rsi < 55:  # Neutral-bullish
        tech_bullish += 5

    if 25 < rsi < 45:  # Bearish zone, not oversold
        tech_bearish += 10
    elif rsi < 25:  # Oversold - reduce score
        tech_bearish += 3
    elif 45 < rsi < 60:  # Neutral-bearish
        tech_bearish += 5

    # MACD (0-10 points)
    if macd_hist > 0 and macd_line > 0:
        tech_bullish += 10
    elif macd_hist > 0:
        tech_bullish += 6
    elif macd_hist < 0 and macd_line < 0:
        tech_bearish += 10
    elif macd_hist < 0:
        tech_bearish += 6

    # PCR (0-10 points)
    if pcr < 0.9:
        tech_bullish += 10
    elif pcr < 1.0:
        tech_bullish += 7
    elif pcr > 1.3:
        tech_bearish += 10
    elif pcr > 1.2:
        tech_bearish += 7

    # Volume (0-10 points)
    if volume_trend == "INCREASING":
        if bullish_count > bearish_count:
            tech_bullish += 10
        elif bearish_count > bullish_count:
            tech_bearish += 10

    # Price vs EMA/VWAP (0-10 points)
    avg_price = (ce_price + pe_price) / 2
    if avg_price > vwap and avg_price > ema_slow:
        tech_bullish += 10
    elif avg_price > vwap or avg_price > ema_slow:
        tech_bullish += 5
    elif avg_price < vwap and avg_price < ema_slow:
        tech_bearish += 10
    elif avg_price < vwap or avg_price < ema_slow:
        tech_bearish += 5

    # ============================================================
    # COMBINED SCORING & SIGNAL DETERMINATION
    # ============================================================

    total_bullish = timeframe_score + tech_bullish
    total_bearish = bearish_timeframe_score + tech_bearish

    # Determine raw signal
    now = datetime.now()

    if total_bullish >= total_bearish and total_bullish >= CONSIDER_THRESHOLD:
        raw_confidence = total_bullish
        if timeframe_agreement >= 4 and raw_confidence >= STRONG_BUY_THRESHOLD:
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
    elif total_bearish > total_bullish and total_bearish >= CONSIDER_THRESHOLD:
        raw_confidence = total_bearish
        if timeframe_agreement >= 4 and raw_confidence >= STRONG_BUY_THRESHOLD:
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
        raw_confidence = max(total_bullish, total_bearish)
        raw_action = "HOLD"
        signal_type = "NONE"

    # ============================================================
    # ANTI-FLIP & PERSISTENCE LOGIC
    # ============================================================

    current_time = time.time()

    # If same direction as current signal, increment confirmation
    if raw_action == signal_memory["current_action"] and raw_action != "HOLD":
        signal_memory["confirmation_count"] += 1
    else:
        signal_memory["confirmation_count"] = 0

    # Check minimum duration before allowing signal change
    min_duration_met = True
    if signal_memory["signal_start_time"] is not None:
        elapsed = current_time - signal_memory["signal_start_time"]
        if elapsed < signal_memory["min_signal_duration_seconds"]:
            min_duration_met = False

    # Determine final action
    if signal_memory["current_action"] == "HOLD" or signal_memory["current_action"] == "WAITING":
        # From HOLD, need 2 consecutive confirmations to change
        if raw_action != "HOLD" and signal_memory["confirmation_count"] >= signal_memory["required_confirmations"]:
            final_action = raw_action
            final_signal_type = signal_type
            signal_memory["signal_start_time"] = current_time
            signal_memory["confirmation_count"] = 0
        else:
            final_action = "HOLD"
            final_signal_type = "NONE"
    else:
        # Already in a signal - only change if:
        # 1. Minimum duration met, AND
        # 2. New signal has enough confirmations, AND
        # 3. Direction actually changed (not just HOLD)
        if raw_action == "HOLD":
            # Check if we should force exit to HOLD (sideways too long)
            if signal_memory["signal_start_time"] is not None:
                elapsed = current_time - signal_memory["signal_start_time"]
                if elapsed > signal_memory["max_sideways_duration"]:
                    final_action = "HOLD"
                    final_signal_type = "NONE"
                    signal_memory["signal_start_time"] = None
                    signal_memory["confirmation_count"] = 0
                else:
                    # Stay in current signal during brief sideways
                    final_action = signal_memory["current_action"]
                    final_signal_type = signal_memory["current_signal_type"]
            else:
                final_action = "HOLD"
                final_signal_type = "NONE"
        elif raw_action != signal_memory["current_action"]:
            # Direction change requested
            if min_duration_met and signal_memory["confirmation_count"] >= signal_memory["required_confirmations"]:
                final_action = raw_action
                final_signal_type = signal_type
                signal_memory["signal_start_time"] = current_time
                signal_memory["confirmation_count"] = 0
            else:
                # Stay in current signal
                final_action = signal_memory["current_action"]
                final_signal_type = signal_memory["current_signal_type"]
        else:
            # Same direction, continue
            final_action = signal_memory["current_action"]
            final_signal_type = signal_memory["current_signal_type"]

    # Update memory
    signal_memory["current_action"] = final_action
    signal_memory["current_signal_type"] = final_signal_type

    # Calculate signal duration
    signal_duration = 0
    if signal_memory["signal_start_time"] is not None:
        signal_duration = int((current_time - signal_memory["signal_start_time"]) / 60)

    # ============================================================
    # UPDATE ALL STATE OBJECTS
    # ============================================================

    # Market state
    market_state.update({
        "rsi": round(rsi, 2),
        "momentum": "UPTREND" if bullish_count > bearish_count else "DOWNTREND" if bearish_count > bullish_count else "NEUTRAL",
        "strength": "HIGH" if final_signal_type == "TRENDING" else "MODERATE" if final_signal_type == "MOMENTUM" else "LOW",
        "trend": "BULLISH" if bullish_count > bearish_count else "BEARISH" if bearish_count > bullish_count else "SIDEWAYS",
        "action": final_action,
        "confidence": raw_confidence,
        "volatility": "HIGH" if atr > 15 else "NORMAL" if atr > 5 else "LOW",
        "alert": final_action,
        "signal_duration_minutes": signal_duration,
        "trend_1min": trend_1min,
        "trend_5min": trend_5min,
        "trend_10min": trend_10min,
        "trend_15min": trend_15min,
        "trend_20min": trend_20min,
        "timeframe_agreement": timeframe_agreement
    })

    # Institutional state
    institutional_state.update({
        "vwap": round(vwap, 2),
        "ema_fast": round(ema_fast, 2),
        "ema_slow": round(ema_slow, 2),
        "ema_signal": "BULLISH" if ema_fast > ema_slow else "BEARISH",
        "atr": round(atr, 2),
        "oi_buildup": "BULLISH" if pcr < 0.9 else "BEARISH" if pcr > 1.2 else "NEUTRAL",
        "iv_state": "HIGH" if vega > 2 else "NORMAL",
        "candle_structure": "BULLISH" if trend_5min == "BULLISH" and rsi > 55 else "BEARISH" if trend_5min == "BEARISH" and rsi < 45 else "SIDEWAYS",
        "market_breadth": "BULLISH" if bullish_count >= 3 else "BEARISH" if bearish_count >= 3 else "BALANCED",
        "volume_profile": volume_trend,
        "smart_money_flow": "BULLISH" if vwap > ema_slow and volume_trend == "INCREASING" else "BEARISH" if vwap < ema_slow and volume_trend == "INCREASING" else "NEUTRAL",
        "volume_trend": volume_trend,
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "institutional_signal": final_action,
        "institutional_confidence": raw_confidence,
        "consecutive_confirmations": signal_memory["confirmation_count"]
    })

    # Market signal
    market_signal.update({
        "signal": "BULLISH" if final_action in ["BUY CE", "STRONG BUY CE", "TRENDING: CE", "CONSIDER CE"] else "BEARISH" if final_action in ["BUY PE", "STRONG BUY PE", "TRENDING: PE", "CONSIDER PE"] else "NEUTRAL",
        "ce_price": ce_price,
        "pe_price": pe_price,
        "spread": round(spread, 2),
        "rsi": round(rsi, 2),
        "macd": round(macd_hist, 2),
        "pcr": round(pcr, 2),
        "vwap": round(vwap, 2),
        "atr": round(atr, 2),
        "ema_fast": round(ema_fast, 2),
        "ema_slow": round(ema_slow, 2),
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "volume": round(volume_ratio * 100, 0),
        "volume_avg": 100,
        "oi_signal": institutional_state["oi_buildup"],
        "timestamp": datetime.now().isoformat()
    })

    logger.info(f"PRO SIGNAL: {final_action} [Type:{final_signal_type}] [Dur:{signal_duration}m] [Conf:{raw_confidence}] [TF:{bullish_count}B/{bearish_count}Be/{sideways_count}S] [PCR:{pcr:.2f}@{pcr_source}] [Vol:{volume_trend}] [RSI:{rsi:.1f}] [MACD:{macd_hist:.2f}]")

# --------------------------------------------------
# Flask endpoints
# --------------------------------------------------
@app.route("/")
def home():
    return jsonify({"status": "online", "message": "Nifty Alpha Engine - Professional Trading Signals"})

@app.route("/api/live-signals")
def live_signals():
    return jsonify({
        "status": "active",
        "data": market_signal,
        "market": market_state,
        "institutional": institutional_state,
        "timeframes": {k: {"trend": v["trend"], "strength": v["strength"]} for k, v in get_all_timeframe_trends().items()},
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
        "price_history_len": len(price_history),
        "timestamp": datetime.now().isoformat()
    }), 200

@app.route("/debug/ws-status")
def debug_ws():
    return jsonify({
        "ws_running": ws_running,
        "ce_token": CE_TOKEN,
        "pe_token": PE_TOKEN,
        "latest_ce": latest_ticks["ce_price"],
        "latest_pe": latest_ticks["pe_price"],
        "price_history_len": len(price_history),
        "timeframe_data": {k: len(v) for k, v in timeframe_history.items()},
        "signal_memory": signal_memory
    })

# --------------------------------------------------
# Start background engine
# --------------------------------------------------
engine_started = False

def start_background_engine():
    global engine_started
    if not engine_started:
        ws_thread = threading.Thread(target=start_angel_websocket, daemon=True)
        ws_thread.start()
        engine_started = True
        logger.info("Angel One WebSocket engine started (auto-reconnecting)")

start_background_engine()

if __name__ == "__main__":
    start_background_engine()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, threaded=True)
import os
import time
import logging
import threading
import json
import requests
import pandas as pd
from collections import deque
from datetime import datetime
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
price_history = deque(maxlen=500)
tick_counter = 0
UPDATE_INTERVAL = 5
ws_running = False
sws = None

# --------------------------------------------------
# Enhanced Signal State
# --------------------------------------------------
market_signal = {
    "signal": "WAITING",
    "sentiment": "NEUTRAL",
    "sentiment_score": 0,
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
    "confidence": 0,
    "confidence_color": "RED",
    "trend_strength": 0,
    "volatility_regime": "NORMAL",
    "market_regime": "SIDEWAYS",
    "entry_zone": False,
    "timestamp": ""
}

market_state = {
    "action": "HOLD",
    "confidence": 0,
    "momentum": "NEUTRAL",
    "trend": "SIDEWAYS",
    "strength": "LOW",
    "volatility": "NORMAL",
    "alert": "NONE"
}

institutional_state = {
    "vwap": 0, "ema_fast": 0, "ema_slow": 0, "ema_signal": "NEUTRAL", "atr": 0,
    "oi_buildup": "NEUTRAL", "iv_state": "NORMAL", "candle_structure": "SIDEWAYS",
    "market_breadth": "BALANCED", "volume_profile": "NORMAL", "smart_money_flow": "NEUTRAL",
    "delta": 0, "gamma": 0, "theta": 0, "vega": 0,
    "institutional_signal": "HOLD", "institutional_confidence": 0
}

# --------------------------------------------------
# Professional Signal Parameters
# --------------------------------------------------
MIN_CONFIDENCE = 70          # Minimum confidence to generate signal
MAX_SPREAD_FOR_ENTRY = 15   # Max spread to consider valid entry
RSI_OVERBOUGHT = 75
RSI_OVERSOLD = 25
MACD_THRESHOLD = 0.5
ATR_VOLATILE = 10
CONFIRMATION_MIN = 3        # Minimum confirming factors required

SPREAD_THRESHOLD = 5.0

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
            tokens = [{"exchangeType": 5, "tokens": [CE_TOKEN, PE_TOKEN]}]
            sws.subscribe(correlation_id, mode, tokens)
            logger.info(f"Subscribed to tokens: {CE_TOKEN}, {PE_TOKEN}")
        except Exception as e:
            logger.error(f"Subscribe error: {e}")

def on_ws_data(wsapp, message, *args):
    global latest_ticks, price_history, tick_counter
    try:
        data = json.loads(message) if isinstance(message, str) else message
        ticks = data if isinstance(data, list) else [data]
        for tick in ticks:
            token = str(tick.get("tk"))
            ltp = tick.get("ltp", 0) / 100
            if token == CE_TOKEN:
                latest_ticks["ce_price"] = ltp
                latest_ticks["ce_timestamp"] = datetime.now().isoformat()
                price_history.append(ltp)
                tick_counter += 1
            elif token == PE_TOKEN:
                latest_ticks["pe_price"] = ltp
                latest_ticks["pe_timestamp"] = datetime.now().isoformat()

            ce = latest_ticks["ce_price"]
            pe = latest_ticks["pe_price"]
            if ce > 0 and pe > 0 and tick_counter % UPDATE_INTERVAL == 0:
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
# Professional Signal Engine
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

def calculate_macd(prices, fast=12, slow=26):
    if len(prices) < slow:
        return 0.0
    def ema(data, period):
        alpha = 2 / (period + 1)
        val = data[0]
        for p in data[1:]:
            val = alpha * p + (1 - alpha) * val
        return val
    return ema(prices, fast) - ema(prices, slow)

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

def calculate_vwap(prices):
    if not prices:
        return 0
    vol = [100] * len(prices)
    pv = sum(p * v for p, v in zip(prices, vol))
    tv = sum(vol)
    return pv / tv if tv else 0

def estimate_greeks(ce, pe):
    delta = round((ce - pe) / 100, 2)
    gamma = round(abs(delta) / 10, 2)
    theta = round(-(ce + pe) / 1000, 2)
    vega = round((ce + pe) / 500, 2)
    return delta, gamma, theta, vega

def detect_market_regime(atr, spread, price_list):
    """Detect if market is trending, ranging, or volatile."""
    if len(price_list) < 30:
        return "INSUFFICIENT_DATA"

    recent_range = max(price_list[-30:]) - min(price_list[-30:])
    avg_price = sum(price_list[-30:]) / 30
    volatility_pct = (recent_range / avg_price) * 100 if avg_price else 0

    if atr > ATR_VOLATILE and volatility_pct > 2:
        return "VOLATILE"
    elif volatility_pct < 0.5:
        return "SIDEWAYS"
    else:
        return "TRENDING"

def calculate_sentiment_score(bullish_score, bearish_score, pcr, rsi, macd, spread, ema_signal, trend_strength):
    """Calculate sentiment score from -100 (Extreme Fear) to +100 (Extreme Greed)."""
    sentiment = 0

    # PCR contribution (-30 to +30)
    if pcr < 0.8:
        sentiment += 30  # Extreme bullish (low PCR)
    elif pcr < 1.0:
        sentiment += 15
    elif pcr > 1.3:
        sentiment -= 30  # Extreme bearish (high PCR)
    elif pcr > 1.1:
        sentiment -= 15

    # RSI contribution (-25 to +25)
    if rsi > 70:
        sentiment += 25
    elif rsi > 60:
        sentiment += 15
    elif rsi < 30:
        sentiment -= 25
    elif rsi < 40:
        sentiment -= 15

    # MACD contribution (-20 to +20)
    if macd > 2:
        sentiment += 20
    elif macd > 0.5:
        sentiment += 10
    elif macd < -2:
        sentiment -= 20
    elif macd < -0.5:
        sentiment -= 10

    # Spread contribution (-15 to +15)
    if spread > 10:
        sentiment += 15
    elif spread > 0:
        sentiment += 5
    elif spread < -10:
        sentiment -= 15
    elif spread < 0:
        sentiment -= 5

    # EMA contribution (-10 to +10)
    if ema_signal == "BULLISH":
        sentiment += 10
    elif ema_signal == "BEARISH":
        sentiment -= 10

    # Clamp to -100 to +100
    return max(-100, min(100, sentiment))

def get_confidence_color(confidence):
    """Return color zone based on confidence."""
    if confidence >= 70:
        return "GREEN"
    elif confidence >= 40:
        return "YELLOW"
    else:
        return "RED"

def get_sentiment_label(score):
    """Convert sentiment score to label."""
    if score >= 80:
        return "EXTREME_GREED"
    elif score >= 50:
        return "GREED"
    elif score >= 20:
        return "OPTIMISM"
    elif score > -20:
        return "NEUTRAL"
    elif score > -50:
        return "FEAR"
    elif score > -80:
        return "EXTREME_FEAR"
    else:
        return "PANIC"

pcr_cache = {"value": 1.0, "time": 0}
PCR_TTL = 60

def get_nifty_pcr():
    now = time.time()
    if now - pcr_cache["time"] < PCR_TTL:
        return pcr_cache["value"]
    try:
        url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        headers = {"User-Agent": "Mozilla/5.0"}
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=5)
        time.sleep(0.5)
        resp = session.get(url, headers=headers, timeout=5)
        data = resp.json()
        ce_oi = sum(x.get("CE", {}).get("openInterest", 0) for x in data["records"]["data"] if "CE" in x)
        pe_oi = sum(x.get("PE", {}).get("openInterest", 0) for x in data["records"]["data"] if "PE" in x)
        pcr = pe_oi / ce_oi if ce_oi else 1.0
        pcr_cache["value"] = pcr
        pcr_cache["time"] = now
        return pcr
    except Exception as e:
        logger.error(f"PCR fetch failed: {e}")
        return pcr_cache["value"]

def run_signal_engine(ce_price, pe_price, price_list):
    global market_signal, market_state, institutional_state

    # Need minimum data
    if len(price_list) < 30:
        return

    spread = ce_price - pe_price
    rsi = calculate_rsi(price_list)
    macd = calculate_macd(price_list)
    vwap = calculate_vwap(price_list)
    ema_fast = calculate_ema(price_list, 9)
    ema_slow = calculate_ema(price_list, 21)
    atr = calculate_atr(price_list)
    pcr = get_nifty_pcr()
    delta, gamma, theta, vega = estimate_greeks(ce_price, pe_price)

    ema_signal = "BULLISH" if ema_fast > ema_slow else "BEARISH"
    market_regime = detect_market_regime(atr, spread, price_list)

    # --------------------------------------------------
    # PROFESSIONAL SCORING SYSTEM
    # --------------------------------------------------

    # Factor 1: Trend (EMA) - Weight: 25
    trend_score = 0
    trend_confirming = False
    if ema_signal == "BULLISH" and ema_fast > vwap:
        trend_score = 25
        trend_confirming = True
    elif ema_signal == "BEARISH" and ema_fast < vwap:
        trend_score = -25
        trend_confirming = True

    # Factor 2: Momentum (RSI) - Weight: 20
    momentum_score = 0
    momentum_confirming = False
    if rsi > 60 and rsi < RSI_OVERBOUGHT:
        momentum_score = 20
        momentum_confirming = True
    elif rsi < 40 and rsi > RSI_OVERSOLD:
        momentum_score = -20
        momentum_confirming = True
    elif rsi >= RSI_OVERBOUGHT or rsi <= RSI_OVERSOLD:
        # Extreme RSI = avoid (overbought/oversold)
        momentum_score = 0

    # Factor 3: Spread Direction - Weight: 15
    spread_score = 0
    spread_confirming = False
    if spread > 5:
        spread_score = 15
        spread_confirming = True
    elif spread < -5:
        spread_score = -15
        spread_confirming = True
    elif abs(spread) <= 2:
        # Too tight spread = no clear direction
        spread_score = 0

    # Factor 4: PCR (Contrarian) - Weight: 15
    pcr_score = 0
    pcr_confirming = False
    if pcr < 0.9:
        pcr_score = 15  # Low PCR = bullish (CE writers aggressive)
        pcr_confirming = True
    elif pcr > 1.2:
        pcr_score = -15  # High PCR = bearish (PE writers aggressive)
        pcr_confirming = True

    # Factor 5: MACD - Weight: 15
    macd_score = 0
    macd_confirming = False
    if macd > MACD_THRESHOLD:
        macd_score = 15
        macd_confirming = True
    elif macd < -MACD_THRESHOLD:
        macd_score = -15
        macd_confirming = True

    # Factor 6: Volatility Filter - Weight: 10
    volat_score = 0
    if atr < ATR_VOLATILE:
        volat_score = 10  # Low volatility = good for entry
    else:
        volat_score = -10  # High volatility = avoid

    # Calculate total scores
    bullish_score = max(0, trend_score) + max(0, momentum_score) + max(0, spread_score) + max(0, pcr_score) + max(0, macd_score) + max(0, volat_score)
    bearish_score = max(0, -trend_score) + max(0, -momentum_score) + max(0, -spread_score) + max(0, -pcr_score) + max(0, -macd_score) + max(0, -volat_score)

    # Count confirming factors
    confirming_factors = sum([
        trend_confirming,
        momentum_confirming,
        spread_confirming,
        pcr_confirming,
        macd_confirming
    ])

    # Calculate confidence (0-100)
    raw_confidence = max(bullish_score, bearish_score)

    # Apply penalties for low confirmation or bad regime
    if confirming_factors < CONFIRMATION_MIN:
        confidence = raw_confidence * 0.5  # Halve confidence if weak confirmation
    elif market_regime == "VOLATILE":
        confidence = raw_confidence * 0.3  # Heavy penalty in volatile markets
    elif market_regime == "SIDEWAYS":
        confidence = raw_confidence * 0.6  # Penalty in sideways markets
    else:
        confidence = raw_confidence

    confidence = min(100, confidence)
    confidence_color = get_confidence_color(confidence)

    # Calculate sentiment
    sentiment_score = calculate_sentiment_score(bullish_score, bearish_score, pcr, rsi, macd, spread, ema_signal, confirming_factors)
    sentiment_label = get_sentiment_label(sentiment_score)

    # Determine action - STRICT CRITERIA
    entry_zone = False

    if confidence >= MIN_CONFIDENCE and confirming_factors >= CONFIRMATION_MIN and market_regime in ["TRENDING", "SIDEWAYS"] and abs(spread) <= MAX_SPREAD_FOR_ENTRY:
        entry_zone = True

        if bullish_score > bearish_score and bullish_score >= 60:
            if confidence >= 85:
                action = "STRONG BUY CE"
            else:
                action = "BUY CE"
        elif bearish_score > bullish_score and bearish_score >= 60:
            if confidence >= 85:
                action = "STRONG BUY PE"
            else:
                action = "BUY PE"
        else:
            action = "HOLD"
            entry_zone = False
    else:
        action = "HOLD"
        entry_zone = False

    # Update market state
    market_state.update({
        "rsi": round(rsi, 2),
        "momentum": "UPTREND" if spread > 0 else "DOWNTREND" if spread < 0 else "NEUTRAL",
        "strength": "HIGH" if confidence > 70 else "MEDIUM" if confidence > 40 else "LOW",
        "trend": ema_signal,
        "action": action,
        "confidence": round(confidence, 1),
        "volatility": "HIGH" if atr > ATR_VOLATILE else "NORMAL",
        "alert": action if entry_zone else "HOLD"
    })

    institutional_state.update({
        "vwap": round(vwap, 2),
        "ema_fast": round(ema_fast, 2),
        "ema_slow": round(ema_slow, 2),
        "ema_signal": ema_signal,
        "atr": round(atr, 2),
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "institutional_signal": action,
        "institutional_confidence": round(confidence, 1)
    })

    market_signal.update({
        "signal": "BULLISH" if spread > SPREAD_THRESHOLD else "BEARISH" if spread < -SPREAD_THRESHOLD else "NEUTRAL",
        "sentiment": sentiment_label,
        "sentiment_score": round(sentiment_score, 1),
        "ce_price": ce_price,
        "pe_price": pe_price,
        "spread": round(spread, 2),
        "rsi": round(rsi, 2),
        "macd": round(macd, 2),
        "pcr": round(pcr, 2),
        "vwap": round(vwap, 2),
        "atr": round(atr, 2),
        "ema_fast": round(ema_fast, 2),
        "ema_slow": round(ema_slow, 2),
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "confidence": round(confidence, 1),
        "confidence_color": confidence_color,
        "trend_strength": confirming_factors,
        "volatility_regime": market_regime,
        "market_regime": market_regime,
        "entry_zone": entry_zone,
        "timestamp": datetime.now().isoformat()
    })

    logger.info(f"[{market_regime}] {action} | Conf={confidence:.1f}% ({confidence_color}) | Sentiment={sentiment_label}({sentiment_score:+.0f}) | Confirms={confirming_factors}/5 | Spread={spread:.2f}")

# --------------------------------------------------
# Flask endpoints
# --------------------------------------------------
@app.route("/")
def home():
    return jsonify({"status": "online", "message": "Angel One WebSocket Engine"})

@app.route("/api/live-signals")
def live_signals():
    return jsonify({
        "status": "active",
        "data": market_signal,
        "market": market_state,
        "institutional": institutional_state
    })

@app.route("/api/health")
def health():
    return jsonify({
        "status": "ok", 
        "ws_running": ws_running,
        "ce_token": CE_TOKEN,
        "pe_token": PE_TOKEN,
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
        "price_history_len": len(price_history)
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
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
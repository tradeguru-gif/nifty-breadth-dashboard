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

# ============================================================
# MONKEY‑PATCH FOR SmartWebSocketV2 (fixes token parsing)
# ============================================================
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# Save the original _parse_binary_data method
_original_parse = SmartWebSocketV2._parse_binary_data

def _patched_parse(self, binary_data):
    # Call the original first to get all fields except token
    try:
        result = _original_parse(self, binary_data)
    except:
        result = {}
    # Manually extract the token from bytes 2 to 26 (little‑endian int)
    try:
        token_bytes = binary_data[2:26]
        token_int = int.from_bytes(token_bytes, byteorder='little')
        result['token'] = str(token_int)
    except Exception as e:
        logging.getLogger(__name__).error(f"Token extraction failed: {e}")
    return result

# Replace the method with our patched version
SmartWebSocketV2._parse_binary_data = _patched_parse
# Also fix the _on_close signature mismatch
_original_on_close = SmartWebSocketV2._on_close
def _patched_on_close(self, wsapp, *args):
    try:
        _original_on_close(self, wsapp)
    except:
        pass
SmartWebSocketV2._on_close = _patched_on_close
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

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing Angel One credentials")

# --------------------------------------------------
# Global state
# --------------------------------------------------
CE_TOKEN = None
PE_TOKEN = None
latest_ticks = {"ce_price": 0.0, "pe_price": 0.0}
price_history = deque(maxlen=200)
tick_counter = 0
UPDATE_INTERVAL = 10
ws_running = False
sws = None

# --------------------------------------------------
# Market Signal State
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
    "alert": "NONE"
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
    "delta": 0,
    "gamma": 0,
    "theta": 0,
    "vega": 0,
    "institutional_signal": "HOLD",
    "institutional_confidence": 0
}

# Confidence thresholds
SPREAD_THRESHOLD = 5.0
STRONG_BUY_THRESHOLD = 80
BUY_THRESHOLD = 60
CONSIDER_THRESHOLD = 40

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
    except Exception as e:
        logger.error(f"Failed to load instrument master: {e}")
        return None, None

    nifty_opts = df[
        (df["name"].astype(str) == "NIFTY") & 
        (df["instrumenttype"].astype(str) == "OPTIDX") &
        (df["exch_seg"].astype(str) == "NFO")
    ].copy()
    if nifty_opts.empty:
        nifty_opts = df[df["symbol"].astype(str).str.match(r'^NIFTY\d{2}[A-Z]{3}\d{2}', na=False)].copy()
    if nifty_opts.empty:
        logger.error("No NIFTY symbols found")
        return None, None

    for fmt in ["%d%b%Y", "%d-%b%Y", "%Y-%m-%d", "%d%m%Y"]:
        nifty_opts["expiry_date"] = pd.to_datetime(nifty_opts["expiry"], format=fmt, errors="coerce")
        if nifty_opts["expiry_date"].notna().sum() > 0:
            break
    nifty_opts = nifty_opts.dropna(subset=["expiry_date"])
    nifty_opts["strike"] = pd.to_numeric(nifty_opts["strike"], errors="coerce") / 100
    nifty_opts = nifty_opts.dropna(subset=["strike"])

    today = datetime.now()
    future_expiries = nifty_opts[nifty_opts["expiry_date"] > today]
    if future_expiries.empty:
        logger.error("No future expiries found")
        return None, None
    nearest_expiry = future_expiries["expiry_date"].min()
    nearest_opts = nifty_opts[nifty_opts["expiry_date"] == nearest_expiry]
    available_strikes = sorted(nearest_opts["strike"].unique())
    if atm_strike not in available_strikes:
        atm_strike = min(available_strikes, key=lambda x: abs(x - atm_strike))
        logger.info(f"Using nearest strike: {atm_strike}")

    atm_opts = nifty_opts[(nifty_opts["strike"] == atm_strike) & (nifty_opts["expiry_date"] == nearest_expiry)]
    ce_row = atm_opts[atm_opts["symbol"].str.upper().str.contains("CE", na=False)]
    pe_row = atm_opts[atm_opts["symbol"].str.upper().str.contains("PE", na=False)]
    if ce_row.empty or pe_row.empty:
        logger.error(f"CE or PE not found for strike {atm_strike}")
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
            # Use exchangeType=5 for NFO options (as per your original)
            sws.subscribe("tradeguru_001", 2, [{"exchangeType": 5, "tokens": [CE_TOKEN, PE_TOKEN]}])
            logger.info(f"Subscribed to tokens: {CE_TOKEN}, {PE_TOKEN}")
        except Exception as e:
            logger.error(f"Subscribe error: {e}")

def on_ws_data(wsapp, message, *args):
    global latest_ticks, price_history, tick_counter
    try:
        # The message is already parsed by the patched library into a dict
        if isinstance(message, bytes):
            logger.debug("Raw bytes received, ignoring")
            return
        data = json.loads(message) if isinstance(message, str) else message
        ticks = data if isinstance(data, list) else [data]
        for tick in ticks:
            token = str(tick.get("tk"))
            ltp = tick.get("ltp", 0)
            if isinstance(ltp, (int, float)) and ltp > 1000:
                ltp = ltp / 100
            if token == CE_TOKEN:
                latest_ticks["ce_price"] = ltp
                price_history.append(ltp)
                tick_counter += 1
                logger.debug(f"CE tick: {ltp}")
            elif token == PE_TOKEN:
                latest_ticks["pe_price"] = ltp
                logger.debug(f"PE tick: {ltp}")

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
# Signal Engine (keep your existing implementation)
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
    if len(price_list) < 20:
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

    bullish_score = 0
    if ema_signal == "BULLISH":
        bullish_score += 20
    if rsi > 60:
        bullish_score += 20
    if spread > 0:
        bullish_score += 20
    if pcr < 1.0:
        bullish_score += 20
    if macd > 0:
        bullish_score += 20

    bearish_score = 0
    if ema_signal == "BEARISH":
        bearish_score += 20
    if rsi < 40:
        bearish_score += 20
    if spread < 0:
        bearish_score += 20
    if pcr > 1.2:
        bearish_score += 20
    if macd < 0:
        bearish_score += 20

    if bullish_score >= bearish_score and bullish_score >= CONSIDER_THRESHOLD:
        confidence = bullish_score
        if confidence >= STRONG_BUY_THRESHOLD:
            action = "STRONG BUY CE"
        elif confidence >= BUY_THRESHOLD:
            action = "BUY CE"
        elif confidence >= CONSIDER_THRESHOLD:
            action = "CONSIDER CE"
        else:
            action = "HOLD"
    elif bearish_score > bullish_score and bearish_score >= CONSIDER_THRESHOLD:
        confidence = bearish_score
        if confidence >= STRONG_BUY_THRESHOLD:
            action = "STRONG BUY PE"
        elif confidence >= BUY_THRESHOLD:
            action = "BUY PE"
        elif confidence >= CONSIDER_THRESHOLD:
            action = "CONSIDER PE"
        else:
            action = "HOLD"
    else:
        confidence = max(bullish_score, bearish_score)
        action = "HOLD"

    market_state.update({
        "rsi": round(rsi, 2),
        "momentum": "UPTREND" if spread > 0 else "DOWNTREND" if spread < 0 else "NEUTRAL",
        "strength": "HIGH" if confidence >= BUY_THRESHOLD else "MODERATE" if confidence >= CONSIDER_THRESHOLD else "LOW",
        "trend": ema_signal,
        "action": action,
        "confidence": confidence,
        "volatility": "HIGH" if atr > 15 else "NORMAL" if atr > 5 else "LOW",
        "alert": action
    })

    institutional_state.update({
        "vwap": round(vwap, 2),
        "ema_fast": round(ema_fast, 2),
        "ema_slow": round(ema_slow, 2),
        "ema_signal": ema_signal,
        "atr": round(atr, 2),
        "oi_buildup": "BULLISH" if pcr < 0.8 else "BEARISH" if pcr > 1.2 else "NEUTRAL",
        "iv_state": "HIGH" if vega > 2 else "NORMAL",
        "candle_structure": "BULLISH" if ema_signal == "BULLISH" and rsi > 60 else "BEARISH" if ema_signal == "BEARISH" and rsi < 40 else "SIDEWAYS",
        "market_breadth": "BULLISH" if bullish_score > bearish_score else "BEARISH" if bearish_score > bullish_score else "BALANCED",
        "volume_profile": "HIGH" if abs(spread) > 20 else "NORMAL",
        "smart_money_flow": "BULLISH" if vwap > ema_slow else "BEARISH" if vwap < ema_slow else "NEUTRAL",
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "institutional_signal": action,
        "institutional_confidence": confidence
    })

    market_signal.update({
        "signal": "BULLISH" if spread > SPREAD_THRESHOLD else "BEARISH" if spread < -SPREAD_THRESHOLD else "NEUTRAL",
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
        "timestamp": datetime.now().isoformat()
    })

    logger.info(f"Signal: {action} (Bull={bullish_score} Bear={bearish_score}) | RSI={rsi:.1f} | Spread={spread:.2f} | EMA={ema_signal}")

# --------------------------------------------------
# Flask endpoints
# --------------------------------------------------
@app.route("/")
def home():
    return jsonify({
        "status": "online", 
        "message": "Nifty Signal Engine v4.7 (Patched)",
        "worker_type": "PRIMARY (WebSocket)",
        "market_open": datetime.now().strftime("%H:%M")
    })

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
        "latest_ce": latest_ticks["ce_price"],
        "latest_pe": latest_ticks["pe_price"],
        "timestamp": datetime.now().isoformat()
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
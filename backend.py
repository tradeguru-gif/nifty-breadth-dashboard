# backend.py – Angel One WebSocket 2.0 Signal Engine

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
from SmartApi import SmartWebSocketV2

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------------------------------------------------
# Flask app
# --------------------------------------------------
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
    raise ValueError("Missing Angel One credentials in environment")

# --------------------------------------------------
# Global state
# --------------------------------------------------
CE_TOKEN = None
PE_TOKEN = None
latest_ticks = {"ce_price": 0.0, "pe_price": 0.0}
price_history = deque(maxlen=200)
tick_counter = 0
UPDATE_INTERVAL = 10          # Re-run signal engine every 10 ticks
ws = None                     # WebSocket instance
ws_running = False

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
    "rsi": 50, "momentum": "NEUTRAL", "strength": "LOW", "trend": "SIDEWAYS",
    "action": "HOLD", "confidence": 0, "volatility": "NORMAL", "alert": "NONE"
}
institutional_state = {
    "vwap": 0, "ema_fast": 0, "ema_slow": 0, "ema_signal": "NEUTRAL", "atr": 0,
    "oi_buildup": "NEUTRAL", "iv_state": "NORMAL", "candle_structure": "SIDEWAYS",
    "market_breadth": "BALANCED", "volume_profile": "NORMAL", "smart_money_flow": "NEUTRAL",
    "delta": 0, "gamma": 0, "theta": 0, "vega": 0,
    "institutional_signal": "HOLD", "institutional_confidence": 0
}
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
        spot = data['data'][0]['lastPrice']
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
    nifty_opts = df[df['symbol'].astype(str).str.contains('NIFTY', na=False)]
    nifty_opts['expiry_date'] = pd.to_datetime(nifty_opts['expiry'], errors='coerce')
    nifty_opts = nifty_opts.dropna(subset=['expiry_date'])
    today = datetime.now()
    future_expiries = nifty_opts[nifty_opts['expiry_date'] > today]
    if future_expiries.empty:
        return None, None
    nearest_expiry = future_expiries['expiry_date'].min()
    atm_opts = nifty_opts[(nifty_opts['strike'] == atm_strike) & (nifty_opts['expiry_date'] == nearest_expiry)]
    ce_row = atm_opts[atm_opts['symbol'].str.contains('CE', na=False)]
    pe_row = atm_opts[atm_opts['symbol'].str.contains('PE', na=False)]
    if ce_row.empty or pe_row.empty:
        return None, None
    ce_token = str(ce_row.iloc[0]['token'])
    pe_token = str(pe_row.iloc[0]['token'])
    logger.info(f"CE token = {ce_token}, PE token = {pe_token}")
    return ce_token, pe_token

# --------------------------------------------------
# WebSocket Callbacks
# --------------------------------------------------
def on_ws_open(wsapp):
    logger.info("✅ Angel One WebSocket opened")
    correlation_id = "tradeguru_001"
    mode = 2
    tokens = [{"exchangeType": 5, "tokens": [CE_TOKEN, PE_TOKEN]}]  # 5 = NFO
    wsapp.subscribe(correlation_id, mode, tokens)

def on_ws_message(wsapp, message):
    global latest_ticks, price_history, tick_counter, CE_TOKEN, PE_TOKEN
    try:
        data = json.loads(message)
        for tick in data:
            token = str(tick.get('tk'))
            ltp = tick.get('ltp', 0) / 100
            if token == CE_TOKEN:
                latest_ticks["ce_price"] = ltp
                latest_ticks["ce_timestamp"] = datetime.now()
                price_history.append(ltp)
                tick_counter += 1
            elif token == PE_TOKEN:
                latest_ticks["pe_price"] = ltp
                latest_ticks["pe_timestamp"] = datetime.now()

            ce = latest_ticks["ce_price"]
            pe = latest_ticks["pe_price"]
            if ce > 0 and pe > 0 and tick_counter % UPDATE_INTERVAL == 0:
                run_signal_engine(ce, pe, list(price_history))
    except Exception as e:
        logger.error(f"WebSocket message error: {e}")

def on_ws_error(wsapp, error):
    logger.error(f"WebSocket error: {error}")

def on_ws_close(wsapp):
    logger.warning("WebSocket closed")
    global ws_running
    ws_running = False

# --------------------------------------------------
# Start WebSocket connection
# --------------------------------------------------
def start_angel_websocket():
    global ws, ws_running, CE_TOKEN, PE_TOKEN
    # 1. Authenticate
    totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
    obj = SmartConnect(api_key=ANGEL_API_KEY)
    session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
    if not session['status']:
        logger.error(f"Login failed: {session}")
        return
    auth_token = session['data']['jwtToken']
    feed_token = obj.getfeedToken()
    logger.info("Authenticated, feed token obtained")

    # 2. Get current ATM tokens
    CE_TOKEN, PE_TOKEN = get_current_atm_tokens()
    if not CE_TOKEN or not PE_TOKEN:
        logger.error("Could not fetch ATM tokens. Retrying in 60s...")
        time.sleep(60)
        return

    # 3. Setup and run WebSocket
    ws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)
    ws.on_open = on_ws_open
    ws.on_data = on_ws_message
    ws.on_error = on_ws_error
    ws.on_close = on_ws_close
    ws_running = True
    ws.connect()
    while ws_running:
        time.sleep(1)

# --------------------------------------------------
# Signal Engine (unchanged from your working version)
# --------------------------------------------------
# ... [Your full signal logic remains exactly as it was] ...

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

@app.route("/health")
def health():
    return "OK", 200

# --------------------------------------------------
# Main
# --------------------------------------------------
def start_background_engine():
    thread = threading.Thread(target=start_angel_websocket, daemon=True)
    thread.start()
    logger.info("✅ Angel One WebSocket background engine started")

if __name__ == "__main__":
    start_background_engine()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
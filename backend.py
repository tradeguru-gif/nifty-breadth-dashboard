import os
import time
import logging
import threading
from collections import deque
from datetime import datetime

from flask import Flask, jsonify
from flask_cors import CORS

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

import pyotp

# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)
CORS(app)

application = app

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

# ============================================================
# ENV VARIABLES
# ============================================================

ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

# ============================================================
# GLOBALS
# ============================================================

ws_running = False
engine_started = False
last_tick_time = time.time()

sws = None

# NIFTY CE / PE TOKENS
CE_TOKEN = "60266"
PE_TOKEN = "60267"

latest_ticks = {
    "ce_price": 0,
    "pe_price": 0,
}

ce_price_history = deque(maxlen=100)
pe_price_history = deque(maxlen=100)

market_signal = {
    "signal": "WAITING",
    "ce_price": 0,
    "pe_price": 0,
    "timestamp": "",
}

market_state = {
    "action": "HOLD",
    "confidence": 0,
}

institutional_state = {
    "institutional_signal": "HOLD"
}

# ============================================================
# SIGNAL ENGINE
# ============================================================

def run_signal_engine():

    ce = latest_ticks["ce_price"]
    pe = latest_ticks["pe_price"]

    if ce > pe:

        signal = "BULLISH"

        action = "BUY CE"

        confidence = 80

    elif pe > ce:

        signal = "BEARISH"

        action = "BUY PE"

        confidence = 80

    else:

        signal = "NEUTRAL"

        action = "HOLD"

        confidence = 50

    market_signal.update({
        "signal": signal,
        "ce_price": ce,
        "pe_price": pe,
        "timestamp": datetime.now().isoformat()
    })

    market_state.update({
        "action": action,
        "confidence": confidence
    })

    institutional_state.update({
        "institutional_signal": action
    })

    logger.info(f"SIGNAL => {action}")

# ============================================================
# WEBSOCKET EVENTS
# ============================================================

def on_open(wsapp):

    global sws

    logger.info("WebSocket Opened")

    correlation_id = "tradeguru"

    mode = 1

    token_list = [
        {
            "exchangeType": 2,
            "tokens": [CE_TOKEN, PE_TOKEN]
        }
    ]

    sws.subscribe(
        correlation_id,
        mode,
        token_list
    )

    logger.info("Subscribed Successfully")


def on_data(wsapp, message):

    global last_tick_time

    try:

        last_tick_time = time.time()

        token = str(message.get("token"))

        ltp = float(message.get("last_traded_price", 0)) / 100

        if token == CE_TOKEN:

            latest_ticks["ce_price"] = ltp

            ce_price_history.append(ltp)

            logger.info(f"CE => {ltp}")

        elif token == PE_TOKEN:

            latest_ticks["pe_price"] = ltp

            pe_price_history.append(ltp)

            logger.info(f"PE => {ltp}")

        if (
            len(ce_price_history) >= 5
            and len(pe_price_history) >= 5
        ):

            run_signal_engine()

    except Exception as e:

        logger.error(f"Data Error: {e}")


def on_error(wsapp, error):

    global ws_running

    logger.error(f"WebSocket Error: {error}")

    ws_running = False


def on_close(wsapp):

    global ws_running

    logger.warning("WebSocket Closed")

    ws_running = False

# ============================================================
# ANGEL LOGIN
# ============================================================

def angel_login():

    try:

        totp = pyotp.TOTP(
            ANGEL_TOTP_SECRET
        ).now()

        obj = SmartConnect(
            api_key=ANGEL_API_KEY
        )

        data = obj.generateSession(
            ANGEL_CLIENT_ID,
            ANGEL_PASSWORD,
            totp
        )

        auth_token = data["data"]["jwtToken"]

        feed_token = obj.getfeedToken()

        logger.info("Angel Login Success")

        return auth_token, feed_token

    except Exception as e:

        logger.error(f"Login Error: {e}")

        return None, None

# ============================================================
# START WS
# ============================================================

def start_websocket():

    global sws
    global ws_running

    while True:

        try:

            auth_token, feed_token = angel_login()

            if not auth_token:

                time.sleep(10)

                continue

            sws = SmartWebSocketV2(
                auth_token,
                ANGEL_API_KEY,
                ANGEL_CLIENT_ID,
                feed_token
            )

            sws.on_open = on_open
            sws.on_data = on_data
            sws.on_error = on_error
            sws.on_close = on_close

            ws_running = True

            logger.info("Connecting WebSocket")

            sws.connect()

        except Exception as e:

            logger.error(f"WS Engine Error: {e}")

            time.sleep(10)

# ============================================================
# BACKGROUND ENGINE
# ============================================================

def start_background_engine():

    global engine_started

    if not engine_started:

        thread = threading.Thread(
            target=start_websocket,
            daemon=True
        )

        thread.start()

        engine_started = True

        logger.info("Live Engine Started")

# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():

    return jsonify({
        "status": "online",
        "message": "TradeGuru Live Engine"
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
        "latest_ce": latest_ticks["ce_price"],
        "latest_pe": latest_ticks["pe_price"],
        "last_tick_age": round(
            time.time() - last_tick_time,
            1
        )
    })

# ============================================================
# START
# ============================================================

start_background_engine()

if __name__ == "__main__":

    port = int(os.environ.get("PORT", 10000))

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )
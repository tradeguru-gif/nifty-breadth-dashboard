import os
import time
import logging
import threading
from collections import deque
from datetime import datetime

from flask import Flask, jsonify
from flask_cors import CORS

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
# GLOBALS
# ============================================================

ws_running = False
engine_started = False
last_tick_time = time.time()

CE_TOKEN = "99926000"
PE_TOKEN = "99926001"

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
# MARKET
# ============================================================

def is_market_open():

    now = datetime.now()

    if now.weekday() >= 5:
        return False

    return True

# ============================================================
# SIGNAL ENGINE
# ============================================================

def run_signal_engine():

    ce = latest_ticks["ce_price"]
    pe = latest_ticks["pe_price"]

    if ce > pe:

        signal = "BULLISH"

        action = "BUY CE"

        confidence = 78

    elif pe > ce:

        signal = "BEARISH"

        action = "BUY PE"

        confidence = 74

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
# FAKE LIVE DATA ENGINE
# ============================================================

def market_data_engine():

    global ws_running
    global last_tick_time

    ws_running = True

    ce = 100
    pe = 100

    while True:

        try:

            ce += 1
            pe -= 1

            latest_ticks["ce_price"] = ce
            latest_ticks["pe_price"] = pe

            ce_price_history.append(ce)
            pe_price_history.append(pe)

            last_tick_time = time.time()

            logger.info(f"CE => {ce}")
            logger.info(f"PE => {pe}")

            if len(ce_price_history) >= 5:
                run_signal_engine()

            time.sleep(2)

        except Exception as e:

            logger.error(f"Engine Error: {e}")

            time.sleep(5)

# ============================================================
# START ENGINE
# ============================================================

def start_background_engine():

    global engine_started

    if not engine_started:

        thread = threading.Thread(
            target=market_data_engine,
            daemon=True
        )

        thread.start()

        engine_started = True

        logger.info("Background Engine Started")

# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():

    return jsonify({
        "status": "online",
        "message": "TradeGuru Signal Engine Running",
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
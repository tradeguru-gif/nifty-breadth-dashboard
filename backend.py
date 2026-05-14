# backend.py - Nifty Options Signal Engine (Stable Dhan SDK Version)

import os
import threading
import time
import logging
import requests
import csv

from collections import deque
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

from dhanhq import DhanContext
from dhanhq.marketfeed import MarketFeed

# --------------------------------------------------
# LOGGING
# --------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------------------------------------------------
# FLASK
# --------------------------------------------------
app = Flask(__name__)
CORS(app)
application = app

# --------------------------------------------------
# ENV
# --------------------------------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

# --------------------------------------------------
# GLOBALS
# --------------------------------------------------
LAST_KNOWN_CE = "35000"
LAST_KNOWN_PE = "35001"

SELECTED_CE_ID = LAST_KNOWN_CE
SELECTED_PE_ID = LAST_KNOWN_PE

reconnect_delay = 10

price_history = deque(maxlen=200)
volume_history = deque(maxlen=200)

tick_counter = 0
UPDATE_INTERVAL = 10
SPREAD_THRESHOLD = 5.0

# --------------------------------------------------
# DATA STATES
# --------------------------------------------------
latest_data = {
    "signal": "WAITING",
    "ce_price": 0.0,
    "pe_price": 0.0,
    "spread": 0.0,
    "rsi": 50,
    "macd": 0.0,
    "pcr": 1.0,
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

# --------------------------------------------------
# CONTRACT SELECTION
# --------------------------------------------------
def update_contracts():

    global SELECTED_CE_ID
    global SELECTED_PE_ID
    global LAST_KNOWN_CE
    global LAST_KNOWN_PE

    try:

        logger.info("Loading Dhan instruments...")

        url = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        reader = csv.DictReader(response.text.splitlines())

        options = []

        for row in reader:

            if row.get("SEGMENT") != "D":
                continue

            if row.get("INSTRUMENT") != "OPTIDX":
                continue

            symbol = row.get("SYMBOL_NAME") or row.get("SYMBOL") or ""

            if "NIFTY" not in symbol:
                continue

            if "BANK" in symbol or "FIN" in symbol:
                continue

            expiry_str = row.get("SM_EXPIRY_DATE") or row.get("EXPIRY_DATE")

            if not expiry_str:
                continue

            expiry = None

            try:
                expiry = datetime.strptime(expiry_str, "%Y-%m-%d")
            except:
                try:
                    expiry = datetime.strptime(expiry_str, "%d-%b-%Y")
                except:
                    continue

            try:
                strike = float(row.get("STRIKE_PRICE") or row.get("STRIKE", 0))
            except:
                continue

            options.append({
                "expiry": expiry,
                "strike": strike,
                "option_type": row.get("OPTION_TYPE"),
                "security_id": row.get("SECURITY_ID")
            })

        if not options:
            raise Exception("No NIFTY options found")

        nearest_expiry = min(options, key=lambda x: x["expiry"])["expiry"]

        near_options = [
            x for x in options
            if x["expiry"] == nearest_expiry
        ]

        # fallback spot
        spot = 24000

        try:

            headers = {
                "User-Agent": "Mozilla/5.0"
            }

            s = requests.Session()

            s.get(
                "https://www.nseindia.com",
                headers=headers,
                timeout=5
            )

            r = s.get(
                "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050",
                headers=headers,
                timeout=5
            )

            if r.status_code == 200:
                spot = float(r.json()["data"][0]["lastPrice"])

        except:
            logger.warning("NSE blocked spot request, using fallback")

        strikes = sorted(set(x["strike"] for x in near_options))

        atm = min(strikes, key=lambda x: abs(x - spot))

        ce = next(
            (
                x["security_id"]
                for x in near_options
                if x["strike"] == atm and x["option_type"] == "CE"
            ),
            None
        )

        pe = next(
            (
                x["security_id"]
                for x in near_options
                if x["strike"] == atm and x["option_type"] == "PE"
            ),
            None
        )

        if not ce or not pe:
            raise Exception("ATM CE/PE not found")

        SELECTED_CE_ID = str(int(float(ce)))
        SELECTED_PE_ID = str(int(float(pe)))

        LAST_KNOWN_CE = SELECTED_CE_ID
        LAST_KNOWN_PE = SELECTED_PE_ID

        logger.info(
            f"✅ Contracts selected: "
            f"CE={SELECTED_CE_ID} "
            f"PE={SELECTED_PE_ID}"
        )

    except Exception as e:

        logger.error(f"Contract update failed: {e}")

        SELECTED_CE_ID = LAST_KNOWN_CE
        SELECTED_PE_ID = LAST_KNOWN_PE

# --------------------------------------------------
# STARTUP CONTRACT LOAD
# --------------------------------------------------
update_contracts()

# --------------------------------------------------
# INDICATORS
# --------------------------------------------------
def calculate_rsi(prices, period=14):

    if len(prices) < period + 1:
        return 50.0

    deltas = [
        prices[i] - prices[i - 1]
        for i in range(1, len(prices))
    ]

    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))

# --------------------------------------------------
# PCR
# --------------------------------------------------
pcr_cache = {
    "value": 1.0,
    "time": 0
}

PCR_TTL = 60

def get_pcr():

    now = time.time()

    if now - pcr_cache["time"] < PCR_TTL:
        return pcr_cache["value"]

    try:

        headers = {
            "User-Agent": "Mozilla/5.0"
        }

        s = requests.Session()

        s.get(
            "https://www.nseindia.com",
            headers=headers,
            timeout=5
        )

        r = s.get(
            "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY",
            headers=headers,
            timeout=5
        )

        data = r.json()

        ce_oi = sum(
            x.get("CE", {}).get("openInterest", 0)
            for x in data["records"]["data"]
            if "CE" in x
        )

        pe_oi = sum(
            x.get("PE", {}).get("openInterest", 0)
            for x in data["records"]["data"]
            if "PE" in x
        )

        value = pe_oi / ce_oi if ce_oi else 1.0

        pcr_cache["value"] = value
        pcr_cache["time"] = now

        return value

    except Exception as e:

        logger.error(f"PCR fetch failed: {e}")

        return pcr_cache["value"]

# --------------------------------------------------
# MESSAGE HANDLER
# --------------------------------------------------
def on_message(instance, tick):

    global tick_counter

    try:

        security_id = str(tick.get("security_id"))
        ltp = float(tick.get("ltp", 0))

        volume = int(tick.get("volume", 100))

        if security_id == SELECTED_CE_ID:

            latest_data["ce_price"] = ltp

            if ltp > 0:
                price_history.append(ltp)
                volume_history.append(volume)

        elif security_id == SELECTED_PE_ID:

            latest_data["pe_price"] = ltp

        ce = latest_data["ce_price"]
        pe = latest_data["pe_price"]

        if ce > 0 and pe > 0:

            spread = ce - pe

            latest_data["spread"] = round(spread, 2)

            if spread > SPREAD_THRESHOLD:
                latest_data["signal"] = "BULLISH"

            elif spread < -SPREAD_THRESHOLD:
                latest_data["signal"] = "BEARISH"

            else:
                latest_data["signal"] = "NEUTRAL"

            tick_counter += 1

            if tick_counter >= UPDATE_INTERVAL:

                tick_counter = 0

                latest_data["rsi"] = round(
                    calculate_rsi(list(price_history)),
                    2
                )

                latest_data["pcr"] = round(get_pcr(), 2)

            latest_data["timestamp"] = datetime.now().isoformat()

    except Exception as e:

        logger.error(f"on_message error: {e}")

# --------------------------------------------------
# CALLBACKS
# --------------------------------------------------
def on_connect(instance):

    global reconnect_delay

    reconnect_delay = 10

    logger.info("✅ WebSocket connected and authorized")

def on_error(instance, error):

    logger.error(f"❌ WebSocket error: {error}")

def on_close(instance):

    logger.warning("🔌 WebSocket closed")

# --------------------------------------------------
# FEED RUNNER
# --------------------------------------------------
def run_feed():

    ctx = DhanContext(
        client_id=CLIENT_ID,
        access_token=ACCESS_TOKEN
    )

    reconnect_delay = 900

    while True:
        try:

            logger.info(
                f"Connecting to MarketFeed CE={SELECTED_CE_ID} PE={SELECTED_PE_ID}"
            )

            instruments = [
                (MarketFeed.NSE_FNO, str(SELECTED_CE_ID), MarketFeed.Ticker),
                (MarketFeed.NSE_FNO, str(SELECTED_PE_ID), MarketFeed.Ticker)
            ]

            feed = MarketFeed(
                ctx,
                instruments,
                version="v2"
            )

            feed.on_connect = on_connect
            feed.on_message = on_message
            feed.on_error = on_error
            feed.on_close = on_close

            feed.run_forever()

            logger.warning("WebSocket exited unexpectedly")

        except Exception as e:
            logger.error(f"Feed crashed: {e}")

        logger.info(f"Sleeping {reconnect_delay}s before reconnect")
        time.sleep(reconnect_delay)

# --------------------------------------------------
# START THREAD
# --------------------------------------------------
thread_started = False

if not thread_started:

    thread_started = True

    thread = threading.Thread(
        target=run_feed,
        daemon=True
    )

    thread.start()

    logger.info("✅ Background engine started")

# --------------------------------------------------
# ROUTES
# --------------------------------------------------
@app.route("/")
def home():

    return jsonify({
        "status": "active",
        "data": latest_data,
        "market": market_state,
        "institutional": institutional_state
    })

@app.route("/api/health")
def health():

    return "OK", 200

@app.route("/debug/version")
def version():

    return jsonify({
        "sdk": "modern",
        "ce": SELECTED_CE_ID,
        "pe": SELECTED_PE_ID
    })

# --------------------------------------------------
# MAIN
# --------------------------------------------------
if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 10000))
    )
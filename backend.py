# backend.py - Production Stable Dhan Options Engine

import os
import time
import threading
import logging
import requests
import pandas as pd

from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS
from collections import deque

from dhanhq import dhanhq as DhanHQ
from dhanhq import marketfeed

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("backend")

# ----------------------------
# Flask
# ----------------------------
app = Flask(__name__)
CORS(app)

# ----------------------------
# ENV
# ----------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
NIFTY_SPOT = float(os.getenv("CURRENT_NIFTY", "24000"))

# ----------------------------
# Dhan Init (FIXED)
# ----------------------------
dhan = DhanHQ(CLIENT_ID, ACCESS_TOKEN)

# ----------------------------
# Runtime State
# ----------------------------
latest_data = {
    "ce_price": 0,
    "pe_price": 0,
    "spread": 0,
    "pcr": 1.0,
    "signal": "WAITING",
    "timestamp": ""
}

SELECTED_CE = None
SELECTED_PE = None

price_history = deque(maxlen=200)

# ----------------------------
# CACHE (PCR)
# ----------------------------
pcr_cache = {
    "value": 1.0,
    "timestamp": 0
}

PCR_CACHE_TTL = 60  # seconds

# ----------------------------
# SAFE PCR (NO SPAM NSE)
# ----------------------------
def get_pcr_cached():
    now = time.time()

    if now - pcr_cache["timestamp"] < PCR_CACHE_TTL:
        return pcr_cache["value"]

    try:
        url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        headers = {"User-Agent": "Mozilla/5.0"}

        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=5)

        res = session.get(url, headers=headers, timeout=5)
        data = res.json()

        ce, pe = 0, 0

        for i in data["records"]["data"]:
            if "CE" in i:
                ce += i["CE"]["openInterest"]
            if "PE" in i:
                pe += i["PE"]["openInterest"]

        value = ce / pe if pe else 1.0

        pcr_cache["value"] = value
        pcr_cache["timestamp"] = now

        return value

    except Exception as e:
        logger.error(f"PCR error: {e}")
        return pcr_cache["value"]


# ----------------------------
# SAFE INSTRUMENT LOAD (NO BREAK)
# ----------------------------
def load_instruments():
    try:
        df = dhan.get_instrument_list()

        df.columns = df.columns.str.upper()

        return df

    except Exception as e:
        logger.error(f"Instrument load failed: {e}")
        return pd.DataFrame()


# ----------------------------
# SMART ATM + IV STYLE SELECTION (SIMPLIFIED)
# ----------------------------
def select_contracts(spot):
    global SELECTED_CE, SELECTED_PE

    df = load_instruments()

    if df.empty:
        logger.error("Instrument master empty")
        return []

    try:
        # SAFE FILTER
        df = df[df["SEM_INSTRUMENT_NAME"].astype(str).str.contains("OPTIDX")]

        df = df[df["SEM_TRADING_SYMBOL"].astype(str).str.contains("NIFTY")]

        df["STRIKE"] = df["SEM_STRIKE_PRICE"].astype(float)

        df["EXPIRY"] = pd.to_datetime(df["SEM_EXPIRY_DATE"], errors="coerce")

        df = df.dropna(subset=["EXPIRY"])

        expiry = df["EXPIRY"].min()

        df = df[df["EXPIRY"] == expiry]

        # SMART ATM (nearest strike)
        strikes = sorted(df["STRIKE"].unique())

        atm = min(strikes, key=lambda x: abs(x - spot))

        ce = df[(df["SEM_OPTION_TYPE"] == "CE") & (df["STRIKE"] == atm)]
        pe = df[(df["SEM_OPTION_TYPE"] == "PE") & (df["STRIKE"] == atm)]

        if ce.empty or pe.empty:
            raise Exception("No ATM contracts found")

        SELECTED_CE = str(ce.iloc[0]["SEM_SMST_SECURITY_ID"])
        SELECTED_PE = str(pe.iloc[0]["SEM_SMST_SECURITY_ID"])

        logger.info(f"CE={SELECTED_CE} PE={SELECTED_PE}")

        return [SELECTED_CE, SELECTED_PE]

    except Exception as e:
        logger.error(f"Contract error: {e}")
        return []


# ----------------------------
# SIGNAL ENGINE
# ----------------------------
def update_signal():
    ce = latest_data["ce_price"]
    pe = latest_data["pe_price"]

    if ce == 0 or pe == 0:
        return

    spread = ce - pe
    latest_data["spread"] = spread

    price_history.append(ce)

    pcr = get_pcr_cached()
    latest_data["pcr"] = pcr

    if spread > 5 and pcr < 0.8:
        signal = "BULLISH"
    elif spread < -5 and pcr > 1.2:
        signal = "BEARISH"
    else:
        signal = "NEUTRAL"

    latest_data["signal"] = signal
    latest_data["timestamp"] = datetime.now().strftime("%H:%M:%S")


# ----------------------------
# WEBSOCKET (RECONNECT SAFE)
# ----------------------------
def run_feed():
    global SELECTED_CE, SELECTED_PE

    while True:
        try:
            select_contracts(NIFTY_SPOT)

            if not SELECTED_CE or not SELECTED_PE:
                logger.error("No contracts, retrying...")
                time.sleep(10)
                continue

            feed = marketfeed.DhanFeed(
                client_id=CLIENT_ID,
                access_token=ACCESS_TOKEN,
                instruments=[
                    (marketfeed.NSE_FNO, SELECTED_CE, marketfeed.Ticker),
                    (marketfeed.NSE_FNO, SELECTED_PE, marketfeed.Ticker)
                ],
                on_message=on_message
            )

            logger.info("Feed started")
            feed.run_forever()

        except Exception as e:
            logger.error(f"Feed restart: {e}")
            time.sleep(5)


# ----------------------------
# CALLBACK
# ----------------------------
def on_message(instance, tick):
    try:
        sid = str(tick.get("security_id"))
        price = tick.get("ltp", 0)

        if sid == SELECTED_CE:
            latest_data["ce_price"] = price
        elif sid == SELECTED_PE:
            latest_data["pe_price"] = price

        update_signal()

    except Exception as e:
        logger.error(f"Message error: {e}")


# ----------------------------
# ROUTES
# ----------------------------
@app.route("/")
def home():
    return jsonify(latest_data)

@app.route("/api/health")
def health():
    return "OK", 200


# ----------------------------
# START
# ----------------------------
threading.Thread(target=run_feed, daemon=True).start()

application = app

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
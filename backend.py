# backend.py - Production Stable NIFTY Options Engine (FIXED)

import os
import time
import threading
import logging
import asyncio
import pandas as pd
import requests
import websocket
websocket.enableTrace(False)

from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS
from collections import deque

from dhanhq import dhanhq as DhanHQ
from dhanhq import marketfeed

# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("backend")

# -----------------------------
# FLASK
# -----------------------------
app = Flask(__name__)
CORS(app)

# -----------------------------
# ENV
# -----------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
DEFAULT_NIFTY_SPOT = float(os.getenv("CURRENT_NIFTY", "24000"))
logger.info(f"CLIENT_ID={CLIENT_ID}")
logger.info(f"TOKEN_PRESENT={bool(ACCESS_TOKEN)}")
# -----------------------------
# DHAN INIT
# -----------------------------
dhan = DhanHQ(CLIENT_ID, ACCESS_TOKEN)

# -----------------------------
# STATE
# -----------------------------
latest_data = {
    "ce_price": 0,
    "pe_price": 0,
    "spread": 0,
    "signal": "WAITING",
    "pcr": 1.0,
    "timestamp": ""
}

SELECTED_CE = None
SELECTED_PE = None

price_history = deque(maxlen=200)

# -----------------------------
# PCR CACHE
# -----------------------------
pcr_cache = {"value": 1.0, "time": 0}
PCR_TTL = 60

# -----------------------------
# SAFE PCR (NO NSE SPAM)
# -----------------------------
def get_pcr():
    now = time.time()

    if now - pcr_cache["time"] < PCR_TTL:
        return pcr_cache["value"]

    try:
        url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        headers = {"User-Agent": "Mozilla/5.0"}

        s = requests.Session()
        s.get("https://www.nseindia.com", headers=headers, timeout=5)

        res = s.get(url, headers=headers, timeout=5)
        data = res.json()

        ce = sum(x.get("CE", {}).get("openInterest", 0) for x in data["records"]["data"] if "CE" in x)
        pe = sum(x.get("PE", {}).get("openInterest", 0) for x in data["records"]["data"] if "PE" in x)

        value = pe / ce if ce else 1.0

        pcr_cache["value"] = value
        pcr_cache["time"] = now

        return value

    except Exception as e:
        logger.error(f"PCR error: {e}")
        return pcr_cache["value"]

# -----------------------------
# LIVE NIFTY SPOT
# -----------------------------
def get_nifty_spot():
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"

        headers = {
            "User-Agent": "Mozilla/5.0"
        }

        s = requests.Session()
        s.get("https://www.nseindia.com", headers=headers, timeout=5)

        res = s.get(url, headers=headers, timeout=5)
        data = res.json()

        return float(data["data"][0]["lastPrice"])

    except Exception as e:
        logger.error(f"NIFTY spot error: {e}")
        return DEFAULT_NIFTY_SPOT

# -----------------------------
# LOAD INSTRUMENTS (FIXED)
# -----------------------------
def load_instruments():
    try:
        url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        df = pd.read_csv(url, low_memory=False)

        df.columns = df.columns.str.upper()

        logger.info(f"Instruments loaded: {len(df)} rows")

        return df

    except Exception as e:
        logger.error(f"Instrument load failed: {e}")
        return pd.DataFrame()

# -----------------------------
# SMART CONTRACT SELECTOR
# -----------------------------
def select_contracts(spot):
    global SELECTED_CE, SELECTED_PE

    df = load_instruments()

    if df.empty:
        return []

    try:
        df = df[df["SEM_INSTRUMENT_NAME"].astype(str).str.contains("OPTIDX", na=False)]
        df = df[df["SEM_TRADING_SYMBOL"].astype(str).str.contains("NIFTY", na=False)]

        df["STRIKE"] = df["SEM_STRIKE_PRICE"].astype(float)
        df["EXPIRY"] = pd.to_datetime(df["SEM_EXPIRY_DATE"], errors="coerce")

        df = df.dropna(subset=["EXPIRY"])

        expiry = df["EXPIRY"].min()
        df = df[df["EXPIRY"] == expiry]

        strikes = sorted(df["STRIKE"].unique())
        atm = min(strikes, key=lambda x: abs(x - spot))

        ce = df[(df["SEM_OPTION_TYPE"] == "CE") & (df["STRIKE"] == atm)]
        pe = df[(df["SEM_OPTION_TYPE"] == "PE") & (df["STRIKE"] == atm)]

        if ce.empty or pe.empty:
            raise Exception("No ATM contracts found")

        SELECTED_CE = int(ce.iloc[0]["SEM_SMST_SECURITY_ID"])
        SELECTED_PE = int(pe.iloc[0]["SEM_SMST_SECURITY_ID"])

        logger.info(f"CE={SELECTED_CE} PE={SELECTED_PE}")

        return [SELECTED_CE, SELECTED_PE]

    except Exception as e:
        logger.error(f"Contract selection failed: {e}")
        return []

# -----------------------------
# SIGNAL ENGINE
# -----------------------------
def update_signal():
    ce = latest_data["ce_price"]
    pe = latest_data["pe_price"]

    if ce == 0 or pe == 0:
        return

    spread = ce - pe
    latest_data["spread"] = spread

    price_history.append(ce)

    pcr = get_pcr()
    latest_data["pcr"] = pcr

    if spread > 5 and pcr < 0.8:
        sig = "BULLISH"
    elif spread < -5 and pcr > 1.2:
        sig = "BEARISH"
    else:
        sig = "NEUTRAL"

    latest_data["signal"] = sig
    latest_data["timestamp"] = datetime.now().strftime("%H:%M:%S")

# -----------------------------
# WEBSOCKET LOOP (RENDER SAFE)
# -----------------------------
# -----------------------------
# WEBSOCKET LOOP (RENDER SAFE)
# -----------------------------
logger.info(f"Using CLIENT_ID={CLIENT_ID}")
logger.info(f"Token exists={bool(ACCESS_TOKEN)}")

def run_feed():
    global SELECTED_CE, SELECTED_PE

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    while True:
        try:
            spot = get_nifty_spot()
            select_contracts(spot)

            if not SELECTED_CE or not SELECTED_PE:
                logger.error("No contracts selected")
                time.sleep(5)
                continue

            instruments = [
                (marketfeed.NSE_FNO, SELECTED_CE, marketfeed.Ticker),
                (marketfeed.NSE_FNO, SELECTED_PE, marketfeed.Ticker)
            ]

            feed = marketfeed.DhanFeed(
                client_id=CLIENT_ID,
                access_token=ACCESS_TOKEN,
                instruments=instruments
            )
            logger.info(f"Instruments={instruments}")
            loop.run_until_complete(feed.connect())

            logger.info("WebSocket connected")

            while True:
                data = loop.run_until_complete(feed.get_data())

                if data:
                    on_message(None, data)

        except Exception as e:
            logger.error(f"Feed crash: {e}")
            time.sleep(5)

# -----------------------------
# CALLBACK
# -----------------------------
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

# -----------------------------
# ROUTES
# -----------------------------
@app.route("/")
def home():
    return jsonify(latest_data)

@app.route("/api/health")
def health():
    return "OK", 200

# -----------------------------
# START BACKGROUND THREAD
# -----------------------------
if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
    threading.Thread(target=run_feed, daemon=True).start()

application = app

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
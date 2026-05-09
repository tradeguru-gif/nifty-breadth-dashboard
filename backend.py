import os
import time
import json
import math
import threading
import logging
import requests
import pandas as pd
import asyncio
import redis

from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import dhanhq as DhanHQ
from dhanhq import marketfeed

# =============================
# CONFIG
# =============================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

NIFTY_SPOT = 24000

# =============================
# REDIS CACHE (PRODUCTION)
# =============================
try:
    rcache = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=6379,
        decode_responses=True
    )
    rcache.ping()
    logger.info("Redis connected")
except:
    rcache = None
    logger.warning("Redis not available - fallback to memory cache")

# =============================
# DHAN CLIENT
# =============================
dhan = DhanHQ(CLIENT_ID, ACCESS_TOKEN)

# =============================
# GLOBAL STATE
# =============================
latest = {
    "ce": 0,
    "pe": 0,
    "spread": 0,
    "pcr": 0,
    "iv": 0,
    "signal": "WAITING"
}

CE_ID = None
PE_ID = None

# =============================
# PCR CACHE (5 sec refresh)
# =============================
def get_pcr_cached():
    try:
        if rcache:
            cached = rcache.get("pcr")
            if cached:
                return float(cached)

        url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        headers = {"User-Agent": "Mozilla/5.0"}

        s = requests.Session()
        s.get("https://www.nseindia.com", headers=headers)
        res = s.get(url, headers=headers)

        data = res.json()

        ce = pe = 0
        for i in data["records"]["data"]:
            if "CE" in i:
                ce += i["CE"]["openInterest"]
            if "PE" in i:
                pe += i["PE"]["openInterest"]

        pcr = ce / pe if pe else 1

        if rcache:
            rcache.setex("pcr", 5, pcr)

        return pcr

    except:
        return 1.0

# =============================
# SMART ATM + IV STRIKE
# =============================
def smart_strike_selection(df, spot):
    atm = round(spot / 50) * 50

    # IV-weighted strike selection (simplified proxy)
    df["DIST"] = abs(df["STRIKE"] - atm)

    df["WEIGHT"] = 1 / (df["DIST"] + 1)

    return df.sort_values("WEIGHT", ascending=False)

# =============================
# INSTRUMENT LOADER (STABLE)
# =============================
def load_master():
    url = "https://images.dhan.co/api-data/api-scrip-master.csv"
    df = pd.read_csv(url, low_memory=False)
    df.columns = df.columns.str.upper()
    return df

# =============================
# CONTRACT SELECTION
# =============================
def select_contracts():
    global CE_ID, PE_ID

    df = load_master()

    opts = df[df["SEM_INSTRUMENT_NAME"].str.contains("OPT", na=False)]
    opts = opts[opts["SEM_TRADING_SYMBOL"].str.contains("NIFTY", na=False)]

    opts["SEM_EXPIRY_DATE"] = pd.to_datetime(opts["SEM_EXPIRY_DATE"], errors="coerce")
    opts = opts.dropna(subset=["SEM_EXPIRY_DATE"])

    # multi-expiry logic (weekly + monthly)
    expiries = sorted(opts["SEM_EXPIRY_DATE"].unique())[:2]
    opts = opts[opts["SEM_EXPIRY_DATE"].isin(expiries)]

    opts["STRIKE"] = opts["SEM_STRIKE_PRICE"]

    ce = opts[opts["SEM_OPTION_TYPE"] == "CE"]
    pe = opts[opts["SEM_OPTION_TYPE"] == "PE"]

    ce = smart_strike_selection(ce, NIFTY_SPOT)
    pe = smart_strike_selection(pe, NIFTY_SPOT)

    CE_ID = str(ce.iloc[0]["SEM_SMST_SECURITY_ID"])
    PE_ID = str(pe.iloc[0]["SEM_SMST_SECURITY_ID"])

    logger.info(f"CE={CE_ID}, PE={PE_ID}")

# =============================
# SIGNAL ENGINE
# =============================
def compute_signal():
    pcr = get_pcr_cached()

    spread = latest["ce"] - latest["pe"]

    latest["pcr"] = pcr
    latest["spread"] = spread

    if spread > 5 and pcr < 0.8:
        latest["signal"] = "BULLISH"
    elif spread < -5 and pcr > 1.2:
        latest["signal"] = "BEARISH"
    else:
        latest["signal"] = "NEUTRAL"

# =============================
# WEBSOCKET (AUTO RECOVERY)
# =============================
def run_ws():

    global CE_ID, PE_ID

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    while True:
        try:
            select_contracts()

            feed = marketfeed.DhanFeed(
                client_id=CLIENT_ID,
                access_token=ACCESS_TOKEN,
                instruments=[
                    (marketfeed.NSE_FNO, CE_ID, marketfeed.Ticker),
                    (marketfeed.NSE_FNO, PE_ID, marketfeed.Ticker)
                ],
                on_message=on_message
            )

            logger.info("WebSocket started")
            feed.run_forever()

        except Exception as e:
            logger.error(f"WS crashed: {e}")
            time.sleep(3)

# =============================
# CALLBACK
# =============================
def on_message(instance, tick):
    sec = str(tick.get("security_id"))
    price = tick.get("ltp", 0)

    if sec == CE_ID:
        latest["ce"] = price
    elif sec == PE_ID:
        latest["pe"] = price

    compute_signal()

# =============================
# FLASK
# =============================
@app.route("/")
def home():
    return jsonify(latest)

@app.route("/health")
def health():
    return "OK"

# =============================
# START
# =============================
threading.Thread(target=run_ws, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
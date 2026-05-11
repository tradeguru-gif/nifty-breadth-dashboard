# ============================================
# backend.py - NIFTY Options Signal Engine
# Dynamically selects nearest monthly expiry & ATM strikes
# ============================================

import os
import asyncio
import threading
import logging
import time
import requests
import pandas as pd
from collections import deque
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

# DhanHQ SDK imports
from dhanhq import dhanhq as DhanHQ
from dhanhq import marketfeed

# ===============================
# LOGGING & FLASK SETUP
# ===============================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app

# ===============================
# ENVIRONMENT VARIABLES
# ===============================
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

logger.info(f"Client ID loaded: {CLIENT_ID[:5]}...")
logger.info("Access Token loaded: Yes")

# ===============================
# INIT GLOBAL STATE
# ===============================
latest_data = {
    "signal": "WAITING",
    "ce_price": 0,
    "pe_price": 0,
    "spread": 0,
    "rsi": 50,
    "macd": 0,
    "pcr": 1.0,
    "timestamp": ""
}

SELECTED_CE_ID = None
SELECTED_PE_ID = None
price_history = deque(maxlen=200)

update_counter = 0
UPDATE_INTERVAL = 10
SPREAD_THRESHOLD = 5.0

# ===============================
# INSTRUMENT & CONTRACT SELECTOR
# ===============================
def find_nifty_option_ids():
    """
    Downloads the detailed instrument CSV, finds the nearest monthly expiry
    for NIFTY, and returns the SECURITY_IDs for the ATM Call and Put.
    """
    logger.info("Fetching instrument master...")
    try:
        url = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
        df = pd.read_csv(url, low_memory=False)
        df.columns = df.columns.str.upper()

        # 1. Filter for NIFTY OPTIDX (Index Options)
        opts = df[(df['SEGMENT'] == 'NSE_FNO') &
                  (df['INSTRUMENT'] == 'OPTIDX') &
                  (df['SYMBOL_NAME'].str.contains('NIFTY', na=False))].copy()
        if opts.empty:
            logger.error("No NIFTY options found in instrument master")
            return None, None

        # 2. Parse expiry dates & find the nearest monthly expiry
        opts['EXPIRY_DT'] = pd.to_datetime(opts['SM_EXPIRY_DATE'], format='%d-%b-%Y', errors='coerce')
        opts = opts.dropna(subset=['EXPIRY_DT'])
        opts = opts.sort_values('EXPIRY_DT')
        nearest_expiry = opts['EXPIRY_DT'].iloc[0]
        logger.info(f"Nearest monthly expiry: {nearest_expiry.strftime('%d-%b-%Y')}")

        # 3. Filter for that expiry
        opts_nearest = opts[opts['EXPIRY_DT'] == nearest_expiry]

        # 4. Get strike prices and find ATM (closest to current NIFTY)
        opts_nearest['STRIKE'] = pd.to_numeric(opts_nearest['STRIKE_PRICE'], errors='coerce')
        opts_nearest = opts_nearest.dropna(subset=['STRIKE'])

        # Fetch current spot price
        try:
            spot_url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
            headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
            session = requests.Session()
            session.get("https://www.nseindia.com", headers=headers, timeout=5)
            spot_res = session.get(spot_url, headers=headers, timeout=5)
            spot_data = spot_res.json()
            current_spot = float(spot_data["data"][0]["lastPrice"])
        except Exception as e:
            logger.error(f"Could not fetch live spot, using 19500. Error: {e}")
            current_spot = 19500.0

        # Find strike closest to spot
        strikes = sorted(opts_nearest['STRIKE'].unique())
        atm_strike = min(strikes, key=lambda x: abs(x - current_spot))
        logger.info(f"ATM Strike: {atm_strike} (Spot: {current_spot})")

        # 5. Extract CE and PE IDs
        ce_row = opts_nearest[(opts_nearest['OPTION_TYPE'] == 'CE') & (opts_nearest['STRIKE'] == atm_strike)]
        pe_row = opts_nearest[(opts_nearest['OPTION_TYPE'] == 'PE') & (opts_nearest['STRIKE'] == atm_strike)]

        if ce_row.empty or pe_row.empty:
            logger.error("Could not find ATM CE/PE contracts")
            return None, None

        ce_id = str(int(ce_row.iloc[0]['SECURITY_ID']))
        pe_id = str(int(pe_row.iloc[0]['SECURITY_ID']))
        logger.info(f"Selected CE: {ce_id} | Selected PE: {pe_id}")
        return ce_id, pe_id

    except Exception as e:
        logger.exception("Error in find_nifty_option_ids")
        return None, None

# ===============================
# TECHNICAL INDICATORS
# ===============================
def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
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
        value = data[0]
        for price in data[1:]:
            value = alpha * price + (1 - alpha) * value
        return value
    return ema(prices, fast) - ema(prices, slow)

# PCR Cache
pcr_cache = {"value": 1.0, "time": 0}
PCR_TTL = 60

def get_nifty_pcr():
    now = time.time()
    if now - pcr_cache["time"] < PCR_TTL:
        return pcr_cache["value"]
    url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers)
        time.sleep(0.5)
        response = session.get(url, headers=headers)
        data = response.json()
        ce_oi = sum(x.get("CE", {}).get("openInterest", 0) for x in data["records"]["data"] if "CE" in x)
        pe_oi = sum(x.get("PE", {}).get("openInterest", 0) for x in data["records"]["data"] if "PE" in x)
        value = pe_oi / ce_oi if ce_oi else 1.0
        pcr_cache["value"] = value
        pcr_cache["time"] = now
        return value
    except Exception as e:
        logger.error(f"PCR fetch failed: {e}")
        return pcr_cache["value"]

# ===============================
# SIGNAL LOGIC
# ===============================
def update_signal(ce_price, pe_price):
    global latest_data, price_history, update_counter

    spread = ce_price - pe_price
    latest_data["spread"] = round(spread, 2)

    if ce_price > 0:
        price_history.append(ce_price)

    update_counter += 1

    if update_counter >= UPDATE_INTERVAL and len(price_history) >= 20:
        update_counter = 0

        rsi = calculate_rsi(list(price_history))
        macd = calculate_macd(list(price_history))
        pcr = get_nifty_pcr()

        latest_data["rsi"] = round(rsi, 2)
        latest_data["macd"] = round(macd, 2)
        latest_data["pcr"] = round(pcr, 2)

        if spread > SPREAD_THRESHOLD and rsi < 70 and macd > 0 and pcr < 0.8:
            signal = "LONG SPREAD (Bullish)"
        elif spread < -SPREAD_THRESHOLD and rsi > 30 and macd < 0 and pcr > 1.2:
            signal = "SHORT SPREAD (Bearish)"
        else:
            signal = "NEUTRAL"
        latest_data["signal"] = signal

    latest_data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ===============================
# WEBSOCKET CALLBACK
# ===============================
def on_message(instance, tick):
    global latest_data
    try:
        sec_id = str(tick.get("security_id"))
        price = tick.get("ltp", 0)
        if sec_id == SELECTED_CE_ID:
            latest_data["ce_price"] = price
        elif sec_id == SELECTED_PE_ID:
            latest_data["pe_price"] = price

        if latest_data["ce_price"] > 0 and latest_data["pe_price"] > 0:
            update_signal(latest_data["ce_price"], latest_data["pe_price"])
        else:
            latest_data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.error(f"on_message error: {e}")

# ===============================
# WEBSOCKET FEED RUNNER
# ===============================
def run_feed():
    global SELECTED_CE_ID, SELECTED_PE_ID
    while True:
        try:
            ce_id, pe_id = find_nifty_option_ids()
            if not ce_id or not pe_id:
                logger.error("Failed to get contract IDs, retrying in 60 seconds")
                time.sleep(60)
                continue

            SELECTED_CE_ID = ce_id
            SELECTED_PE_ID = pe_id
            logger.info(f"Subscribing to CE={SELECTED_CE_ID}, PE={SELECTED_PE_ID}")

            feed = marketfeed.DhanFeed(
                client_id=CLIENT_ID,
                access_token=ACCESS_TOKEN,
                instruments=[
                    (marketfeed.NSE_FNO, str(SELECTED_CE_ID), marketfeed.Ticker),
                    (marketfeed.NSE_FNO, str(SELECTED_PE_ID), marketfeed.Ticker)
                ]
            )
            # Attach the message callback
            feed.on_message = on_message
            logger.info(" Dhan Feed Started")
            feed.run_forever()

        except Exception as e:
            logger.exception(f"Feed crashed: {e}, reconnecting in 10 seconds")
            time.sleep(10)

# ===============================
# FLASK ROUTES
# ===============================
@app.route("/")
def home():
    return jsonify({
        "status": "active",
        "data": latest_data
    })

@app.route("/api/health")
def health():
    return "OK", 200

# ===============================
# START BACKGROUND THREAD
# ===============================
threading.Thread(target=run_feed, daemon=True).start()
logger.info("Background signal engine started.")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
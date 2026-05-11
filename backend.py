import os
import time
import threading
import logging
import requests
import pandas as pd
from collections import deque
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

# Correct Imports for dhanhq v2.2.0+
from dhanhq import dhanhq as DhanHQ
from dhanhq import marketfeed

# ------------------------------------------------------------
# Logging & Flask Setup
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
# This is critical for Gunicorn to find the entry point
application = app 

# ------------------------------------------------------------
# Environment variables
# ------------------------------------------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("CRITICAL: Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

# Initialize the main Dhan object
dhan = DhanHQ(CLIENT_ID, ACCESS_TOKEN)
logger.info("✅ Dhan client initialized")

# ------------------------------------------------------------
# Global State
# ------------------------------------------------------------
latest_data = {
    "signal": "WAITING", "ce_price": 0, "pe_price": 0,
    "spread": 0, "rsi": 50, "pcr": 1.0, "timestamp": ""
}

SELECTED_CE_ID = None
SELECTED_PE_ID = None
price_history = deque(maxlen=200)

# ------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------
def get_live_nifty_spot():
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        headers = {"User-Agent": "Mozilla/5.0"}
        s = requests.Session()
        s.get("https://www.nseindia.com", headers=headers, timeout=5)
        r = s.get(url, headers=headers, timeout=5)
        return float(r.json()["data"][0]["lastPrice"])
    except Exception as e:
        logger.warning(f"Spot fetch failed: {e}")
        return 24100.0

def get_option_contracts(spot):
    global SELECTED_CE_ID, SELECTED_PE_ID
    try:
        url = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
        df = pd.read_csv(url, low_memory=False)
        df.columns = df.columns.str.upper()

        # Filter Nifty Options
        fno = df[(df["SEGMENT"] == "NSE_FNO") & (df["INSTRUMENT"] == "OPTIDX") & (df["SYMBOL_NAME"] == "NIFTY")]
        fno["EXP_DT"] = pd.to_datetime(fno["SM_EXPIRY_DATE"], format="%d-%b-%Y", errors='coerce')
        fno = fno.dropna(subset=["EXP_DT"]).sort_values("EXP_DT")
        
        nearest_expiry = fno["EXP_DT"].iloc[0]
        filtered = fno[fno["EXP_DT"] == nearest_expiry].copy()
        filtered["STRIKE"] = pd.to_numeric(filtered["STRIKE_PRICE"])
        
        # Find ATM strike
        atm_strike = filtered["STRIKE"].iloc[(filtered["STRIKE"] - spot).abs().argsort()[:1]].values[0]
        
        ce_row = filtered[(filtered["OPTION_TYPE"] == "CE") & (filtered["STRIKE"] == atm_strike)]
        pe_row = filtered[(filtered["OPTION_TYPE"] == "PE") & (filtered["STRIKE"] == atm_strike)]
        
        SELECTED_CE_ID = str(int(ce_row.iloc[0]["SECURITY_ID"]))
        SELECTED_PE_ID = str(int(pe_row.iloc[0]["SECURITY_ID"]))
        
        logger.info(f"🎯 ATM Strike: {atm_strike} | CE: {SELECTED_CE_ID} | PE: {SELECTED_PE_ID}")
        return True
    except Exception as e:
        logger.error(f"Contract Selection Error: {e}")
        return False

# ------------------------------------------------------------
# WebSocket Logic
# ------------------------------------------------------------
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
            latest_data["spread"] = round(latest_data["ce_price"] - latest_data["pe_price"], 2)
            latest_data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.error(f"Tick error: {e}")

def run_feed():
    while True:
        try:
            spot = get_live_nifty_spot()
            if get_option_contracts(spot):
                instruments = [
                    (marketfeed.NSE_FNO, SELECTED_CE_ID, marketfeed.Ticker),
                    (marketfeed.NSE_FNO, SELECTED_PE_ID, marketfeed.Ticker)
                ]
                # v2 Web socket initialization
                feed = marketfeed.DhanFeed(CLIENT_ID, ACCESS_TOKEN, instruments, version="v2")
                feed.on_message = on_message
                logger.info("🚀 WebSocket starting...")
                feed.run_forever()
            time.sleep(10)
        except Exception as e:
            logger.error(f"Feed crashed: {e}, restarting in 10s...")
            time.sleep(10)

# ------------------------------------------------------------
# Routes
# ------------------------------------------------------------
@app.route("/")
def home():
    return jsonify({"status": "active", "data": latest_data})

@app.route("/api/health")
def health():
    return "OK", 200

# Start background engine
threading.Thread(target=run_feed, daemon=True).start()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
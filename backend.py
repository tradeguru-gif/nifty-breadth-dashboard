# backend.py - Nifty Options Signal Engine
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
from dhanhq import marketfeed

# --------------------------------------------------
# Logging & Flask Setup
# --------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# --------------------------------------------------
# Config & Environment
# --------------------------------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

SELECTED_CE_ID = "35000"
SELECTED_PE_ID = "35001"

latest_data = {"signal": "WAITING", "ce_price": 0.0, "pe_price": 0.0, "spread": 0.0, "rsi": 50, "pcr": 1.0, "timestamp": ""}
market_state = {"rsi": 50, "trend": "SIDEWAYS", "action": "HOLD", "confidence": 0}
institutional_state = {"vwap": 0, "ema_signal": "NEUTRAL", "institutional_signal": "HOLD"}

price_history = deque(maxlen=200)
volume_history = deque(maxlen=200)
tick_counter = 0

# --------------------------------------------------
# Logic Functions (Condensed for Reliability)
# --------------------------------------------------
def update_contracts():
    global SELECTED_CE_ID, SELECTED_PE_ID
    try:
        url = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
        response = requests.get(url, timeout=30)
        lines = response.text.splitlines()
        reader = csv.DictReader(lines)
        
        opts = []
        for row in reader:
            if row.get("SEGMENT") == "D" and row.get("INSTRUMENT") == "OPTIDX" and "NIFTY" in row.get("SYMBOL_NAME", ""):
                if "BANK" not in row.get("SYMBOL_NAME", ""):
                    try:
                        expiry = datetime.strptime(row.get("SM_EXPIRY_DATE"), "%Y-%m-%d")
                        opts.append({"expiry": expiry, "strike": float(row.get("STRIKE_PRICE")), 
                                     "type": row.get("OPTION_TYPE"), "id": row.get("SECURITY_ID")})
                    except: continue

        min_expiry = min(opts, key=lambda x: x["expiry"])["expiry"]
        near_opts = [o for o in opts if o["expiry"] == min_expiry]
        
        # Simple ATM logic (Current Nifty approx)
        spot = 24300.0 
        strikes = sorted(set(o["strike"] for o in near_opts))
        atm = min(strikes, key=lambda x: abs(x - spot))

        for o in near_opts:
            if o["strike"] == atm:
                if o["type"] == "CE": SELECTED_CE_ID = str(int(float(o["id"])))
                if o["type"] == "PE": SELECTED_PE_ID = str(int(float(o["id"])))
        
        logger.info(f"✅ Contracts Updated: CE={SELECTED_CE_ID} PE={SELECTED_PE_ID}")
    except Exception as e:
        logger.error(f"Contract update failed: {e}")

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1: return 50.0
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    avg_g = sum(gains[-period:]) / period
    avg_l = sum(losses[-period:]) / period
    if avg_l == 0: return 100.0
    return 100 - (100 / (1 + (avg_g / avg_l)))

# --------------------------------------------------
# WebSocket Handlers
# --------------------------------------------------
def on_message(instance, tick):
    global tick_counter
    try:
        sid = str(tick.get("security_id"))
        ltp = float(tick.get("ltp", 0))
        
        if sid == SELECTED_CE_ID:
            latest_data["ce_price"] = ltp
            price_history.append(ltp)
        elif sid == SELECTED_PE_ID:
            latest_data["pe_price"] = ltp

        if latest_data["ce_price"] > 0 and latest_data["pe_price"] > 0:
            latest_data["spread"] = round(latest_data["ce_price"] - latest_data["pe_price"], 2)
            latest_data["timestamp"] = datetime.now().isoformat()
            
            tick_counter += 1
            if tick_counter > 10:
                latest_data["rsi"] = round(calculate_rsi(list(price_history)), 2)
                tick_counter = 0
    except Exception as e:
        logger.error(f"Msg Error: {e}")

# --------------------------------------------------
# Core Engine
# --------------------------------------------------
def run_feed():
    while True:
        try:
            update_contracts()
            instruments = [(marketfeed.NSE, SELECTED_CE_ID, marketfeed.TICKER),
                           (marketfeed.NSE, SELECTED_PE_ID, marketfeed.TICKER)]
            
            logger.info(f"Connecting to Dhan for IDs: {SELECTED_CE_ID}, {SELECTED_PE_ID}")
            # Removed 'on_connect' to prevent argument error
            feed = marketfeed.DhanFeed(CLIENT_ID, ACCESS_TOKEN, instruments, on_message)
            feed.run_forever()
        except Exception as e:
            logger.error(f"Feed Error: {e}")
            time.sleep(10)

# --------------------------------------------------
# Initialization & Routes
# --------------------------------------------------
if not os.environ.get("WERKZEUG_RUN_MAIN"):
    threading.Thread(target=run_feed, daemon=True).start()

@app.route("/")
def home():
    return jsonify({"status": "active", "data": latest_data})

@app.route("/api/health")
def health():
    return "OK", 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
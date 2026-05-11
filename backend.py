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

# ------------------------------------------------------------
# Environment variables
# ------------------------------------------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

# Initialize the main Dhan object (New Syntax)
dhan = DhanHQ(CLIENT_ID, ACCESS_TOKEN)
logger.info("Dhan client initialized successfully")

# ------------------------------------------------------------
# Global State Management
# ------------------------------------------------------------
latest_data = {
    "signal": "WAITING", "ce_price": 0, "pe_price": 0,
    "spread": 0, "rsi": 50, "macd": 0, "pcr": 1.0, "timestamp": ""
}

market_state = {
    "rsi": 50, "momentum": "NEUTRAL", "strength": "LOW",
    "trend": "SIDEWAYS", "action": "HOLD", "confidence": 0,
    "volatility": "NORMAL", "alert": "NONE"
}

institutional_state = {
    "vwap": 0, "ema_fast": 0, "ema_slow": 0, "ema_signal": "NEUTRAL",
    "atr": 0, "oi_buildup": "NEUTRAL", "iv_state": "NORMAL",
    "candle_structure": "SIDEWAYS", "market_breadth": "BALANCED",
    "volume_profile": "NORMAL", "smart_money_flow": "NEUTRAL",
    "delta": 0, "gamma": 0, "theta": 0, "vega": 0,
    "institutional_signal": "HOLD", "institutional_confidence": 0
}

SELECTED_CE_ID = None
SELECTED_PE_ID = None
price_history = deque(maxlen=200)
update_counter = 0
UPDATE_INTERVAL = 10
SPREAD_THRESHOLD = 5.0

# ------------------------------------------------------------
# Technical Indicators (Consolidated)
# ------------------------------------------------------------
def calculate_rsi(prices, period=14):
    if len(prices) < period + 1: return 50.0
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    if avg_loss == 0: return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calculate_macd(prices, fast=12, slow=26):
    if len(prices) < slow: return 0.0
    def ema(data, period):
        alpha = 2 / (period + 1)
        value = data[0]
        for p in data[1:]: value = alpha * p + (1 - alpha) * value
        return value
    return ema(prices, fast) - ema(prices, slow)

def calculate_ema(prices, period):
    if len(prices) < period: return prices[-1] if prices else 0
    alpha = 2 / (period + 1)
    e_val = prices[0]
    for p in prices[1:]: e_val = alpha * p + (1 - alpha) * e_val
    return round(e_val, 2)

def calculate_atr(prices, period=14):
    if len(prices) < period + 1: return 0
    trs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    return round(sum(trs[-period:]) / period, 2)

def calculate_vwap(prices):
    if not prices: return 0
    vol = [100] * len(prices)
    pv = sum(p * v for p, v in zip(prices, vol))
    tv = sum(vol)
    return round(pv / tv, 2) if tv else 0

def estimate_greeks(ce, pe):
    delta = round((ce - pe) / 100, 2)
    gamma = round(abs(delta) / 10, 2)
    theta = round(-(ce + pe) / 1000, 2)
    vega = round((ce + pe) / 500, 2)
    return delta, gamma, theta, vega

# ------------------------------------------------------------
# Data Fetching (NSE Live)
# ------------------------------------------------------------
def get_live_nifty_spot():
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        s = requests.Session()
        s.get("https://www.nseindia.com", headers=headers, timeout=5)
        r = s.get(url, headers=headers, timeout=5)
        return float(r.json()["data"][0]["lastPrice"])
    except: return 24100.0 # Safety fallback to recent levels

def get_nifty_pcr():
    url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        s = requests.Session()
        s.get("https://www.nseindia.com", headers=headers)
        r = s.get(url, headers=headers)
        data = r.json()
        ce_oi = sum(x.get("CE", {}).get("openInterest", 0) for x in data["records"]["data"] if "CE" in x)
        pe_oi = sum(x.get("PE", {}).get("openInterest", 0) for x in data["records"]["data"] if "PE" in x)
        return pe_oi / ce_oi if ce_oi else 1.0
    except: return 1.0

# ------------------------------------------------------------
# Contract Selection Logic
# ------------------------------------------------------------
def get_weekly_option_contracts(spot):
    global SELECTED_CE_ID, SELECTED_PE_ID
    try:
        url = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
        df = pd.read_csv(url, low_memory=False)
        df.columns = df.columns.str.upper()

        # Filter for Nifty Options
        fno = df[(df["SEGMENT"] == "NSE_FNO") & (df["INSTRUMENT"] == "OPTIDX") & (df["SYMBOL_NAME"] == "NIFTY")]
        
        # Determine nearest expiry
        fno["EXP_DT"] = pd.to_datetime(fno["SM_EXPIRY_DATE"], format="%d-%b-%Y", errors='coerce')
        fno = fno.dropna(subset=["EXP_DT"]).sort_values("EXP_DT")
        nearest_expiry = fno["EXP_DT"].iloc[0]
        
        filtered = fno[fno["EXP_DT"] == nearest_expiry].copy()
        filtered["STRIKE"] = pd.to_numeric(filtered["STRIKE_PRICE"])
        
        # Find ATM strike
        atm_strike = filtered["STRIKE"].iloc[(filtered["STRIKE"] - spot).abs().argsort()[:1]].values[0]
        
        ce_id = filtered[(filtered["OPTION_TYPE"] == "CE") & (filtered["STRIKE"] == atm_strike)]["SECURITY_ID"].iloc[0]
        pe_id = filtered[(filtered["OPTION_TYPE"] == "PE") & (filtered["STRIKE"] == atm_strike)]["SECURITY_ID"].iloc[0]
        
        SELECTED_CE_ID = str(int(ce_id))
        SELECTED_PE_ID = str(int(pe_id))
        logger.info(f"Selected ATM {atm_strike} | CE: {SELECTED_CE_ID} | PE: {SELECTED_PE_ID}")
        return True
    except Exception as e:
        logger.error(f"Contract Selection Error: {e}")
        return False

# ------------------------------------------------------------
# Signal Engine & WebSocket
# ------------------------------------------------------------
def on_message(instance, tick):
    global latest_data
    try:
        sec_id = str(tick.get("security_id"))
        price = tick.get("ltp", 0)
        if sec_id == SELECTED_CE_ID: latest_data["ce_price"] = price
        elif sec_id == SELECTED_PE_ID: latest_data["pe_price"] = price

        if latest_data["ce_price"] > 0 and latest_data["pe_price"] > 0:
            latest_data["spread"] = round(latest_data["ce_price"] - latest_data["pe_price"], 2)
            latest_data["timestamp"] = datetime.now().strftime("%H:%M:%S")
    except Exception as e: logger.error(f"Feed error: {e}")

def run_feed():
    while True:
        try:
            spot = get_live_nifty_spot()
            if get_weekly_option_contracts(spot):
                # New Dhan v2 WebSocket setup
                instruments = [
                    (marketfeed.NSE_FNO, SELECTED_CE_ID, marketfeed.Ticker),
                    (marketfeed.NSE_FNO, SELECTED_PE_ID, marketfeed.Ticker)
                ]
                feed = marketfeed.DhanFeed(CLIENT_ID, ACCESS_TOKEN, instruments, version="v2")
                feed.on_message = on_message
                feed.run_forever()
            time.sleep(10)
        except Exception as e:
            logger.error(f"Restarting feed: {e}")
            time.sleep(10)

# ------------------------------------------------------------
# API Endpoints
# ------------------------------------------------------------
@app.route("/")
def home():
    return jsonify({"status": "running", "data": latest_data})

threading.Thread(target=run_feed, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
# backend.py - Nifty Options Signal Engine (Corrected Expiry Column)

import os
import threading
import logging
import time
import requests
from collections import deque
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

from dhanhq import dhanhq, marketfeed

# ------------------------------------------------------------
# Helper functions (RSI, MACD, PCR)
# ------------------------------------------------------------
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

def calculate_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal:
        return 0.0
    def ema(data, period):
        alpha = 2 / (period + 1)
        result = data[0]
        for price in data[1:]:
            result = alpha * price + (1 - alpha) * result
        return result
    ema_fast = ema(prices, fast)
    ema_slow = ema(prices, slow)
    return ema_fast - ema_slow

def get_nifty_pcr():
    url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers)
        time.sleep(0.5)
        response = session.get(url, headers=headers)
        data = response.json()
        total_ce_oi = 0
        total_pe_oi = 0
        for record in data['records']['data']:
            if 'CE' in record:
                total_ce_oi += record['CE']['openInterest']
            if 'PE' in record:
                total_pe_oi += record['PE']['openInterest']
        return total_ce_oi / total_pe_oi if total_pe_oi > 0 else 1.0
    except Exception as e:
        logging.error(f"PCR fetch failed: {e}")
        return None

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
CURRENT_NIFTY = int(os.getenv("CURRENT_NIFTY", "24000"))

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
SELECTED_CE_ID = ""
SELECTED_PE_ID = ""

price_history = deque(maxlen=100)
update_counter = 0
UPDATE_INTERVAL = 10
SPREAD_THRESHOLD = 5.0

# ------------------------------------------------------------
# Dynamic Option Contract Selection (Correct column: SM_EXPIRY_DATE)
# ------------------------------------------------------------
def get_option_contracts(nifty_spot):
    global SELECTED_CE_ID, SELECTED_PE_ID
    try:
        import pandas as pd
        dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
        logger.info("Fetching instrument master...")
        df = dhan.fetch_security_list("detailed")
        fno = df[df["SEGMENT"] == "NSE_FNO"]
        opts = fno[fno["INSTRUMENT"] == "OPTIDX"].copy()
        # Use the correct expiry column name
        expiry_col = "SM_EXPIRY_DATE"
        if expiry_col not in opts.columns:
            # Fallback: try to find any column with 'EXPIRY'
            for col in opts.columns:
                if 'EXPIRY' in col.upper():
                    expiry_col = col
                    break
            else:
                raise KeyError(f"No expiry column found. Columns: {opts.columns.tolist()}")
        logger.info(f"Using expiry column: {expiry_col}")
        opts["EXPIRY_DT"] = pd.to_datetime(opts[expiry_col], format="%d-%b-%Y", errors="coerce")
        opts = opts.dropna(subset=["EXPIRY_DT"])
        opts = opts.sort_values("EXPIRY_DT")
        nearest_expiry = opts["EXPIRY_DT"].iloc[0]
        opts_nearest = opts[opts["EXPIRY_DT"] == nearest_expiry]
        opts_nearest["STRIKE"] = pd.to_numeric(opts_nearest["STRIKE_PRICE"], errors="coerce")
        opts_nearest = opts_nearest.dropna(subset=["STRIKE"])
        calls = opts_nearest[opts_nearest["OPTION_TYPE"] == "CE"]
        puts = opts_nearest[opts_nearest["OPTION_TYPE"] == "PE"]
        # Find call above spot
        calls_above = calls[calls["STRIKE"] >= nifty_spot]
        if not calls_above.empty:
            call_contract = calls_above.sort_values("STRIKE").iloc[0]
        else:
            call_contract = calls.iloc[(calls["STRIKE"] - nifty_spot).abs().argmin()]
        # Find put below spot
        puts_below = puts[puts["STRIKE"] <= nifty_spot]
        if not puts_below.empty:
            put_contract = puts_below.sort_values("STRIKE", ascending=False).iloc[0]
        else:
            put_contract = puts.iloc[(puts["STRIKE"] - nifty_spot).abs().argmin()]
        SELECTED_CE_ID = str(call_contract["SECURITY_ID"])
        SELECTED_PE_ID = str(put_contract["SECURITY_ID"])
        logger.info(f"Selected CE ID: {SELECTED_CE_ID} (Strike: {call_contract['STRIKE']})")
        logger.info(f"Selected PE ID: {SELECTED_PE_ID} (Strike: {put_contract['STRIKE']})")
        return [SELECTED_CE_ID, SELECTED_PE_ID]
    except Exception as e:
        logger.error(f"Dynamic selection failed: {e}")
        return []

# ------------------------------------------------------------
# Signal update logic
# ------------------------------------------------------------
def update_signal(ce_price, pe_price):
    global latest_data, price_history, update_counter
    spread = ce_price - pe_price
    latest_data["spread"] = spread
    if ce_price > 0:
        price_history.append(ce_price)
    update_counter += 1
    if update_counter >= UPDATE_INTERVAL and len(price_history) >= 20:
        update_counter = 0
        rsi = calculate_rsi(list(price_history))
        macd_hist = calculate_macd(list(price_history))
        pcr = get_nifty_pcr()
        if pcr is None:
            pcr = latest_data.get("pcr", 1.0)
        latest_data["rsi"] = round(rsi, 2)
        latest_data["macd"] = round(macd_hist, 2)
        latest_data["pcr"] = round(pcr, 2)
        if spread > SPREAD_THRESHOLD and rsi < 70 and macd_hist > 0 and pcr < 0.8:
            signal = "LONG SPREAD (Bullish)"
        elif spread < -SPREAD_THRESHOLD and rsi > 30 and macd_hist < 0 and pcr > 1.2:
            signal = "SHORT SPREAD (Bearish)"
        else:
            signal = "NEUTRAL"
        latest_data["signal"] = signal
    latest_data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ------------------------------------------------------------
# WebSocket callback
# ------------------------------------------------------------
def on_message(instance, tick):
    global latest_data, SELECTED_CE_ID, SELECTED_PE_ID
    try:
        sec_id = str(tick.get('security_id'))
        price = tick.get('ltp', 0)
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

# ------------------------------------------------------------
# WebSocket feed runner (auto-reconnect)
# ------------------------------------------------------------
def run_feed():
    global SELECTED_CE_ID, SELECTED_PE_ID
    while True:
        try:
            get_option_contracts(CURRENT_NIFTY)
            if not SELECTED_CE_ID or not SELECTED_PE_ID:
                logger.error("No valid security IDs. Retrying in 30 seconds...")
                time.sleep(30)
                continue
            logger.info(f"Subscribing to CE: {SELECTED_CE_ID}, PE: {SELECTED_PE_ID}")
            feed = marketfeed.DhanFeed(
                client_id=CLIENT_ID,
                access_token=ACCESS_TOKEN,
                instruments=[
                    (marketfeed.NSE_FNO, SELECTED_CE_ID, marketfeed.Ticker),
                    (marketfeed.NSE_FNO, SELECTED_PE_ID, marketfeed.Ticker)
                ],
                on_message=on_message,
                on_connect=lambda instance: logger.info("✅ Connected to Dhan WebSocket"),
                on_error=lambda instance, err: logger.error(f"WebSocket error: {err}"),
                on_close=lambda instance: logger.warning("WebSocket closed. Reconnecting...")
            )
            feed.run_forever()
        except Exception as e:
            logger.error(f"Feed crashed: {e}. Reconnecting in 10 seconds...")
            time.sleep(10)

# ------------------------------------------------------------
# Flask endpoints
# ------------------------------------------------------------
@app.route('/')
def home():
    return jsonify({"status": "active", "data": latest_data})

@app.route('/api/health')
def health():
    return "OK", 200

application = app

# ------------------------------------------------------------
# Start background thread
# ------------------------------------------------------------
t = threading.Thread(target=run_feed, daemon=True)
t.start()
logger.info("Background signal engine thread started.")

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))
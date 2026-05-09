import os
import threading
import logging
import time
import requests
from collections import deque
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

# Dhan SDK imports (modern way for v2.1.0)
from dhanhq import dhanhq as DhanHQ
from dhanhq import marketfeed

# ------------------------------------------------------------
# Helper functions for technical indicators
# ------------------------------------------------------------
def calculate_rsi(prices, period=14):
    """Calculate RSI from a list of prices."""
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
    """Return the last MACD histogram value."""
    if len(prices) < slow + signal:
        return 0.0
    # Simple EMA calculation (or you can use pandas if installed)
    def ema(data, period):
        alpha = 2 / (period + 1)
        result = data[0]
        for price in data[1:]:
            result = alpha * price + (1 - alpha) * result
        return result
    ema_fast = ema(prices, fast)
    ema_slow = ema(prices, slow)
    macd_line = ema_fast - ema_slow
    # For histogram, we'd need signal line EMA, but we use macd_line as proxy
    return macd_line

def get_nifty_pcr():
    """Fetch Nifty Put/Call Ratio from NSE India (free, no API key)."""
    url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    try:
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers)
        time.sleep(0.5)  # brief pause to mimic browser
        response = session.get(url, headers=headers)
        data = response.json()
        total_ce_oi = 0
        total_pe_oi = 0
        for record in data['records']['data']:
            if 'CE' in record:
                total_ce_oi += record['CE']['openInterest']
            if 'PE' in record:
                total_pe_oi += record['PE']['openInterest']
        if total_pe_oi > 0:
            pcr = total_ce_oi / total_pe_oi
        else:
            pcr = 1.0
        return pcr
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

# Signal & data storage
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

# Price history for RSI/MACD (using CE prices; you could also use spread)
price_history = deque(maxlen=100)   # store last 100 CE prices
# Variables for periodic updates (every 10 ticks or 5 seconds)
update_counter = 0
UPDATE_INTERVAL = 10   # compute signals every 10 ticks
THRESHOLD = 5.0        # spread threshold (adjust as needed)

# ------------------------------------------------------------
# Dynamic contract selection (hardcoded example – replace with real fetch)
# ------------------------------------------------------------
def get_option_contracts(nifty_spot):
    global SELECTED_CE_ID, SELECTED_PE_ID
    try:
        # In production, you would call dhan.fetch_security_list() here.
        # For testing, use valid IDs from Dhan.
        SELECTED_CE_ID = "54321"   # Replace with your CE security ID
        SELECTED_PE_ID = "54322"   # Replace with your PE security ID
        logger.info(f"Contracts loaded: CE={SELECTED_CE_ID}, PE={SELECTED_PE_ID}")
        return [SELECTED_CE_ID, SELECTED_PE_ID]
    except Exception as e:
        logger.error(f"Error fetching contracts: {e}")
        return []

# ------------------------------------------------------------
# Enhanced signal generation using RSI, MACD, PCR
# ------------------------------------------------------------
def update_signal(ce_price, pe_price):
    global latest_data, price_history, update_counter
    spread = ce_price - pe_price
    latest_data["spread"] = spread

    # Add current CE price to history for indicators
    if ce_price > 0:
        price_history.append(ce_price)

    # Only compute indicators periodically (every 10 ticks) to save CPU
    update_counter += 1
    if update_counter >= UPDATE_INTERVAL and len(price_history) >= 20:
        update_counter = 0
        rsi = calculate_rsi(list(price_history))
        macd_hist = calculate_macd(list(price_history))
        pcr = get_nifty_pcr()
        if pcr is None:
            pcr = latest_data.get("pcr", 1.0)   # keep last known

        latest_data["rsi"] = round(rsi, 2)
        latest_data["macd"] = round(macd_hist, 2)
        latest_data["pcr"] = round(pcr, 2)

        # Sentiment‑based signal logic
        if spread > THRESHOLD and rsi < 70 and macd_hist > 0 and pcr < 0.8:
            signal = "LONG SPREAD (Bullish)"
        elif spread < -THRESHOLD and rsi > 30 and macd_hist < 0 and pcr > 1.2:
            signal = "SHORT SPREAD (Bearish)"
        else:
            signal = "NEUTRAL"

        latest_data["signal"] = signal

    latest_data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ------------------------------------------------------------
# WebSocket callback (original untouched except adding update_signal)
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

        # Update signal whenever we have both prices
        if latest_data["ce_price"] > 0 and latest_data["pe_price"] > 0:
            update_signal(latest_data["ce_price"], latest_data["pe_price"])
        else:
            latest_data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    except Exception as e:
        logger.error(f"Error in on_message: {e}")

# ------------------------------------------------------------
# WebSocket feed runner (unchanged)
# ------------------------------------------------------------
def run_feed():
    global SELECTED_CE_ID, SELECTED_PE_ID
    get_option_contracts(CURRENT_NIFTY)
    logger.info(f"Starting feed for CE: {SELECTED_CE_ID}, PE: {SELECTED_PE_ID}")

    feed = marketfeed.DhanFeed(
        client_id=CLIENT_ID,
        access_token=ACCESS_TOKEN,
        instruments=[
            (marketfeed.NSE_FNO, str(SELECTED_CE_ID), marketfeed.Ticker),
            (marketfeed.NSE_FNO, str(SELECTED_PE_ID), marketfeed.Ticker)
        ],
        on_message=on_message,
        on_connect=lambda instance: logger.info("✅ Connected to Dhan WebSocket")
    )
    feed.run_forever()

# ------------------------------------------------------------
# Flask routes (unchanged)
# ------------------------------------------------------------
@app.route('/')
def home():
    return jsonify({"status": "active", "data": latest_data})

@app.route('/api/health')
def health():
    return "OK", 200

application = app

# ------------------------------------------------------------
# Main entry (unchanged)
# ------------------------------------------------------------
if __name__ == '__main__':
    t = threading.Thread(target=run_feed, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))
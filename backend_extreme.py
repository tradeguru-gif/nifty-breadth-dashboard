import os
import asyncio
import threading
import logging
import requests
from collections import deque
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import DhanContext, MarketFeed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app

# ----------------------------------------------
# Environment Variables & Initialization
# ----------------------------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

logger.info("✅ Environment variables loaded")

# ----------------------------------------------
# Global State
# ----------------------------------------------
latest_data = {
    "signal": "WAITING",
    "ce_price": 0.0,
    "pe_price": 0.0,
    "spread": 0.0,
    "timestamp": datetime.now().isoformat()
}
CE_ID = "63719"   # REPLACE with actual CE Security ID
PE_ID = "63720"   # REPLACE with actual PE Security ID
price_history = deque(maxlen=200)

# ----------------------------------------------
# Core Technical Functions
# ----------------------------------------------
def calculate_rsi(prices, period=14):
    """Calculate RSI from a list of prices."""
    if len(prices) < period + 1:
        return 50.0
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = sum(d for d in deltas if d > 0) / period
    losses = sum(-d for d in deltas if d < 0) / period
    if losses == 0:
        return 100.0
    rs = gains / losses
    return 100 - (100 / (1 + rs))

def calculate_macd(prices, fast=12, slow=26):
    """Calculate MACD from a list of prices."""
    if len(prices) < slow:
        return 0.0
    def ema(data, period):
        alpha = 2 / (period + 1)
        value = data[0]
        for price in data[1:]:
            value = alpha * price + (1 - alpha) * value
        return value
    return ema(prices, fast) - ema(prices, slow)

# ----------------------------------------------
# WebSocket Setup
# ----------------------------------------------
async def websocket_loop():
    """Async WebSocket handler using MarketFeed."""
    instruments = [
        (MarketFeed.NSE_FNO, CE_ID, MarketFeed.Ticker),
        (MarketFeed.NSE_FNO, PE_ID, MarketFeed.Ticker)
    ]
    dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
    feed = MarketFeed(dhan_context, instruments, version="v2")

    # Setting up callbacks
    feed.on_connect = lambda: logger.info("✅ WebSocket connected and authorized")
    feed.on_error = lambda err: logger.error(f"WebSocket error: {err}")
    feed.on_close = lambda: logger.warning("WebSocket closed, will reconnect")
    feed.on_message = on_tick

    await feed.connect()
    await feed.subscribe_instruments()
    feed.run_forever()

def start_async_loop():
    """Entry point for the background thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(websocket_loop())

# ----------------------------------------------
# Tick Processing & Signal Generation
# ----------------------------------------------
def on_tick(tick_data):
    """Process each incoming tick."""
    global latest_data, price_history
    try:
        security_id = str(tick_data.get("security_id", ""))
        price = tick_data.get("ltp", 0.0)
        if security_id == CE_ID:
            latest_data["ce_price"] = price
        elif security_id == PE_ID:
            latest_data["pe_price"] = price

        if latest_data["ce_price"] > 0 and latest_data["pe_price"] > 0:
            current_spread = latest_data["ce_price"] - latest_data["pe_price"]
            latest_data["spread"] = round(current_spread, 2)
            price_history.append(latest_data["ce_price"])
            if len(price_history) >= 20:
                rsi = calculate_rsi(list(price_history))
                macd = calculate_macd(list(price_history))
                logger.info(f"🔔 SIGNAL | CE: {latest_data['ce_price']} | PE: {latest_data['pe_price']} | Spread: {current_spread:.2f} | RSI: {rsi:.2f} | MACD: {macd:.2f}")
            latest_data["timestamp"] = datetime.now().isoformat()
    except Exception as e:
        logger.error(f"Tick processing error: {e}")

# ----------------------------------------------
# Flask Routes
# ----------------------------------------------
@app.route("/")
def home():
    return jsonify({
        "status": "active",
        "data": latest_data
    })

@app.route("/api/health")
def health():
    return "OK", 200

# ----------------------------------------------
# Thread & App Execution
# ----------------------------------------------
if __name__ == "__main__":
    try:
        feed_thread = threading.Thread(target=start_async_loop, daemon=True)
        feed_thread.start()
        logger.info("Background signal engine started")
    except Exception as e:
        logger.error(f"Thread start error: {e}")
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))import os
import asyncio
import threading
import logging
import requests
from collections import deque
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import DhanContext, MarketFeed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app

# ----------------------------------------------
# Environment Variables & Initialization
# ----------------------------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

logger.info("✅ Environment variables loaded")

# ----------------------------------------------
# Global State
# ----------------------------------------------
latest_data = {
    "signal": "WAITING",
    "ce_price": 0.0,
    "pe_price": 0.0,
    "spread": 0.0,
    "timestamp": datetime.now().isoformat()
}
CE_ID = "63719"   # REPLACE with actual CE Security ID
PE_ID = "63720"   # REPLACE with actual PE Security ID
price_history = deque(maxlen=200)

# ----------------------------------------------
# Core Technical Functions
# ----------------------------------------------
def calculate_rsi(prices, period=14):
    """Calculate RSI from a list of prices."""
    if len(prices) < period + 1:
        return 50.0
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = sum(d for d in deltas if d > 0) / period
    losses = sum(-d for d in deltas if d < 0) / period
    if losses == 0:
        return 100.0
    rs = gains / losses
    return 100 - (100 / (1 + rs))

def calculate_macd(prices, fast=12, slow=26):
    """Calculate MACD from a list of prices."""
    if len(prices) < slow:
        return 0.0
    def ema(data, period):
        alpha = 2 / (period + 1)
        value = data[0]
        for price in data[1:]:
            value = alpha * price + (1 - alpha) * value
        return value
    return ema(prices, fast) - ema(prices, slow)

# ----------------------------------------------
# WebSocket Setup
# ----------------------------------------------
async def websocket_loop():
    """Async WebSocket handler using MarketFeed."""
    instruments = [
        (MarketFeed.NSE_FNO, CE_ID, MarketFeed.Ticker),
        (MarketFeed.NSE_FNO, PE_ID, MarketFeed.Ticker)
    ]
    dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
    feed = MarketFeed(dhan_context, instruments, version="v2")

    # Setting up callbacks
    feed.on_connect = lambda: logger.info("✅ WebSocket connected and authorized")
    feed.on_error = lambda err: logger.error(f"WebSocket error: {err}")
    feed.on_close = lambda: logger.warning("WebSocket closed, will reconnect")
    feed.on_message = on_tick

    await feed.connect()
    await feed.subscribe_instruments()
    feed.run_forever()

def start_async_loop():
    """Entry point for the background thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(websocket_loop())

# ----------------------------------------------
# Tick Processing & Signal Generation
# ----------------------------------------------
def on_tick(tick_data):
    """Process each incoming tick."""
    global latest_data, price_history
    try:
        security_id = str(tick_data.get("security_id", ""))
        price = tick_data.get("ltp", 0.0)
        if security_id == CE_ID:
            latest_data["ce_price"] = price
        elif security_id == PE_ID:
            latest_data["pe_price"] = price

        if latest_data["ce_price"] > 0 and latest_data["pe_price"] > 0:
            current_spread = latest_data["ce_price"] - latest_data["pe_price"]
            latest_data["spread"] = round(current_spread, 2)
            price_history.append(latest_data["ce_price"])
            if len(price_history) >= 20:
                rsi = calculate_rsi(list(price_history))
                macd = calculate_macd(list(price_history))
                logger.info(f"🔔 SIGNAL | CE: {latest_data['ce_price']} | PE: {latest_data['pe_price']} | Spread: {current_spread:.2f} | RSI: {rsi:.2f} | MACD: {macd:.2f}")
            latest_data["timestamp"] = datetime.now().isoformat()
    except Exception as e:
        logger.error(f"Tick processing error: {e}")

# ----------------------------------------------
# Flask Routes
# ----------------------------------------------
@app.route("/")
def home():
    return jsonify({
        "status": "active",
        "data": latest_data
    })

@app.route("/api/health")
def health():
    return "OK", 200

# ----------------------------------------------
# Thread & App Execution
# ----------------------------------------------
if __name__ == "__main__":
    try:
        feed_thread = threading.Thread(target=start_async_loop, daemon=True)
        feed_thread.start()
        logger.info("Background signal engine started")
    except Exception as e:
        logger.error(f"Thread start error: {e}")
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
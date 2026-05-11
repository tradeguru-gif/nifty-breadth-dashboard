# backend.py - Clean, Working WebSocket for Nifty Options

import os
import threading
import logging
import asyncio
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import DhanContext, MarketFeed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app

# Environment variables
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")
#-----------------------------------------------------------
#---------------------------------------------------------
#--------------------------------------------------------
# Static Security IDs (replace with active near‑month ATM IDs)
CE_ID = 1100808  # Change if expired
PE_ID = 1100863   # Change if expired
#-----------------------------------------------------------
#---------------------------------------------------------
#--------------------------------------------------------


# Global state for WordPress
latest_data = {
    "signal": "WAITING",
    "ce_price": 0.0,
    "pe_price": 0.0,
    "spread": 0.0,
    "timestamp": datetime.now().isoformat()
}

# ----------------------------------------------
# WebSocket callbacks
# ----------------------------------------------
def on_message(instance, tick):
    global latest_data
    try:
        sec_id = str(tick.get("security_id"))
        price = float(tick.get("ltp", 0))
        if sec_id == CE_ID:
            latest_data["ce_price"] = price
        elif sec_id == PE_ID:
            latest_data["pe_price"] = price

        if latest_data["ce_price"] > 0 and latest_data["pe_price"] > 0:
            spread = latest_data["ce_price"] - latest_data["pe_price"]
            latest_data["spread"] = round(spread, 2)
            # Simple signal based on spread
            if spread > 5:
                latest_data["signal"] = "BULLISH"
            elif spread < -5:
                latest_data["signal"] = "BEARISH"
            else:
                latest_data["signal"] = "NEUTRAL"
            latest_data["timestamp"] = datetime.now().isoformat()
            logger.info(f"Tick: CE={latest_data['ce_price']} PE={latest_data['pe_price']} Spread={spread:.2f} Signal={latest_data['signal']}")
    except Exception as e:
        logger.error(f"on_message error: {e}")

def on_connect(instance):
    logger.info("✅ WebSocket connected and authorized")

def on_error(instance, error):
    logger.error(f"WebSocket error: {error}")

def on_close(instance):
    logger.warning("WebSocket closed – reconnecting...")

# ----------------------------------------------
# Async WebSocket runner
# ----------------------------------------------
async def websocket_loop():
    instruments = [
        (MarketFeed.NSE_FNO, CE_ID, MarketFeed.Ticker),
        (MarketFeed.NSE_FNO, PE_ID, MarketFeed.Ticker)
    ]
    ctx = DhanContext(CLIENT_ID, ACCESS_TOKEN)
    feed = MarketFeed(ctx, instruments, version="v2")
    feed.on_connect = on_connect
    feed.on_error = on_error
    feed.on_close = on_close
    feed.on_message = on_message

    await feed.connect()
    await feed.subscribe_instruments()
    logger.info("Subscribed, waiting for ticks...")
    feed.run_forever()

def start_feed():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(websocket_loop())

# ----------------------------------------------
# Flask routes
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
# Start background thread
# ----------------------------------------------
thread = threading.Thread(target=start_feed, daemon=True)
thread.start()
logger.info("Background signal engine started")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
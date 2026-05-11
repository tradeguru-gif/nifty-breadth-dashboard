# backend.py - Minimal Working WebSocket for Nifty Options
import os
import asyncio
import threading
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import DhanContext, MarketFeed

app = Flask(__name__)
CORS(app)
application = app

# --------------------------------------------------
# Environment variables
# --------------------------------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

# --------------------------------------------------
# Replace these with your actual active Security IDs
# (CE = Call, PE = Put, same strike, nearest expiry)
# --------------------------------------------------
CE_ID = "63719"   # <-- CHANGE to your CE Security ID
PE_ID = "63720"   # <-- CHANGE to your PE Security ID

# Global state for WordPress
latest_data = {
    "signal": "WAITING",
    "ce_price": 0.0,
    "pe_price": 0.0,
    "spread": 0.0,
    "timestamp": ""
}

# --------------------------------------------------
# WebSocket callbacks
# --------------------------------------------------
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
            if spread > 5:
                latest_data["signal"] = "BULLISH"
            elif spread < -5:
                latest_data["signal"] = "BEARISH"
            else:
                latest_data["signal"] = "NEUTRAL"
            from datetime import datetime
            latest_data["timestamp"] = datetime.now().isoformat()
            print(f"Tick: CE={latest_data['ce_price']} PE={latest_data['pe_price']} Spread={spread:.2f} Signal={latest_data['signal']}")
    except Exception as e:
        print(f"on_message error: {e}")

def on_connect(instance):
    print("✅ WebSocket connected and authorized")

def on_error(instance, error):
    print(f"❌ WebSocket error: {error}")

def on_close(instance):
    print("🔌 WebSocket closed, reconnecting...")

# --------------------------------------------------
# Async WebSocket runner
# --------------------------------------------------
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
    print("Subscribed, waiting for ticks...")
    feed.run_forever()

def start_feed():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(websocket_loop())

# --------------------------------------------------
# Start background thread
# --------------------------------------------------
thread = threading.Thread(target=start_feed, daemon=True)
thread.start()
print("Background signal engine started")

# --------------------------------------------------
# Flask routes
# --------------------------------------------------
@app.route("/")
def home():
    return jsonify({"status": "active", "data": latest_data})

@app.route("/api/health")
def health():
    return "OK", 200

# --------------------------------------------------
# Debug endpoint to verify version
# --------------------------------------------------
@app.route("/debug/version")
def debug_version():
    return "Running minimal WebSocket version (with MarketFeed)"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
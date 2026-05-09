import os
import asyncio
import threading
import time
import logging
from datetime import datetime
import pandas as pd
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import DhanHQ, MarketFeed

# Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
CURRENT_NIFTY = int(os.getenv("CURRENT_NIFTY", "0"))

latest_data = {"signal": "WAITING", "ce_price": 0, "pe_price": 0, "spread": 0, "timestamp": ""}
SELECTED_CE_ID = ""
SELECTED_PE_ID = ""

def get_option_contracts(nifty_spot):
    global SELECTED_CE_ID, SELECTED_PE_ID
    dhan = DhanHQ(CLIENT_ID, ACCESS_TOKEN)
    df = dhan.fetch_security_list("detailed")
    # ... (Your logic for selecting CE and PE remains the same)
    # Ensure SELECTED_CE_ID and SELECTED_PE_ID are strings
    return [SELECTED_CE_ID, SELECTED_PE_ID]

def on_message(instance, tick):
    global latest_data
    # Use .get() to avoid errors if a key is missing
    sec_id = str(tick.get('security_id'))
    price = tick.get('ltp', 0)
    # ... update latest_data logic ...

async def run_feed():
    get_option_contracts(CURRENT_NIFTY)
    
    # Initialize MarketFeed directly - NO DhanContext
    feed = MarketFeed(CLIENT_ID, ACCESS_TOKEN, version='v2')
    feed.on_message = on_message
    await feed.connect()
    
    # FIXED: Using raw integers to bypass 'marketfeed' attribute errors
    # 1 = NSE_FNO (Options), 15 = Ticker Mode
    subscription = [
        (1, SELECTED_CE_ID, 15), 
        (1, SELECTED_PE_ID, 15)
    ]
    
    await feed.subscribe_symbols(subscription)
    logger.info("✅ SUCCESS: Subscribed using raw integers.")
    
    while True:
        await asyncio.sleep(1)

def start_worker():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run_feed())

@app.route('/')
def home():
    return jsonify({"status": "active", "data": latest_data})

@app.route('/api/health')
def health():
    return "OK", 200

application = app

if __name__ == '__main__':
    threading.Thread(target=start_worker, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))
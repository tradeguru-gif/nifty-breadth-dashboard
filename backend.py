import os
import threading
import logging
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

# This is the modern way to import for version 2.1.0
from dhanhq import dhanhq as DhanHQ
from dhanhq import marketfeed

# ... rest of your code ...

# Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
CURRENT_NIFTY = int(os.getenv("CURRENT_NIFTY", "24000")) 

latest_data = {"signal": "WAITING", "ce_price": 0, "pe_price": 0, "spread": 0, "timestamp": ""}
SELECTED_CE_ID = ""
SELECTED_PE_ID = ""

def get_option_contracts(nifty_spot):
    global SELECTED_CE_ID, SELECTED_PE_ID
    try:
        # Note: If you are hardcoding IDs for testing, ensure they are valid strings
        # In production, you'd use dhan.fetch_security_list() here
        SELECTED_CE_ID = "54321" 
        SELECTED_PE_ID = "54322"
        return [SELECTED_CE_ID, SELECTED_PE_ID]
    except Exception as e:
        logger.error(f"Error fetching contracts: {e}")
        return []

def on_message(instance, tick):
    global latest_data
    try:
        sec_id = str(tick.get('security_id'))
        price = tick.get('ltp', 0)
        
        # Update specific price based on ID
        if sec_id == SELECTED_CE_ID:
            latest_data["ce_price"] = price
        elif sec_id == SELECTED_PE_ID:
            latest_data["pe_price"] = price
            
        latest_data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.error(f"Error in on_message: {e}")

# REMOVED 'async' - Dhan's run_forever handles the loop
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

@app.route('/')
def home():
    return jsonify({"status": "active", "data": latest_data})

@app.route('/api/health')
def health():
    return "OK", 200

application = app

if __name__ == '__main__':
    # Start the feed in a background thread
    t = threading.Thread(target=run_feed, daemon=True)
    t.start()
    # Start Flask
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))
import os
import time
import threading
import logging
import requests
import asyncio
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import dhanhq, MarketFeed

# ------------------------------------------------------------
# Logging & Flask Setup
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app 

# ------------------------------------------------------------
# Environment variables
# ------------------------------------------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

if not CLIENT_ID or not ACCESS_TOKEN:
    logger.error("CRITICAL: Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")
    # We don't raise ValueError here to allow the process to start 
    # so you can see logs, but the app won't function.

# ------------------------------------------------------------
# Dhan Client Initialization
# ------------------------------------------------------------
try:
    # Use keyword arguments to resolve the initialization conflict
    dhan = dhanhq(client_id=CLIENT_ID, access_token=ACCESS_TOKEN)
    logger.info("✅ Dhan client initialized successfully")
except Exception as e:
    logger.error(f"Dhan Init Error: {e}")
    # Final fallback for different library versions
    try:
        dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
    except Exception as final_e:
        logger.error(f"Critical initialization failure: {final_e}")
        dhan = None

# Global Data Store
market_data = {
    "CE": {"id": None, "ltp": 0.0, "signal": "Neutral"},
    "PE": {"id": None, "ltp": 0.0, "signal": "Neutral"},
    "spot": 0.0
}

def get_atm_instruments():
    """Automatically selects Nifty ATM strike IDs."""
    if not dhan:
        return "63719", "63720"
    try:
        quote = dhan.get_quote_data(securities={"IDX_I": [13]})
        # Handle cases where response might be a string error
        if isinstance(quote, str):
            logger.error(f"API returned string error: {quote}")
            return "63719", "63720"
            
        nifty_spot = quote['data']['last_price']
        market_data["spot"] = nifty_spot
        atm_strike = round(nifty_spot / 50) * 50
        
        expiries = dhan.expiry_list(under_security_id=13, under_exchange_segment="IDX_I")
        current_expiry = expiries['data'][0]
        
        oc = dhan.option_chain(13, "IDX_I", current_expiry)
        strike_key = f"{float(atm_strike):.6f}"
        instruments = oc['data']['oc'].get(strike_key)
        
        if instruments:
            return str(instruments['ce']['security_id']), str(instruments['pe']['security_id'])
    except Exception as e:
        logger.error(f"Auto-select failed: {e}")
    return "63719", "63720"

# --- WebSocket Feed Logic ---

async def on_message(instance, message):
    if 'last_price' in message:
        sec_id = str(message['security_id'])
        price = message['last_price']
        if sec_id == market_data["CE"]["id"]:
            market_data["CE"]["ltp"] = price
        elif sec_id == market_data["PE"]["id"]:
            market_data["PE"]["ltp"] = price

def run_dhan_feed():
    if not CLIENT_ID or not ACCESS_TOKEN:
        return
    
    ce_id, pe_id = get_atm_instruments()
    market_data["CE"]["id"] = ce_id
    market_data["PE"]["id"] = pe_id

    instruments = [
        (MarketFeed.NSE_FNO, ce_id, MarketFeed.Ticker),
        (MarketFeed.NSE_FNO, pe_id, MarketFeed.Ticker)
    ]

    try:
        feed = MarketFeed(CLIENT_ID, ACCESS_TOKEN, instruments, version="v2")
        feed.on_message = on_message
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(feed.connect())
    except Exception as e:
        logger.error(f"WebSocket Error: {e}")

# --- Routes ---

@app.route('/api/trading-signals', methods=['GET'])
def get_signals():
    return jsonify(market_data)

@app.route('/health')
def health():
    return "OK", 200

if __name__ == '__main__':
    threading.Thread(target=run_dhan_feed, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
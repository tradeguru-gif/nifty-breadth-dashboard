import os
import threading
import logging
import asyncio
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import dhanhq, MarketFeed

# ------------------------------------------------------------
# Logging Setup
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

# ------------------------------------------------------------
# Initialization Fix
# ------------------------------------------------------------
# The error "takes 2 positional arguments but 3 were given" 
# suggests it wants: dhanhq(token) OR the version you have 
# handles internal context differently.
try:
    dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
    logger.info("✅ Dhan client initialized")
except Exception as e:
    logger.warning(f"Standard init failed, trying token-only: {e}")
    dhan = dhanhq(ACCESS_TOKEN)

market_data = {
    "CE": {"id": None, "ltp": 0.0, "change": 0.0},
    "PE": {"id": None, "ltp": 0.0, "change": 0.0},
    "spot": 0.0,
    "status": "Initializing"
}

def get_nifty_spot():
    try:
        # Fetch Nifty 50 Spot (ID 13)
        response = dhan.get_quote_data(securities={"IDX_I": [13]})
        
        # FIX: Check if response is an error string
        if isinstance(response, str):
            logger.error(f"Dhan API returned Error: {response}")
            market_data["status"] = f"Error: {response}"
            return None
            
        return response.get('data', {}).get('last_price', 0)
    except Exception as e:
        logger.error(f"Spot fetch exception: {e}")
        return None

def setup_instruments():
    spot = get_nifty_spot()
    if not spot:
        return None, None
        
    market_data["spot"] = spot
    atm_strike = round(spot / 50) * 50
    
    try:
        expiries = dhan.expiry_list(13, "IDX_I")
        target_expiry = expiries['data'][0]
        
        oc = dhan.option_chain(13, "IDX_I", target_expiry)
        strike_key = f"{float(atm_strike):.6f}"
        chain = oc['data']['oc'].get(strike_key)
        
        if chain:
            return str(chain['ce']['security_id']), str(chain['pe']['security_id'])
    except Exception as e:
        logger.error(f"Instrument setup failed: {e}")
    return None, None

async def on_message(instance, message):
    if 'last_price' in message:
        sec_id = str(message['security_id'])
        if sec_id == market_data["CE"]["id"]:
            market_data["CE"]["ltp"] = message['last_price']
        elif sec_id == market_data["PE"]["id"]:
            market_data["PE"]["ltp"] = message['last_price']

def run_feed():
    ce_id, pe_id = setup_instruments()
    if not ce_id or not pe_id:
        logger.error("Could not resolve ATM instruments. Check Token.")
        return

    market_data["CE"]["id"] = ce_id
    market_data["PE"]["id"] = pe_id
    market_data["status"] = "Live"

    instruments = [
        (MarketFeed.NSE_FNO, ce_id, MarketFeed.Ticker),
        (MarketFeed.NSE_FNO, pe_id, MarketFeed.Ticker)
    ]

    feed = MarketFeed(CLIENT_ID, ACCESS_TOKEN, instruments, version="v2")
    feed.on_message = on_message
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(feed.connect())

# --- Routes ---

@app.route('/')
def home():
    return jsonify(market_data)

@app.route('/api/trading-signals')
def signals():
    return jsonify(market_data)

if __name__ == '__main__':
    threading.Thread(target=run_feed, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
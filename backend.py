import os
import threading
import logging
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

CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

# ------------------------------------------------------------
# UNIVERSAL INITIALIZATION
# ------------------------------------------------------------
dhan = None
try:
    # Attempt 1: Standard Keyword arguments (Modern version)
    dhan = dhanhq(client_id=CLIENT_ID, access_token=ACCESS_TOKEN)
    logger.info("✅ Init: Keyword arguments successful")
except Exception:
    try:
        # Attempt 2: Positional arguments (Standard version)
        dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
        logger.info("✅ Init: Positional arguments successful")
    except Exception:
        # Attempt 3: Token only (Minimal version)
        dhan = dhanhq(ACCESS_TOKEN)
        logger.info("✅ Init: Token-only successful")

# ------------------------------------------------------------
# Global Data Store
# ------------------------------------------------------------
market_data = {
    "CE": {"id": None, "ltp": 0.0},
    "PE": {"id": None, "ltp": 0.0},
    "spot": 0.0,
    "status": "Starting"
}

def get_nifty_spot():
    """Robust spot fetcher that ignores string errors."""
    if not dhan:
        return None
    try:
        # Security ID 13 is Nifty 50 Index
        response = dhan.get_quote_data(securities={"IDX_I": [13]})
        
        # If response is just a string (Error message), return None
        if isinstance(response, str):
            logger.error(f"Dhan API Error String: {response}")
            return None
            
        # Structure varies by library version: try multiple paths
        data = response.get('data', {})
        if isinstance(data, dict):
            # Path A: Directly in 'last_price'
            if 'last_price' in data:
                return data['last_price']
            # Path B: Inside 'IDX_I' key
            idx = data.get('IDX_I', {})
            if isinstance(idx, list) and len(idx) > 0:
                return idx[0].get('last_price', 0)
            if isinstance(idx, dict):
                return idx.get('last_price', 0)
                
        return None
    except Exception as e:
        logger.error(f"Spot fetch crash: {e}")
        return None

# ... [Keep your setup_instruments, run_feed, and Routes from previous code] ...

@app.route('/api/trading-signals')
def signals():
    return jsonify(market_data)

if __name__ == '__main__':
    # Add a delay to let Gunicorn settle before starting the background thread
    def start_engine():
        time.sleep(5)
        run_feed()
        
    threading.Thread(target=start_engine, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
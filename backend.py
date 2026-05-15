import os
import time
import threading
import logging
import requests
import pandas as pd
from collections import deque
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

# Import the library
from dhanhq import dhanhq

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
    raise ValueError("CRITICAL: Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

# FIXED INITIALIZATION: 
# Most versions of dhanhq take (client_id, access_token). 
# If it fails, we wrap it to ensure it matches the library expectations.
try:
   # ------------------------------------------------------------
# Dhan Client Initialization
# ------------------------------------------------------------
try:
    # Use keyword arguments to ensure the library maps them correctly
    dhan = dhanhq(client_id=CLIENT_ID, access_token=ACCESS_TOKEN)
    logger.info("✅ Dhan client initialized successfully with Keyword Arguments")
except Exception as e:
    logger.error(f"Failed to initialize Dhan: {e}")
    # Final fallback attempt
    try:
        dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
    except Exception as final_e:
        logger.error(f"Critical initialization failure: {final_e}")
        raise
# ------------------------------------------------------------
# Fixed Spot Fetcher
# ------------------------------------------------------------
def get_nifty_spot():
    try:
        # Requesting Nifty 50 Index (Security ID 13)
        ticker_response = dhan.get_quote_data(securities={"IDX_I": [13]})
        
        # SAFETY CHECK: If the response is a string, it means an error occurred (like "Invalid Token")
        if isinstance(ticker_response, str):
            logger.error(f"API Error Response: {ticker_response}")
            return None

        # Standard dictionary extraction
        data = ticker_response.get('data', {})
        # Depending on version, it might be in 'IDX_I' or direct
        idx_data = data.get('IDX_I', {})
        
        # Get ltp from the first item if it's a list, or directly if dict
        if isinstance(idx_data, list) and len(idx_data) > 0:
            return idx_data[0].get('ltp', 0)
        elif isinstance(idx_data, dict):
            # Try to get the first value from the dictionary
            first_val = next(iter(idx_data.values()), {})
            return first_val.get('ltp', 0)
            
        return None
    except Exception as e:
        logger.error(f"Failed to get spot: {e}")
        return None

# ... rest of your code ...

@app.route("/")
def home():
    spot = get_nifty_spot()
    return jsonify({
        "status": "online",
        "nifty_spot": spot,
        "timestamp": datetime.now().isoformat()
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
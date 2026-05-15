import os
import logging
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import dhanhq

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
# Explicitly name the application for Gunicorn
application = app 

# Get Environment Variables
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

def get_dhan_client():
    """
    Fixes the 'str object has no attribute get_dhan_http' error 
    by using explicit keyword arguments for the constructor.
    """
    try:
        if not CLIENT_ID or not ACCESS_TOKEN:
            logger.error("❌ Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")
            return None
            
        # Using keywords (client_id=, access_token=) bypasses the library's internal positional bug
        client = dhanhq(client_id=CLIENT_ID, access_token=ACCESS_TOKEN)
        return client
    except Exception as e:
        logger.error(f"❌ Dhan Init failed: {e}")
        return None

# Global data store
market_data = {"spot": 0.0, "status": "Initializing", "error": None}

# --- ROUTES ---

@app.route('/')
def health_check():
    return jsonify({"status": "live", "message": "Nifty Breadth Backend"})

@app.route('/api/trading-signals')
def signals():
    """
    Handles the 404 issue by ensuring the route is correctly 
    mapped and triggers a fresh fetch.
    """
    client = get_dhan_client()
    if not client:
        return jsonify({"status": "error", "message": "Library Init Failed"}), 500
        
    try:
        # Security ID 13 is the Nifty 50 Index
        resp = client.get_quote_data(securities={"IDX_I": [13]})
        
        # Check if the response is a dictionary to prevent 'str' object errors
        if isinstance(resp, dict) and resp.get('status') == 'success':
            data = resp.get('data', {})
            market_data["spot"] = data.get('last_price', 0.0)
            market_data["status"] = "Connected"
            market_data["error"] = None
        else:
            market_data["status"] = "API_Error"
            market_data["error"] = str(resp)
            
    except Exception as e:
        logger.error(f"Fetch loop error: {e}")
        market_data["status"] = "Exception"
        market_data["error"] = str(e)

    return jsonify(market_data)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
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
application = app 

# Fetch from Render Env
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

# GLOBAL CLIENT
dhan = None

def get_dhan_client():
    """Final fixed initialization for dhanhq 2.2.0"""
    global dhan
    if dhan is not None:
        return dhan
    
    try:
        # In version 2.2.0, the first positional argument is the token.
        # We pass it this way to avoid the "3 arguments given" error.
        dhan = dhanhq(client_id=CLIENT_ID, access_token=ACCESS_TOKEN)
        logger.info("✅ Dhan client successfully started")
    except TypeError:
        # Fallback: some 2.x versions only take the token in the constructor
        # and handle the Client ID internally or via keyword.
        try:
            dhan = dhanhq(ACCESS_TOKEN)
            logger.info("✅ Dhan client started with Token fallback")
        except Exception as e:
            logger.error(f"❌ All initialization attempts failed: {e}")
            dhan = None
    return dhan

# DATA STORE
market_data = {"spot": 0.0, "status": "online"}

@app.route('/')
def home():
    client = get_dhan_client()
    if client:
        try:
            # Security ID 13 = Nifty 50 Index
            resp = client.get_quote_data(securities={"IDX_I": [13]})
            
            # Robust dictionary checking to prevent the 'str' error
            if isinstance(resp, dict) and resp.get('status') == 'success':
                data = resp.get('data', {})
                # Fetching the price safely
                market_data["spot"] = data.get('last_price', 0.0)
                market_data["status"] = "Connected"
            else:
                market_data["status"] = f"API Error: {resp}"
        except Exception as e:
            logger.error(f"Quote fetch error: {e}")
            market_data["status"] = "Fetch Error"
            
    return jsonify(market_data)

@app.route('/api/trading-signals')
def signals():
    return jsonify(market_data)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
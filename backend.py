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

CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

# GLOBAL CLIENT
dhan = None

def get_dhan_client():
    """Universal init for dhanhq 2.2.0"""
    global dhan
    if dhan is not None:
        return dhan
    
    try:
        # Standard initialization for 2.2.0
        dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
        logger.info("✅ Dhan client successfully started")
    except Exception as e:
        logger.warning(f"Standard init failed, trying positional: {e}")
        try:
            # Fallback for older positional logic
            dhan = dhanhq(ACCESS_TOKEN)
            logger.info("✅ Dhan client started with fallback")
        except Exception as final_e:
            logger.error(f"❌ Initialization failed: {final_e}")
            dhan = None
    return dhan

market_data = {"spot": 0.0, "status": "online"}

@app.route('/')
def home():
    client = get_dhan_client()
    if client:
        try:
            # Security ID 13 = Nifty 50 Index
            resp = client.get_quote_data(securities={"IDX_I": [13]})
            
            # Check if resp is a dictionary to avoid 'str' attribute errors
            if isinstance(resp, dict) and resp.get('status') == 'success':
                data = resp.get('data', {})
                market_data["spot"] = data.get('last_price', 0.0)
                market_data["status"] = "Connected"
            else:
                market_data["status"] = f"API Error: {resp}"
        except Exception as e:
            logger.error(f"Quote fetch error: {e}")
            market_data["status"] = "Fetch Error"
            
    return jsonify(market_data)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
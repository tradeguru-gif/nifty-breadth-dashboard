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

# Fetch credentials
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

def get_dhan_client():
    """
    Direct Injection Method: This bypasses the library's internal 
    constructor logic that is causing the 'str' object error.
    """
    try:
        # Create a bare client instance
        # We pass the token as the first positional argument
        client = dhanhq(client_id=CLIENT_ID, access_token=ACCESS_TOKEN)
        
        # MANUAL OVERRIDE: 
        # The library error happens because it expects 'access_token' 
        # to be an object with a 'get_dhan_http' method. 
        # We force the internal variables to the raw strings they need to be.
        client.access_token = ACCESS_TOKEN
        client.client_id = CLIENT_ID
        
        return client
    except Exception as e:
        # Fallback for older versions of the library
        try:
            client = dhanhq()
            client.access_token = ACCESS_TOKEN
            client.client_id = CLIENT_ID
            return client
        except:
            logger.error(f"Failed to initialize Dhan: {e}")
            return None

@app.route('/')
def home():
    return jsonify({"status": "online", "backend": "active"})

@app.route('/api/trading-signals')
def signals():
    client = get_dhan_client()
    if not client:
        return jsonify({"status": "error", "message": "API Client Fail"}), 500
        
    try:
        # Get Nifty 50 Index (Security ID 13)
        # We use keyword arguments here to stay safe
        resp = client.get_quote_data(securities={"IDX_I": [13]})
        
        # Check if response is valid JSON/Dict
        if isinstance(resp, dict) and resp.get('status') == 'success':
            last_price = resp.get('data', {}).get('last_price', 0)
            return jsonify({
                "spot": last_price,
                "status": "online",
                "timestamp": "Live"
            })
        
        return jsonify({"status": "api_offline", "response": str(resp)})

    except Exception as e:
        logger.error(f"Fetch Error: {e}")
        return jsonify({"status": "exception", "error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
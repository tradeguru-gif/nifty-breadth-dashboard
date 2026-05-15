import os
import logging
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import dhanhq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app 

# Fetching environment variables
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

def get_dhan_client():
    """
    Manual Injection Method: Bypasses the __init__ bug by creating 
    the object and setting attributes manually.
    """
    try:
        # 1. Create the instance with NO arguments
        # If the library forces one argument, we pass the token.
        try:
            client = dhanhq(ACCESS_TOKEN)
        except TypeError:
            client = dhanhq(client_id=CLIENT_ID, access_token=ACCESS_TOKEN)
        
        # 2. Force-inject the attributes into the client object
        # This fixes the 'str object has no attribute get_dhan_http' error
        client.client_id = CLIENT_ID
        client.access_token = ACCESS_TOKEN
        
        return client
    except Exception as e:
        logger.error(f"FATAL: Could not patch Dhan client: {e}")
        return None

@app.route('/')
def home():
    return jsonify({"status": "online", "message": "Backend Fixed"})

@app.route('/api/trading-signals')
def signals():
    client = get_dhan_client()
    
    if not client:
        return jsonify({"status": "error", "message": "Client Patch Failed"}), 500
    
    try:
        # Security ID 13 is Nifty 50 Index
        # We use a very direct call to avoid library internal loops
        resp = client.get_quote_data(securities={"IDX_I": [13]})
        
        if isinstance(resp, dict) and resp.get('status') == 'success':
            price = resp.get('data', {}).get('last_price', 0)
            return jsonify({
                "spot": price,
                "status": "online",
                "source": "Dhan Live API"
            })
        
        return jsonify({"status": "api_error", "details": str(resp)})
        
    except Exception as e:
        logger.error(f"API Fetch Error: {e}")
        return jsonify({"status": "exception", "error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
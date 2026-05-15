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

CID = os.getenv("DHAN_CLIENT_ID")
TKN = os.getenv("DHAN_ACCESS_TOKEN")

def get_dhan_client():
    """Explores 3 different ways to satisfy this specific library version"""
    # Method A: Positional (Client ID then Token)
    try:
        client = dhanhq(CID, TKN)
        logger.info("✅ Init Method A Success")
        return client
    except Exception:
        pass

    # Method B: Single Argument (Token Only)
    try:
        client = dhanhq(TKN)
        # Manually attach client_id if the library needs it later
        client.client_id = CID 
        logger.info("✅ Init Method B Success")
        return client
    except Exception:
        pass

    # Method C: Dictionary/Object bypass
    try:
        # Some versions of this SDK use a 'dhan' class inside the module
        from dhanhq import dhanhq as dh
        client = dh(CID, TKN)
        return client
    except Exception as e:
        logger.error(f"❌ All methods failed. Final error: {e}")
        return None

@app.route('/')
def home():
    return jsonify({"status": "running"})

@app.route('/api/trading-signals')
def signals():
    client = get_dhan_client()
    if not client:
        return jsonify({"error": "Initialization failure"}), 500
    
    try:
        # Security ID 13 = NIFTY 50
        resp = client.get_quote_data(securities={"IDX_I": [13]})
        
        # Guard against the 'str' object error by checking type
        if isinstance(resp, dict) and resp.get('status') == 'success':
            price = resp.get('data', {}).get('last_price', 0)
            return jsonify({"spot": price, "status": "success"})
        
        return jsonify({"status": "error", "api_msg": str(resp)})
    except Exception as e:
        return jsonify({"status": "exception", "details": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))
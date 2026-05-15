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

# GUNICORN LOOKS FOR THIS EXACT VARIABLE
application = app

def get_dhan_client():
    cid = os.getenv("DHAN_CLIENT_ID")
    tkn = os.getenv("DHAN_ACCESS_TOKEN")
    if not cid or not tkn:
        return None
    try:
        # Using positional arguments to avoid 'unexpected keyword' errors
        return dhanhq(cid, tkn)
    except Exception as e:
        logger.error(f"Dhan Init Error: {e}")
        return None

@app.route('/')
def home():
    return jsonify({"status": "online", "message": "ATM Logic Ready"})

@app.route('/api/trading-signals')
def signals():
    client = get_dhan_client()
    if not client:
        return jsonify({"status": "error", "message": "Init Failed"}), 500
        
    try:
        # Security ID 13 = Nifty 50 Index
        quote = client.get_quote_data(securities={"IDX_I": [13]})
        
        if quote.get('status') == 'success':
            spot = quote['data']['last_price']
            # ATM Calculation: Rounds to nearest 50
            atm = round(spot / 50) * 50
            
            return jsonify({
                "status": "success",
                "spot": spot,
                "atm": atm,
                "selection": {
                    "atm": atm,
                    "near_up": atm + 50,
                    "near_down": atm - 50
                }
            })
        return jsonify({"status": "api_error", "msg": str(quote)})
    except Exception as e:
        return jsonify({"status": "exception", "error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
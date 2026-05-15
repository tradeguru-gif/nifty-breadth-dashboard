import os
import logging
import math
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import dhanhq

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Force the application variable for Gunicorn
application = app

# Credentials - Fetched inside the function to ensure they are fresh
def get_dhan_client():
    client_id = os.getenv("DHAN_CLIENT_ID")
    access_token = os.getenv("DHAN_ACCESS_TOKEN")
    
    if not client_id or not access_token:
        logger.error("Environment variables missing")
        return None

    try:
        # Pass credentials as POSITIONAL arguments
        # This prevents the 'keyword argument' error
        return dhanhq(client_id, access_token)
    except Exception as e:
        logger.error(f"Failed to initialize Dhan: {e}")
        return None

def calculate_atm(spot_price):
    """Calculates the Nifty ATM strike (multiples of 50)"""
    return round(spot_price / 50) * 50

@app.route('/')
def home():
    return jsonify({
        "status": "online", 
        "feature": "ATM Selection Active",
        "info": "Nifty Breadth Dashboard API"
    })

@app.route('/api/trading-signals')
def signals():
    client = get_dhan_client()
    if not client:
        return jsonify({
            "status": "error", 
            "message": "Init Failed - Check Render Env Vars"
        }), 500
        
    try:
        # Fetch Nifty 50 Spot Price (Security ID 13)
        quote = client.get_quote_data(securities={"IDX_I": [13]})
        
        if isinstance(quote, dict) and quote.get('status') == 'success':
            spot_price = quote.get('data', {}).get('last_price', 0)
            
            # Automatic ATM Selection Logic
            atm_strike = calculate_atm(spot_price)
            itm_call = atm_strike - 50
            otm_call = atm_strike + 50
            
            return jsonify({
                "status": "success",
                "spot": spot_price,
                "atm": atm_strike,
                "selection": {
                    "atm": atm_strike,
                    "near_down": itm_call,
                    "near_up": otm_call
                },
                "note": "Strike selection based on Nifty 50-point intervals"
            })
        
        return jsonify({"status": "api_error", "msg": str(quote)})

    except Exception as e:
        logger.error(f"Logic Error: {e}")
        return jsonify({"status": "exception", "error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
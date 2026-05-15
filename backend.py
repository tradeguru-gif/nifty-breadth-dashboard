import os
import logging
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import dhanhq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

def get_dhan_client():
    # Get values from Render environment
    client_id = os.getenv("DHAN_CLIENT_ID")
    access_token = os.getenv("DHAN_ACCESS_TOKEN")

    # Check if they exist before trying to use them
    if not client_id or not access_token:
        logger.error("CRITICAL: Environment variables DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN are missing!")
        return None, "Missing Environment Variables"

    try:
        # Pass values as positional arguments
        client = dhanhq(client_id, access_token)
        return client, None
    except Exception as e:
        logger.error(f"Dhan Initialization Failed: {e}")
        return None, str(e)

@app.route('/api/trading-signals')
def signals():
    client, error_msg = get_dhan_client()
    
    if not client:
        return jsonify({
            "status": "error", 
            "message": "Init Failed",
            "debug_info": error_msg  # This will tell you why it failed
        }), 500
        
    try:
        # Fetch Nifty Spot (Security ID 13)
        # Note: Using the v2 'get_quote_data' format
        quote = client.get_quote_data(securities={"IDX_I": [13]})
        
        if quote.get('status') == 'success':
            spot = quote['data']['last_price']
            # ATM is strike nearest to spot (multiples of 50 for Nifty)
            atm = round(spot / 50) * 50
            
            return jsonify({
                "status": "success",
                "spot_price": spot,
                "atm_strike": atm,
                "segments": {
                    "atm": atm,
                    "near_up": atm + 50,
                    "near_down": atm - 50
                }
            })
        return jsonify({"status": "api_error", "response": quote}), 400

    except Exception as e:
        return jsonify({"status": "exception", "error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
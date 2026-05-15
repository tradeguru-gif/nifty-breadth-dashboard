import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import DhanContext, dhanhq

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------------------------------------------------
# Flask app
# --------------------------------------------------
app = Flask(__name__)
CORS(app)
application = app  # for gunicorn

# --------------------------------------------------
# Environment variables
# --------------------------------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

# --------------------------------------------------
# Dhan client helper (using DhanContext for v2.2.0)
# --------------------------------------------------
def get_dhan_client():
    try:
        ctx = DhanContext(CLIENT_ID, ACCESS_TOKEN)
        return dhanhq(ctx)
    except Exception as e:
        logger.error(f"Failed to create Dhan client: {e}")
        return None

# --------------------------------------------------
# Load scrip master CSV from Dhan
# --------------------------------------------------
def load_instruments():
    url = "https://images.dhan.co/api-data/api-scrip-master.csv"
    try:
        df = pd.read_csv(url, low_memory=False)
        logger.info(f"Loaded {len(df)} instruments")
        return df
    except Exception as e:
        logger.error(f"Failed to load CSV: {e}")
        raise

# --------------------------------------------------
# Get Nifty spot price (corrected path)
# --------------------------------------------------
def get_nifty_spot(client):
    try:
        # Quote request for index segment IDX_I, security ID 13 (Nifty 50)
        response = client.quote_data(securities={"IDX_I": ["13"]})
        logger.debug(f"Quote response: {response}")

        if response.get("status") != "success":
            raise Exception(f"Quote API failed: {response.get('remarks')}")

        # Navigate the nested structure
        data = response.get("data", {})
        # The actual data is inside another "data" key
        inner_data = data.get("data", {})
        nifty_data = inner_data.get("IDX_I", {}).get("13", {})

        spot = nifty_data.get("last_price")
        if spot is None:
            raise Exception("Spot price missing in response")

        spot = float(spot)
        logger.info(f"NIFTY spot = {spot}")
        return spot

    except Exception as e:
        logger.error(f"Failed to get spot: {e}")
        logger.exception(e)
        return None

# --------------------------------------------------
# Select nearest ATM weekly option contracts
# --------------------------------------------------
def get_option_contracts():
    client = get_dhan_client()
    if not client:
        return {"error": "Dhan client not available"}

    # 1. Get spot price
    spot = get_nifty_spot(client)
    if spot is None:
        return {"error": "Could not fetch spot price"}

    # 2. Calculate ATM strike (multiple of 50)
    atm_strike = round(spot / 50) * 50
    logger.info(f"ATM strike = {atm_strike}")

    # 3. Load scrip master
    df = load_instruments()

    # 4. Filter Nifty options for the calculated strike
    nifty_options = df[
        (df["SEM_CUSTOM_SYMBOL"].astype(str).str.contains("NIFTY")) &
        (df["SEM_STRIKE_PRICE"].fillna(0).astype(float) == float(atm_strike))
    ]

    if nifty_options.empty:
        logger.error(f"No options found for strike {atm_strike}")
        return {"error": f"No options found for strike {atm_strike}"}

    # 5. Convert expiry dates and sort
    nifty_options["EXPIRY"] = pd.to_datetime(
        nifty_options["SEM_EXPIRY_DATE"],
        errors="coerce"
    )
    nifty_options = nifty_options.dropna(subset=["EXPIRY"])
    nifty_options = nifty_options.sort_values("EXPIRY")

    if nifty_options.empty:
        return {"error": "No valid expiry dates found"}

    # 6. Take the nearest expiry
    nearest_expiry = nifty_options["EXPIRY"].iloc[0]
    nearest_options = nifty_options[nifty_options["EXPIRY"] == nearest_expiry]

    # 7. Extract CE and PE records
    ce_records = nearest_options[nearest_options["SEM_OPTION_TYPE"] == "CE"]
    pe_records = nearest_options[nearest_options["SEM_OPTION_TYPE"] == "PE"]

    if ce_records.empty or pe_records.empty:
        return {"error": "CE or PE record missing for nearest expiry"}

    ce = ce_records.iloc[0]
    pe = pe_records.iloc[0]

    # 8. Return all details
    return {
        "spot": spot,
        "atm_strike": atm_strike,
        "expiry": str(nearest_expiry.date()),
        "ce_security_id": str(ce["SEM_SMST_SECURITY_ID"]),
        "pe_security_id": str(pe["SEM_SMST_SECURITY_ID"]),
        "ce_symbol": ce["SEM_CUSTOM_SYMBOL"],
        "pe_symbol": pe["SEM_CUSTOM_SYMBOL"]
    }

# --------------------------------------------------
# Flask routes
# --------------------------------------------------
@app.route("/")
def home():
    return jsonify({
        "status": "online",
        "message": "NIFTY ATM API Running (fixed spot extraction)"
    })

@app.route("/api/trading-signals")
def trading_signals():
    data = get_option_contracts()
    if "error" in data:
        return jsonify({
            "status": "error",
            "message": data["error"]
        }), 500
    return jsonify({
        "status": "success",
        "data": data
    })

# --------------------------------------------------
# Main
# --------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
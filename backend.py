import os
import math
import logging
import pandas as pd

from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import dhanhq

# =========================
# LOGGING
# =========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# FLASK APP
# =========================
app = Flask(__name__)
CORS(app)

application = app

# =========================
# DHAN CLIENT
# =========================
def get_dhan_client():
    try:
        client_id = os.getenv("DHAN_CLIENT_ID")
        access_token = os.getenv("DHAN_ACCESS_TOKEN")

        if not client_id or not access_token:
            logger.error("Missing Dhan credentials")
            return None

        client = dhanhq(client_id, access_token)

        logger.info("Dhan client initialized successfully")

        return client

    except Exception as e:
        logger.error(f"Dhan Init Error: {e}")
        return None

# =========================
# LOAD DHAN CSV
# =========================
def load_instruments():

    url = "https://images.dhan.co/api-data/api-scrip-master.csv"

    df = pd.read_csv(url, low_memory=False)

    return df

# =========================
# OPTION CONTRACT SELECTION
# =========================
def get_option_contracts():

    client = get_dhan_client()

    if not client:
        raise Exception("Dhan client failed")

    try:

        # =========================
        # NIFTY SPOT
        # =========================
        quote = client.quote_data({
            "IDX_I": [13]
        })

        logger.info(f"Nifty Quote Response: {quote}")

        if quote.get("status") != "success":
            raise Exception("Quote API failed")

        data = quote.get("data", {})

        nifty_data = data.get("IDX_I", {}).get("13", {})

        spot = nifty_data.get("last_price")

        if not spot:
            raise Exception("Spot price missing")

        spot = float(spot)

        logger.info(f"NIFTY Spot = {spot}")

        # =========================
        # ATM STRIKE
        # =========================
        atm = round(spot / 50) * 50

        logger.info(f"ATM Strike = {atm}")

        # =========================
        # LOAD CSV
        # =========================
        df = load_instruments()

        # =========================
        # FILTER NIFTY OPTIONS
        # =========================
        nifty = df[
            (df["SEM_CUSTOM_SYMBOL"].astype(str).str.contains("NIFTY")) &
            (df["SEM_STRIKE_PRICE"].fillna(0).astype(float) == float(atm))
        ]

        if nifty.empty:
            raise Exception("No option contracts found")

        # =========================
        # EXPIRY
        # =========================
        nifty["EXPIRY"] = pd.to_datetime(
            nifty["SEM_EXPIRY_DATE"],
            errors="coerce"
        )

        nifty = nifty.sort_values("EXPIRY")

        nearest_expiry = nifty["EXPIRY"].iloc[0]

        nifty = nifty[nifty["EXPIRY"] == nearest_expiry]

        # =========================
        # CE / PE
        # =========================
        ce = nifty[
            nifty["SEM_OPTION_TYPE"] == "CE"
        ].iloc[0]

        pe = nifty[
            nifty["SEM_OPTION_TYPE"] == "PE"
        ].iloc[0]

        return {
            "spot": spot,
            "atm": atm,
            "expiry": str(nearest_expiry.date()),
            "ce_security_id": str(ce["SEM_SMST_SECURITY_ID"]),
            "pe_security_id": str(pe["SEM_SMST_SECURITY_ID"]),
            "ce_symbol": ce["SEM_CUSTOM_SYMBOL"],
            "pe_symbol": pe["SEM_CUSTOM_SYMBOL"]
        }

    except Exception as e:
        logger.exception(f"Contract Selection Error: {e}")
        return None

# =========================
# HOME
# =========================
@app.route("/")
def home():
    return jsonify({
        "status": "online",
        "message": "NIFTY ATM API Running"
    })

# =========================
# API
# =========================
@app.route("/api/trading-signals")
def trading_signals():

    data = get_option_contracts()

    if not data:
        return jsonify({
            "status": "error",
            "message": "Unable to fetch contracts"
        }), 500

    return jsonify({
        "status": "success",
        "data": data
    })

# =========================
# MAIN
# =========================
if __name__ == "__main__":

    port = int(os.environ.get("PORT", 10000))

    app.run(
        host="0.0.0.0",
        port=port
    )
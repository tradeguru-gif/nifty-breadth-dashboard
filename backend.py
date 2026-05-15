import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import DhanContext, dhanhq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app

CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

def get_dhan_client():
    try:
        ctx = DhanContext(CLIENT_ID, ACCESS_TOKEN)
        return dhanhq(ctx)
    except Exception as e:
        logger.error(f"Failed to create Dhan client: {e}")
        return None

def get_nifty_spot():
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        time.sleep(0.5)
        response = session.get(url, headers=headers, timeout=10)
        data = response.json()
        spot = data['data'][0]['lastPrice']
        logger.info(f"NIFTY spot from NSE = {spot}")
        return float(spot)
    except Exception as e:
        logger.error(f"Failed to get spot from NSE: {e}")
        return None

def load_instruments():
    url = "https://images.dhan.co/api-data/api-scrip-master.csv"
    df = pd.read_csv(url, low_memory=False)
    logger.info(f"Loaded {len(df)} instruments")
    return df

def get_option_contracts():
    spot = get_nifty_spot()
    if spot is None:
        return {"error": "Could not fetch Nifty spot price from NSE"}

    atm_strike = round(spot / 50) * 50
    logger.info(f"ATM strike = {atm_strike}")

    try:
        df = load_instruments()
    except Exception as e:
        return {"error": f"Failed to load instruments: {str(e)}"}

    nifty_options = df[
        (df["SEM_CUSTOM_SYMBOL"].astype(str).str.contains("NIFTY")) &
        (df["SEM_STRIKE_PRICE"].fillna(0).astype(float) == float(atm_strike))
    ]
    if nifty_options.empty:
        return {"error": f"No options found for strike {atm_strike}"}

    nifty_options["EXPIRY"] = pd.to_datetime(nifty_options["SEM_EXPIRY_DATE"], errors="coerce")
    nifty_options = nifty_options.dropna(subset=["EXPIRY"]).sort_values("EXPIRY")
    if nifty_options.empty:
        return {"error": "No valid expiry dates found"}

    nearest_expiry = nifty_options["EXPIRY"].iloc[0]
    nearest_options = nifty_options[nifty_options["EXPIRY"] == nearest_expiry]
    ce_records = nearest_options[nearest_options["SEM_OPTION_TYPE"] == "CE"]
    pe_records = nearest_options[nearest_options["SEM_OPTION_TYPE"] == "PE"]

    if ce_records.empty or pe_records.empty:
        return {"error": "CE or PE record missing for nearest expiry"}

    ce = ce_records.iloc[0]
    pe = pe_records.iloc[0]
    return {
        "spot": spot,
        "atm_strike": atm_strike,
        "expiry": str(nearest_expiry.date()),
        "ce_security_id": str(ce["SEM_SMST_SECURITY_ID"]),
        "pe_security_id": str(pe["SEM_SMST_SECURITY_ID"]),
        "ce_symbol": ce["SEM_CUSTOM_SYMBOL"],
        "pe_symbol": pe["SEM_CUSTOM_SYMBOL"]
    }

@app.route("/")
def home():
    return jsonify({"status": "online", "message": "NIFTY ATM API Running"})

@app.route("/api/trading-signals")
def trading_signals():
    data = get_option_contracts()
    if "error" in data:
        return jsonify({"status": "error", "message": data["error"]}), 500
    return jsonify({"status": "success", "data": data})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
import os
import logging
import pandas as pd
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import dhanhq
from datetime import datetime

# =========================
# LOGGING
# =========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# FLASK
# =========================
app = Flask(__name__)
CORS(app)

application = app

# =========================
# ENV VARIABLES
# =========================
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

# =========================
# DHAN CLIENT
# =========================
def get_dhan_client():
    try:
        client = dhanhq(CLIENT_ID, ACCESS_TOKEN)
        logger.info("Dhan client initialized successfully")
        return client
    except Exception as e:
        logger.error(f"Dhan Init Error: {e}")
        return None

# =========================
# LOAD DHAN INSTRUMENT CSV
# =========================
def load_instruments():

    try:
        logger.info("Loading Dhan instrument master...")

        url = "https://images.dhan.co/api-data/api-scrip-master.csv"

        df = pd.read_csv(url, low_memory=False)

        df.columns = [c.strip().upper() for c in df.columns]

        return df

    except Exception as e:
        logger.error(f"Instrument Load Error: {e}")
        return None

# =========================
# GET LIVE NIFTY PRICE
# =========================
def get_nifty_spot(client):

    try:
        response = client.get_quote_data(
            {
                "IDX_I": ["13"]
            }
        )

        logger.info(f"Nifty Quote Response: {response}")

        data = response.get("data", {})

        if "IDX_I" in data:
            nifty_data = data["IDX_I"]["13"]

            spot = nifty_data.get("last_price")

            return float(spot)

        return None

    except Exception as e:
        logger.error(f"Nifty Spot Error: {e}")
        return None

# =========================
# FIND ATM CONTRACTS
# =========================
def get_option_contracts():

    try:

        client = get_dhan_client()

        if not client:
            return None

        spot = get_nifty_spot(client)

        if not spot:
            raise Exception("Unable to fetch NIFTY spot")

        logger.info(f"NIFTY Spot = {spot}")

        atm = round(spot / 50) * 50

        logger.info(f"ATM Strike = {atm}")

        df = load_instruments()

        if df is None:
            raise Exception("Instrument CSV not loaded")

        # FILTER NIFTY OPTIONS
        opts = df[
            (df["SEM_INSTRUMENT_NAME"] == "OPTIDX")
            &
            (
                df["SEM_TRADING_SYMBOL"]
                .astype(str)
                .str.contains("NIFTY", na=False)
            )
        ].copy()

        logger.info(f"Option rows = {len(opts)}")

        if len(opts) == 0:
            raise Exception("No option rows found")

        # CONVERT STRIKE
        opts["SEM_STRIKE_PRICE"] = pd.to_numeric(
            opts["SEM_STRIKE_PRICE"],
            errors="coerce"
        )

        # EXPIRY
        opts["SEM_EXPIRY_DATE"] = pd.to_datetime(
            opts["SEM_EXPIRY_DATE"],
            errors="coerce"
        )

        opts = opts.dropna(subset=["SEM_EXPIRY_DATE"])

        nearest_expiry = opts["SEM_EXPIRY_DATE"].min()

        logger.info(f"Nearest expiry = {nearest_expiry}")

        opts = opts[
            opts["SEM_EXPIRY_DATE"] == nearest_expiry
        ]

        # CE
        ce = opts[
            (
                opts["SEM_OPTION_TYPE"]
                .astype(str)
                .str.upper() == "CE"
            )
            &
            (
                opts["SEM_STRIKE_PRICE"] == atm
            )
        ]

        # PE
        pe = opts[
            (
                opts["SEM_OPTION_TYPE"]
                .astype(str)
                .str.upper() == "PE"
            )
            &
            (
                opts["SEM_STRIKE_PRICE"] == atm
            )
        ]

        if len(ce) == 0:
            raise Exception("No CE found")

        if len(pe) == 0:
            raise Exception("No PE found")

        ce_row = ce.iloc[0]
        pe_row = pe.iloc[0]

        result = {
            "spot": spot,
            "atm": atm,
            "expiry": str(nearest_expiry.date()),
            "ce_security_id": str(ce_row["SEM_SMST_SECURITY_ID"]),
            "pe_security_id": str(pe_row["SEM_SMST_SECURITY_ID"]),
            "ce_symbol": ce_row["SEM_TRADING_SYMBOL"],
            "pe_symbol": pe_row["SEM_TRADING_SYMBOL"],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        logger.info(f"Selected Contracts: {result}")

        return result

    except Exception as e:
        logger.exception(f"Contract Selection Error: {e}")
        return None

# =========================
# ROUTES
# =========================
@app.route("/")
def home():
    return jsonify({
        "status": "online",
        "service": "NIFTY ATM Selector"
    })

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

@app.route("/api/health")
def health():
    return "OK", 200

# =========================
# MAIN
# =========================
if __name__ == "__main__":

    port = int(os.environ.get("PORT", 10000))

    app.run(
        host="0.0.0.0",
        port=port
    )
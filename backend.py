import os
import threading
import logging
import time
import pandas as pd

from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

from dhanhq import dhanhq as DhanHQ
from dhanhq import marketfeed

# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# Flask App
# ------------------------------------------------------------
app = Flask(__name__)
CORS(app)

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
CURRENT_NIFTY = int(os.getenv("CURRENT_NIFTY", "24000"))

# ------------------------------------------------------------
# Dhan Client
# ------------------------------------------------------------
dhan = DhanHQ(CLIENT_ID, ACCESS_TOKEN)

# ------------------------------------------------------------
# Global State
# ------------------------------------------------------------
latest_data = {
    "signal": "WAITING",
    "ce_price": 0,
    "pe_price": 0,
    "spread": 0,
    "rsi": 50,
    "macd": 0,
    "timestamp": ""
}

SELECTED_CE_ID = None
SELECTED_PE_ID = None

price_history = []

# ------------------------------------------------------------
# Load Instrument Master
# ------------------------------------------------------------
def load_instruments():
    url = "https://images.dhan.co/api-data/api-scrip-master.csv"
    df = pd.read_csv(url, low_memory=False)
    df.columns = df.columns.str.strip()

    df["SEM_INSTRUMENT_NAME"] = df["SEM_INSTRUMENT_NAME"].astype(str).str.upper()
    df["SM_SYMBOL_NAME"] = df["SM_SYMBOL_NAME"].astype(str).str.upper()

    return df

# ------------------------------------------------------------
# Contract Selection
# ------------------------------------------------------------
def get_option_contracts(nifty_price):
    global SELECTED_CE_ID, SELECTED_PE_ID

    try:
        logger.info("Loading instrument master...")

        df = load_instruments()

        opts = df[
            df["SEM_INSTRUMENT_NAME"].str.contains("OPT", na=False)
            & df["SM_SYMBOL_NAME"].str.contains("NIFTY", na=False)
        ].copy()

        logger.info(f"Option rows: {len(opts)}")

        if opts.empty:
            raise Exception("No option data found")

        opts["SEM_STRIKE_PRICE"] = pd.to_numeric(opts["SEM_STRIKE_PRICE"], errors="coerce")
        opts["SEM_EXPIRY_DATE"] = pd.to_datetime(opts["SEM_EXPIRY_DATE"], errors="coerce")

        opts = opts.dropna(subset=["SEM_STRIKE_PRICE", "SEM_EXPIRY_DATE"])

        expiry = opts["SEM_EXPIRY_DATE"].min()
        opts = opts[opts["SEM_EXPIRY_DATE"] == expiry]

        atm = round(nifty_price / 50) * 50

        ce = opts[(opts["SEM_OPTION_TYPE"] == "CE")]
        pe = opts[(opts["SEM_OPTION_TYPE"] == "PE")]

        if ce.empty or pe.empty:
            raise Exception("CE/PE missing")

        ce["diff"] = abs(ce["SEM_STRIKE_PRICE"] - atm)
        pe["diff"] = abs(pe["SEM_STRIKE_PRICE"] - atm)

        ce_row = ce.sort_values("diff").iloc[0]
        pe_row = pe.sort_values("diff").iloc[0]

        SELECTED_CE_ID = str(ce_row["SEM_SMST_SECURITY_ID"])
        SELECTED_PE_ID = str(pe_row["SEM_SMST_SECURITY_ID"])

        logger.info(f"CE ID: {SELECTED_CE_ID}")
        logger.info(f"PE ID: {SELECTED_PE_ID}")

        return [SELECTED_CE_ID, SELECTED_PE_ID]

    except Exception as e:
        logger.exception(f"Contract error: {e}")
        return []

# ------------------------------------------------------------
# Signal Update
# ------------------------------------------------------------
def update_signal(ce, pe):
    global latest_data

    spread = ce - pe

    latest_data["ce_price"] = ce
    latest_data["pe_price"] = pe
    latest_data["spread"] = spread
    latest_data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if spread > 5:
        latest_data["signal"] = "BULLISH"
    elif spread < -5:
        latest_data["signal"] = "BEARISH"
    else:
        latest_data["signal"] = "NEUTRAL"

# ------------------------------------------------------------
# Websocket Callback
# ------------------------------------------------------------
def on_message(instance, tick):
    global latest_data

    try:
        sid = str(tick.get("security_id"))
        price = tick.get("ltp", 0)

        if sid == SELECTED_CE_ID:
            latest_data["ce_price"] = price

        elif sid == SELECTED_PE_ID:
            latest_data["pe_price"] = price

        if latest_data["ce_price"] and latest_data["pe_price"]:
            update_signal(
                latest_data["ce_price"],
                latest_data["pe_price"]
            )

    except Exception as e:
        logger.error(f"on_message error: {e}")

# ------------------------------------------------------------
# Feed Runner
# ------------------------------------------------------------
def run_feed():

    global SELECTED_CE_ID, SELECTED_PE_ID

    while True:

        try:
            logger.info("Selecting contracts...")

            get_option_contracts(CURRENT_NIFTY)

            if not SELECTED_CE_ID or not SELECTED_PE_ID:
                logger.error("No contracts found")
                time.sleep(10)
                continue

            feed = marketfeed.DhanFeed(
                client_id=CLIENT_ID,
                access_token=ACCESS_TOKEN,
                instruments=[
                    (marketfeed.NSE_FNO, SELECTED_CE_ID, marketfeed.Ticker),
                    (marketfeed.NSE_FNO, SELECTED_PE_ID, marketfeed.Ticker)
                ],
                on_message=on_message
            )

            logger.info("Starting feed...")
            feed.run_forever()

        except Exception as e:
            logger.exception(f"Feed crashed: {e}")
            time.sleep(5)

# ------------------------------------------------------------
# Routes
# ------------------------------------------------------------
@app.route("/")
def home():
    return jsonify(latest_data)

@app.route("/api/health")
def health():
    return "OK", 200

# ------------------------------------------------------------
# Start
# ------------------------------------------------------------
threading.Thread(target=run_feed, daemon=True).start()

application = app

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
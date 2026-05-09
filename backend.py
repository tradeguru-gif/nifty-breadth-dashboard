# backend.py - Nifty Options Signal Engine (Fully Corrected)

import os
import threading
import logging
import time
import requests
import pandas as pd

from collections import deque
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

from dhanhq import dhanhq, marketfeed

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
# Environment Variables
# ------------------------------------------------------------
# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
CURRENT_NIFTY = float(os.getenv("CURRENT_NIFTY", "24000"))

# ------------------------------------------------------------
# Initialize Dhan Client
# ------------------------------------------------------------
dhan = dhanhq(
    CLIENT_ID,
    ACCESS_TOKEN
)
# ------------------------------------------------------------
# Global Variables
# ------------------------------------------------------------
latest_data = {
    "signal": "WAITING",
    "ce_price": 0,
    "pe_price": 0,
    "spread": 0,
    "rsi": 50,
    "macd": 0,
    "pcr": 1.0,
    "timestamp": ""
}

SELECTED_CE_ID = None
SELECTED_PE_ID = None

price_history = deque(maxlen=100)

update_counter = 0

UPDATE_INTERVAL = 10

SPREAD_THRESHOLD = 5.0

# ------------------------------------------------------------
# Indicators
# ------------------------------------------------------------
def calculate_rsi(prices, period=14):

    if len(prices) < period + 1:
        return 50.0

    deltas = [
        prices[i] - prices[i - 1]
        for i in range(1, len(prices))
    ]

    gains = [d if d > 0 else 0 for d in deltas]

    losses = [-d if d < 0 else 0 for d in deltas]

    avg_gain = sum(gains[:period]) / period

    avg_loss = sum(losses[:period]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


def calculate_macd(prices, fast=12, slow=26):

    if len(prices) < slow:
        return 0.0

    def ema(data, period):

        alpha = 2 / (period + 1)

        value = data[0]

        for price in data[1:]:
            value = alpha * price + (1 - alpha) * value

        return value

    ema_fast = ema(prices, fast)

    ema_slow = ema(prices, slow)

    return ema_fast - ema_slow


# ------------------------------------------------------------
# PCR
# ------------------------------------------------------------
def get_nifty_pcr():

    url = (
        "https://www.nseindia.com/api/"
        "option-chain-indices?symbol=NIFTY"
    )

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    try:

        session = requests.Session()

        session.get(
            "https://www.nseindia.com",
            headers=headers
        )

        time.sleep(0.5)

        response = session.get(
            url,
            headers=headers
        )

        data = response.json()

        total_ce_oi = 0
        total_pe_oi = 0

        for record in data["records"]["data"]:

            if "CE" in record:
                total_ce_oi += record["CE"]["openInterest"]

            if "PE" in record:
                total_pe_oi += record["PE"]["openInterest"]

        if total_pe_oi == 0:
            return 1.0

        return total_ce_oi / total_pe_oi

    except Exception as e:

        logger.error(f"PCR fetch failed: {e}")

        return 1.0



# ------------------------------------------------------------
# Dynamic Option Contract Selection
# ------------------------------------------------------------
# ------------------------------------------------------------
# Dynamically select nearest NIFTY CE/PE contracts
# ------------------------------------------------------------
def get_option_contracts(nifty_price):
    global SELECTED_CE_ID, SELECTED_PE_ID

    try:
        logger.info("Fetching instrument master...")

        # ------------------------------------------------
        # Fetch instrument master
        # ------------------------------------------------
        df = dhan.get_instrument_list()

        # Normalize columns
        df.columns = df.columns.str.strip().str.upper()

        logger.info(f"Columns: {df.columns.tolist()}")

        # ------------------------------------------------
        # REQUIRED COLUMNS
        # ------------------------------------------------
        security_col = "SEM_SMST_SECURITY_ID"
        symbol_col = "SEM_TRADING_SYMBOL"
        strike_col = "SEM_STRIKE_PRICE"
        option_col = "SEM_OPTION_TYPE"
        expiry_col = "SEM_EXPIRY_DATE"
        instrument_col = "SEM_INSTRUMENT_NAME"

        # ------------------------------------------------
        # FILTER ONLY NIFTY OPTIONS
        # ------------------------------------------------
        opts = df[
            df[instrument_col]
            .astype(str)
            .str.upper()
            .str.contains("OPTIDX", na=False)
        ].copy()

        logger.info(f"OPTION rows: {len(opts)}")

        # ------------------------------------------------
        # FILTER NIFTY ONLY
        # ------------------------------------------------
       opts = opts[
    opts[symbol_col]
    .astype(str)
    .str.upper()
    .str.contains("NIFTY", na=False)
].copy()
        logger.info(f"NIFTY option rows: {len(opts)}")

        # ------------------------------------------------
        # CONVERT STRIKE
        # ------------------------------------------------
        opts[strike_col] = (
            opts[strike_col]
            .astype(str)
            .str.replace(",", "")
            .astype(float)
        )

        # ------------------------------------------------
        # CONVERT EXPIRY
        # ------------------------------------------------
        opts[expiry_col] = pd.to_datetime(
            opts[expiry_col],
            errors="coerce"
        )

        opts = opts.dropna(subset=[expiry_col])

        if opts.empty:
            raise Exception("No valid expiry rows")

        # ------------------------------------------------
        # NEAREST EXPIRY
        # ------------------------------------------------
        nearest_expiry = opts[expiry_col].min()

        logger.info(f"Nearest expiry: {nearest_expiry}")

        opts_nearest = opts[
            opts[expiry_col] == nearest_expiry
        ].copy()

        logger.info(f"Nearest expiry rows: {len(opts_nearest)}")

        # ------------------------------------------------
        # ATM STRIKE
        # ------------------------------------------------
        atm = round(nifty_price / 50) * 50

        logger.info(f"NIFTY PRICE: {nifty_price}")
        logger.info(f"ATM STRIKE: {atm}")

        # ------------------------------------------------
        # CE CONTRACTS
        # ------------------------------------------------
        ce_df = opts_nearest[
            opts_nearest[option_col]
            .astype(str)
            .str.upper()
            .str.contains("CE", na=False)
        ].copy()

        # ------------------------------------------------
        # PE CONTRACTS
        # ------------------------------------------------
        pe_df = opts_nearest[
            opts_nearest[option_col]
            .astype(str)
            .str.upper()
            .str.contains("PE", na=False)
        ].copy()

        logger.info(f"CE contracts found: {len(ce_df)}")
        logger.info(f"PE contracts found: {len(pe_df)}")

        if ce_df.empty:
            raise Exception("No CE contract found")

        if pe_df.empty:
            raise Exception("No PE contract found")

        # ------------------------------------------------
        # FIND NEAREST CE STRIKE
        # ------------------------------------------------
        ce_df["DIFF"] = (
            ce_df[strike_col] - atm
        ).abs()

        ce_row = ce_df.sort_values("DIFF").iloc[0]

        # ------------------------------------------------
        # FIND NEAREST PE STRIKE
        # ------------------------------------------------
        pe_df["DIFF"] = (
            pe_df[strike_col] - atm
        ).abs()

        pe_row = pe_df.sort_values("DIFF").iloc[0]

        # ------------------------------------------------
        # SAVE IDS
        # ------------------------------------------------
        SELECTED_CE_ID = str(ce_row[security_col])

        SELECTED_PE_ID = str(pe_row[security_col])

        logger.info(
            f"Selected CE: {SELECTED_CE_ID} | "
            f"{ce_row[symbol_col]}"
        )

        logger.info(
            f"Selected PE: {SELECTED_PE_ID} | "
            f"{pe_row[symbol_col]}"
        )

        return [SELECTED_CE_ID, SELECTED_PE_ID]

    except Exception as e:
        logger.exception(
            f"Dynamic contract selection failed: {e}"
        )

        SELECTED_CE_ID = None
        SELECTED_PE_ID = None

        return []
# ------------------------------------------------------------
# Signal Logic
# ------------------------------------------------------------
def update_signal(ce_price, pe_price):

    global latest_data
    global price_history
    global update_counter

    spread = ce_price - pe_price

    latest_data["spread"] = round(spread, 2)

    if ce_price > 0:
        price_history.append(ce_price)

    update_counter += 1

    if (
        update_counter >= UPDATE_INTERVAL
        and len(price_history) >= 20
    ):

        update_counter = 0

        rsi = calculate_rsi(
            list(price_history)
        )

        macd = calculate_macd(
            list(price_history)
        )

        pcr = get_nifty_pcr()

        latest_data["rsi"] = round(rsi, 2)

        latest_data["macd"] = round(macd, 2)

        latest_data["pcr"] = round(pcr, 2)

        # ----------------------------------------------------
        # Signal Rules
        # ----------------------------------------------------
        if (
            spread > SPREAD_THRESHOLD
            and rsi < 70
            and macd > 0
            and pcr < 0.8
        ):

            signal = "LONG SPREAD (Bullish)"

        elif (
            spread < -SPREAD_THRESHOLD
            and rsi > 30
            and macd < 0
            and pcr > 1.2
        ):

            signal = "SHORT SPREAD (Bearish)"

        else:

            signal = "NEUTRAL"

        latest_data["signal"] = signal

    latest_data["timestamp"] = (
        datetime.now()
        .strftime("%Y-%m-%d %H:%M:%S")
    )


# ------------------------------------------------------------
# WebSocket Callback
# ------------------------------------------------------------
def on_message(instance, tick):

    global latest_data

    try:

        sec_id = str(
            tick.get("security_id")
        )

        price = tick.get("ltp", 0)

        if sec_id == SELECTED_CE_ID:

            latest_data["ce_price"] = price

        elif sec_id == SELECTED_PE_ID:

            latest_data["pe_price"] = price

        # ----------------------------------------------------
        # Update signal
        # ----------------------------------------------------
        if (
            latest_data["ce_price"] > 0
            and latest_data["pe_price"] > 0
        ):

            update_signal(
                latest_data["ce_price"],
                latest_data["pe_price"]
            )

        else:

            latest_data["timestamp"] = (
                datetime.now()
                .strftime("%Y-%m-%d %H:%M:%S")
            )

    except Exception as e:

        logger.error(
            f"on_message error: {e}"
        )


# ------------------------------------------------------------
# WebSocket Feed Runner
# ------------------------------------------------------------
def run_feed():

    global SELECTED_CE_ID
    global SELECTED_PE_ID

    while True:

        try:

            logger.info(
                "Selecting option contracts..."
            )

            # ------------------------------------------------
            # Get CE/PE contracts
            # ------------------------------------------------
            get_option_contracts(
                CURRENT_NIFTY
            )

            # ------------------------------------------------
            # Validate IDs
            # ------------------------------------------------
            if (
                not SELECTED_CE_ID
                or not SELECTED_PE_ID
            ):

                logger.error(
                    "No valid contracts found. "
                    "Retrying in 30 seconds..."
                )

                time.sleep(30)

                continue

            logger.info(
                f"Subscribing to "
                f"CE={SELECTED_CE_ID}, "
                f"PE={SELECTED_PE_ID}"
            )

            # ------------------------------------------------
            # Create Feed
            # ------------------------------------------------
            feed = marketfeed.DhanFeed(

                client_id=CLIENT_ID,

                access_token=ACCESS_TOKEN,

                instruments=[

                    (
                        marketfeed.NSE_FNO,
                        str(SELECTED_CE_ID),
                        marketfeed.Ticker
                    ),

                    (
                        marketfeed.NSE_FNO,
                        str(SELECTED_PE_ID),
                        marketfeed.Ticker
                    )
                ],

                on_connect=lambda instance:
                logger.info(
                    "✅ Connected to Dhan WebSocket"
                ),

                on_message=on_message,

                on_error=lambda instance, err:
                logger.error(
                    f"❌ WebSocket Error: {err}"
                ),

                on_close=lambda instance:
                logger.warning(
                    "⚠️ WebSocket closed. "
                    "Reconnecting..."
                )
            )

            logger.info(
                "Starting Dhan live feed..."
            )

            feed.run_forever()

        except Exception as e:

            logger.exception(
                f"Feed crashed: {e}"
            )

            logger.info(
                "Retrying in 10 seconds..."
            )

            time.sleep(10)


# ------------------------------------------------------------
# Flask Routes
# ------------------------------------------------------------
@app.route("/")
def home():

    return jsonify({
        "status": "active",
        "data": latest_data
    })


@app.route("/api/health")
def health():

    return "OK", 200


# ------------------------------------------------------------
# WSGI
# ------------------------------------------------------------
application = app

# ------------------------------------------------------------
# Start Background Thread
# ------------------------------------------------------------
threading.Thread(
    target=run_feed,
    daemon=True
).start()

logger.info(
    "Background signal engine started."
)

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(
            os.environ.get("PORT", 10000)
        )
    )
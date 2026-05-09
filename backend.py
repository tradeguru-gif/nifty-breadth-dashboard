# backend.py - Nifty Options Signal Engine (Fully Corrected)

import os
import asyncio
import threading
import logging
import time
import requests
import pandas as pd

from collections import deque
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
# Environment Variables
# ------------------------------------------------------------

CLIENT_ID = os.getenv("DHAN_CLIENT_ID")

ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

CURRENT_NIFTY = float(os.getenv("CURRENT_NIFTY", "24000"))

# ------------------------------------------------------------
# Initialize Dhan Client
# ------------------------------------------------------------
dhan = DhanHQ(
    CLIENT_ID,
    ACCESS_TOKEN
)

logger.info("Dhan client initialized successfully")

#-----------------------------------------------------
# INSTRUMENT METER
#---------------------------------------------------------

import pandas as pd

def load_instruments():

    url = "https://images.dhan.co/api-data/api-scrip-master.csv"

    df = pd.read_csv(url)

    return df

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

    global SELECTED_CE_ID
    global SELECTED_PE_ID

    try:

        logger.info("Loading Dhan instrument master...")

        url = "https://images.dhan.co/api-data/api-scrip-master.csv"

        df = pd.read_csv(url)

        # Only NIFTY options
        opts = df[
            (df["SEM_INSTRUMENT_NAME"] == "OPTIDX") &
            (df["SM_SYMBOL_NAME"] == "NIFTY")
        ].copy()

        logger.info(f"NIFTY option rows: {len(opts)}")

        # Convert strike
        opts["SEM_STRIKE_PRICE"] = (
            opts["SEM_STRIKE_PRICE"]
            .astype(float)
        )

        # Convert expiry
        opts["SEM_EXPIRY_DATE"] = pd.to_datetime(
            opts["SEM_EXPIRY_DATE"],
            errors="coerce"
        )

        opts = opts.dropna(
            subset=["SEM_EXPIRY_DATE"]
        )

        # Nearest expiry
        nearest_expiry = opts[
            "SEM_EXPIRY_DATE"
        ].min()

        opts = opts[
            opts["SEM_EXPIRY_DATE"] == nearest_expiry
        ]

        logger.info(
            f"Nearest expiry: {nearest_expiry}"
        )

        # ATM
        atm = round(nifty_price / 50) * 50

        logger.info(f"ATM Strike: {atm}")

        # CE
        ce_df = opts[
            (opts["SEM_OPTION_TYPE"] == "CE") &
            (opts["SEM_STRIKE_PRICE"] == atm)
        ]

        # PE
        pe_df = opts[
            (opts["SEM_OPTION_TYPE"] == "PE") &
            (opts["SEM_STRIKE_PRICE"] == atm)
        ]

        if ce_df.empty:
            raise Exception("No CE found")

        if pe_df.empty:
            raise Exception("No PE found")

        ce_row = ce_df.iloc[0]
        pe_row = pe_df.iloc[0]

        SELECTED_CE_ID = str(
            ce_row["SEM_SMST_SECURITY_ID"]
        )

        SELECTED_PE_ID = str(
            pe_row["SEM_SMST_SECURITY_ID"]
        )

        logger.info(
            f"Selected CE: {SELECTED_CE_ID}"
        )

        logger.info(
            f"Selected PE: {SELECTED_PE_ID}"
        )

        logger.info(
            f"CE Symbol: {ce_row['SEM_TRADING_SYMBOL']}"
        )

        logger.info(
            f"PE Symbol: {pe_row['SEM_TRADING_SYMBOL']}"
        )

        return [
            SELECTED_CE_ID,
            SELECTED_PE_ID
        ]

    except Exception as e:

        logger.exception(
            f"Contract selection failed: {e}"
        )

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

    # ------------------------------------------------
    # CREATE EVENT LOOP FOR THIS THREAD
    # ------------------------------------------------
    asyncio.set_event_loop(
        asyncio.new_event_loop()
    )

    while True:

        try:

            logger.info(
                "Selecting option contracts..."
            )

            get_option_contracts(
                CURRENT_NIFTY
            )

            if (
                not SELECTED_CE_ID
                or not SELECTED_PE_ID
            ):

                logger.error(
                    "No valid contracts found."
                )

                time.sleep(30)

                continue

            logger.info(
                f"Subscribing to "
                f"CE={SELECTED_CE_ID}, "
                f"PE={SELECTED_PE_ID}"
            )

            # ------------------------------------------------
            # CREATE FEED
            # ------------------------------------------------
            feed = marketfeed.DhanFeed(

                CLIENT_ID,

                ACCESS_TOKEN,

                [
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
                ]
            )

            logger.info(
                "✅ Dhan Feed Started"
            )

            # ------------------------------------------------
            # START FEED
            # ------------------------------------------------
            feed.run_forever()

        except Exception as e:

            logger.exception(
                f"Feed crashed: {e}"
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
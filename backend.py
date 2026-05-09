import os
import asyncio
import threading
import time
import logging
from datetime import datetime
import pandas as pd
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import DhanHQ, MarketFeed

# ===================================================
# CONFIGURATION
# ===================================================
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
CURRENT_NIFTY = int(os.getenv("CURRENT_NIFTY", "0"))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app) # CRITICAL for WordPress connection

latest_data = {
    "signal": "INITIALIZING",
    "ce_price": 0.0,
    "pe_price": 0.0,
    "spread": 0.0,
    "timestamp": ""
}

SELECTED_CE_ID = ""
SELECTED_PE_ID = ""

# ===================================================
# 1. DYNAMIC OPTION CONTRACT SELECTION
# ===================================================
def get_option_contracts(nifty_spot):
    global SELECTED_CE_ID, SELECTED_PE_ID
    dhan = DhanHQ(CLIENT_ID, ACCESS_TOKEN)
    logger.info("Downloading instrument master...")
    
    df = dhan.fetch_security_list("detailed")
    if df is None or df.empty:
        return ["0", "0"] 

    fno = df[df["SEGMENT"] == "NSE_FNO"]
    opts = fno[fno["INSTRUMENT"] == "OPTIDX"].copy()
    
    opts["EXPIRY_DT"] = pd.to_datetime(opts["SEM_EXPIRY_DATE"], errors="coerce")
    nearest_expiry = opts["EXPIRY_DT"].min()
    opts_nearest = opts[opts["EXPIRY_DT"] == nearest_expiry].copy()
    
    opts_nearest["STRIKE"] = pd.to_numeric(opts_nearest["STRIKE_PRICE"], errors="coerce")
    
    ce_contract = opts_nearest[opts_nearest["OPTION_TYPE"] == "CE"].sort_values("STRIKE")
    ce_contract = ce_contract[ce_contract["STRIKE"] >= nifty_spot].iloc[0]
    
    pe_contract = opts_nearest[opts_nearest["OPTION_TYPE"] == "PE"].sort_values("STRIKE", ascending=False)
    pe_contract = pe_contract[pe_contract["STRIKE"] <= nifty_spot].iloc[0]
    
    SELECTED_CE_ID = str(ce_contract["SECURITY_ID"])
    SELECTED_PE_ID = str(pe_contract["SECURITY_ID"])
    
    return [SELECTED_CE_ID, SELECTED_PE_ID]

# ===================================================
# 2. WEBSOCKET LOGIC
# ===================================================
ticker_prices = {}

def on_message(instance, tick):
    global latest_data
    sec_id = str(tick.get('security_id'))
    price = tick.get('ltp', 0)
    
    if price > 0:
        ticker_prices[sec_id] = price
        
    if SELECTED_CE_ID in ticker_prices and SELECTED_PE_ID in ticker_prices:
        ce_p = ticker_prices[SELECTED_CE_ID]
        pe_p = ticker_prices[SELECTED_PE_ID]
        spread = ce_p - pe_p
        
        latest_data.update({
            "signal": "BULLISH" if spread > 5 else "BEARISH" if spread < -5 else "NEUTRAL",
            "ce_price": ce_p,
            "pe_price": pe_p,
            "spread": round(spread, 2),
            "timestamp": datetime.now().strftime("%H:%M:%S")
        })

async def run_feed():
    try:
        instrument_ids = get_option_contracts(CURRENT_NIFTY)
        logger.info(f"Targeting IDs: CE={SELECTED_CE_ID}, PE={SELECTED_PE_ID}")
    except Exception as e:
        logger.error(f"Error selecting contracts: {e}")
        return

    # Initialize directly without DhanContext to support v2.0+
    feed = MarketFeed(CLIENT_ID, ACCESS_TOKEN, version='v2')
    feed.on_message = on_message
    
    await feed.connect()
    
    # 1 = NSE_FNO, 15 = Full/Ticker mode
   # Use '1' for NSE_FNO and '15' for the data mode
subscription = [(1, SELECTED_CE_ID, 15), (1, SELECTED_PE_ID, 15)]
    ]
    
    await feed.subscribe_symbols(subscription)
    logger.info("🚀 WebSocket Subscription Active!")
    
    while True:
        await asyncio.sleep(1)

def start_engine():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run_feed())

# ===================================================
# 3. ROUTES & MAIN
# ===================================================
@app.route('/')
def home():
    return jsonify({"status": "running", "data": latest_data})

@app.route('/api/health')
def health():
    return "OK", 200

# Required for Gunicorn to find the app instance
application = app

if __name__ == '__main__':
    threading.Thread(target=start_engine, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
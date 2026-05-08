import os
import asyncio
import threading
import time
import logging
from datetime import datetime
import pandas as pd
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import DhanContext, DhanHQ, MarketFeed

# ===================================================
# CONFIGURATION
# ===================================================
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
# Note: On Render, ensure CURRENT_NIFTY is set in Environment Variables (e.g., 24300)
CURRENT_NIFTY = int(os.getenv("CURRENT_NIFTY", "0"))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Global storage for the API to read
latest_data = {
    "signal": "INITIALIZING",
    "ce_price": 0.0,
    "pe_price": 0.0,
    "spread": 0.0,
    "timestamp": ""
}

# ===================================================
# 1. DYNAMIC OPTION CONTRACT SELECTION
# ===================================================
def get_option_contracts(nifty_spot):
    dhan = DhanHQ(CLIENT_ID, ACCESS_TOKEN)
    logger.info("Downloading instrument master...")
    
    # Note: detailed list is heavy; for Nifty, we filter for NSE_FNO
    df = dhan.fetch_security_list("detailed")
    if df is None or df.empty:
        return ["13", "13"] # Fallback to Index ID if master fails

    fno = df[df["SEGMENT"] == "NSE_FNO"]
    opts = fno[fno["INSTRUMENT"] == "OPTIDX"].copy()
    
    opts["EXPIRY_DT"] = pd.to_datetime(opts["SEM_EXPIRY_DATE"], errors="coerce")
    nearest_expiry = opts["EXPIRY_DT"].min()
    opts_nearest = opts[opts["EXPIRY_DT"] == nearest_expiry].copy()
    
    opts_nearest["STRIKE"] = pd.to_numeric(opts_nearest["STRIKE_PRICE"], errors="coerce")
    
    # CE just above spot
    ce = opts_nearest[opts_nearest["OPTION_TYPE"] == "CE"].sort_values("STRIKE")
    ce_contract = ce[ce["STRIKE"] >= nifty_spot].iloc[0]
    
    # PE just below spot
    pe = opts_nearest[opts_nearest["OPTION_TYPE"] == "PE"].sort_values("STRIKE", ascending=False)
    pe_contract = pe[pe["STRIKE"] <= nifty_spot].iloc[0]
    
    return [str(ce_contract["SECURITY_ID"]), str(pe_contract["SECURITY_ID"])]

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
        
    if len(ticker_prices) >= 2:
        ids = list(ticker_prices.keys())
        ce_p = ticker_prices.get(ids[0], 0)
        pe_p = ticker_prices.get(ids[1], 0)
        spread = ce_p - pe_p
        
        latest_data.update({
            "signal": "BULLISH" if spread > 5 else "BEARISH" if spread < -5 else "NEUTRAL",
            "ce_price": ce_p,
            "pe_price": pe_p,
            "spread": round(spread, 2),
            "timestamp": datetime.now().strftime("%H:%M:%S")
        })

async def run_feed():
    if CURRENT_NIFTY == 0:
        logger.error("CURRENT_NIFTY environment variable not set!")
        return

    # 1. Get IDs
    try:
        instrument_ids = get_option_contracts(CURRENT_NIFTY)
    except Exception as e:
        logger.error(f"Error selecting contracts: {e}")
        instrument_ids = ["13"] # Fallback

    # 2. Init Feed
    ctx = DhanContext(CLIENT_ID, ACCESS_TOKEN)
    feed = MarketFeed(ctx, version='v2')
    
    feed.on_message = on_message
    feed.on_connect = lambda instance: logger.info("✅ Connected to Dhan")
    
    await feed.connect()
    
    # 3. Subscribe (2 = NSE_INDEX for Index, but 1 = NSE_FNO for Options)
    # We use 1 because we are subscribing to Option Security IDs
    subscription = [(1, instrument_ids[0], 15), (1, instrument_ids[1], 15)]
    await feed.subscribe_symbols(subscription)
    
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

if __name__ == '__main__':
    # Start the background engine
    threading.Thread(target=start_engine, daemon=True).start()
    # Run Flask
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
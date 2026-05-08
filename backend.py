# backend.py - Nifty Options Signal Engine (No external libraries required)
import os
import asyncio
import threading
import time
import logging
from typing import List, Tuple
from datetime import datetime

import pandas as pd
from flask import Flask
from dhanhq import DhanContext, dhanhq, marketfeed

# ===================================================
# CONFIGURATION
# ===================================================
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
CONTRACT_TYPE = os.getenv("CONTRACT_TYPE", "OPTIONS")
CURRENT_NIFTY = int(os.getenv("CURRENT_NIFTY", "0"))

if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")
if CONTRACT_TYPE != "OPTIONS":
    raise ValueError("CONTRACT_TYPE must be 'OPTIONS'")
if CURRENT_NIFTY == 0:
    raise ValueError("CURRENT_NIFTY must be set to today's Nifty spot")

# Signal thresholds
SPREAD_THRESHOLD = 5.0
UPPER_PCR = 1.2
LOWER_PCR = 0.8

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ===================================================
# 1. DYNAMIC OPTION CONTRACT SELECTION
# ===================================================
def get_option_contracts(current_nifty: int) -> Tuple[List[str], List[dict]]:
    dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
    dhan = dhanhq(dhan_context)
    logger.info("Downloading instrument master...")
    df = dhan.fetch_security_list("detailed")
    fno = df[df["SEGMENT"] == "NSE_FNO"]
    opts = fno[fno["INSTRUMENT"] == "OPTIDX"].copy()
    opts["EXPIRY_DT"] = pd.to_datetime(opts["SEM_EXPIRY_DATE"], format="%d-%b-%Y", errors="coerce")
    opts = opts.dropna(subset=["EXPIRY_DT"])
    nearest_expiry = opts["EXPIRY_DT"].min()
    opts_nearest = opts[opts["EXPIRY_DT"] == nearest_expiry]
    opts_nearest["STRIKE"] = pd.to_numeric(opts_nearest["STRIKE_PRICE"], errors="coerce")
    opts_nearest = opts_nearest.dropna(subset=["STRIKE"])
    calls = opts_nearest[opts_nearest["OPTION_TYPE"] == "CE"]
    puts = opts_nearest[opts_nearest["OPTION_TYPE"] == "PE"]
    # Find call above current_nifty
    calls_above = calls[calls["STRIKE"] >= current_nifty]
    if not calls_above.empty:
        call_contract = calls_above.sort_values("STRIKE").iloc[0]
    else:
        call_contract = calls.iloc[(calls["STRIKE"] - current_nifty).abs().argmin()]
    # Find put below current_nifty
    puts_below = puts[puts["STRIKE"] <= current_nifty]
    if not puts_below.empty:
        put_contract = puts_below.sort_values("STRIKE", ascending=False).iloc[0]
    else:
        put_contract = puts.iloc[(puts["STRIKE"] - current_nifty).abs().argmin()]
    selected_ids = [str(call_contract["SECURITY_ID"]), str(put_contract["SECURITY_ID"])]
    logger.info(f"Selected CE {call_contract['STRIKE']} (ID {selected_ids[0]})")
    logger.info(f"Selected PE {put_contract['STRIKE']} (ID {selected_ids[1]})")
    return selected_ids, []


# ===================================================
# 2. WEBSOCKET HANDLER
# ===================================================
class SignalFeedHandler(marketfeed.MarketFeed):
    def __init__(self, dhan_context, instruments, version):
        super().__init__(dhan_context, instruments, version)
        self.ticker_data = {}
        self.last_signal = None

    def on_ticker(self, ticker_data):
        security_id = ticker_data.get('security_id')
        if 'ltp' in ticker_data and security_id:
            ltp = float(ticker_data['ltp'])
            self.ticker_data[security_id] = {"ltp": ltp, "ts": time.time()}
        if len(self.ticker_data) >= 2:
            self.generate_signal()

    def generate_signal(self):
        ids = list(self.ticker_data.keys())
        if len(ids) < 2:
            return
        ce_ltp = self.ticker_data[ids[0]]["ltp"]
        pe_ltp = self.ticker_data[ids[1]]["ltp"]
        spread = ce_ltp - pe_ltp
        # Simple signal logic (replace with your own)
        if spread > SPREAD_THRESHOLD:
            signal = "LONG SPREAD (Buy CE / Sell PE)"
        elif spread < -SPREAD_THRESHOLD:
            signal = "SHORT SPREAD (Sell CE / Buy PE)"
        else:
            signal = "NEUTRAL"
        if signal != self.last_signal:
            logger.info(f"[SIGNAL] CE:{ce_ltp:.2f} PE:{pe_ltp:.2f} Spread:{spread:.2f} -> {signal}")
            self.last_signal = signal


# ===================================================
# 3. ASYNC MAIN LOOP (runs in background thread)
# ===================================================
async def main():
    try:
        logger.info("Starting Nifty 50 Options Signal Engine...")
        security_ids, _ = get_option_contracts(CURRENT_NIFTY)
        if len(security_ids) < 2:
            logger.error("Could not find two option contracts.")
            return
        instruments = [(marketfeed.NSE_FNO, security_id, marketfeed.Ticker) for security_id in security_ids]
        dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
        feed = SignalFeedHandler(dhan_context, instruments, version="v1")
        await feed.connect()
        await feed.subscribe_instruments()
        logger.info("✅ WebSocket connected. Waiting for ticks...")
        while True:
            await asyncio.sleep(2)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        if 'feed' in locals():
            await feed.disconnect()

def start_signal_engine():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())

# ===================================================
# 4. FLASK APP
# ===================================================
app = Flask(__name__)
application = app

@app.route('/')
def home():
    return "Nifty Options Signal Engine is running."

@app.route('/api/health')
def health():
    return "OK", 200

# ===================================================
# 5. START BACKGROUND THREAD
# ===================================================
thread = threading.Thread(target=start_signal_engine, daemon=True)
thread.start()
logger.info("Background signal engine thread started.")
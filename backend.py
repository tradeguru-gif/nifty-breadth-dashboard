# backend.py - Advanced Nifty 50 Options Signal Engine (Strangle/Straddle)
# For Render deployment: uses background thread to keep WebSocket alive alongside Flask.

import os
import asyncio
import threading
import time
import logging
from typing import List, Tuple
from datetime import datetime

import pandas as pd
from flask import Flask

# Dhan SDK imports
from dhanhq import DhanContext, dhanhq, marketfeed

# Optional technical libraries (fallback if not installed)
try:
    from technopulse import RSI, MACD
    TECH_AVAILABLE = True
except ImportError:
    TECH_AVAILABLE = False
    logging.warning("technopulse not installed. Using fallback RSI/MACD.")

try:
    from option_chain_analyzer import OptionChainAnalyzer
    PCR_AVAILABLE = True
except ImportError:
    PCR_AVAILABLE = False
    logging.warning("option_chain_analyzer not installed. PCR will be unavailable.")

# ===================================================
# CONFIGURATION (from environment variables)
# ===================================================
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
CONTRACT_TYPE = os.getenv("CONTRACT_TYPE", "OPTIONS")   # Must be "OPTIONS"
CURRENT_NIFTY = int(os.getenv("CURRENT_NIFTY", "0"))    # Today's Nifty spot

if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN must be set.")
if CONTRACT_TYPE != "OPTIONS":
    raise ValueError("CONTRACT_TYPE must be 'OPTIONS' for this engine.")
if CURRENT_NIFTY == 0:
    raise ValueError("CURRENT_NIFTY must be set to today's Nifty index level.")

# Signal thresholds (adjust as needed)
SPREAD_THRESHOLD = 5.0      # Premium difference (CE - PE)
UPPER_PCR = 1.2
LOWER_PCR = 0.8

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ===================================================
# 1. DYNAMIC OPTION CONTRACT SELECTION
# ===================================================
def get_option_contracts(current_nifty: int) -> Tuple[List[str], List[dict]]:
    """
    Returns two option contracts (CE and PE) of the nearest expiry,
    with strikes just above and below the current Nifty price.
    """
    dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
    dhan = dhanhq(dhan_context)

    logger.info("Downloading instrument master...")
    df = dhan.fetch_security_list("detailed")
    fno = df[df["SEGMENT"] == "NSE_FNO"]
    if fno.empty:
        raise ValueError("No NSE_FNO data.")

    # Filter index options (OPTIDX)
    opts = fno[fno["INSTRUMENT"] == "OPTIDX"].copy()
    if opts.empty:
        raise ValueError("No OPTIDX (Index Options) found.")

    # Parse expiry dates
    opts["EXPIRY_DT"] = pd.to_datetime(opts["SEM_EXPIRY_DATE"], format="%d-%b-%Y", errors="coerce")
    opts = opts.dropna(subset=["EXPIRY_DT"])
    # Sort by expiry and take the nearest (first)
    nearest_expiry = opts["EXPIRY_DT"].min()
    opts_nearest = opts[opts["EXPIRY_DT"] == nearest_expiry]

    # Parse strike prices
    opts_nearest["STRIKE"] = pd.to_numeric(opts_nearest["STRIKE_PRICE"], errors="coerce")
    opts_nearest = opts_nearest.dropna(subset=["STRIKE"])

    # Separate calls and puts
    calls = opts_nearest[opts_nearest["OPTION_TYPE"] == "CE"]
    puts = opts_nearest[opts_nearest["OPTION_TYPE"] == "PE"]

    # Find the call with strike just above current_nifty
    calls_above = calls[calls["STRIKE"] >= current_nifty]
    if not calls_above.empty:
        call_contract = calls_above.sort_values("STRIKE").iloc[0]
    else:
        # Fallback: nearest call
        call_contract = calls.iloc[(calls["STRIKE"] - current_nifty).abs().argmin()]

    # Find the put with strike just below current_nifty
    puts_below = puts[puts["STRIKE"] <= current_nifty]
    if not puts_below.empty:
        put_contract = puts_below.sort_values("STRIKE", ascending=False).iloc[0]
    else:
        put_contract = puts.iloc[(puts["STRIKE"] - current_nifty).abs().argmin()]

    selected_ids = [str(call_contract["SECURITY_ID"]), str(put_contract["SECURITY_ID"])]
    selected_details = [
        {"type": "CE", "strike": call_contract["STRIKE"], "symbol": call_contract["SYMBOL_NAME"]},
        {"type": "PE", "strike": put_contract["STRIKE"], "symbol": put_contract["SYMBOL_NAME"]},
    ]
    logger.info(f"Selected Option Contracts: CE Strike {call_contract['STRIKE']} (ID {selected_ids[0]}), "
                f"PE Strike {put_contract['STRIKE']} (ID {selected_ids[1]})")
    return selected_ids, selected_details


# ===================================================
# 2. CUSTOM WEBSOCKET HANDLER (Signal Logic)
# ===================================================
class SignalFeedHandler(marketfeed.MarketFeed):
    def __init__(self, dhan_context, instruments, version):
        super().__init__(dhan_context, instruments, version)
        self.ticker_data = {}
        self.price_history = []   # store ticks for RSI/MACD
        self.last_signal = None
        if PCR_AVAILABLE:
            self.pcr_analyzer = OptionChainAnalyzer(underlying="NIFTY")
        logger.info("SignalFeedHandler initialized for Options.")

    def on_ticker(self, ticker_data):
        security_id = ticker_data.get('security_id')
        if 'ltp' in ticker_data and security_id:
            ltp = float(ticker_data['ltp'])
            self.ticker_data[security_id] = {"ltp": ltp, "ts": time.time()}
            self.price_history.append({"price": ltp, "ts": time.time()})
            if len(self.price_history) > 200:
                self.price_history = self.price_history[-200:]

        if len(self.ticker_data) >= 2:
            self.generate_signal()

    # ---------- Technical Indicators (fallback if technopulse missing) ----------
    def calculate_rsi(self, prices, period=14):
        if len(prices) < period + 1:
            return 50
        if TECH_AVAILABLE:
            return RSI(pd.Series(prices), period).values.iloc[-1]
        # Manual RSI
        deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        gains = [d if d>0 else 0 for d in deltas]
        losses = [-d if d<0 else 0 for d in deltas]
        avg_gain = sum(gains[:period])/period
        avg_loss = sum(losses[:period])/period
        if avg_loss == 0:
            return 100
        rs = avg_gain/avg_loss
        return 100 - (100/(1+rs))

    def calculate_macd(self, prices):
        if len(prices) < 26 + 9 or not TECH_AVAILABLE:
            return {"histogram": 0}
        macd = MACD(pd.Series(prices), 12, 26, 9)
        hist = macd.histogram.values.iloc[-1] if len(macd.histogram) > 0 else 0
        return {"histogram": hist}

    def get_put_call_ratio(self):
        if PCR_AVAILABLE:
            try:
                return self.pcr_analyzer.calculate_pcr()
            except Exception as e:
                logger.warning(f"PCR fetch failed: {e}")
        return None

    def generate_signal(self):
        ids = list(self.ticker_data.keys())
        if len(ids) < 2:
            return
        # Assume ids[0] is CE, ids[1] is PE (order based on selection)
        ce_ltp = self.ticker_data[ids[0]]["ltp"]
        pe_ltp = self.ticker_data[ids[1]]["ltp"]
        spread = ce_ltp - pe_ltp

        # Technical analysis from last 50 ticks of one contract
        prices = [p["price"] for p in self.price_history[-50:]]
        rsi = self.calculate_rsi(prices)
        macd = self.calculate_macd(prices)
        pcr = self.get_put_call_ratio() or 1.0

        bullish_sentiment = rsi < 70 and macd["histogram"] > 0 and pcr < LOWER_PCR
        bearish_sentiment = rsi > 30 and macd["histogram"] <= 0 and pcr > UPPER_PCR

        if spread > SPREAD_THRESHOLD and bullish_sentiment:
            signal = "LONG SPREAD (Buy CE / Sell PE)"
        elif spread < -SPREAD_THRESHOLD and bearish_sentiment:
            signal = "SHORT SPREAD (Sell CE / Buy PE)"
        else:
            signal = "NEUTRAL"

        if signal != self.last_signal:
            logger.info(f"[SIGNAL] CE:{ce_ltp:.2f} PE:{pe_ltp:.2f} Spread:{spread:.2f} "
                        f"RSI:{rsi:.1f} PCR:{pcr:.2f} -> {signal}")
            self.last_signal = signal


# ===================================================
# 3. ASYNC MAIN LOOP (to be run in background thread)
# ===================================================
async def main():
    try:
        logger.info("Starting Nifty 50 Options Signal Engine...")
        security_ids, _ = get_option_contracts(CURRENT_NIFTY)
        if len(security_ids) < 2:
            logger.error("Could not find two option contracts.")
            return

        instruments = [(marketfeed.NSE_FNO, sid, marketfeed.Ticker) for sid in security_ids]
        dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
        feed = SignalFeedHandler(dhan_context, instruments, version="v1")

        await feed.connect()
        await feed.subscribe_instruments()
        logger.info("✅ WebSocket connected. Awaiting option ticks...")

        # Keep running forever
        while True:
            await asyncio.sleep(2)

    except KeyboardInterrupt:
        logger.info("Shutting down.")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        if 'feed' in locals():
            await feed.disconnect()


def start_signal_engine():
    """Run the async main() in a separate event loop (called from a thread)."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())


# ===================================================
# 4. FLASK APPLICATION (for health checks)
# ===================================================
app = Flask(__name__)
application = app   # Gunicorn looks for 'application'

@app.route('/')
def home():
    return "Nifty Options Signal Engine is running."

@app.route('/api/health')
def health():
    return "OK", 200


# ===================================================
# 5. START THE BACKGROUND SIGNAL ENGINE
# ===================================================
# Launch the signal engine in a daemon thread so it doesn't block Gunicorn.
thread = threading.Thread(target=start_signal_engine, daemon=True)
thread.start()
logger.info("Background signal engine thread started.")

# For local testing (not used on Render because __name__ != '__main__' due to Gunicorn)
if __name__ == "__main__":
    asyncio.run(main())
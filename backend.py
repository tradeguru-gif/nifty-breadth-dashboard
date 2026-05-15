import os
import asyncio
import threading
import time
import logging
import requests
import csv
import numpy as np
from datetime import datetime, timedelta
from collections import deque

from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import DhanContext, MarketFeed

# --------------------------------------------------
# Setup & App Configuration
# --------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

# --------------------------------------------------
# Global State Management
# --------------------------------------------------
CE_ID = None
PE_ID = None
current_strike = None
current_expiry = None
need_restart = False
price_history = deque(maxlen=200)
tick_counter = 0

latest_data = {
    "signal": "WAITING",
    "ce_price": 0.0,
    "pe_price": 0.0,
    "spread": 0.0,
    "rsi": 50,
    "macd": 0.0,
    "pcr": 1.0,
    "timestamp": datetime.now().isoformat(),
    "strike": None,
    "expiry": None
}

market_state = {"rsi": 50, "trend": "SIDEWAYS", "confidence": 0, "system_status": "STARTING"}
institutional_state = {"vwap": 0, "ema_fast": 0, "ema_slow": 0, "smart_money_flow": "NEUTRAL"}

# --------------------------------------------------
# Data Fetchers (NSE & Scrip Master)
# --------------------------------------------------
def get_nifty_spot():
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=5)
        resp = session.get(url, headers=headers, timeout=5)
        return float(resp.json()['data'][0]['lastPrice'])
    except Exception as e:
        logger.error(f"Spot fetch failed: {e}")
        return None

def find_option_security_id(symbol, expiry, strike, option_type):
    try:
        url = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
        response = requests.get(url, timeout=15)
        decoded_content = response.content.decode('utf-8')
        cr = csv.DictReader(decoded_content.splitlines())
        target_expiry = datetime.strptime(expiry, "%Y-%m-%d").date()

        for row in cr:
            if row.get('SEM_INSTRUMENT_NAME') != 'OPTIDX': continue
            if symbol not in row.get('SEM_SYMBOL_NAME', ''): continue
            if row.get('SEM_OPTION_TYPE') != option_type: continue
            
            try:
                if int(float(row.get('SEM_STRIKE_PRICE', 0))) != strike: continue
                row_exp = datetime.strptime(row.get('SEM_EXPIRY_DATE', '').split(' ')[0], "%Y-%m-%d").date()
                if row_exp != target_expiry: continue
                return str(row.get('SEM_SMST_SECURITY_ID'))
            except: continue
        return None
    except Exception as e:
        logger.error(f"CSV Parse error: {e}")
        return None

# --------------------------------------------------
# Logic & Indicators
# --------------------------------------------------
def calculate_rsi(prices, period=14):
    if len(prices) < period + 1: return 50.0
    deltas = np.diff(prices)
    seed = deltas[:period]
    up = seed[seed >= 0].sum()/period
    down = -seed[seed < 0].sum()/period
    rs = up/down if down != 0 else 100
    return 100 - (100/(1+rs))

def update_atm_option_ids(force=False):
    global CE_ID, PE_ID, current_strike, current_expiry
    spot = get_nifty_spot()
    if not spot: return False
    
    strike = int(round(spot / 50.0) * 50)
    # Get next Thursday
    today = datetime.now()
    expiry = (today + timedelta(days=(3 - today.weekday()) % 7 or 7)).strftime("%Y-%m-%d")

    if not force and strike == current_strike: return False

    ce = find_option_security_id("NIFTY", expiry, strike, "CE")
    pe = find_option_security_id("NIFTY", expiry, strike, "PE")

    if ce and pe:
        CE_ID, PE_ID, current_strike, current_expiry = ce, pe, strike, expiry
        latest_data.update({"strike": strike, "expiry": expiry})
        logger.info(f"✅ NEW STRADDLE: {strike} @ {expiry}")
        return True
    return False

# --------------------------------------------------
# WebSocket & Feed Handling
# --------------------------------------------------
class CustomFeed(MarketFeed):
    def on_message(self, message):
        global tick_counter
        try:
            sec_id = str(message.get("security_id", ""))
            price = float(message.get("LTP", 0))
            if price <= 0: return

            if sec_id == CE_ID: latest_data["ce_price"] = price
            elif sec_id == PE_ID: latest_data["pe_price"] = price
            
            ce, pe = latest_data["ce_price"], latest_data["pe_price"]
            if ce > 0 and pe > 0:
                latest_data["spread"] = round(ce - pe, 2)
                latest_data["signal"] = "BULLISH" if ce - pe > 5 else "BEARISH" if ce - pe < -5 else "NEUTRAL"
                latest_data["timestamp"] = datetime.now().isoformat()
                price_history.append(ce)
                
                # Periodic Heavy Analysis
                tick_counter += 1
                if tick_counter % 10 == 0:
                    prices = list(price_history)
                    latest_data["rsi"] = round(calculate_rsi(prices), 2)
                    market_state["system_status"] = "LIVE"
        except Exception as e:
            logger.error(f"Tick error: {e}")

async def websocket_loop():
    global need_restart
    while True:
        try:
            update_atm_option_ids(force=True)
            if not CE_ID:
                await asyncio.sleep(30); continue

            instruments = [
                (MarketFeed.NSE_FNO, str(CE_ID), MarketFeed.Ticker),
                (MarketFeed.NSE_FNO, str(PE_ID), MarketFeed.Ticker)
            ]
            
            feed = CustomFeed(CLIENT_ID, ACCESS_TOKEN, instruments, version="v2")
            await feed.connect()
            await feed.subscribe_instruments()
            
            while not need_restart:
                # Heartbeat check
                last_ts = datetime.fromisoformat(latest_data["timestamp"])
                if (datetime.now() - last_ts).total_seconds() > 60:
                    logger.warning("Data Stale - Force Restarting")
                    break
                await asyncio.sleep(1)
            
            need_restart = False
            await feed.disconnect()
        except Exception as e:
            logger.error(f"WS Crash: {e}")
            await asyncio.sleep(10)

# --------------------------------------------------
# Threads & Routes
# --------------------------------------------------
def run_feed():
    asyncio.run(websocket_loop())

def run_id_updater():
    global need_restart
    while True:
        time.sleep(600) # Check every 10 mins
        if update_atm_option_ids():
            need_restart = True

threading.Thread(target=run_feed, daemon=True).start()
threading.Thread(target=run_id_updater, daemon=True).start()

@app.route("/api/trading-signals")
def trading_signals():
    return jsonify({"data": latest_data, "market": market_state, "institutional": institutional_state})

@app.route("/")
def health(): return "SYSTEM_ONLINE", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
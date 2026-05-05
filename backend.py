import os
import time
import threading
import asyncio
from datetime import datetime, timedelta
from flask import Flask, jsonify
from flask_cors import CORS
import pandas as pd
import numpy as np
from dhanhq import dhanhq, marketfeed

# ==================================================
# CREDENTIALS FROM ENVIRONMENT (no hardcoding)
# ==================================================
DHAN_CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")

if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN in environment")

NIFTY_SECURITY_ID = None
EXCHANGE_SEGMENT = "NSE"

# --------------------------------------------------
# Fetch NIFTY security ID (using static access token)
# --------------------------------------------------
def get_nifty_security_id():
    global NIFTY_SECURITY_ID
    try:
        dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
        instruments = dhan.get_instruments()
        for inst in instruments:
            if inst.get("instrument_name") == "NIFTY 50" and inst.get("segment") == "NSE":
                NIFTY_SECURITY_ID = str(inst.get("security_id"))
                print(f"✅ Found NIFTY security_id = {NIFTY_SECURITY_ID}")
                return NIFTY_SECURITY_ID
        NIFTY_SECURITY_ID = "116"
        print("⚠️ Using fallback NIFTY security_id = 116")
        return NIFTY_SECURITY_ID
    except Exception as e:
        print(f"⚠️ Instrument fetch error: {e}, using fallback 116")
        NIFTY_SECURITY_ID = "116"
        return NIFTY_SECURITY_ID

# --------------------------------------------------
# Real‑time data structures (same as before)
# --------------------------------------------------
minute_candles = []
current_candle = None
lock = threading.Lock()
latest_market_data = {
    'current_price': None,
    'vwap': None,
    'total_pv': 0,
    'total_vol': 0,
    'day_high': None,
    'day_low': None,
    'last_update': None
}

def on_ticks(ticks):
    global current_candle, minute_candles, latest_market_data
    now = datetime.now()
    with lock:
        for tick in ticks:
            price = float(tick.get('ltp', 0))
            volume = int(tick.get('volume', 0))
            tick_time = datetime.fromtimestamp(tick.get('exchange_time', time.time()))
            
            latest_market_data['current_price'] = float(price)
            latest_market_data['last_update'] = now.isoformat()
            if latest_market_data['day_high'] is None or price > latest_market_data['day_high']:
                latest_market_data['day_high'] = float(price)
            if latest_market_data['day_low'] is None or price < latest_market_data['day_low']:
                latest_market_data['day_low'] = float(price)

            latest_market_data['total_pv'] += price * volume
            latest_market_data['total_vol'] += volume
            if latest_market_data['total_vol'] > 0:
                latest_market_data['vwap'] = float(latest_market_data['total_pv'] / latest_market_data['total_vol'])

            current_minute = tick_time.replace(second=0, microsecond=0)
            if current_candle is None or current_candle['timestamp'] != current_minute:
                if current_candle is not None:
                    minute_candles.append(current_candle)
                    if len(minute_candles) > 500:
                        minute_candles.pop(0)
                current_candle = {
                    'timestamp': current_minute,
                    'open': float(price),
                    'high': float(price),
                    'low': float(price),
                    'close': float(price),
                    'volume': int(volume)
                }
            else:
                current_candle['high'] = max(current_candle['high'], float(price))
                current_candle['low'] = min(current_candle['low'], float(price))
                current_candle['close'] = float(price)
                current_candle['volume'] += int(volume)

async def start_market_feed():
    global NIFTY_SECURITY_ID
    while NIFTY_SECURITY_ID is None:
        get_nifty_security_id()
        await asyncio.sleep(2)
    
    dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
    dhan_context = dhan.get_dhan_context()
    mf = marketfeed.MarketFeed(dhan_context, [(EXCHANGE_SEGMENT, NIFTY_SECURITY_ID)], on_ticks)
    await mf.connect()
    await mf.subscribe_instruments()
    while True:
        await asyncio.sleep(1)

def run_websocket():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start_market_feed())

# --------------------------------------------------
# (All your indicator functions – calculate_ema, get_next_expiry, suggest_strike, calculate_dynamic_signal, to_native – remain exactly as before)
# --------------------------------------------------
# ... (paste your existing indicator functions here, unchanged)

# --------------------------------------------------
# Flask app and endpoints (unchanged)
# --------------------------------------------------
application = Flask(__name__)
CORS(application)

# ... (keep all your route definitions exactly as before)

# --------------------------------------------------
# START WEBSOCKET THREAD (module level)
# --------------------------------------------------
print("🚀 Initializing Dhan WebSocket...")
get_nifty_security_id()
ws_thread = threading.Thread(target=run_websocket, daemon=True)
ws_thread.start()
print("🚀 Dhan WebSocket thread started. Waiting for ticks...")

if __name__ == '__main__':
    time.sleep(5)
    application.run(debug=False, host='0.0.0.0', port=5000)
import os
import time
import threading
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
from dhanhq import dhanhq, marketfeed

# ============================================
# INITIALIZE FLASK
# ============================================
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})
application = app
latest_signal = {}

# ============================================
# CREDENTIALS & INSTRUMENT
# ============================================
DHAN_CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")
if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
    raise ValueError("Missing credentials")

# Use NIFTY futures – known to work
NIFTY_SECURITY_ID = "13"
EXCHANGE_SEGMENT = "NSE_FNO"

# --------------------------------------------------
# Real‑time data structures
# --------------------------------------------------
minute_candles = []
current_candle = None
lock = threading.Lock()
latest_market_data = {
    'current_price': None, 'vwap': None, 'total_pv': 0, 'total_vol': 0,
    'day_high': None, 'day_low': None, 'last_update': None
}
current_position = {
    'active': False, 'type': None, 'entry_price': 0.0, 'stop_loss': 0.0,
    'take_profit': 0.0, 'highest_price': 0.0, 'lowest_price': 0.0,
    'trailing_stop': 0.0, 'entry_time': None
}

def process_tick(price, volume, tick_time):
    global current_candle, minute_candles, latest_market_data
    with lock:
        latest_market_data['current_price'] = float(price)
        latest_market_data['last_update'] = tick_time.isoformat()
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
                'open': float(price), 'high': float(price), 'low': float(price),
                'close': float(price), 'volume': int(volume)
            }
        else:
            current_candle['high'] = max(current_candle['high'], float(price))
            current_candle['low'] = min(current_candle['low'], float(price))
            current_candle['close'] = float(price)
            current_candle['volume'] += int(volume)

# --------------------------------------------------
# WebSocket – async with callbacks
# --------------------------------------------------
async def run_feed():
    print("🚀 run_feed() started")
    instruments = [(marketfeed.NSE_FNO, NIFTY_SECURITY_ID, marketfeed.Ticker)]
    print(f"🔧 Instruments: {instruments}")
    feed = marketfeed.DhanFeed(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, instruments, "v2")
    print("✅ DhanFeed created")

    def on_connect(instance):
        print("✅ WebSocket connected and authorized.")

    def on_error(instance, error):
        print(f"❌ WebSocket error: {error}")

    def on_close(instance):
        print("🔌 WebSocket closed.")

    def on_message(instance, tick):
        try:
            # Print full tick for debugging (remove after it works)
            print(f"📦 Tick: {tick}")
            price = float(tick.get('ltp', 0))
            volume = int(tick.get('volume', 0))
            tick_time = datetime.now()
            process_tick(price, volume, tick_time)
        except Exception as e:
            print(f"Error: {e}")

    feed.on_connect = on_connect
    feed.on_error = on_error
    feed.on_close = on_close
    feed.on_message = on_message

    print("⏳ Calling feed.connect()...")
    await feed.connect()
    print("✅ feed.connect() completed")
    print("⏳ Calling feed.subscribe_instruments()...")
    await feed.subscribe_instruments()
    print("✅ Subscribed to instruments")
    print("🚀 Dhan WebSocket is live. Waiting for ticks...")
    while True:
        await asyncio.sleep(1)

def start_market_feed():
    print("🚀 Starting Dhan WebSocket feed thread...")
    asyncio.run(run_feed())

# --------------------------------------------------
# Flask endpoints (keep your original ones)
# --------------------------------------------------
@app.route('/api/health')
def health_check():
    return jsonify({
        'status': 'running',
        'version': '7.3.0',
        'candles_available': len(minute_candles),
        'timestamp': datetime.now().isoformat()
    })

# ... (add your other endpoints: /api/trading-signals, /api/breadth, etc.)
# For brevity, keep them as they were – no change needed.

# --------------------------------------------------
# START WEBSOCKET THREAD
# --------------------------------------------------
print("🚀 Initializing Dhan WebSocket...")
ws_thread = threading.Thread(target=start_market_feed, daemon=True)
ws_thread.start()
print("🚀 Dhan WebSocket thread started.")

if __name__ == '__main__':
    time.sleep(5)
    app.run(debug=False, host='0.0.0.0', port=5000)
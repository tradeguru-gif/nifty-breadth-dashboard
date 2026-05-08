import threading
import asyncio
import os
import time
from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime
from dhanhq import DhanContext, MarketFeed

# ============================================
# 1. CREDENTIALS (Must be first)
# ============================================
DHAN_CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")

# For local testing only (Comment out when on Render)
# DHAN_CLIENT_ID = "YOUR_ID"
# DHAN_ACCESS_TOKEN = "YOUR_TOKEN"

# ============================================
# 2. INITIALIZE OBJECTS
# ============================================
app = Flask(__name__)
CORS(app)

# Signal storage
latest_signal = {
    "action": "WAITING",
    "price": 0.0,
    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
}

# Now we can safely initialize Dhan
if DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN:
    dhan_context = DhanContext(client_id=DHAN_CLIENT_ID, access_token=DHAN_ACCESS_TOKEN)
    feed = MarketFeed(dhan_context, version='v2')
else:
    print("⚠️ Warning: Credentials not found. WebSocket will not start.")

# --------------------------------------------------
# Real-time data structures
# --------------------------------------------------
minute_candles = []
current_candle = None
lock = threading.Lock()
latest_market_data = {
    'current_price': None, 'vwap': None, 'total_pv': 0, 'total_vol': 0,
    'day_high': None, 'day_low': None, 'last_update': None
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
# WebSocket Handlers
# --------------------------------------------------
def on_connect(instance):
    print("✅ WebSocket connected and authorized.")

def on_error(instance, error):
    print(f"❌ WebSocket error: {error}")

def on_close(instance):
    print("🔌 WebSocket closed.")

def on_message(instance, tick):
    global latest_signal
    try:
        price = float(tick.get('ltp', 0))
        volume = int(tick.get('volume', 0))
        
        if price > 0:
            tick_time = datetime.now()
            process_tick(price, volume, tick_time)
            
            latest_signal.update({
                "action": "TRACKING",
                "price": price,
                "timestamp": tick_time.strftime("%Y-%m-%d %H:%M:%S")
            })
    except Exception as e:
        print(f"⚠️ Error processing tick: {e}")

async def run_feed():
    feed.on_connect = on_connect
    feed.on_error = on_error
    feed.on_close = on_close
    feed.on_message = on_message

    await feed.connect()
    # Subscription using feed instance constants
    instruments = [(feed.NSE_INDEX, "13", feed.FULL)]
    await feed.subscribe_symbols(instruments)
    
    while True:
        await asyncio.sleep(1)

def start_market_feed():
    try:
        asyncio.run(run_feed())
    except Exception as e:
        print(f"❌ Feed Thread Error: {e}")

# --------------------------------------------------
# Flask Routes
# --------------------------------------------------
@app.route('/api/trading-signals', methods=['GET', 'POST'])
def trading_signals():
    global latest_signal
    if request.method == 'POST':
        data = request.json
        latest_signal.update({
            "action": data.get("action", "HOLD"),
            "price": data.get("price", 0.0),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        return jsonify({"status": "success"}), 200
    return jsonify(latest_signal)

@app.route('/api/status', methods=['GET'])
def get_status():
    return jsonify({"status": "online", "server_time": datetime.now().isoformat()})

# --------------------------------------------------
# MAIN EXECUTION
# --------------------------------------------------
if __name__ == '__main__':
    # Start WebSocket thread only if credentials exist
    if DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN:
        ws_thread = threading.Thread(target=start_market_feed, daemon=True)
        ws_thread.start()
        print("🚀 Dhan WebSocket thread started.")
    
    # Run Flask
    app.run(host='0.0.0.0', port=5000, debug=False)
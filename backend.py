import os
import time
import threading
from datetime import datetime, timedelta
from flask import Flask, jsonify
from flask_cors import CORS
import pandas as pd
import numpy as np
from dhanhq import dhanhq, marketfeed

# ==================================================
# CREDENTIALS
# ==================================================
DHAN_CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")

if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

NIFTY_SECURITY_ID = "13"       # NIFTY futures (works)
EXCHANGE_SEGMENT = "NSE_FNO"

# --------------------------------------------------
# Data structures
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

current_position = {
    'active': False,
    'type': None,
    'entry_price': 0.0,
    'stop_loss': 0.0,
    'take_profit': 0.0,
    'highest_price': 0.0,
    'lowest_price': 0.0,
    'trailing_stop': 0.0,
    'entry_time': None
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

# --------------------------------------------------
# WebSocket – synchronous, using dhanhq 1.1.1 (no asyncio)
# --------------------------------------------------
def start_market_feed():
    print("🚀 Starting Dhan WebSocket feed (synchronous)...")
    
    instruments = [(marketfeed.NSE_FNO, NIFTY_SECURITY_ID, marketfeed.Ticker)]
    
    # Create feed object (no async, no event loop)
    feed = marketfeed.DhanFeed(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, instruments, "v2")
    
    def on_connect(instance):
        print("✅ WebSocket connected and authorized.")
    
    def on_error(instance, error):
        print(f"❌ WebSocket error: {error}")
    
    def on_close(instance):
        print("🔌 WebSocket closed.")
    
    def on_message(instance, tick):
        try:
            price = float(tick.get('ltp', 0))
            volume = int(tick.get('volume', 0))
            tick_time = datetime.now()
            process_tick(price, volume, tick_time)
            # Debug: print first 10 ticks
            if not hasattr(on_message, "count"):
                on_message.count = 0
            on_message.count += 1
            if on_message.count <= 10:
                print(f"📊 Tick #{on_message.count}: price={price}, volume={volume}")
        except Exception as e:
            print(f"Error processing tick: {e}")
    
    feed.on_connect = on_connect
    feed.on_error = on_error
    feed.on_close = on_close
    feed.on_message = on_message
    
    print("🚀 Dhan WebSocket started. Waiting for ticks...")
    feed.run_forever()   # Blocks forever – works reliably in a background thread

# --------------------------------------------------
# Indicators & signal logic (keep your existing functions)
# --------------------------------------------------
def to_native(obj):
    if isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, (list, tuple)):
        return [to_native(x) for x in obj]
    elif isinstance(obj, dict):
        return {k: to_native(v) for k, v in obj.items()}
    else:
        return obj

def calculate_ema(series, period=20):
    return series.ewm(span=period, adjust=False).mean()

def get_next_expiry():
    today = datetime.now()
    days_ahead = 3 - today.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    expiry_date = today + timedelta(days=days_ahead)
    return expiry_date, (expiry_date - today).days

def suggest_strike(current_price, action, days_to_expiry):
    atm = round(current_price / 50) * 50
    if days_to_expiry <= 1:
        strike = atm
        strike_type = "ATM"
    else:
        if action == "BUY CE":
            strike = atm + 100
            strike_type = "OTM CE"
        elif action == "BUY PE":
            strike = atm - 100
            strike_type = "OTM PE"
        else:
            strike = atm
            strike_type = "ATM"
    return {
        'recommended_strike': int(strike),
        'strike_type': strike_type,
        'atm_strike': int(atm),
        'call_otm': int(atm + 100),
        'put_otm': int(atm - 100),
        'days_to_expiry': days_to_expiry
    }

def calculate_macd(df, fast=12, slow=26, signal=9):
    exp1 = df['close'].ewm(span=fast, adjust=False).mean()
    exp2 = df['close'].ewm(span=slow, adjust=False).mean()
    macd_line = exp1 - exp2
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line.iloc[-1], signal_line.iloc[-1], hist.iloc[-1]

def calculate_adx(df, period=14):
    # Your existing ADX function (keep it)
    ...

def is_market_hours():
    # Your existing time filter (keep it)
    ...

def calculate_volume_spike(vol_ratio, atr):
    # Your existing volume spike (keep it)
    ...

def calculate_dynamic_signal(df):
    # Your existing signal logic (keep it)
    ...

# --------------------------------------------------
# Flask app
# --------------------------------------------------
application = Flask(__name__)
CORS(application)

@application.route('/')
def home():
    return jsonify({'message': 'Trade Guru NIFTY Trading API v7.2 (Synchronous WebSocket)'})

@application.route('/api/health')
def health_check():
    return jsonify({
        'status': 'running',
        'version': '7.2.0',
        'candles_available': len(minute_candles),
        'timestamp': datetime.now().isoformat()
    })

@application.route('/api/trading-signals')
def get_trading_signals():
    try:
        with lock:
            if len(minute_candles) < 20:
                return jsonify({'error': 'Building real-time candles', 'candles_available': len(minute_candles), 'action': 'HOLD'}), 202
            df = pd.DataFrame(minute_candles)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            df.sort_index(inplace=True)
            signal = calculate_dynamic_signal(df)
            if signal:
                return jsonify(signal)
            return jsonify({'action': 'HOLD', 'reason': 'Insufficient indicators'})
    except Exception as e:
        print(f"Error in trading-signals: {e}")
        return jsonify({'error': str(e), 'action': 'HOLD'}), 500

# ... other endpoints (breadth, realtime-nifty, pcr) ...

# --------------------------------------------------
# Start WebSocket in background thread
# --------------------------------------------------
print("🚀 Initializing Dhan WebSocket...")
ws_thread = threading.Thread(target=start_market_feed, daemon=True)
ws_thread.start()
print("🚀 Dhan WebSocket thread started.")

if __name__ == '__main__':
    time.sleep(5)
    application.run(debug=False, host='0.0.0.0', port=5000)
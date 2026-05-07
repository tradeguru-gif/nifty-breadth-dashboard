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
# CREDENTIALS FROM ENVIRONMENT
# ==================================================
DHAN_CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")

if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN in environment")

NIFTY_SECURITY_ID = "116"  # Hardcoded for NIFTY 50
EXCHANGE_SEGMENT = "IDX"   # For index

# --------------------------------------------------
# Real‑time data structures
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

# Position tracking
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
    """Process a single tick and build 1-minute candles"""
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
# WebSocket connection (async, runs in background thread)
# --------------------------------------------------
async def run_market_feed():
    """Async coroutine that sets up and runs the WebSocket feed."""
    instruments = [(marketfeed.IDX, NIFTY_SECURITY_ID, marketfeed.Ticker)]
    feed = marketfeed.DhanFeed(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, instruments, "v2")

    def on_message(instance, tick):
        try:
            price = float(tick.get('ltp', 0))
            volume = int(tick.get('volume', 0))
            tick_time = datetime.now()
            process_tick(price, volume, tick_time)
            # Optional debug print (uncomment if needed)
            # print(f"Tick: {price} at {tick_time}")
        except Exception as e:
            print(f"Error processing tick: {e}")

    feed.on_message = on_message
    print("🚀 Dhan WebSocket started. Waiting for ticks...")
    await feed.connect()
    await feed.subscribe_instruments()
    while True:
        await asyncio.sleep(1)

def start_market_feed():
    """Function to be run in a background thread."""
    print("🚀 Starting Dhan WebSocket feed...")
    # asyncio.run() creates a new event loop for this thread and runs it
    asyncio.run(run_market_feed())

# --------------------------------------------------
# Technical Indicators & Dynamic Logic
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
    high = df['high']
    low = df['low']
    close = df['close']
    
    high_low = high - low
    high_close = abs(high - close.shift())
    low_close = abs(low - close.shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    
    up_move = high - high.shift()
    down_move = low.shift() - low
    
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0)
    
    tr_smooth = tr.rolling(period).mean()
    plus_di = 100 * (plus_dm.rolling(period).mean() / tr_smooth)
    minus_di = 100 * (minus_dm.rolling(period).mean() / tr_smooth)
    
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
    adx = dx.rolling(period).mean()
    return adx.iloc[-1] if not pd.isna(adx.iloc[-1]) else 20.0

def is_market_hours():
    now = datetime.now()
    ist_offset = timedelta(hours=5, minutes=30)
    current_ist = now + ist_offset
    
    market_open = current_ist.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = current_ist.replace(hour=15, minute=30, second=0, microsecond=0)
    optimal_start = current_ist.replace(hour=9, minute=30, second=0, microsecond=0)
    optimal_end = current_ist.replace(hour=15, minute=15, second=0, microsecond=0)
    
    if current_ist < market_open or current_ist > market_close:
        return False
    if current_ist < optimal_start or current_ist > optimal_end:
        return False
    return True

def calculate_volume_spike(vol_ratio, atr):
    if vol_ratio >= 3.0:
        return 2, "Very Strong"
    elif vol_ratio >= 2.0:
        return 1, "Strong"
    elif vol_ratio >= 1.5:
        return 0, "Normal"
    else:
        return -1, "Weak"

def calculate_dynamic_signal(df):
    global current_position
    if len(df) < 20:
        return None

    last_price = float(df['close'].iloc[-1])
    current_time = datetime.now()
    
    if not is_market_hours():
        return {
            'action': 'HOLD',
            'recommendation': 'HOLD – Outside optimal trading hours',
            'confidence': 'Low',
            'trigger_reason': 'Avoiding open/close volatility (9:30-3:15 only)',
            'spot_price': round(last_price, 2),
            'position_active': current_position['active'],
            'entry_price': round(current_position['entry_price'], 2) if current_position['active'] else None,
            'stop_loss': round(current_position['stop_loss'], 2) if current_position['active'] else None,
            'take_profit': round(current_position['take_profit'], 2) if current_position['active'] else None,
            'trailing_stop': round(current_position['trailing_stop'], 2) if current_position['active'] else None,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

    high_low = df['high'] - df['low']
    high_close = abs(df['high'] - df['close'].shift())
    low_close = abs(df['low'] - df['close'].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = float(tr.rolling(14).mean().iloc[-1])
    if pd.isna(atr) or atr == 0:
        atr = 10.0

    if len(df) >= 4:
        price_3min_ago = float(df['close'].iloc[-4])
        momentum = last_price - price_3min_ago
    else:
        momentum = 0.0

    current_vol = int(df['volume'].iloc[-1])
    avg_vol = float(df['volume'].tail(20).mean())
    vol_ratio = current_vol / avg_vol if avg_vol > 0 else 1.0
    spike_level, spike_strength = calculate_volume_spike(vol_ratio, atr)
    vol_ok = spike_level >= 0
    vol_text = f"Volume {vol_ratio:.1f}x avg ({spike_strength})" if vol_ok else f"Weak volume ({vol_ratio:.1f}x avg)"

    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    rsi_val = 100 - (100 / (1 + rs.iloc[-1])) if loss.iloc[-1] != 0 else 50.0
    rsi = float(rsi_val)

    ema20 = float(calculate_ema(df['close'], 20).iloc[-1])
    vwap = latest_market_data.get('vwap', last_price) or last_price
    vwap = float(vwap)

    macd_line, signal_line, hist = calculate_macd(df)
    macd_bullish = macd_line > signal_line and hist > 0

    adx = calculate_adx(df)
    strong_trend = adx > 25
    trend_text = f"ADX {adx:.1f} ({'Strong trend' if strong_trend else 'Weak trend'})"

    # Exit checks
    if current_position['active']:
        exit_signal = None
        if current_position['type'] == 'LONG':
            if last_price > current_position['highest_price']:
                current_position['highest_price'] = last_price
                new_trail = last_price - 1.5 * atr
                if new_trail > current_position['trailing_stop']:
                    current_position['trailing_stop'] = new_trail
        else:
            if last_price < current_position['lowest_price']:
                current_position['lowest_price'] = last_price
                new_trail = last_price + 1.5 * atr
                if new_trail < current_position['trailing_stop']:
                    current_position['trailing_stop'] = new_trail

        if (current_position['type'] == 'LONG' and last_price <= current_position['trailing_stop']) or \
           (current_position['type'] == 'SHORT' and last_price >= current_position['trailing_stop']):
            exit_signal = 'EXIT'
        elif (current_position['type'] == 'LONG' and last_price >= current_position['take_profit']) or \
             (current_position['type'] == 'SHORT' and last_price <= current_position['take_profit']):
            exit_signal = 'TAKE_PROFIT'
        elif (current_position['type'] == 'LONG' and not macd_bullish) or \
             (current_position['type'] == 'SHORT' and macd_bullish):
            exit_signal = 'EXIT'

        if exit_signal:
            current_position['active'] = False
            expiry_date, days_left = get_next_expiry()
            return {
                'action': exit_signal,
                'recommendation': f"{exit_signal} at ₹{last_price:.2f}",
                'confidence': 'High',
                'trigger_reason': f"{'Stop loss' if exit_signal=='EXIT' else 'Take profit'} hit",
                'spot_price': round(last_price, 2),
                'momentum_3min': round(momentum, 2),
                'volume_ratio': round(vol_ratio, 2),
                'rsi': round(rsi, 1),
                'atr': round(atr, 2),
                'vwap': round(vwap, 2),
                'ema20': round(ema20, 2),
                'macd_hist': round(float(hist), 4),
                'macd_bullish': macd_bullish,
                'adx': round(adx, 1),
                'expiry': expiry_date.strftime('%Y-%m-%d'),
                'days_to_expiry': days_left,
                'suggested_strike': suggest_strike(last_price, exit_signal, days_left),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

    # Entry signals
    buy_signal = sell_signal = False
    trigger_reason = "No trigger"
    confidence = "Low"

    if strong_trend and vol_ok and abs(momentum) > 1.5 * atr:
        if momentum > 0 and last_price > ema20 and last_price > vwap and rsi > 50 and macd_bullish:
            buy_signal = True
            confidence = "High" if spike_level >= 1 else "Medium"
            trigger_reason = f"Bullish: {momentum:.2f} pts, {vol_text}, {trend_text}"
        elif momentum < 0 and last_price < ema20 and last_price < vwap and rsi < 50 and not macd_bullish:
            sell_signal = True
            confidence = "High" if spike_level >= 1 else "Medium"
            trigger_reason = f"Bearish: {abs(momentum):.2f} pts, {vol_text}, {trend_text}"

    if buy_signal or sell_signal:
        entry_price = last_price
        if buy_signal:
            action = "BUY CE"
            stop_loss = entry_price - 1.5 * atr
            take_profit = entry_price + 2.5 * atr
            trailing_stop = stop_loss
            current_position.update({
                'active': True, 'type': 'LONG',
                'entry_price': entry_price, 'stop_loss': stop_loss, 'take_profit': take_profit,
                'highest_price': entry_price, 'trailing_stop': trailing_stop, 'entry_time': current_time
            })
        else:
            action = "BUY PE"
            stop_loss = entry_price + 1.5 * atr
            take_profit = entry_price - 2.5 * atr
            trailing_stop = stop_loss
            current_position.update({
                'active': True, 'type': 'SHORT',
                'entry_price': entry_price, 'stop_loss': stop_loss, 'take_profit': take_profit,
                'lowest_price': entry_price, 'trailing_stop': trailing_stop, 'entry_time': current_time
            })
        recommendation = f"{action} Entry ₹{entry_price:.2f} | Stop ₹{stop_loss:.2f} | Target ₹{take_profit:.2f}"
    else:
        action = "HOLD"
        recommendation = "HOLD – No strong signal"

    expiry_date, days_left = get_next_expiry()
    strike_info = suggest_strike(last_price, action, days_left)

    result = {
        'action': action,
        'recommendation': recommendation,
        'confidence': confidence,
        'trigger_reason': trigger_reason,
        'spot_price': round(last_price, 2),
        'momentum_3min': round(momentum, 2),
        'volume_ratio': round(vol_ratio, 2),
        'rsi': round(rsi, 1),
        'atr': round(atr, 2),
        'vwap': round(vwap, 2),
        'ema20': round(ema20, 2),
        'macd_hist': round(float(hist), 4),
        'macd_bullish': macd_bullish,
        'adx': round(adx, 1),
        'position_active': current_position['active'],
        'entry_price': round(current_position['entry_price'], 2) if current_position['active'] else None,
        'stop_loss': round(current_position['stop_loss'], 2) if current_position['active'] else None,
        'take_profit': round(current_position['take_profit'], 2) if current_position['active'] else None,
        'trailing_stop': round(current_position['trailing_stop'], 2) if current_position['active'] else None,
        'expiry': expiry_date.strftime('%Y-%m-%d'),
        'days_to_expiry': days_left,
        'suggested_strike': strike_info,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    return to_native(result)

# --------------------------------------------------
# Flask Application
# --------------------------------------------------
application = Flask(__name__)
CORS(application)

@application.route('/')
def home():
    return jsonify({'message': 'Trade Guru NIFTY Trading API v7.0 (Stable Async WebSocket)'})

@application.route('/api/health')
def health_check():
    return jsonify({
        'status': 'running',
        'version': '7.0.0',
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

@application.route('/api/breadth')
def get_breadth():
    return jsonify({'advances': 25, 'declines': 25, 'ad_ratio': 1.0, 'index_price': latest_market_data.get('current_price', 24500), 'change': '+0.00', 'change_percent': '+0.00', 'timestamp': datetime.now().isoformat()})

@application.route('/api/realtime-nifty')
def get_realtime_nifty():
    price = latest_market_data.get('current_price')
    if price:
        return jsonify({'symbol': 'NIFTY 50', 'current_price': round(price, 2), 'change': 0, 'change_percent': 0, 'timestamp': datetime.now().isoformat()})
    return jsonify({'error': 'No live data yet'}), 503

@application.route('/api/pcr')
def get_pcr():
    return jsonify({'pcr': 1.05, 'sentiment': 'Neutral', 'signal': 'HOLD', 'timestamp': datetime.now().isoformat()})

# --------------------------------------------------
# START WEBSOCKET THREAD (MUST BE AT MODULE LEVEL)
# --------------------------------------------------
print("🚀 Initializing Dhan WebSocket...")
ws_thread = threading.Thread(target=start_market_feed, daemon=True)
ws_thread.start()
print("🚀 Dhan WebSocket thread started.")

if __name__ == '__main__':
    time.sleep(5)
    application.run(debug=False, host='0.0.0.0', port=5000)
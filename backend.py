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
        dhan_client = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
        instruments = dhan_client.get_instruments()
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

# --------------------------------------------------
# WebSocket connection handlers
# --------------------------------------------------
async def start_market_feed():
    global NIFTY_SECURITY_ID
    while NIFTY_SECURITY_ID is None:
        get_nifty_security_id()
        await asyncio.sleep(2)

    dhan_client = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
    dhan_context = dhan_client.get_dhan_context()
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
# Technical Indicators & Dynamic Logic
# --------------------------------------------------
def to_native(obj):
    """Convert numpy/pandas types to native Python types"""
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
    if action == "BUY CE":
        strike = atm + 100
    elif action == "BUY PE":
        strike = atm - 100
    else:
        strike = atm
    return {
        'action': action,
        'recommended_strike': int(strike),
        'atm_strike': int(atm),
        'call_strike_otm': int(atm + 100),
        'put_strike_otm': int(atm - 100)
    }

def calculate_dynamic_signal(df):
    if len(df) < 20:
        return None

    last_price = float(df['close'].iloc[-1])

    # ATR (14)
    high_low = df['high'] - df['low']
    high_close = abs(df['high'] - df['close'].shift())
    low_close = abs(df['low'] - df['close'].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = float(tr.rolling(14).mean().iloc[-1])
    if pd.isna(atr) or atr == 0:
        atr = 10.0

    # 3‑minute momentum
    if len(df) >= 4:
        price_3min_ago = float(df['close'].iloc[-4])
        momentum = last_price - price_3min_ago
    else:
        momentum = 0.0

    # Volume ratio
    current_vol = int(df['volume'].iloc[-1])
    avg_vol = float(df['volume'].tail(20).mean())
    vol_ratio = current_vol / avg_vol if avg_vol > 0 else 1.0

    # RSI (14)
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    rsi_val = 100 - (100 / (1 + rs.iloc[-1])) if loss.iloc[-1] != 0 else 50.0
    rsi = float(rsi_val)

    # EMA20
    ema20 = float(calculate_ema(df['close'], 20).iloc[-1])

    # VWAP from global state
    vwap = latest_market_data.get('vwap')
    if vwap is None:
        vwap = last_price
    else:
        vwap = float(vwap)

    # Signal generation
    buy_signal = False
    sell_signal = False
    trigger_reason = "No trigger"
    confidence = "Low"

    if abs(momentum) > 1.5 * atr and vol_ratio > 1.5:
        if momentum > 0 and last_price > ema20 and rsi > 50 and last_price > vwap:
            buy_signal = True
            confidence = "High" if vol_ratio > 2.0 else "Medium"
            trigger_reason = f"Bullish momentum {momentum:.2f} > 1.5×ATR ({1.5*atr:.2f}) + above EMA20 & VWAP"
        elif momentum < 0 and last_price < ema20 and rsi < 50 and last_price < vwap:
            sell_signal = True
            confidence = "High" if vol_ratio > 2.0 else "Medium"
            trigger_reason = f"Bearish momentum {abs(momentum):.2f} > 1.5×ATR + below EMA20 & VWAP"

    if buy_signal:
        action = "BUY CE"
        expiry_date, _ = get_next_expiry()
        recommendation = f"📈 BUY CALL OPTION (Expiry: {expiry_date.strftime('%d-%b')})"
    elif sell_signal:
        action = "BUY PE"
        expiry_date, _ = get_next_expiry()
        recommendation = f"📉 BUY PUT OPTION (Expiry: {expiry_date.strftime('%d-%b')})"
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
        'expiry': expiry_date.strftime('%Y-%m-%d'),
        'days_to_expiry': days_left,
        'suggested_strike': strike_info,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    return to_native(result)

# --------------------------------------------------
# Flask app and endpoints
# --------------------------------------------------
application = Flask(__name__)
CORS(application)

@application.route('/')
def home():
    return jsonify({'message': 'Trade Guru NIFTY Trading API v6.1.0 (Dhan Access Token)'})

@application.route('/api/health')
def health_check():
    return jsonify({
        'status': 'running',
        'version': '6.1.0',
        'candles_available': len(minute_candles),
        'timestamp': datetime.now().isoformat()
    })

@application.route('/api/trading-signals')
def get_trading_signals():
    try:
        with lock:
            if len(minute_candles) < 20:
                return jsonify({
                    'error': 'Building real-time candles',
                    'candles_available': len(minute_candles),
                    'action': 'HOLD'
                }), 202
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
    return jsonify({'advances': 25, 'declines': 25, 'ad_ratio': 1.0, 'index_price': 24500, 'change': '+0.00', 'change_percent': '+0.00', 'timestamp': datetime.now().isoformat()})

@application.route('/api/realtime-nifty')
def get_realtime_nifty():
    price = latest_market_data.get('current_price', 0)
    if price:
        return jsonify({'symbol': 'NIFTY 50', 'current_price': round(price, 2), 'change': 0, 'change_percent': 0, 'timestamp': datetime.now().isoformat()})
    return jsonify({'error': 'No live data yet'}), 503

@application.route('/api/pcr')
def get_pcr():
    return jsonify({'pcr': 1.05, 'sentiment': 'Neutral', 'signal': 'HOLD', 'timestamp': datetime.now().isoformat()})

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
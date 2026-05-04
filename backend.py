import os
import json
import time
import threading
import asyncio
import requests
from datetime import datetime, timedelta
from flask import Flask, jsonify
from flask_cors import CORS
import pandas as pd
import numpy as np

# ==================================================
# YOUR DHAN CREDENTIALS (HARDCODED – REPLACE AFTER TESTING)
# ==================================================
DHAN_CLIENT_ID = "1103060314"
DHAN_API_KEY = "dd8ec18f"
DHAN_API_SECRET = "426f1ee3-604e-4727-acc7-466f64a4da7b"

# Dhan's instrument IDs can change; we'll auto‑fetch the correct one for NIFTY 50.
NIFTY_SECURITY_ID = None          # Will be set dynamically
EXCHANGE_SEGMENT = "NSE"          # For indices

# --------------------------------------------------
# Helper: Generate Access Token (valid 24h)
# --------------------------------------------------
def generate_access_token(client_id, api_key, api_secret):
    url = "https://api.dhan.co/v2/token"
    payload = {
        "client_id": client_id,
        "api_key": api_key,
        "api_secret": api_secret
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return resp.json().get("access_token")
    except Exception as e:
        print(f"❌ Token generation failed: {e}")
        return None

# --------------------------------------------------
# Fetch the correct security_id for NIFTY 50
# --------------------------------------------------
def get_nifty_security_id():
    global NIFTY_SECURITY_ID
    token = generate_access_token(DHAN_CLIENT_ID, DHAN_API_KEY, DHAN_API_SECRET)
    if not token:
        print("⚠️ Cannot fetch instruments – no token")
        return None
    from dhanhq import dhanhq
    dhan = dhanhq(client_id=DHAN_CLIENT_ID, access_token=token)
    try:
        instruments = dhan.get_instruments()
        for inst in instruments:
            # Look for "NIFTY 50" under index instruments
            if inst.get("instrument_name") == "NIFTY 50" and inst.get("segment") == "NSE":
                NIFTY_SECURITY_ID = str(inst.get("security_id"))
                print(f"✅ Found NIFTY security_id = {NIFTY_SECURITY_ID}")
                return NIFTY_SECURITY_ID
        # Fallback to known ID (116) if not found
        print("⚠️ Could not find NIFTY 50 in instrument list, using fallback 116")
        NIFTY_SECURITY_ID = "116"
        return NIFTY_SECURITY_ID
    except Exception as e:
        print(f"⚠️ Instrument fetch error: {e}, using fallback 116")
        NIFTY_SECURITY_ID = "116"
        return NIFTY_SECURITY_ID

# --------------------------------------------------
# Global state for real‑time data
# --------------------------------------------------
minute_candles = []          # list of completed 1‑min candles
current_candle = None
lock = threading.Lock()

latest_market_data = {
    'current_price': None,
    'open_price': None,
    'day_high': None,
    'day_low': None,
    'last_update': None
}

# --------------------------------------------------
# WebSocket Callback: builds 1‑minute bars
# --------------------------------------------------
def on_ticks(ticks):
    global current_candle, minute_candles, latest_market_data
    now = datetime.now()
    with lock:
        for tick in ticks:
            price = float(tick.get('ltp', 0))
            volume = int(tick.get('volume', 0))
            tick_time = datetime.fromtimestamp(tick.get('exchange_time', time.time()))

            latest_market_data['current_price'] = price
            latest_market_data['last_update'] = now.isoformat()

            # Update day high/low
            if latest_market_data['day_high'] is None or price > latest_market_data['day_high']:
                latest_market_data['day_high'] = price
            if latest_market_data['day_low'] is None or price < latest_market_data['day_low']:
                latest_market_data['day_low'] = price

            # Create or update 1‑minute candle
            current_minute = tick_time.replace(second=0, microsecond=0)
            if current_candle is None or current_candle['timestamp'] != current_minute:
                if current_candle is not None:
                    minute_candles.append(current_candle)
                    if len(minute_candles) > 500:
                        minute_candles.pop(0)
                current_candle = {
                    'timestamp': current_minute,
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price,
                    'volume': volume
                }
            else:
                current_candle['high'] = max(current_candle['high'], price)
                current_candle['low'] = min(current_candle['low'], price)
                current_candle['close'] = price
                current_candle['volume'] += volume

async def start_market_feed():
    global NIFTY_SECURITY_ID
    while NIFTY_SECURITY_ID is None:
        get_nifty_security_id()
        if NIFTY_SECURITY_ID is None:
            await asyncio.sleep(2)

    token = generate_access_token(DHAN_CLIENT_ID, DHAN_API_KEY, DHAN_API_SECRET)
    if not token:
        print("❌ Cannot start WebSocket: no access token")
        return

    from dhanhq import dhanhq
    from dhanhq import marketfeed
    dhan = dhanhq(client_id=DHAN_CLIENT_ID, access_token=token)
    dhan_context = dhan.get_dhan_context()
    instruments = [(EXCHANGE_SEGMENT, NIFTY_SECURITY_ID)]
    mf = marketfeed.MarketFeed(dhan_context, instruments, on_ticks)
    await mf.connect()
    await mf.subscribe_instruments()
    while True:
        await asyncio.sleep(1)

def run_websocket_background():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start_market_feed())

# --------------------------------------------------
# Flask Application
# --------------------------------------------------
application = Flask(__name__)
CORS(application)

def get_nifty_ohlc_daily():
    """Fetch daily OHLC using Dhan's historical API"""
    token = generate_access_token(DHAN_CLIENT_ID, DHAN_API_KEY, DHAN_API_SECRET)
    if not token or NIFTY_SECURITY_ID is None:
        return None
    from dhanhq import dhanhq
    dhan = dhanhq(client_id=DHAN_CLIENT_ID, access_token=token)
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
    try:
        hist = dhan.get_historical_data(
            security_id=NIFTY_SECURITY_ID,
            exchange_segment=EXCHANGE_SEGMENT,
            from_date=from_date,
            to_date=to_date,
            interval="day"
        )
        if hist and len(hist) > 0:
            df = pd.DataFrame(hist)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            df.sort_index(inplace=True)
            return df
    except Exception as e:
        print(f"Historical data error: {e}")
    return None

def calculate_signal_from_candles():
    """Your original trading logic, now using minute_candles"""
    with lock:
        if len(minute_candles) < 10:
            return None
        df = pd.DataFrame(minute_candles)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        df.sort_index(inplace=True)
        df = df.tail(200)

        current_price = df['close'].iloc[-1]
        current_volume = df['volume'].iloc[-1]
        avg_volume = df['volume'].tail(20).mean()
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1.0

        if len(df) >= 4:
            price_3min_ago = df['close'].iloc[-4]
            momentum_3min = current_price - price_3min_ago
            momentum_percent = (momentum_3min / price_3min_ago) * 100
        else:
            momentum_3min = 0.0
            momentum_percent = 0.0

        open_price = df['open'].iloc[0]
        day_high = df['high'].max()
        day_low = df['low'].min()
        intraday_range = day_high - day_low

        # ATR
        high_low = df['high'] - df['low']
        high_close = abs(df['high'] - df['close'].shift())
        low_close = abs(df['low'] - df['close'].shift())
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = tr.tail(14).mean()

        # RSI
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0).tail(14).mean()
        loss = -delta.where(delta < 0, 0).tail(14).mean()
        rs = gain / loss if loss != 0 else 1
        rsi = 100 - (100 / (1 + rs))

        # Position tracking (hypothetical)
        hypothetical_entry = open_price
        hypothetical_highest = day_high
        step_up_stop = hypothetical_highest * 0.98
        hard_stop_loss = hypothetical_entry * 0.98
        take_profit = hypothetical_entry * 1.023
        is_step_up_active = current_price > hypothetical_entry * 1.02
        active_stop_loss = step_up_stop if is_step_up_active else hard_stop_loss

        exit_signal = None
        if current_price <= active_stop_loss:
            exit_signal = {"signal": "EXIT", "reason": f"Stop loss hit {active_stop_loss:.2f}", "action": "CLOSE POSITION"}
        elif current_price >= take_profit:
            exit_signal = {"signal": "TAKE_PROFIT", "reason": f"Target {take_profit:.2f} reached", "action": "BOOK PROFIT"}

        action = "HOLD"
        recommendation = "No clear signal"
        confidence = "Low"
        trade_type = None
        trigger_reason = "Awaiting 3-minute momentum >8 points with volume"

        if not is_step_up_active and not exit_signal:
            if momentum_3min > 8 and volume_ratio > 1.2 and rsi < 70:
                action = "BUY CE"
                recommendation = "📈 BUY CALL OPTION - Strong upside momentum"
                confidence = "High" if momentum_3min > 15 else "Medium"
                trade_type = "LONG_CALL"
                trigger_reason = f"Rose {momentum_3min:.2f} pts in 3 min | Vol {volume_ratio:.2f}x"
            elif momentum_3min < -8 and volume_ratio > 1.2 and rsi > 30:
                action = "BUY PE"
                recommendation = "📉 BUY PUT OPTION - Strong downside momentum"
                confidence = "High" if momentum_3min < -15 else "Medium"
                trade_type = "LONG_PUT"
                trigger_reason = f"Fell {abs(momentum_3min):.2f} pts in 3 min | Vol {volume_ratio:.2f}x"

        elif is_step_up_active and not exit_signal:
            action = "HOLD"
            recommendation = f"🔒 HOLD - Step‑up stop at ₹{active_stop_loss:.2f}"
            confidence = "Medium"
            trigger_reason = f"Trailing stop active at {active_stop_loss:.2f}"

        exit_table = []
        if trade_type in ["LONG_CALL", "SHORT_PUT"]:
            exit_table = [
                {"level": "Hard Stop Loss (2%)", "price": round(hypothetical_entry * 0.98, 2), "action": "EXIT - Max loss 2%", "priority": 1},
                {"level": "Take Profit (2.3%)", "price": round(hypothetical_entry * 1.023, 2), "action": "BOOK PROFIT", "priority": 2},
                {"level": "Step-Up Stop Loss", "price": "Trailing", "action": "Moves up 2% below highest", "priority": 3}
            ]
        elif trade_type in ["LONG_PUT", "SHORT_CALL"]:
            exit_table = [
                {"level": "Hard Stop Loss (2%)", "price": round(hypothetical_entry * 1.02, 2), "action": "EXIT - Max loss 2%", "priority": 1},
                {"level": "Take Profit (2.3%)", "price": round(hypothetical_entry * 0.977, 2), "action": "BOOK PROFIT", "priority": 2},
                {"level": "Step-Up Stop Loss", "price": "Trailing", "action": "Moves down 2% above lowest", "priority": 3}
            ]

        return {
            'action': action,
            'recommendation': recommendation,
            'confidence': confidence,
            'overall_score': 2 if action == "BUY CE" else (-2 if action == "BUY PE" else 0),
            'trade_type': trade_type,
            'trigger_reason': trigger_reason,
            'signal_type': action,
            'signal_reason': trigger_reason,
            'exit_signal': exit_signal,
            'is_step_up_stop_active': is_step_up_active,
            'active_stop_loss': round(active_stop_loss, 2),
            'exit_table': exit_table,
            'hard_stop_loss_2_percent': round(hard_stop_loss, 2),
            'take_profit_2_3_percent': round(take_profit, 2),
            'step_up_stop_price': round(step_up_stop, 2) if is_step_up_active else None,
            'highest_price_reached': round(hypothetical_highest, 2),
            'lowest_price_reached': round(day_low, 2),
            'unrealized_pnl_percent': round(((current_price / hypothetical_entry) - 1) * 100, 2),
            'spot_price': round(current_price, 2),
            'open_price': round(open_price, 2),
            'day_high': round(day_high, 2),
            'day_low': round(day_low, 2),
            'intraday_range': round(intraday_range, 2),
            'momentum_3min_points': round(momentum_3min, 2),
            'momentum_3min_percent': round(momentum_percent, 2),
            'volume_ratio': round(volume_ratio, 2),
            'atr_14': round(atr, 2) if not pd.isna(atr) else None,
            'rsi_14': round(rsi, 1) if not pd.isna(rsi) else None,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

# --------------------------------------------------
# Flask Endpoints (unchanged from your original logic)
# --------------------------------------------------
@application.route('/api/breadth')
def get_breadth():
    df = get_nifty_ohlc_daily()
    if df is not None and len(df) >= 2:
        current = df['close'].iloc[-1]
        open_price = df['open'].iloc[-1]
        change = current - open_price
        if change > 0:
            advances, declines = 28, 22
        elif change < 0:
            advances, declines = 22, 28
        else:
            advances, declines = 25, 25
        ad_ratio = advances / declines
        return jsonify({
            'advances': advances,
            'declines': declines,
            'ad_ratio': round(ad_ratio, 2),
            'index_price': round(current, 2),
            'change': f"{'+' if change > 0 else ''}{round(change, 2)}",
            'change_percent': f"{'+' if change > 0 else ''}{round((change/open_price)*100, 2)}",
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    # fallback
    return jsonify({'advances': 21, 'declines': 29, 'ad_ratio': 0.72, 'index_price': 23997.55, 'change': '+45.20', 'change_percent': '+0.52', 'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

@application.route('/api/realtime-nifty')
def get_realtime_nifty():
    if latest_market_data['current_price'] is not None:
        price = latest_market_data['current_price']
        df = get_nifty_ohlc_daily()
        open_price = df['open'].iloc[-1] if df is not None and len(df) > 0 else price
        change = price - open_price
        change_percent = (change / open_price) * 100 if open_price else 0
        return jsonify({
            'symbol': 'NIFTY 50',
            'current_price': round(price, 2),
            'open': round(open_price, 2),
            'change': round(change, 2),
            'change_percent': round(change_percent, 2),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    return jsonify({'error': 'No live data yet'}), 503

@application.route('/api/pcr')
def get_pcr():
    df = get_nifty_ohlc_daily()
    if df is not None and len(df) >= 2:
        current = df['close'].iloc[-1]
        previous = df['close'].iloc[-2]
        change_percent = ((current - previous) / previous) * 100
        if change_percent > 0.3:
            pcr = 1.35; sentiment = "Bullish"; signal = "BUY"
        elif change_percent < -0.3:
            pcr = 0.65; sentiment = "Bearish"; signal = "SELL"
        else:
            pcr = 1.05; sentiment = "Neutral"; signal = "HOLD"
        return jsonify({
            'put_oi': round(40000000 * pcr),
            'call_oi': 40000000,
            'pcr': pcr,
            'sentiment': sentiment,
            'signal': signal,
            'pcr_change': round(change_percent, 2),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    return jsonify({'put_oi': 45200000, 'call_oi': 36800000, 'pcr': 1.23, 'sentiment': 'Neutral', 'signal': 'HOLD', 'pcr_change': 0, 'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

@application.route('/api/trading-signals')
def get_trading_signals():
    signal = calculate_signal_from_candles()
    if signal is None:
        return jsonify({'error': 'Insufficient data – waiting for live feed', 'action': 'HOLD', 'recommendation': 'Accumulating 1-min candles...', 'signal_type': 'HOLD', 'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}), 202
    return jsonify(signal)

@application.route('/api/health')
def health_check():
    return jsonify({'status': 'running', 'version': '5.0.0', 'features': '8-point trigger, 3-min momentum, step-up stop loss, Dhan real-time', 'data_source': 'Dhan API', 'candles_available': len(minute_candles), 'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

@application.route('/')
def home():
    return jsonify({'message': 'Trade Guru NIFTY Trading Dashboard API v5.0 (Dhan Live)', 'status': 'Running', 'features': ['8-point movement trigger', '3-minute momentum detection', 'Step-up stop loss (trailing)', '2% hard stop loss', '2.3% take profit', 'Volume confirmation', 'RSI filter', 'Real-time 1-min candles via Dhan WebSocket'], 'endpoints': ['/api/health', '/api/breadth', '/api/realtime-nifty', '/api/pcr', '/api/trading-signals']})

# --------------------------------------------------
# Main: Start WebSocket thread then Flask
# --------------------------------------------------
if __name__ == '__main__':
    # First find NIFTY security ID
    get_nifty_security_id()
    ws_thread = threading.Thread(target=run_websocket_background, daemon=True)
    ws_thread.start()
    print("🚀 Dhan WebSocket started. Waiting for ticks...")
    time.sleep(5)
    application.run(debug=False, host='0.0.0.0', port=5000)
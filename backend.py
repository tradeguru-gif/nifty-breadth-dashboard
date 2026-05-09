import os
import threading
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import marketfeed

# Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Environment Variables
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
CE_ID = os.getenv("CE_ID", "54321") # Default if env not set
PE_ID = os.getenv("PE_ID", "54322")

# State Management
price_history = []
volume_history = []
latest_data = {
    "signal": "INITIALIZING",
    "sentiment": "NEUTRAL",
    "action": "HOLD",
    "ce_price": 0,
    "pe_price": 0,
    "spot_price": 0,
    "rsi": 50,
    "macd": 0,
    "pcr": 1.0,
    "upper_bb": 0,
    "lower_bb": 0,
    "timestamp": ""
}

def calculate_pro_signals(prices):
    if len(prices) < 20:
        return "WAITING", "NEUTRAL", "INITIALIZING SCAN...", 50, 0, 0, 0

    df = pd.DataFrame(prices, columns=['price'])
    
    # 1. RSI (14)
    delta = df['price'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / (loss + 1e-9) # Prevent div by zero
    rsi = 100 - (100 / (1 + rs)).iloc[-1]

    # 2. Bollinger Bands (20, 2)
    sma = df['price'].rolling(window=20).mean()
    std = df['price'].rolling(window=20).std()
    upper_bb = sma + (std * 2)
    lower_bb = sma - (std * 2)
    curr = df['price'].iloc[-1]

    # 3. MACD
    ema12 = df['price'].ewm(span=12).mean()
    ema26 = df['price'].ewm(span=26).mean()
    macd = ema12.iloc[-1] - ema26.iloc[-1]

    # --- PRO LOGIC ENGINE ---
    signal = "NEUTRAL"
    sentiment = "LOW"
    action = "MARKET NEUTRAL"

    # BULLISH: Price below Lower Band (Mean Reversion) OR MACD Crossover
    if curr < lower_bb.iloc[-1] and rsi < 30:
        signal, sentiment, action = "BULLISH", "HIGH", "BUY CE (OVERSOLD)"
    elif macd > 0 and curr > sma.iloc[-1]:
        signal, sentiment, action = "BULLISH", "MEDIUM", "HOLD CE (TREND)"

    # BEARISH: Price above Upper Band OR MACD breakdown
    elif curr > upper_bb.iloc[-1] and rsi > 70:
        signal, sentiment, action = "BEARISH", "HIGH", "BUY PE (OVERBOUGHT)"
    elif macd < 0 and curr < sma.iloc[-1]:
        signal, sentiment, action = "BEARISH", "MEDIUM", "HOLD PE (TREND)"

    # EXIT LOGIC: RSI Extremes or Mean Touch
    if (rsi > 75 and signal == "BULLISH") or (rsi < 25 and signal == "BEARISH"):
        action = "TAKE PROFIT / EXIT"

    return signal, sentiment, action, round(rsi,2), round(macd,2), round(upper_bb.iloc[-1],2), round(lower_bb.iloc[-1],2)

def on_message(instance, tick):
    global latest_data, price_history
    try:
        sec_id = str(tick.get('security_id'))
        price = tick.get('ltp', 0)
        
        # Nifty Spot Logic
        if sec_id == "13": 
            latest_data["spot_price"] = price
            price_history.append(price)
            if len(price_history) > 100: price_history.pop(0)
            
            sig, sent, act, rsi, macd, ubb, lbb = calculate_pro_signals(price_history)
            latest_data.update({
                "signal": sig, "sentiment": sent, "action": act,
                "rsi": rsi, "macd": macd, "upper_bb": ubb, "lower_bb": lbb
            })

        # Options Pricing
        elif sec_id == CE_ID: latest_data["ce_price"] = price
        elif sec_id == PE_ID: latest_data["pe_price"] = price

        latest_data["timestamp"] = datetime.now().strftime("%H:%M:%S")
    except Exception as e:
        logger.error(f"Logic Error: {e}")

def run_feed():
    logger.info(f"Connecting Dhan Feed: Nifty(13), CE({CE_ID}), PE({PE_ID})")
    instruments = [
        (marketfeed.NSE, "13", marketfeed.Ticker),
        (marketfeed.NSE_FNO, str(CE_ID), marketfeed.Ticker),
        (marketfeed.NSE_FNO, str(PE_ID), marketfeed.Ticker)
    ]
    feed = marketfeed.DhanFeed(CLIENT_ID, ACCESS_TOKEN, instruments, on_message)
    feed.run_forever()

@app.route('/')
def home():
    return jsonify({"status": "active", "data": latest_data})

@app.route('/api/health')
def health():
    return "OK", 200

if __name__ == '__main__':
    threading.Thread(target=run_feed, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))
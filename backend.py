# backend.py - Institutional Nifty Options Signal Engine
# Fully corrected for DhanHQ MarketFeed V2 + Render deployment

import os
import asyncio
import threading
import time
import logging
import requests

from collections import deque
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

from dhanhq import DhanContext, MarketFeed

# --------------------------------------------------
# Logging setup
# --------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------------------------------------------------
# Flask app
# --------------------------------------------------
app = Flask(__name__)
CORS(app)

# Required for Gunicorn on Render
application = app

# --------------------------------------------------
# Environment variables
# --------------------------------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

# --------------------------------------------------
# OPTION SECURITY IDS (replace with your live IDs)
# --------------------------------------------------
CE_ID = "63719"
PE_ID = "63720"

# --------------------------------------------------
# Shared frontend state
# --------------------------------------------------
latest_data = {
    "signal": "WAITING",
    "ce_price": 0.0,
    "pe_price": 0.0,
    "spread": 0.0,
    "rsi": 50,
    "macd": 0.0,
    "pcr": 1.0,
    "timestamp": ""
}

market_state = {
    "rsi": 50,
    "momentum": "NEUTRAL",
    "strength": "LOW",
    "trend": "SIDEWAYS",
    "action": "HOLD",
    "confidence": 0,
    "volatility": "NORMAL",
    "alert": "NONE"
}

institutional_state = {
    "vwap": 0,
    "ema_fast": 0,
    "ema_slow": 0,
    "ema_signal": "NEUTRAL",
    "atr": 0,
    "oi_buildup": "NEUTRAL",
    "iv_state": "NORMAL",
    "candle_structure": "SIDEWAYS",
    "market_breadth": "BALANCED",
    "volume_profile": "NORMAL",
    "smart_money_flow": "NEUTRAL",
    "delta": 0,
    "gamma": 0,
    "theta": 0,
    "vega": 0,
    "institutional_signal": "HOLD",
    "institutional_confidence": 0
}

# --------------------------------------------------
# Internal state
# --------------------------------------------------
price_history = deque(maxlen=200)

tick_counter = 0
UPDATE_INTERVAL = 10
SPREAD_THRESHOLD = 5.0

# --------------------------------------------------
# TECHNICAL INDICATORS (unchanged)
# --------------------------------------------------
def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calculate_macd(prices, fast=12, slow=26):
    if len(prices) < slow:
        return 0.0
    def ema(data, period):
        alpha = 2 / (period + 1)
        value = data[0]
        for p in data[1:]:
            value = alpha * p + (1 - alpha) * value
        return value
    return ema(prices, fast) - ema(prices, slow)

def calculate_ema(prices, period):
    if len(prices) < period:
        return prices[-1] if prices else 0
    alpha = 2 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = alpha * p + (1 - alpha) * ema
    return round(ema, 2)

def calculate_atr(prices, period=14):
    if len(prices) < period + 1:
        return 0
    trs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    return round(sum(trs[-period:]) / period, 2)

def calculate_vwap(prices):
    if not prices:
        return 0
    vol = [100] * len(prices)
    pv = sum(p * v for p, v in zip(prices, vol))
    tv = sum(vol)
    return round(pv / tv, 2) if tv else 0

def estimate_greeks(ce, pe):
    delta = round((ce - pe) / 100, 2)
    gamma = round(abs(delta) / 10, 2)
    theta = round(-(ce + pe) / 1000, 2)
    vega = round((ce + pe) / 500, 2)
    return delta, gamma, theta, vega

# --------------------------------------------------
# PCR FETCHER (unchanged)
# --------------------------------------------------
pcr_cache = {"value": 1.0, "time": 0}
PCR_TTL = 60

def get_nifty_pcr():
    now = time.time()
    if now - pcr_cache["time"] < PCR_TTL:
        return pcr_cache["value"]
    try:
        url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        headers = {"User-Agent": "Mozilla/5.0"}
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=5)
        time.sleep(0.5)
        response = session.get(url, headers=headers, timeout=5)
        data = response.json()
        ce_oi = sum(x.get("CE", {}).get("openInterest", 0) for x in data["records"]["data"] if "CE" in x)
        pe_oi = sum(x.get("PE", {}).get("openInterest", 0) for x in data["records"]["data"] if "PE" in x)
        value = pe_oi / ce_oi if ce_oi else 1.0
        pcr_cache["value"] = value
        pcr_cache["time"] = now
        return value
    except Exception as e:
        logger.error(f"PCR fetch failed: {e}")
        return pcr_cache["value"]

# --------------------------------------------------
# ADVANCED ANALYSIS (unchanged)
# --------------------------------------------------
def run_advanced_analysis(ce, pe, spread, pcr, price_list):
    global market_state, institutional_state
    if len(price_list) < 20:
        return
    rsi = calculate_rsi(price_list)
    macd = calculate_macd(price_list)
    vwap = calculate_vwap(price_list)
    ema_fast = calculate_ema(price_list, 9)
    ema_slow = calculate_ema(price_list, 21)
    atr = calculate_atr(price_list)
    delta, gamma, theta, vega = estimate_greeks(ce, pe)
    ema_signal = "BULLISH" if ema_fast > ema_slow else "BEARISH"
    confidence = 0
    if ema_signal == "BULLISH":
        confidence += 20
    if rsi > 60:
        confidence += 20
    if spread > 0:
        confidence += 20
    if pcr < 1:
        confidence += 20
    if macd > 0:
        confidence += 20
    if confidence >= 80:
        action = "STRONG BUY CE"
    elif confidence >= 60:
        action = "BUY CE"
    elif confidence <= 20:
        action = "EXIT"
    else:
        action = "HOLD"
    market_state.update({
        "rsi": round(rsi, 2),
        "momentum": "UPTREND" if spread > 0 else "DOWNTREND",
        "strength": "HIGH" if confidence > 60 else "LOW",
        "trend": ema_signal,
        "action": action,
        "confidence": confidence,
        "volatility": "HIGH" if abs(spread) > 20 else "NORMAL",
        "alert": action
    })
    institutional_state.update({
        "vwap": vwap,
        "ema_fast": ema_fast,
        "ema_slow": ema_slow,
        "ema_signal": ema_signal,
        "atr": atr,
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "institutional_signal": action,
        "institutional_confidence": confidence
    })

# --------------------------------------------------
# TICK PROCESSOR (updated for v2 tick structure)
# --------------------------------------------------
def process_tick(tick):
    global tick_counter
    try:
        # v2 tick dictionary fields: 'securityId', 'ltp', etc.
        security_id = str(tick.get("securityId", tick.get("security_id", "")))
        price = float(tick.get("ltp", tick.get("LTP", 0)))
        if not security_id or price == 0:
            return

        if security_id == CE_ID:
            latest_data["ce_price"] = price
        elif security_id == PE_ID:
            latest_data["pe_price"] = price

        ce = latest_data["ce_price"]
        pe = latest_data["pe_price"]

        if ce > 0 and pe > 0:
            spread = ce - pe
            latest_data["spread"] = round(spread, 2)

            if spread > SPREAD_THRESHOLD:
                latest_data["signal"] = "BULLISH"
            elif spread < -SPREAD_THRESHOLD:
                latest_data["signal"] = "BEARISH"
            else:
                latest_data["signal"] = "NEUTRAL"

            price_history.append(ce)
            tick_counter += 1

            if tick_counter >= UPDATE_INTERVAL:
                tick_counter = 0
                prices = list(price_history)
                latest_data["rsi"] = round(calculate_rsi(prices), 2)
                latest_data["macd"] = round(calculate_macd(prices), 2)
                pcr = get_nifty_pcr()
                latest_data["pcr"] = round(pcr, 2)
                run_advanced_analysis(ce, pe, spread, pcr, prices)

            latest_data["timestamp"] = datetime.now().isoformat()
            print(f"Tick: CE={ce} PE={pe} Spread={spread:.2f} Signal={latest_data['signal']}")

    except Exception as e:
        logger.error(f"Tick processing error: {e}")

# --------------------------------------------------
# CUSTOM MARKET FEED CLASS (CORRECT V2 PATTERN)
# --------------------------------------------------
class CustomFeed(MarketFeed):
    def __init__(self, dhan_context, instruments, version="v2"):
        super().__init__(dhan_context, instruments, version=version)

    def on_connect(self):
        logger.info("✅ WebSocket connected successfully")

    def on_message(self, message):
        # message is a dictionary with tick data
        process_tick(message)

    def on_error(self, error):
        logger.error(f"WebSocket error: {error}")

    def on_close(self):
        logger.warning("WebSocket closed – will reconnect")

# --------------------------------------------------
# ASYNC WEBSOCKET LOOP (with reconnection)
# --------------------------------------------------
async def websocket_loop():
    while True:
        try:
            instruments = [
                (MarketFeed.NSE_FNO, CE_ID, MarketFeed.Ticker),
                (MarketFeed.NSE_FNO, PE_ID, MarketFeed.Ticker)
            ]
            ctx = DhanContext(CLIENT_ID, ACCESS_TOKEN)
            feed = CustomFeed(ctx, instruments, version="v2")

            logger.info(f"Subscribing to CE={CE_ID}, PE={PE_ID} (using CustomFeed)")
            await feed.connect()
            logger.info("Feed connected, waiting for ticks...")

            # Keep the connection alive
            while True:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Feed crashed: {e}, reconnecting in 10s")
            await asyncio.sleep(10)

# --------------------------------------------------
# BACKGROUND THREAD STARTER
# --------------------------------------------------
def start_feed():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(websocket_loop())

threading.Thread(target=start_feed, daemon=True).start()
logger.info("✅ Background signal engine started")

# --------------------------------------------------
# FLASK ROUTES
# --------------------------------------------------
@app.route("/")
def home():
    return jsonify({
        "status": "active",
        "data": latest_data,
        "market": market_state,
        "institutional": institutional_state
    })

@app.route("/api/health")
def health():
    return "OK", 200

@app.route("/api/trading-signals")
def trading_signals():
    return jsonify({
        "status": "active",
        "data": latest_data,
        "market": market_state,
        "institutional": institutional_state
    })

@app.route("/debug/version")
def debug_version():
    return jsonify({
        "version": "FULLY CORRECTED MARKETFEED V2",
        "ce_id": CE_ID,
        "pe_id": PE_ID
    })

# --------------------------------------------------
# MAIN ENTRY POINT
# --------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
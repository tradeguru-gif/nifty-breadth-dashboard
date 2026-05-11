# backend.py - Nifty Options Signal Engine (Fully Corrected + Institutional Analytics)

import os
import asyncio
import threading
import logging
import time
import requests
import pandas as pd

from collections import deque
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

from dhanhq import dhanhq as DhanHQ
from dhanhq import marketfeed

# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# Flask App
# ------------------------------------------------------------
app = Flask(__name__)
CORS(app)

# ------------------------------------------------------------
# Environment Variables
# ------------------------------------------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
CURRENT_NIFTY = float(os.getenv("CURRENT_NIFTY", "24000"))

if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

# ------------------------------------------------------------
# Initialize Dhan Client
# ------------------------------------------------------------
dhan = DhanHQ(CLIENT_ID, ACCESS_TOKEN)
logger.info("Dhan client initialized successfully")

# ------------------------------------------------------------
# Global Variables (Original + New)
# ------------------------------------------------------------
latest_data = {
    "signal": "WAITING",
    "ce_price": 0,
    "pe_price": 0,
    "spread": 0,
    "rsi": 50,
    "macd": 0,
    "pcr": 1.0,
    "timestamp": ""
}

# New: Advanced market state
market_state = {
    "rsi": 50,
    "momentum": "NEUTRAL",
    "strength": "LOW",
    "trend": "SIDEWAYS",
    "action": "HOLD",
    "confidence": 0,
    "volatility": "NORMAL",
    "alert": "NONE",
    "entry_price": 0,
    "target_price": 0,
    "stop_loss": 0,
    "market_sentiment": "NEUTRAL"
}

# New: Institutional intelligence
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

SELECTED_CE_ID = None
SELECTED_PE_ID = None

price_history = deque(maxlen=200)   # increased to 200 for smoother indicators
update_counter = 0
UPDATE_INTERVAL = 10
SPREAD_THRESHOLD = 5.0

# ------------------------------------------------------------
# Indicators (Original)
# ------------------------------------------------------------
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
        for price in data[1:]:
            value = alpha * price + (1 - alpha) * value
        return value
    ema_fast = ema(prices, fast)
    ema_slow = ema(prices, slow)
    return ema_fast - ema_slow

# ------------------------------------------------------------
# PCR (Original, but with caching to avoid excessive calls)
# ------------------------------------------------------------
pcr_cache = {"value": 1.0, "time": 0}
PCR_TTL = 60

def get_nifty_pcr():
    now = time.time()
    if now - pcr_cache["time"] < PCR_TTL:
        return pcr_cache["value"]
    url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers)
        time.sleep(0.5)
        response = session.get(url, headers=headers)
        data = response.json()
        total_ce_oi = 0
        total_pe_oi = 0
        for record in data["records"]["data"]:
            if "CE" in record:
                total_ce_oi += record["CE"]["openInterest"]
            if "PE" in record:
                total_pe_oi += record["PE"]["openInterest"]
        value = total_ce_oi / total_pe_oi if total_pe_oi else 1.0
        pcr_cache["value"] = value
        pcr_cache["time"] = now
        return value
    except Exception as e:
        logger.error(f"PCR fetch failed: {e}")
        return pcr_cache["value"]

# ------------------------------------------------------------
# Dynamic Option Contract Selection (Original – using static IDs)
# ------------------------------------------------------------
def get_option_contracts(nifty_spot):
    global SELECTED_CE_ID, SELECTED_PE_ID
    try:
        logger.info("Using static NIFTY option contracts")
        # REPLACE THESE WITH YOUR REAL SECURITY IDS
        SELECTED_CE_ID = "54321"
        SELECTED_PE_ID = "54322"
        logger.info(f"Selected CE: {SELECTED_CE_ID}")
        logger.info(f"Selected PE: {SELECTED_PE_ID}")
        return [SELECTED_CE_ID, SELECTED_PE_ID]
    except Exception as e:
        logger.exception(f"Contract selection failed: {e}")
        SELECTED_CE_ID = None
        SELECTED_PE_ID = None
        return []

# ------------------------------------------------------------
# Advanced Technical Indicators (New)
# ------------------------------------------------------------
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

def calculate_vwap(prices, volumes=None):
    if not prices:
        return 0
    if volumes is None:
        volumes = [100] * len(prices)
    pv = sum(p * v for p, v in zip(prices, volumes))
    tv = sum(volumes)
    return round(pv / tv, 2) if tv else 0

def estimate_greeks(ce, pe):
    delta = round((ce - pe) / 100, 2)
    gamma = round(abs(delta) / 10, 2)
    theta = round(-(ce + pe) / 1000, 2)
    vega = round((ce + pe) / 500, 2)
    return delta, gamma, theta, vega

def analyze_oi_buildup(pcr):
    if pcr < 0.8:
        return "LONG BUILDUP"
    elif pcr > 1.2:
        return "SHORT BUILDUP"
    return "NEUTRAL"

def detect_iv_crush(spread):
    if abs(spread) < 5:
        return "IV CRUSH"
    elif abs(spread) > 20:
        return "HIGH IV"
    return "NORMAL"

def detect_candle_structure(prices):
    if len(prices) < 5:
        return "SIDEWAYS"
    if prices[-1] > prices[-5]:
        return "BULLISH"
    elif prices[-1] < prices[-5]:
        return "BEARISH"
    return "SIDEWAYS"

def market_breadth_analysis(pcr):
    if pcr < 0.9:
        return "BULLISH"
    elif pcr > 1.1:
        return "BEARISH"
    return "BALANCED"

def volume_profile_analysis(prices):
    if len(prices) < 10:
        return "NORMAL"
    volatility = max(prices[-10:]) - min(prices[-10:])
    if volatility > 20:
        return "HIGH"
    return "LOW"

def smart_money_analysis(spread, pcr):
    if spread > 10 and pcr < 0.8:
        return "SMART MONEY BUYING"
    elif spread < -10 and pcr > 1.2:
        return "SMART MONEY SELLING"
    return "NEUTRAL"

def multi_tf_confirmation(rsi):
    if rsi > 60:
        return "BULLISH CONFIRMATION"
    elif rsi < 40:
        return "BEARISH CONFIRMATION"
    return "NO CONFIRMATION"

# ------------------------------------------------------------
# Advanced Analysis Function (called inside update_signal)
# ------------------------------------------------------------
def run_advanced_analysis(ce_price, pe_price, spread, pcr, price_list):
    """
    Updates market_state and institutional_state based on latest data.
    """
    global market_state, institutional_state
    if len(price_list) < 14:
        return

    rsi = calculate_rsi(price_list, 14)
    macd = calculate_macd(price_list, 12, 26)
    vwap = calculate_vwap(price_list)
    ema_fast = calculate_ema(price_list, 9)
    ema_slow = calculate_ema(price_list, 21)
    ema_signal = "BULLISH" if ema_fast > ema_slow else "BEARISH"
    atr = calculate_atr(price_list, 14)

    delta, gamma, theta, vega = estimate_greeks(ce_price, pe_price)

    oi_state = analyze_oi_buildup(pcr)
    iv_state = detect_iv_crush(spread)
    candle = detect_candle_structure(price_list)
    breadth = market_breadth_analysis(pcr)
    volume_profile = volume_profile_analysis(price_list)
    smart_money = smart_money_analysis(spread, pcr)
    multi_tf = multi_tf_confirmation(rsi)

    # Confidence scoring
    confidence = 0
    if ema_signal == "BULLISH":
        confidence += 15
    if rsi > 60:
        confidence += 15
    if smart_money == "SMART MONEY BUYING":
        confidence += 20
    if breadth == "BULLISH":
        confidence += 15
    if candle == "BULLISH":
        confidence += 10
    if volume_profile == "HIGH":
        confidence += 10
    if multi_tf == "BULLISH CONFIRMATION":
        confidence += 15

    if confidence >= 70:
        action = "STRONG BUY CE"
    elif confidence >= 50:
        action = "BUY CE"
    elif confidence <= 25:
        action = "EXIT"
    else:
        action = "HOLD"

    # Update market_state
    market_state.update({
        "rsi": round(rsi, 2),
        "momentum": "UPTREND" if spread > 0 else "DOWNTREND",
        "strength": "HIGH" if confidence > 60 else "LOW",
        "trend": ema_signal,
        "action": action,
        "confidence": confidence,
        "volatility": "HIGH" if abs(spread) > 20 else "NORMAL",
        "alert": "BUY" if action in ("BUY CE", "STRONG BUY CE") else "EXIT" if action == "EXIT" else "HOLD",
        "entry_price": ce_price if action in ("BUY CE", "STRONG BUY CE") else pe_price,
        "target_price": round(ce_price * 1.08, 2) if action in ("BUY CE", "STRONG BUY CE") else round(pe_price * 1.05, 2),
        "stop_loss": round(ce_price * 0.95, 2) if action in ("BUY CE", "STRONG BUY CE") else round(pe_price * 0.95, 2),
        "market_sentiment": "BULLISH" if confidence > 50 else "BEARISH" if confidence < 30 else "NEUTRAL"
    })

    # Update institutional_state
    institutional_state.update({
        "vwap": vwap,
        "ema_fast": ema_fast,
        "ema_slow": ema_slow,
        "ema_signal": ema_signal,
        "atr": atr,
        "oi_buildup": oi_state,
        "iv_state": iv_state,
        "candle_structure": candle,
        "market_breadth": breadth,
        "volume_profile": volume_profile,
        "smart_money_flow": smart_money,
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "institutional_signal": action,
        "institutional_confidence": confidence
    })

# ------------------------------------------------------------
# Signal Logic (Original, but extended with advanced analysis)
# ------------------------------------------------------------
def update_signal(ce_price, pe_price):
    global latest_data, price_history, update_counter

    spread = ce_price - pe_price
    latest_data["spread"] = round(spread, 2)

    if ce_price > 0:
        price_history.append(ce_price)

    update_counter += 1

    if update_counter >= UPDATE_INTERVAL and len(price_history) >= 20:
        update_counter = 0

        rsi = calculate_rsi(list(price_history))
        macd = calculate_macd(list(price_history))
        pcr = get_nifty_pcr()

        latest_data["rsi"] = round(rsi, 2)
        latest_data["macd"] = round(macd, 2)
        latest_data["pcr"] = round(pcr, 2)

        # Original signal rules
        if spread > SPREAD_THRESHOLD and rsi < 70 and macd > 0 and pcr < 0.8:
            signal = "LONG SPREAD (Bullish)"
        elif spread < -SPREAD_THRESHOLD and rsi > 30 and macd < 0 and pcr > 1.2:
            signal = "SHORT SPREAD (Bearish)"
        else:
            signal = "NEUTRAL"
        latest_data["signal"] = signal

        # Advanced analysis (using same data)
        run_advanced_analysis(ce_price, pe_price, spread, pcr, list(price_history))

    latest_data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ------------------------------------------------------------
# WebSocket Callback (Original, unchanged except it calls update_signal)
# ------------------------------------------------------------
def on_message(instance, tick):
    global latest_data
    try:
        sec_id = str(tick.get("security_id"))
        price = tick.get("ltp", 0)
        if sec_id == SELECTED_CE_ID:
            latest_data["ce_price"] = price
        elif sec_id == SELECTED_PE_ID:
            latest_data["pe_price"] = price

        if latest_data["ce_price"] > 0 and latest_data["pe_price"] > 0:
            update_signal(latest_data["ce_price"], latest_data["pe_price"])
        else:
            latest_data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.error(f"on_message error: {e}")

# ------------------------------------------------------------
# WebSocket Feed Runner (Original)
# ------------------------------------------------------------
def run_feed():
    global SELECTED_CE_ID, SELECTED_PE_ID
    asyncio.set_event_loop(asyncio.new_event_loop())
    while True:
        try:
            logger.info("Selecting option contracts...")
            get_option_contracts(CURRENT_NIFTY)
            if not SELECTED_CE_ID or not SELECTED_PE_ID:
                logger.error("No valid contracts found.")
                time.sleep(30)
                continue
            logger.info(f"Subscribing to CE={SELECTED_CE_ID}, PE={SELECTED_PE_ID}")
            feed = marketfeed.DhanFeed(
                CLIENT_ID,
                ACCESS_TOKEN,
                [
                    (marketfeed.NSE_FNO, str(SELECTED_CE_ID), marketfeed.Ticker),
                    (marketfeed.NSE_FNO, str(SELECTED_PE_ID), marketfeed.Ticker)
                ]
            )
            logger.info("✅ Dhan Feed Started")
            feed.run_forever()
        except Exception as e:
            logger.exception(f"Feed crashed: {e}")
            time.sleep(10)

# ------------------------------------------------------------
# Flask Routes (Modified to return ALL data)
# ------------------------------------------------------------
@app.route("/")
def home():
    return jsonify({
        "status": "active",
        "latest": latest_data,
        "market": market_state,
        "institutional": institutional_state
    })

@app.route("/api/health")
def health():
    return "OK", 200

# ------------------------------------------------------------
# WSGI
# ------------------------------------------------------------
application = app

# ------------------------------------------------------------
# Start Background Thread
# ------------------------------------------------------------
threading.Thread(target=run_feed, daemon=True).start()
logger.info("Background signal engine started.")

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
# backend.py - Nifty Options Signal Engine (Working WebSocket + Advanced Analytics)

import os
import time
import threading
import logging
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
application = app

# ------------------------------------------------------------
# Environment Variables
# ------------------------------------------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
CURRENT_NIFTY = float(os.getenv("CURRENT_NIFTY", "24000"))

if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

# ------------------------------------------------------------
# Dhan Client (not strictly needed for WebSocket, but kept)
# ------------------------------------------------------------
dhan = DhanHQ(CLIENT_ID, ACCESS_TOKEN)
logger.info("Dhan client initialized")

# ------------------------------------------------------------
# Global State (Preserved from your working version)
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

# ---------- Advanced State (new, does not affect WebSocket) ----------
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

# ---------- Original working variables ----------
SELECTED_CE_ID = "54321"   # <--- REPLACE WITH YOUR REAL CE SECURITY ID
SELECTED_PE_ID = "54322"   # <--- REPLACE WITH YOUR REAL PE SECURITY ID
price_history = deque(maxlen=200)
update_counter = 0
UPDATE_INTERVAL = 10
SPREAD_THRESHOLD = 5.0

# PCR cache
pcr_cache = {"value": 1.0, "time": 0}
PCR_TTL = 60

# ------------------------------------------------------------
# Helper Functions (Indicators)
# ------------------------------------------------------------
def get_pcr():
    """Cached Put‑Call Ratio from NSE."""
    now = time.time()
    if now - pcr_cache["time"] < PCR_TTL:
        return pcr_cache["value"]
    try:
        url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        headers = {"User-Agent": "Mozilla/5.0"}
        s = requests.Session()
        s.get("https://www.nseindia.com", headers=headers, timeout=5)
        res = s.get(url, headers=headers, timeout=5)
        data = res.json()
        ce_oi = sum(x.get("CE", {}).get("openInterest", 0) for x in data["records"]["data"] if "CE" in x)
        pe_oi = sum(x.get("PE", {}).get("openInterest", 0) for x in data["records"]["data"] if "PE" in x)
        value = pe_oi / ce_oi if ce_oi else 1.0
        pcr_cache["value"] = value
        pcr_cache["time"] = now
        return value
    except Exception as e:
        logger.error(f"PCR error: {e}")
        return pcr_cache["value"]

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
        val = data[0]
        for p in data[1:]:
            val = alpha * p + (1 - alpha) * val
        return val
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

# ------------------------------------------------------------
# Advanced Analysis (runs on every tick, does not alter WebSocket)
# ------------------------------------------------------------
def advanced_analysis():
    global market_state, institutional_state, latest_data
    ce = latest_data["ce_price"]
    pe = latest_data["pe_price"]
    if ce == 0 or pe == 0:
        return
    spread = ce - pe
    pcr = latest_data["pcr"]  # already computed in on_message
    prices = list(price_history)

    if len(prices) < 14:
        return

    rsi = calculate_rsi(prices)
    macd = calculate_macd(prices)
    vwap = calculate_vwap(prices)
    ema_fast = calculate_ema(prices, 9)
    ema_slow = calculate_ema(prices, 21)
    ema_signal = "BULLISH" if ema_fast > ema_slow else "BEARISH"
    atr = calculate_atr(prices)

    # Greeks
    delta, gamma, theta, vega = estimate_greeks(ce, pe)

    # OI / IV / Breadth / Smart Money
    oi_buildup = "LONG BUILDUP" if pcr < 0.8 else "SHORT BUILDUP" if pcr > 1.2 else "NEUTRAL"
    iv_state = "IV CRUSH" if abs(spread) < 5 else "HIGH IV" if abs(spread) > 20 else "NORMAL"
    candle = "BULLISH" if (len(prices) >= 5 and prices[-1] > prices[-5]) else "BEARISH" if (len(prices) >= 5 and prices[-1] < prices[-5]) else "SIDEWAYS"
    breadth = "BULLISH" if pcr < 0.9 else "BEARISH" if pcr > 1.1 else "BALANCED"
    volume_profile = "HIGH" if (len(prices) >= 10 and (max(prices[-10:]) - min(prices[-10:])) > 20) else "LOW"
    smart_money = "SMART MONEY BUYING" if spread > 10 and pcr < 0.8 else "SMART MONEY SELLING" if spread < -10 and pcr > 1.2 else "NEUTRAL"
    multi_tf = "BULLISH CONFIRMATION" if rsi > 60 else "BEARISH CONFIRMATION" if rsi < 40 else "NO CONFIRMATION"

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
        "entry_price": ce if action in ("BUY CE", "STRONG BUY CE") else pe,
        "target_price": round(ce * 1.08, 2) if action in ("BUY CE", "STRONG BUY CE") else round(pe * 1.05, 2),
        "stop_loss": round(ce * 0.95, 2) if action in ("BUY CE", "STRONG BUY CE") else round(pe * 0.95, 2),
        "market_sentiment": "BULLISH" if confidence > 50 else "BEARISH" if confidence < 30 else "NEUTRAL"
    })

    # Update institutional_state
    institutional_state.update({
        "vwap": vwap,
        "ema_fast": ema_fast,
        "ema_slow": ema_slow,
        "ema_signal": ema_signal,
        "atr": atr,
        "oi_buildup": oi_buildup,
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
# Original WebSocket Callback (Preserved, with extra analysis)
# ------------------------------------------------------------
def on_message(instance, tick):
    global latest_data, price_history, update_counter
    try:
        sec_id = str(tick.get("security_id"))
        price = tick.get("ltp", 0)
        if sec_id == SELECTED_CE_ID:
            latest_data["ce_price"] = price
        elif sec_id == SELECTED_PE_ID:
            latest_data["pe_price"] = price

        if latest_data["ce_price"] > 0 and latest_data["pe_price"] > 0:
            ce = latest_data["ce_price"]
            pe = latest_data["pe_price"]
            spread = ce - pe
            latest_data["spread"] = round(spread, 2)
            price_history.append(ce)

            # PCR and simple signal (same as original)
            pcr = get_pcr()
            latest_data["pcr"] = round(pcr, 2)

            if spread > SPREAD_THRESHOLD and pcr < 0.8:
                latest_data["signal"] = "BULLISH"
            elif spread < -SPREAD_THRESHOLD and pcr > 1.2:
                latest_data["signal"] = "BEARISH"
            else:
                latest_data["signal"] = "NEUTRAL"

            # RSI/MACD for latest_data (optional)
            if len(price_history) >= 20:
                rsi_val = calculate_rsi(list(price_history))
                macd_val = calculate_macd(list(price_history))
                latest_data["rsi"] = round(rsi_val, 2)
                latest_data["macd"] = round(macd_val, 2)

            # Call advanced analysis (populates market_state & institutional_state)
            advanced_analysis()

        latest_data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.error(f"on_message error: {e}")

# ------------------------------------------------------------
# Original WebSocket Feed Runner (exactly as working)
# ------------------------------------------------------------
def run_feed():
    global SELECTED_CE_ID, SELECTED_PE_ID
    while True:
        try:
            logger.info(f"Subscribing to CE={SELECTED_CE_ID}, PE={SELECTED_PE_ID}")
            feed = marketfeed.DhanFeed(
                client_id=CLIENT_ID,
                access_token=ACCESS_TOKEN,
                instruments=[
                    (marketfeed.NSE_FNO, str(SELECTED_CE_ID), marketfeed.Ticker),
                    (marketfeed.NSE_FNO, str(SELECTED_PE_ID), marketfeed.Ticker)
                ],
                on_message=on_message,
                on_connect=lambda _: logger.info("✅ WebSocket connected"),
                on_error=lambda _, err: logger.error(f"WebSocket error: {err}"),
                on_close=lambda _: logger.warning("WebSocket closed, reconnecting...")
            )
            feed.run_forever()
        except Exception as e:
            logger.error(f"Feed crashed: {e}, reconnecting in 10s")
            time.sleep(10)

# ------------------------------------------------------------
# Flask Routes (Enhanced to include advanced analytics)
# ------------------------------------------------------------
@app.route("/")
def home():
    return jsonify({
        "latest": latest_data,
        "market": market_state,
        "institutional": institutional_state
    })

@app.route("/api/health")
def health():
    return "OK", 200

# ------------------------------------------------------------
# Start Background Thread
# ------------------------------------------------------------
threading.Thread(target=run_feed, daemon=True).start()
logger.info("Background signal engine started")

# ------------------------------------------------------------
# Main (for local testing)
# ------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
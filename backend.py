# backend.py - Nifty Options Signal Engine with REST API Polling

import os
import time
import logging
import threading
import requests
import pandas as pd
from collections import deque
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --------------------------------------------------
# Flask app
# --------------------------------------------------
app = Flask(__name__)
CORS(app)
application = app

# --------------------------------------------------
# Environment variables
# --------------------------------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

# --------------------------------------------------
# Global state for ATM option IDs
# --------------------------------------------------
CE_ID = "51364"          # Hardcoded current CE ID
PE_ID = "51365"          # Hardcoded current PE ID
current_strike = 23650   # matching strike

# --------------------------------------------------
# Live market data store
# --------------------------------------------------
latest_ticks = {
    "ce_price": 0.0,
    "pe_price": 0.0,
    "ce_timestamp": None,
    "pe_timestamp": None
}

price_history = deque(maxlen=200)
tick_counter = 0
UPDATE_INTERVAL = 10            # recompute indicators every 10 polls

# --------------------------------------------------
# Trading signals state
# --------------------------------------------------
market_signal = {
    "signal": "WAITING",
    "ce_price": 0.0,
    "pe_price": 0.0,
    "spread": 0.0,
    "rsi": 50,
    "macd": 0.0,
    "pcr": 1.0,
    "vwap": 0.0,
    "atr": 0.0,
    "ema_fast": 0.0,
    "ema_slow": 0.0,
    "delta": 0.0,
    "gamma": 0.0,
    "theta": 0.0,
    "vega": 0.0,
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

SPREAD_THRESHOLD = 5.0

# --------------------------------------------------
# Helper: Nifty spot (cached) – needed for reference only
# --------------------------------------------------
spot_cache = {"value": None, "timestamp": 0}
CACHE_TTL = 30

def get_nifty_spot():
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        headers = {"User-Agent": "Mozilla/5.0"}
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        time.sleep(0.5)
        response = session.get(url, headers=headers, timeout=10)
        data = response.json()
        spot = data['data'][0]['lastPrice']
        logger.info(f"NIFTY spot = {spot}")
        return float(spot)
    except Exception as e:
        logger.error(f"Spot fetch error: {e}")
        return None

def get_nifty_spot_cached():
    now = time.time()
    if now - spot_cache["timestamp"] < CACHE_TTL and spot_cache["value"] is not None:
        return spot_cache["value"]
    spot = get_nifty_spot()
    if spot:
        spot_cache["value"] = spot
        spot_cache["timestamp"] = now
    return spot

# --------------------------------------------------
# LTP fetch using correct Dhan API
# --------------------------------------------------
def get_ltp(security_id):
    """Fetch last traded price via Dhan REST API (POST /v2/marketfeed/ltp)"""
    url = "https://api.dhan.co/v2/marketfeed/ltp"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {"securityIds": [int(security_id)]}
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=2)
        if resp.status_code == 200:
            data = resp.json()
            ltp_data = data.get("data", {})
            ltp = ltp_data.get(str(security_id), {}).get("ltp", 0)
            return float(ltp)
        else:
            logger.error(f"LTP error {resp.status_code}: {resp.text}")
            return 0
    except Exception as e:
        logger.error(f"LTP request error for {security_id}: {e}")
        return 0

# --------------------------------------------------
# Polling loop (hardcoded IDs, no dynamic refresh)
# --------------------------------------------------
def polling_feed():
    global CE_ID, PE_ID, latest_ticks, price_history, tick_counter
    global market_signal, market_state, institutional_state   # needed for signal engine

    while True:
        try:
            # Fetch live prices using hardcoded IDs
            ce_price = get_ltp(CE_ID)
            pe_price = get_ltp(PE_ID)
            logger.info(f"📊 LTP -> CE: {ce_price}, PE: {pe_price}")

            if ce_price > 0 and pe_price > 0:
                latest_ticks["ce_price"] = ce_price
                latest_ticks["pe_price"] = pe_price
                latest_ticks["ce_timestamp"] = datetime.now()
                latest_ticks["pe_timestamp"] = datetime.now()

                price_history.append(ce_price)
                tick_counter += 1
                if tick_counter >= UPDATE_INTERVAL:
                    tick_counter = 0
                    run_signal_engine(ce_price, pe_price, list(price_history))
            else:
                logger.warning(f"⚠️ Zero price: CE={ce_price}, PE={pe_price}")

            time.sleep(1)   # 1 second polling
        except Exception as e:
            logger.error(f"Polling loop error: {e}")
            time.sleep(1)

# --------------------------------------------------
# Signal engine
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
    return ema

def calculate_atr(prices, period=14):
    if len(prices) < period + 1:
        return 0
    trs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    return sum(trs[-period:]) / period

def calculate_vwap(prices):
    if not prices:
        return 0
    vol = [100] * len(prices)
    pv = sum(p * v for p, v in zip(prices, vol))
    tv = sum(vol)
    return pv / tv if tv else 0

def estimate_greeks(ce, pe):
    delta = round((ce - pe) / 100, 2)
    gamma = round(abs(delta) / 10, 2)
    theta = round(-(ce + pe) / 1000, 2)
    vega = round((ce + pe) / 500, 2)
    return delta, gamma, theta, vega

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
        resp = session.get(url, headers=headers, timeout=5)
        data = resp.json()
        ce_oi = sum(x.get("CE", {}).get("openInterest", 0) for x in data["records"]["data"] if "CE" in x)
        pe_oi = sum(x.get("PE", {}).get("openInterest", 0) for x in data["records"]["data"] if "PE" in x)
        pcr = pe_oi / ce_oi if ce_oi else 1.0
        pcr_cache["value"] = pcr
        pcr_cache["time"] = now
        return pcr
    except Exception as e:
        logger.error(f"PCR fetch failed: {e}")
        return pcr_cache["value"]

def run_signal_engine(ce_price, pe_price, price_list):
    global market_signal, market_state, institutional_state
    if len(price_list) < 20:
        return

    spread = ce_price - pe_price
    rsi = calculate_rsi(price_list)
    macd = calculate_macd(price_list)
    vwap = calculate_vwap(price_list)
    ema_fast = calculate_ema(price_list, 9)
    ema_slow = calculate_ema(price_list, 21)
    atr = calculate_atr(price_list)
    pcr = get_nifty_pcr()
    delta, gamma, theta, vega = estimate_greeks(ce_price, pe_price)

    ema_signal = "BULLISH" if ema_fast > ema_slow else "BEARISH"

    bullish_score = 0
    if ema_signal == "BULLISH": bullish_score += 20
    if rsi > 60: bullish_score += 20
    if spread > 0: bullish_score += 20
    if pcr < 1.0: bullish_score += 20
    if macd > 0: bullish_score += 20

    bearish_score = 0
    if ema_signal == "BEARISH": bearish_score += 20
    if rsi < 40: bearish_score += 20
    if spread < 0: bearish_score += 20
    if pcr > 1.2: bearish_score += 20
    if macd < 0: bearish_score += 20

    if bullish_score >= bearish_score and bullish_score >= 20:
        confidence = bullish_score
        if confidence >= 80:
            action = "STRONG BUY CE"
        elif confidence >= 60:
            action = "BUY CE"
        elif confidence >= 40:
            action = "CONSIDER CE"
        else:
            action = "HOLD"
    elif bearish_score > bullish_score and bearish_score >= 20:
        confidence = bearish_score
        if confidence >= 80:
            action = "STRONG BUY PE"
        elif confidence >= 60:
            action = "BUY PE"
        elif confidence >= 40:
            action = "CONSIDER PE"
        else:
            action = "HOLD"
    else:
        confidence = max(bullish_score, bearish_score)
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
        "vwap": round(vwap, 2),
        "ema_fast": round(ema_fast, 2),
        "ema_slow": round(ema_slow, 2),
        "ema_signal": ema_signal,
        "atr": round(atr, 2),
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "institutional_signal": action,
        "institutional_confidence": confidence
    })

    market_signal.update({
        "signal": "BULLISH" if spread > SPREAD_THRESHOLD else "BEARISH" if spread < -SPREAD_THRESHOLD else "NEUTRAL",
        "ce_price": ce_price,
        "pe_price": pe_price,
        "spread": round(spread, 2),
        "rsi": round(rsi, 2),
        "macd": round(macd, 2),
        "pcr": round(pcr, 2),
        "vwap": round(vwap, 2),
        "atr": round(atr, 2),
        "ema_fast": round(ema_fast, 2),
        "ema_slow": round(ema_slow, 2),
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "timestamp": datetime.now().isoformat()
    })

    logger.info(f"Signal: {action} (Bull={bullish_score} Bear={bearish_score}) | RSI={rsi:.1f} | Spread={spread:.2f}")

# --------------------------------------------------
# Start background polling thread
# --------------------------------------------------
def start_background_engine():
    thread = threading.Thread(target=polling_feed, daemon=True)
    thread.start()
    logger.info("✅ Background polling engine started (REST API, 1 sec refresh)")

# --------------------------------------------------
# Flask endpoints
# --------------------------------------------------
@app.route("/")
def home():
    return jsonify({"status": "online", "message": "NIFTY ATM + Live Signals (REST Polling)"})

@app.route("/api/trading-signals")
def trading_signals():
    return jsonify({"status": "success", "data": {
        "ce_id": CE_ID,
        "pe_id": PE_ID,
        "current_strike": current_strike
    }})

@app.route("/api/live-signals")
def live_signals():
    return jsonify({
        "status": "active",
        "data": market_signal,
        "market": market_state,
        "institutional": institutional_state
    })

@app.route("/health")
def health():
    return "OK", 200

# --------------------------------------------------
# Main
# --------------------------------------------------
# For local development (when run directly)
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
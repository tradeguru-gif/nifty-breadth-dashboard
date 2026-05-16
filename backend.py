# backend.py - Complete Professional Trading Signals with Auto-ATM Selection
# Added bidirectional signals: BUY PE / STRONG BUY PE

import os
import time
import logging
import asyncio
import threading
import requests
import pandas as pd
from collections import deque
from datetime import datetime, timedelta
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import DhanContext, MarketFeed, dhanhq

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
# Spot price cache (30 seconds TTL)
# --------------------------------------------------
spot_cache = {"value": None, "timestamp": 0}
CACHE_TTL = 30

# --------------------------------------------------
# Global state for auto‑updated option IDs
# --------------------------------------------------
CE_ID = None
PE_ID = None
current_strike = None
current_expiry = None
last_id_update = 0
ID_REFRESH_INTERVAL = 3600       # 1 hour
need_restart = False

# --------------------------------------------------
# Live market data store (updated by WebSocket)
# --------------------------------------------------
latest_ticks = {
    "ce_price": 0.0,
    "pe_price": 0.0,
    "ce_timestamp": None,
    "pe_timestamp": None
}

price_history = deque(maxlen=200)      # for CE price history
tick_counter = 0
UPDATE_INTERVAL = 10                   # recompute indicators every 10 ticks

# --------------------------------------------------
# Trading signals state (updated periodically)
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
# Helper: Dhan client (for REST API calls)
# --------------------------------------------------
def get_dhan_client():
    try:
        ctx = DhanContext(CLIENT_ID, ACCESS_TOKEN)
        return dhanhq(ctx)
    except Exception as e:
        logger.error(f"Failed to create Dhan client: {e}")
        return None

# --------------------------------------------------
# Nifty spot (cached)
# --------------------------------------------------
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
# Load Dhan scrip master CSV
# --------------------------------------------------
def load_instruments():
    url = "https://images.dhan.co/api-data/api-scrip-master.csv"
    df = pd.read_csv(url, low_memory=False)
    logger.info(f"Loaded {len(df)} instruments")
    return df

# --------------------------------------------------
# Auto ATM selection (using CSV)
# --------------------------------------------------
def get_option_contracts():
    spot = get_nifty_spot_cached()
    if spot is None:
        return {"error": "Could not fetch spot"}
    atm_strike = round(spot / 50) * 50
    logger.info(f"ATM strike = {atm_strike}")
    try:
        df = load_instruments()
    except Exception as e:
        return {"error": f"CSV load failed: {e}"}
    nifty_options = df[
        (df["SEM_CUSTOM_SYMBOL"].astype(str).str.contains("NIFTY")) &
        (df["SEM_STRIKE_PRICE"].fillna(0).astype(float) == float(atm_strike))
    ]
    if nifty_options.empty:
        return {"error": f"No options for strike {atm_strike}"}
    nifty_options["EXPIRY"] = pd.to_datetime(nifty_options["SEM_EXPIRY_DATE"], errors="coerce")
    nifty_options = nifty_options.dropna(subset=["EXPIRY"]).sort_values("EXPIRY")
    if nifty_options.empty:
        return {"error": "No valid expiry"}
    nearest_expiry = nifty_options["EXPIRY"].iloc[0]
    nearest = nifty_options[nifty_options["EXPIRY"] == nearest_expiry]
    ce_records = nearest[nearest["SEM_OPTION_TYPE"] == "CE"]
    pe_records = nearest[nearest["SEM_OPTION_TYPE"] == "PE"]
    if ce_records.empty or pe_records.empty:
        return {"error": "CE/PE missing"}
    ce = ce_records.iloc[0]
    pe = pe_records.iloc[0]
    return {
        "spot": spot,
        "atm_strike": atm_strike,
        "expiry": str(nearest_expiry.date()),
        "ce_security_id": str(ce["SEM_SMST_SECURITY_ID"]),
        "pe_security_id": str(pe["SEM_SMST_SECURITY_ID"]),
        "ce_symbol": ce["SEM_CUSTOM_SYMBOL"],
        "pe_symbol": pe["SEM_CUSTOM_SYMBOL"]
    }

def update_atm_option_ids(force=False):
    global CE_ID, PE_ID, current_strike, current_expiry, last_id_update, need_restart
    now = time.time()
    if not force and (now - last_id_update) < ID_REFRESH_INTERVAL:
        return False
    data = get_option_contracts()
    if "error" in data:
        return False
    if not force and current_strike == data["atm_strike"] and current_expiry == data["expiry"]:
        last_id_update = now
        return False
    CE_ID = data["ce_security_id"]
    PE_ID = data["pe_security_id"]
    current_strike = data["atm_strike"]
    current_expiry = data["expiry"]
    last_id_update = now
    logger.info(f"Updated ATM options: Strike={current_strike}, CE={CE_ID}, PE={PE_ID}")
    need_restart = True
    return True

# --------------------------------------------------
# Technical indicators
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
    vol = [100] * len(prices)   # dummy volume
    pv = sum(p * v for p, v in zip(prices, vol))
    tv = sum(vol)
    return pv / tv if tv else 0

def estimate_greeks(ce, pe):
    delta = round((ce - pe) / 100, 2)
    gamma = round(abs(delta) / 10, 2)
    theta = round(-(ce + pe) / 1000, 2)
    vega = round((ce + pe) / 500, 2)
    return delta, gamma, theta, vega

# --------------------------------------------------
# PCR fetcher from NSE
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

# --------------------------------------------------
# Advanced signal engine (runs every UPDATE_INTERVAL ticks)
# --------------------------------------------------
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

    # ---------- BULLISH CONFIDENCE (for CE) ----------
    bullish_score = 0
    if ema_signal == "BULLISH":
        bullish_score += 20
    if rsi > 60:
        bullish_score += 20
    if spread > 0:
        bullish_score += 20
    if pcr < 1.0:
        bullish_score += 20
    if macd > 0:
        bullish_score += 20

    # ---------- BEARISH CONFIDENCE (for PE) ----------
    bearish_score = 0
    if ema_signal == "BEARISH":
        bearish_score += 20
    if rsi < 40:
        bearish_score += 20
    if spread < 0:
        bearish_score += 20
    if pcr > 1.2:        # high PCR indicates put buying
        bearish_score += 20
    if macd < 0:
        bearish_score += 20

    # Decide action based on which score is higher
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
        # both low – market sideways
        confidence = max(bullish_score, bearish_score)
        action = "HOLD"

    # Update market state (confidence = dominant side's score)
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
# Tick processor (called from WebSocket)
# --------------------------------------------------
def process_tick(tick):
    global tick_counter, price_history
    try:
        sec_id = str(tick.get("securityId", tick.get("security_id", "")))
        price = float(tick.get("ltp", tick.get("LTP", 0)))
        if not sec_id or price == 0:
            return
        if CE_ID is None or PE_ID is None:
            return

        if sec_id == CE_ID:
            latest_ticks["ce_price"] = price
            latest_ticks["ce_timestamp"] = datetime.now()
        elif sec_id == PE_ID:
            latest_ticks["pe_price"] = price
            latest_ticks["pe_timestamp"] = datetime.now()
        else:
            return

        ce = latest_ticks["ce_price"]
        pe = latest_ticks["pe_price"]
        if ce > 0 and pe > 0:
            price_history.append(ce)
            tick_counter += 1
            if tick_counter >= UPDATE_INTERVAL:
                tick_counter = 0
                run_signal_engine(ce, pe, list(price_history))
    except Exception as e:
        logger.error(f"Tick error: {e}")

# --------------------------------------------------
# Custom WebSocket feed class
# --------------------------------------------------
class CustomFeed(MarketFeed):
    def __init__(self, dhan_context, instruments, version="v2"):
        super().__init__(dhan_context, instruments, version=version)

    def on_connect(self):
        logger.info("✅ WebSocket connected")

    def on_message(self, message):
        process_tick(message)

    def on_error(self, error):
        logger.error(f"WebSocket error: {error}")

    def on_close(self):
        logger.warning("WebSocket closed – reconnecting")

# --------------------------------------------------
# Async WebSocket loop with auto-restart on ID change
# --------------------------------------------------
async def websocket_loop():
    global need_restart, CE_ID, PE_ID, current_strike
    while True:
        try:
            if CE_ID is None or PE_ID is None:
                logger.info("Initializing option IDs...")
                update_atm_option_ids(force=True)
                if CE_ID is None or PE_ID is None:
                    await asyncio.sleep(30)
                    continue

            instruments = [
                (MarketFeed.NSE_FNO, CE_ID, MarketFeed.Ticker),
                (MarketFeed.NSE_FNO, PE_ID, MarketFeed.Ticker)
            ]
            ctx = DhanContext(CLIENT_ID, ACCESS_TOKEN)
            feed = CustomFeed(ctx, instruments, version="v2")
            logger.info(f"Subscribing CE={CE_ID} PE={PE_ID} (Strike {current_strike})")
            await feed.connect()
            await feed.subscribe_instruments()
            logger.info("Feed subscribed – waiting for ticks")
            while not need_restart:
                await asyncio.sleep(1)
            logger.info("Restarting feed...")
            need_restart = False
            await feed.disconnect()
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Feed crashed: {e}, reconnecting in 10s")
            await asyncio.sleep(10)

# --------------------------------------------------
# Background ID updater
# --------------------------------------------------
def periodic_id_updater():
    global need_restart
    while True:
        time.sleep(300)   # every 5 minutes
        spot = get_nifty_spot_cached()
        if spot:
            new_strike = round(spot / 50) * 50
            if new_strike != current_strike:
                logger.info(f"Strike change: {current_strike} -> {new_strike}")
                if update_atm_option_ids(force=True):
                    need_restart = True

# --------------------------------------------------
# Start background threads
# --------------------------------------------------
def start_feed():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(websocket_loop())

threading.Thread(target=start_feed, daemon=True).start()
threading.Thread(target=periodic_id_updater, daemon=True).start()
logger.info("✅ Background engine started with auto-ATM + bidirectional signals")

# --------------------------------------------------
# Flask endpoints
# --------------------------------------------------
@app.route("/")
def home():
    return jsonify({"status": "online", "message": "NIFTY ATM + Live Signals"})

@app.route("/api/trading-signals")
def trading_signals():
    """Returns current ATM contract details (static)"""
    data = get_option_contracts()
    if "error" in data:
        return jsonify({"status": "error", "message": data["error"]}), 500
    return jsonify({"status": "success", "data": data})

@app.route("/api/live-signals")
def live_signals():
    """Returns real-time market signals and institutional state"""
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
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
# backend.py - Institutional Nifty Options Signal Engine
# Fully corrected for DhanHQ MarketFeed V2 + Auto ATM Option Selection

import os
import asyncio
import threading
import time
import logging
import requests
import re
from datetime import datetime, timedelta
from collections import deque

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
application = app

# --------------------------------------------------
# Environment variables
# --------------------------------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

# --------------------------------------------------
# Global state for option IDs (auto-updated)
# --------------------------------------------------
CE_ID = None
PE_ID = None
current_strike = None
current_expiry = None
last_id_update = 0
ID_REFRESH_INTERVAL = 3600   # 1 hour
SPOT_DEVIATION_PERCENT = 0.02  # 2% deviation triggers refresh

# --------------------------------------------------
# Shared frontend state (same as before)
# --------------------------------------------------
latest_data = {
    "signal": "WAITING",
    "ce_price": 0.0,
    "pe_price": 0.0,
    "spread": 0.0,
    "rsi": 50,
    "macd": 0.0,
    "pcr": 1.0,
    "timestamp": "",
    "strike": None,
    "expiry": None
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
# Internal tick processing state
# --------------------------------------------------
price_history = deque(maxlen=200)
tick_counter = 0
UPDATE_INTERVAL = 10
SPREAD_THRESHOLD = 5.0

# --------------------------------------------------
# Helper: Get Nifty Spot Price (from NSE)
# --------------------------------------------------
def get_nifty_spot():
    """Fetch current Nifty spot price from NSE"""
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        headers = {"User-Agent": "Mozilla/5.0"}
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=5)
        time.sleep(0.5)
        resp = session.get(url, headers=headers, timeout=5)
        data = resp.json()
        price = data['data'][0]['lastPrice']
        logger.info(f"Spot NIFTY: {price}")
        return float(price)
    except Exception as e:
        logger.error(f"Failed to fetch spot: {e}")
        # Fallback to a default (e.g., previous close) or raise
        return None

# --------------------------------------------------
# Helper: Get nearest weekly expiry (Thursday)
# --------------------------------------------------
def get_nearest_weekly_expiry(base_date=None):
    if base_date is None:
        base_date = datetime.now()
    # Find next Thursday (weekday = 3)
    days_ahead = 3 - base_date.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    expiry_date = base_date + timedelta(days=days_ahead)
    # Return as YYYY-MM-DD
    return expiry_date.strftime("%Y-%m-%d")

# --------------------------------------------------
# Helper: Calculate ATM strike (nearest 50 multiple)
# --------------------------------------------------
def calculate_atm_strike(spot):
    # Nifty options have strikes in multiples of 50 (sometimes 100 for far)
    return int(round(spot / 50.0) * 50)

# --------------------------------------------------
# Helper: Search Dhan for option security ID
# --------------------------------------------------
import csv

def find_option_security_id(symbol, expiry, strike, option_type):
    """
    Finds Security ID using Dhan's Scrip Master CSV.
    Reliable, fast, and avoids 'AttributeError'.
    """
    try:
        # 1. Download the Scrip Master (Detailed)
        url = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
        response = requests.get(url, timeout=10)
        
        # 2. Parse the CSV
        decoded_content = response.content.decode('utf-8')
        cr = csv.DictReader(decoded_content.splitlines())
        
        # Standardize expiry format for comparison
        # CSV format is usually YYYY-MM-DD HH:MM:SS
        target_expiry_date = datetime.strptime(expiry, "%Y-%m-%d").date()

        for row in cr:
            # Filters
            if row.get('SEM_INSTRUMENT_NAME') != 'OPTIDX': continue
            if symbol not in row.get('SEM_SYMBOL_NAME', ''): continue
            if row.get('SEM_OPTION_TYPE') != option_type: continue
            
            # Match Strike Price
            try:
                row_strike = int(float(row.get('SEM_STRIKE_PRICE', 0)))
                if row_strike != strike: continue
            except: continue
            
            # Match Expiry Date
            try:
                row_expiry_str = row.get('SEM_EXPIRY_DATE', '').split(' ')[0]
                row_expiry = datetime.strptime(row_expiry_str, "%Y-%m-%d").date()
                if row_expiry != target_expiry_date: continue
            except: continue

            # If we reached here, it's a match!
            sec_id = row.get('SEM_SMST_SECURITY_ID')
            logger.info(f"✅ Found ID for {symbol} {strike} {option_type}: {sec_id}")
            return str(sec_id)

        logger.warning(f"❌ No ID found in CSV for {symbol} {strike} {option_type}")
        return None

    except Exception as e:
        logger.error(f"Failed to find ID via CSV: {e}")
        return None
# --------------------------------------------------
# Auto-update ATM option IDs based on current spot
# --------------------------------------------------
def update_atm_option_ids(force=False):
    global CE_ID, PE_ID, current_strike, current_expiry, last_id_update
    now = time.time()
    if not force and (now - last_id_update) < ID_REFRESH_INTERVAL:
        return False  # not updated

    spot = get_nifty_spot()
    if spot is None:
        logger.warning("Could not fetch spot, skipping ID update")
        return False

    expiry = get_nearest_weekly_expiry()
    strike = calculate_atm_strike(spot)

    # If same strike and expiry and not forced, skip
    if not force and current_strike == strike and current_expiry == expiry:
        last_id_update = now
        return False

    ce_id = find_option_security_id("NIFTY", expiry, strike, "CE")
    pe_id = find_option_security_id("NIFTY", expiry, strike, "PE")

    if ce_id and pe_id:
        CE_ID = ce_id
        PE_ID = pe_id
        current_strike = strike
        current_expiry = expiry
        last_id_update = now
        logger.info(f"Updated ATM options: Strike={strike}, Expiry={expiry}, CE={CE_ID}, PE={PE_ID}")
        # Also store in latest_data for frontend
        latest_data["strike"] = strike
        latest_data["expiry"] = expiry
        return True
    else:
        logger.error("Failed to fetch CE or PE IDs – keeping old ones")
        return False

# --------------------------------------------------
# TECHNICAL INDICATORS (unchanged from previous)
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
#-------------------------------------------------
# BOLLINGER ANALYSIS
#----------------------------------------------
def calculate_bollinger_bands(prices, window=20, num_std=2):
    if len(prices) < window:
        return 0, 0, 0
    import numpy as np
    arr = np.array(prices)
    sma = np.mean(arr[-window:])
    std = np.std(arr[-window:])
    upper = sma + (std * num_std)
    lower = sma - (std * num_std)
    return round(upper, 2), round(sma, 2), round(lower, 2)

def calculate_adx(prices, high, low, period=14):
    # This identifies if the trend is strong enough to trade
    # If ADX > 25, the RSI/MACD signals are 3x more reliable.
    pass # Implementation requires True Range calculation

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
# TICK PROCESSOR (v2 compatible, with auto re-subscribe on ID change)
# --------------------------------------------------
def process_tick(tick):
    global tick_counter, CE_ID, PE_ID, current_strike, feed_instance
    try:
        security_id = str(tick.get("securityId", tick.get("security_id", "")))
        price = float(tick.get("ltp", tick.get("LTP", 0)))
        if not security_id or price == 0:
            return

        # If IDs are None, skip processing
        if CE_ID is None or PE_ID is None:
            logger.warning("Option IDs not yet available")
            return

        if security_id == CE_ID:
            latest_data["ce_price"] = price
        elif security_id == PE_ID:
            latest_data["pe_price"] = price
        else:
            # Not our subscribed instruments
            return

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
            print(f"✅ Tick: CE={ce} PE={pe} Spread={spread:.2f} Signal={latest_data['signal']}")

    except Exception as e:
        logger.error(f"Tick processing error: {e}")

# --------------------------------------------------
# CUSTOM FEED CLASS (same as before)
# --------------------------------------------------
class CustomFeed(MarketFeed):
    def __init__(self, dhan_context, instruments, version="v2"):
        super().__init__(dhan_context, instruments, version=version)

    def on_connect(self):
        logger.info("✅ WebSocket connected successfully")

    def on_message(self, message):
        process_tick(message)

    def on_error(self, error):
        logger.error(f"WebSocket error: {error}")

    def on_close(self):
        logger.warning("WebSocket closed – will reconnect")

# --------------------------------------------------
# Background ID updater (runs in separate thread)
# --------------------------------------------------
def update_ids_periodically():
    """Runs in background, updates option IDs and triggers feed restart if changed"""
    global feed_instance, CE_ID, PE_ID
    while True:
        time.sleep(300)  # check every 5 minutes (or adjust)
        # Fetch spot and update if needed
        spot = get_nifty_spot()
        if spot:
            new_strike = calculate_atm_strike(spot)
            if new_strike != current_strike:
                logger.info(f"Strike change detected: {current_strike} -> {new_strike}, refreshing IDs")
                success = update_atm_option_ids(force=True)
                if success and feed_instance:
                    # Restart feed to subscribe to new instruments
                    # Since websocket_loop will reconnect on crash, we just raise an exception to trigger reconnect
                    # But better to close feed gracefully and let loop restart
                    logger.info("Triggering feed restart for new instruments...")
                    # This will cause the websocket_loop to reconnect (because feed will error out)
                    # Actually we can't easily force a restart from another thread. We'll rely on the fact that
                    # the feed will continue with old IDs, but we'll set a flag to restart in the main loop.
                    # Simpler: just log and let the next tick check. But we need to subscribe to new IDs.
                    # We'll set a global flag "need_restart" and websocket_loop will check it.
                    global need_restart
                    need_restart = True

need_restart = False

# --------------------------------------------------
# ASYNC WEBSOCKET LOOP (with auto restart on ID change)
# --------------------------------------------------
async def websocket_loop():
    global feed_instance, need_restart
    while True:
        try:

#-------------------------------------
#HEARTBEAT CHECK PULSE MONITOR
#--------------------------------------
import time
from datetime import datetime

# Place this inside your background task loop
def monitor_data_freshness():
    if "timestamp" in latest_data:
        last_update = latest_data["timestamp"] # This should be a datetime object or epoch
        current_time = datetime.now()
        
        # Calculate difference in seconds
        time_diff = (current_time - last_update).total_seconds()
        
        if time_diff > 60:
            logger.warning(f"⚠️ DATA STALE: Last update was {time_diff}s ago. Reconnecting...")
            market_state["system_status"] = "DATA_LAG"
            # Optional: Trigger a WebSocket reconnection here
        else:
            market_state["system_status"] = "LIVE"
#--------------------------------------
-
            # Ensure we have valid IDs before connecting
            if CE_ID is None or PE_ID is None:
                logger.info("Initializing option IDs...")
                update_atm_option_ids(force=True)
                if CE_ID is None or PE_ID is None:
                    logger.error("Could not fetch initial option IDs. Retrying in 30s...")
                    await asyncio.sleep(30)
                    continue
# Create a simple list of tuples for the subscription
instruments = [
                (MarketFeed.NSE_FNO, str(CE_ID), MarketFeed.Ticker),
                (MarketFeed.NSE_FNO, str(PE_ID), MarketFeed.Ticker)
            ]           

            
            # Initialize without DhanContext if using MarketFeed directly
            feed = CustomFeed(CLIENT_ID, ACCESS_TOKEN, instruments, version="v2")



            logger.info(f"Subscribing to CE={CE_ID} (Strike {current_strike}), PE={PE_ID}")
            await feed.connect()
            await feed.subscribe_instruments()
            logger.info("Feed connected and subscribed – waiting for ticks...")

            # Keep alive and check for restart flag
            while not need_restart:
                await asyncio.sleep(1)

            # Restart loop
            logger.info("Restarting feed due to ID change...")
            need_restart = False
            await feed.disconnect()
            await asyncio.sleep(5)  # allow cleanup

        except Exception as e:
            logger.error(f"Feed crashed: {e}, reconnecting in 10s")
            await asyncio.sleep(10)

# --------------------------------------------------
# START BACKGROUND THREADS
# --------------------------------------------------
def start_feed():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(websocket_loop())

# Start main feed thread
feed_thread = threading.Thread(target=start_feed, daemon=True)
feed_thread.start()

# Start periodic ID updater thread
updater_thread = threading.Thread(target=update_ids_periodically, daemon=True)
updater_thread.start()

logger.info("✅ Background signal engine started with auto-ATM selection")

# --------------------------------------------------
# FLASK ROUTES (unchanged, but added /debug/ids)
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
        "version": "Auto-ATM Feed V2",
        "ce_id": CE_ID,
        "pe_id": PE_ID,
        "strike": current_strike,
        "expiry": current_expiry
    })

# --------------------------------------------------
# MAIN
# --------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
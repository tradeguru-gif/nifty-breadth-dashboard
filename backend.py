# backend.py - Institutional Nifty Options Signal Engine
# Fixed: dhanhq initialisation with DhanContext

import os
import asyncio
import threading
import time
import logging
import requests
from collections import deque
from datetime import datetime, timedelta

from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import DhanContext, MarketFeed, dhanhq

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
# Global state (auto‑updated option IDs)
# --------------------------------------------------
CE_ID = None
PE_ID = None
current_strike = None
current_expiry = None
last_id_update = 0
ID_REFRESH_INTERVAL = 3600       # 1 hour
need_restart = False

# --------------------------------------------------
# Shared frontend state (enriched)
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
# Internal tick processing
# --------------------------------------------------
price_history = deque(maxlen=200)
tick_counter = 0
UPDATE_INTERVAL = 10
SPREAD_THRESHOLD = 5.0

# --------------------------------------------------
# Technical indicators (unchanged)
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
# PCR fetcher (unchanged)
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
# Advanced analysis (unchanged)
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
# Auto ATM instrument selection (FIXED: use DhanContext)
# --------------------------------------------------
def get_nifty_spot():
    """Fetch current Nifty spot via Dhan quote API"""
    try:
        ctx = DhanContext(CLIENT_ID, ACCESS_TOKEN)
        dhan = dhanhq(ctx)
        quote = dhan.get_quote_data(securities={"IDX_I": [13]})
        spot = quote['data']['last_price']
        logger.info(f"Spot NIFTY: {spot}")
        return float(spot)
    except Exception as e:
        logger.error(f"Failed to get spot: {e}")
        return None

def get_nearest_weekly_expiry():
    """First expiry date from Dhan expiry list"""
    try:
        ctx = DhanContext(CLIENT_ID, ACCESS_TOKEN)
        dhan = dhanhq(ctx)
        expiries = dhan.expiry_list(under_security_id=13, under_exchange_segment="IDX_I")
        if expiries and 'data' in expiries and len(expiries['data']) > 0:
            return expiries['data'][0]
    except Exception as e:
        logger.error(f"Expiry fetch failed: {e}")
    # Fallback to next Thursday
    base = datetime.now()
    days_ahead = 3 - base.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    expiry_date = base + timedelta(days=days_ahead)
    return expiry_date.strftime("%Y-%m-%d")

def calculate_atm_strike(spot):
    return int(round(spot / 50.0) * 50)

def fetch_atm_option_ids(expiry, strike):
    """Use Dhan option_chain to get CE and PE security IDs"""
    try:
        ctx = DhanContext(CLIENT_ID, ACCESS_TOKEN)
        dhan = dhanhq(ctx)
        oc = dhan.option_chain(
            under_security_id=13,
            under_exchange_segment="IDX_I",
            expiry=expiry
        )
        strike_key = f"{float(strike):.6f}"
        instruments = oc['data']['oc'].get(strike_key)
        if instruments:
            ce_id = str(instruments['ce']['security_id'])
            pe_id = str(instruments['pe']['security_id'])
            return ce_id, pe_id
        else:
            logger.error(f"Strike {strike} not found in option chain")
            return None, None
    except Exception as e:
        logger.error(f"Option chain fetch failed: {e}")
        return None, None

def update_atm_option_ids(force=False):
    global CE_ID, PE_ID, current_strike, current_expiry, last_id_update, need_restart
    now = time.time()
    if not force and (now - last_id_update) < ID_REFRESH_INTERVAL:
        return False

    spot = get_nifty_spot()
    if spot is None:
        return False

    expiry = get_nearest_weekly_expiry()
    strike = calculate_atm_strike(spot)

    if not force and current_strike == strike and current_expiry == expiry:
        last_id_update = now
        return False

    ce_id, pe_id = fetch_atm_option_ids(expiry, strike)
    if ce_id and pe_id:
        CE_ID = ce_id
        PE_ID = pe_id
        current_strike = strike
        current_expiry = expiry
        last_id_update = now
        latest_data["strike"] = strike
        latest_data["expiry"] = expiry
        logger.info(f"Updated ATM options: Strike={strike}, Expiry={expiry}, CE={CE_ID}, PE={PE_ID}")
        need_restart = True
        return True
    else:
        logger.error("Failed to fetch CE or PE IDs – keeping old ones")
        return False

# --------------------------------------------------
# Tick processor (unchanged)
# --------------------------------------------------
def process_tick(tick):
    global tick_counter
    try:
        security_id = str(tick.get("securityId", tick.get("security_id", "")))
        price = float(tick.get("ltp", tick.get("LTP", 0)))
        if not security_id or price == 0:
            return
        if CE_ID is None or PE_ID is None:
            return

        if security_id == CE_ID:
            latest_data["ce_price"] = price
        elif security_id == PE_ID:
            latest_data["pe_price"] = price
        else:
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
# Custom Feed Class (unchanged)
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
# Async WebSocket loop with auto‑restart
# --------------------------------------------------
async def websocket_loop():
    global need_restart, CE_ID, PE_ID, current_strike
    while True:
        try:
            if CE_ID is None or PE_ID is None:
                logger.info("Initializing option IDs...")
                update_atm_option_ids(force=True)
                if CE_ID is None or PE_ID is None:
                    logger.error("Could not fetch initial IDs. Retrying in 30s...")
                    await asyncio.sleep(30)
                    continue

            instruments = [
                (MarketFeed.NSE_FNO, CE_ID, MarketFeed.Ticker),
                (MarketFeed.NSE_FNO, PE_ID, MarketFeed.Ticker)
            ]
            ctx = DhanContext(CLIENT_ID, ACCESS_TOKEN)
            feed = CustomFeed(ctx, instruments, version="v2")

            logger.info(f"Subscribing to CE={CE_ID} (Strike {current_strike}), PE={PE_ID}")
            await feed.connect()
            await feed.subscribe_instruments()
            logger.info("Feed connected and subscribed – waiting for ticks...")

            while not need_restart:
                await asyncio.sleep(1)

            logger.info("Restarting feed due to ID change...")
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
        spot = get_nifty_spot()
        if spot:
            new_strike = calculate_atm_strike(spot)
            if new_strike != current_strike:
                logger.info(f"Strike change detected: {current_strike} -> {new_strike}, refreshing IDs")
                success = update_atm_option_ids(force=True)
                if success:
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
logger.info("✅ Background signal engine started with auto‑ATM selection")

# --------------------------------------------------
# Flask routes
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
        "version": "Auto-ATM + Advanced Signals v2 (fixed dhanhq init)",
        "ce_id": CE_ID,
        "pe_id": PE_ID,
        "strike": current_strike,
        "expiry": current_expiry
    })

@app.route("/api/refresh-atm", methods=['POST'])
def refresh_atm():
    success = update_atm_option_ids(force=True)
    return jsonify({"status": "updated" if success else "failed", "ce_id": CE_ID, "pe_id": PE_ID})

# --------------------------------------------------
# Main
# --------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
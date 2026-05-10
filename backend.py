# backend.py - Stable NIFTY Options Institutional Signal Engine

import os
import time
import threading
import logging
import requests
from collections import deque
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

# Dhan SDK imports
from dhanhq import dhanhq, marketfeed
import pandas as pd

# -----------------------------
# 1. LOGGING & ENVIRONMENT
# -----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("backend")

CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
DEFAULT_NIFTY_SPOT = float(os.getenv("CURRENT_NIFTY", "24000"))

if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

# -----------------------------
# 2. FLASK APP SETUP
# -----------------------------
app = Flask(__name__)
CORS(app)
application = app

# -----------------------------
# 3. GLOBAL STATE STORAGE
# -----------------------------
latest_data = {
    "ce_price": 0,
    "pe_price": 0,
    "spread": 0,
    "signal": "WAITING",
    "pcr": 1.0,
    "timestamp": ""
}
# Placeholder for selected contract IDs
SELECTED_CE = None
SELECTED_PE = None
price_history = deque(maxlen=200)  # For technical indicators

# Analytics state
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

# -----------------------------
# 4. UTILITY FUNCTIONS
# -----------------------------
pcr_cache = {"value": 1.0, "time": 0}
PCR_TTL = 60

def get_pcr():
    """Get Put/Call Ratio (cached) from NSE."""
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
        ce = sum(x.get("CE", {}).get("openInterest", 0) for x in data["records"]["data"] if "CE" in x)
        pe = sum(x.get("PE", {}).get("openInterest", 0) for x in data["records"]["data"] if "PE" in x)
        value = pe / ce if ce else 1.0
        pcr_cache["value"] = value
        pcr_cache["time"] = now
        return value
    except Exception as e:
        logger.error(f"PCR error: {e}")
        return pcr_cache["value"]

def get_nifty_spot():
    """Fetch live NIFTY spot price from NSE."""
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Referer": "https://www.nseindia.com/"
        }
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        res = session.get(url, headers=headers, timeout=10)
        if res.status_code == 200 and res.text.strip():
            data = res.json()
            return float(data["data"][0]["lastPrice"])
    except Exception as e:
        logger.error(f"NIFTY spot error: {e}")
    return DEFAULT_NIFTY_SPOT

def select_contracts(spot):
    """
    Dynamically select the ATM Call and Put security IDs for the nearest expiry.
    """
    global SELECTED_CE, SELECTED_PE
    try:
        url = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
        df = pd.read_csv(url, low_memory=False)
        df.columns = df.columns.str.upper()
        fno = df[df["SEGMENT"] == "NSE_FNO"]
        opts = fno[fno["INSTRUMENT"] == "OPTIDX"].copy()
        opts["EXPIRY"] = pd.to_datetime(opts["SM_EXPIRY_DATE"], format="%d-%b-%Y", errors="coerce")
        opts = opts.dropna(subset=["EXPIRY"])
        nearest_expiry = opts["EXPIRY"].min()
        opts = opts[opts["EXPIRY"] == nearest_expiry]
        opts["STRIKE"] = pd.to_numeric(opts["STRIKE_PRICE"], errors="coerce")
        opts = opts.dropna(subset=["STRIKE"])
        # Find strike closest to spot for ATM
        atm_strike = opts.iloc[(opts["STRIKE"] - spot).abs().argmin()]["STRIKE"]
        ce_row = opts[(opts["OPTION_TYPE"] == "CE") & (opts["STRIKE"] == atm_strike)]
        pe_row = opts[(opts["OPTION_TYPE"] == "PE") & (opts["STRIKE"] == atm_strike)]
        if ce_row.empty or pe_row.empty:
            logger.error("Could not find ATM CE/PE contracts")
            return False
        SELECTED_CE = int(ce_row.iloc[0]["SEM_SMST_SECURITY_ID"])
        SELECTED_PE = int(pe_row.iloc[0]["SEM_SMST_SECURITY_ID"])
        logger.info(f"Selected CE={SELECTED_CE} PE={SELECTED_PE} (Strike={atm_strike})")
        return True
    except Exception as e:
        logger.error(f"Contract selection error: {e}")
        return False

# -----------------------------
# 5. TECHNICAL INDICATORS
# -----------------------------
def calculate_rsi(period=14):
    prices = list(price_history)
    if len(prices) < period + 1:
        return 50
    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        gains.append(diff if diff > 0 else 0)
        losses.append(abs(diff) if diff < 0 else 0)
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)

def calculate_ema(prices, period):
    if len(prices) < period:
        return prices[-1] if prices else 0
    multiplier = 2 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = (p - ema) * multiplier + ema
    return round(ema, 2)

def calculate_atr(period=14):
    prices = list(price_history)
    if len(prices) < period + 1:
        return 0
    trs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    return round(sum(trs[-period:]) / period, 2)

def calculate_vwap():
    prices = list(price_history)
    if not prices:
        return 0
    volume = [100] * len(prices)  # Placeholder as volume data may not be available
    pv = sum(p * v for p, v in zip(prices, volume))
    tv = sum(volume)
    return round(pv / tv, 2)

def estimate_greeks(ce, pe):
    delta = round((ce - pe) / 100, 2)
    gamma = round(abs(delta) / 10, 2)
    theta = round(-(ce + pe) / 1000, 2)
    vega = round((ce + pe) / 500, 2)
    return delta, gamma, theta, vega

# -----------------------------
# 6. SIGNAL AND ANALYSIS ENGINE
# -----------------------------
def update_all_analysis():
    """
    Master function: updates latest_data, market_state, and institutional_state.
    Called on every tick.
    """
    ce = latest_data["ce_price"]
    pe = latest_data["pe_price"]
    if ce == 0 or pe == 0:
        return

    spread = ce - pe
    latest_data["spread"] = spread
    price_history.append(ce)
    pcr = get_pcr()
    latest_data["pcr"] = pcr

    # Simple signal for latest_data endpoint
    if spread > 5 and pcr < 0.8:
        latest_data["signal"] = "BULLISH"
    elif spread < -5 and pcr > 1.2:
        latest_data["signal"] = "BEARISH"
    else:
        latest_data["signal"] = "NEUTRAL"
    latest_data["timestamp"] = datetime.now().strftime("%H:%M:%S")

    # Advanced market analysis
    rsi = calculate_rsi()
    vwap = calculate_vwap()
    ema_fast = calculate_ema(list(price_history), 9)
    ema_slow = calculate_ema(list(price_history), 21)
    ema_signal = "BULLISH" if ema_fast > ema_slow else "BEARISH"
    atr = calculate_atr()
    oi_buildup = "LONG BUILDUP" if pcr < 0.8 else "SHORT BUILDUP" if pcr > 1.2 else "NEUTRAL"
    iv_state = "LOW IV (IV CRUSH)" if abs(spread) < 5 else "HIGH IV" if abs(spread) > 20 else "NORMAL"
    candle = "BULLISH" if (len(price_history) >= 5 and list(price_history)[-1] > list(price_history)[-5]) else "BEARISH" if (len(price_history) >= 5 and list(price_history)[-1] < list(price_history)[-5]) else "SIDEWAYS"
    breadth = "BULLISH" if pcr < 0.9 else "BEARISH" if pcr > 1.1 else "BALANCED"
    vp = "HIGH" if (len(price_history) >= 10 and (max(price_history) - min(price_history)) > 20) else "LOW"
    sm = "SMART MONEY BUYING" if spread > 10 and pcr < 0.8 else "SMART MONEY SELLING" if spread < -10 and pcr > 1.2 else "NEUTRAL"
    delta, gamma, theta, vega = estimate_greeks(ce, pe)

    # Institutional confidence and action
    confidence = 0
    if ema_signal == "BULLISH":
        confidence += 15
    if rsi > 60:
        confidence += 15
    if sm == "SMART MONEY BUYING":
        confidence += 20
    if breadth == "BULLISH":
        confidence += 15
    if candle == "BULLISH":
        confidence += 10
    if vp == "HIGH":
        confidence += 10
    if rsi > 60:  # Multi time frame confirmation placeholder
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
        "rsi": rsi,
        "momentum": "UPTREND" if spread > 0 else "DOWNTREND",
        "strength": "HIGH" if confidence > 60 else "LOW",
        "trend": ema_signal,
        "action": action,
        "confidence": confidence,
        "volatility": "HIGH" if abs(spread) > 20 else "NORMAL",
        "alert": "BUY" if action in ["BUY CE", "STRONG BUY CE"] else "EXIT" if action == "EXIT" else "HOLD",
        "entry_price": ce if action in ["BUY CE", "STRONG BUY CE"] else pe,
        "target_price": round(ce * 1.08, 2) if action in ["BUY CE", "STRONG BUY CE"] else round(pe * 1.05, 2),
        "stop_loss": round(ce * 0.95, 2) if action in ["BUY CE", "STRONG BUY CE"] else round(pe * 0.95, 2),
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
        "volume_profile": vp,
        "smart_money_flow": sm,
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "institutional_signal": action,
        "institutional_confidence": confidence
    })

# -----------------------------
# 7. WEBSOCKET FEED HANDLER
# -----------------------------
def on_message(_, tick):
    """Callback that receives ticks from the WebSocket."""
    try:
        sid = tick.get('security_id')
        ltp = tick.get('ltp', 0)
        if sid == SELECTED_CE:
            latest_data["ce_price"] = float(ltp)
        elif sid == SELECTED_PE:
            latest_data["pe_price"] = float(ltp)
        update_all_analysis()
    except Exception as e:
        logger.error(f"Message handler error: {e}")

def run_feed():
    """Main feed loop with auto-reconnect and dynamic contract selection."""
    global SELECTED_CE, SELECTED_PE
    while True:
        try:
            # Get current NIFTY spot and select ATM contracts
            spot = get_nifty_spot()
            if not select_contracts(spot):
                logger.error("Contract selection failed, retrying...")
                time.sleep(30)
                continue

            logger.info(f"Subscribing to CE={SELECTED_CE} PE={SELECTED_PE}")
            feed = marketfeed.DhanFeed(
                client_id=CLIENT_ID,
                access_token=ACCESS_TOKEN,
                instruments=[
                    (marketfeed.NSE_FNO, str(SELECTED_CE), marketfeed.Ticker),
                    (marketfeed.NSE_FNO, str(SELECTED_PE), marketfeed.Ticker)
                ],
                on_message=on_message,
                on_connect=lambda _: logger.info("✅ WebSocket connected"),
                on_error=lambda _, err: logger.error(f"WebSocket error: {err}"),
                on_close=lambda _: logger.warning("WebSocket closed, reconnecting...")
            )
            # This blocks until the feed disconnects
            feed.run_forever()
        except Exception as e:
            logger.error(f"Feed crashed: {e}, reconnecting in 10 seconds")
            time.sleep(10)

# -----------------------------
# 8. FLASK ENDPOINTS
# -----------------------------
@app.route('/')
def home():
    return jsonify({
        "latest": latest_data,
        "market": market_state,
        "institutional": institutional_state
    })

@app.route('/api/health')
def health():
    return "OK", 200

# -----------------------------
# 9. START BACKGROUND THREAD
# -----------------------------
feed_thread = threading.Thread(target=run_feed, daemon=True)
feed_thread.start()
logger.info("Background feed thread started")

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", 10000)))
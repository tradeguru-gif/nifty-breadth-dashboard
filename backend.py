# backend.py - NIFTY Options Institutional Signal Engine (FIXED)

import os
import time
import threading
import logging
import pandas as pd
import requests
from collections import deque
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

# Dhan imports (version 2.0.2 compatible)
from dhanhq import dhanhq, marketfeed

# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("backend")

# -----------------------------
# ENVIRONMENT VARIABLES
# -----------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
DEFAULT_NIFTY_SPOT = float(os.getenv("CURRENT_NIFTY", "24000"))

if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

# -----------------------------
# FLASK APP
# -----------------------------
app = Flask(__name__)
CORS(app)
application = app

# -----------------------------
# GLOBAL STATE
# -----------------------------
latest_data = {
    "ce_price": 0,
    "pe_price": 0,
    "spread": 0,
    "signal": "WAITING",
    "pcr": 1.0,
    "timestamp": ""
}

SELECTED_CE = None
SELECTED_PE = None

price_history = deque(maxlen=200)

# -----------------------------
# MARKET STATE (from your advanced engine)
# -----------------------------
market_state = {
    "rsi": 50,
    "momentum": "NEUTRAL",
    "strength": "LOW",
    "trend": "SIDEWAYS",
    "action": "HOLD",
    "confidence": 0,
    "volatility": "NORMAL",
    "alert": "NONE",
    "alert_sound": "",
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
    "banknifty_correlation": "NEUTRAL",
    "multi_tf": "NEUTRAL",
    "smart_money_flow": "NEUTRAL",
    "delta": 0,
    "gamma": 0,
    "theta": 0,
    "vega": 0,
    "institutional_signal": "HOLD",
    "institutional_confidence": 0
}

# -----------------------------
# PCR CACHE
# -----------------------------
pcr_cache = {"value": 1.0, "time": 0}
PCR_TTL = 60

def get_pcr():
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

# -----------------------------
# NIFTY SPOT (live or fallback)
# -----------------------------
def get_nifty_spot():
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        res = session.get(url, headers=headers, timeout=10)
        if res.status_code == 200 and res.text.strip():
            data = res.json()
            return float(data["data"][0]["lastPrice"])
    except Exception as e:
        logger.error(f"NIFTY spot error: {e}")
    return DEFAULT_NIFTY_SPOT

# -----------------------------
# INSTRUMENT LOAD & CONTRACT SELECTION
# -----------------------------
def load_instruments():
    try:
        url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        df = pd.read_csv(url, low_memory=False)
        df.columns = df.columns.str.upper()
        logger.info(f"Instruments loaded: {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Instrument load failed: {e}")
        return pd.DataFrame()

def select_contracts(spot):
    global SELECTED_CE, SELECTED_PE
    df = load_instruments()
    if df.empty:
        return False
    try:
        # Filter for NIFTY OPTIDX (index options)
        df = df[df["SEM_INSTRUMENT_NAME"].astype(str).str.contains("OPTIDX", na=False)]
        df = df[df["SEM_TRADING_SYMBOL"].astype(str).str.contains("NIFTY", na=False)]
        df["STRIKE"] = pd.to_numeric(df["SEM_STRIKE_PRICE"], errors="coerce")
        df["EXPIRY"] = pd.to_datetime(df["SEM_EXPIRY_DATE"], errors="coerce")
        df = df.dropna(subset=["EXPIRY", "STRIKE"])
        # Nearest expiry
        nearest_expiry = df["EXPIRY"].min()
        df = df[df["EXPIRY"] == nearest_expiry]
        # Find ATM strike
        strikes = sorted(df["STRIKE"].unique())
        atm = min(strikes, key=lambda x: abs(x - spot))
        ce = df[(df["SEM_OPTION_TYPE"] == "CE") & (df["STRIKE"] == atm)]
        pe = df[(df["SEM_OPTION_TYPE"] == "PE") & (df["STRIKE"] == atm)]
        if ce.empty or pe.empty:
            logger.error("ATM contracts not found")
            return False
        SELECTED_CE = int(ce.iloc[0]["SEM_SMST_SECURITY_ID"])
        SELECTED_PE = int(pe.iloc[0]["SEM_SMST_SECURITY_ID"])
        logger.info(f"Selected CE={SELECTED_CE} PE={SELECTED_PE} spot={spot} strike={atm}")
        return True
    except Exception as e:
        logger.error(f"Contract selection failed: {e}")
        return False

# -----------------------------
# TECHNICAL INDICATORS
# -----------------------------
def calculate_rsi(period=14):
    prices = list(price_history)
    if len(prices) < period + 1:
        return 50
    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        if diff >= 0:
            gains.append(diff)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(diff))
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
    volume = [100] * len(prices)  # placeholders since we don't have volume
    pv = sum(p * v for p, v in zip(prices, volume))
    tv = sum(volume)
    return round(pv / tv, 2)

def estimate_greeks(ce, pe):
    delta = round((ce - pe) / 100, 2)
    gamma = round(abs(delta) / 10, 2)
    theta = round(-(ce + pe) / 1000, 2)
    vega = round((ce + pe) / 500, 2)
    return delta, gamma, theta, vega

def oi_buildup(pcr):
    if pcr < 0.8:
        return "LONG BUILDUP"
    elif pcr > 1.2:
        return "SHORT BUILDUP"
    return "NEUTRAL"

def iv_state(spread):
    if abs(spread) < 5:
        return "LOW IV (IV CRUSH)"
    elif abs(spread) > 20:
        return "HIGH IV"
    return "NORMAL"

def candle_structure():
    prices = list(price_history)
    if len(prices) < 5:
        return "SIDEWAYS"
    recent = prices[-5:]
    if recent[-1] > recent[0]:
        return "BULLISH"
    elif recent[-1] < recent[0]:
        return "BEARISH"
    return "SIDEWAYS"

def market_breadth(pcr):
    if pcr < 0.9:
        return "BULLISH"
    elif pcr > 1.1:
        return "BEARISH"
    return "BALANCED"

def volume_profile():
    prices = list(price_history)
    if len(prices) < 10:
        return "NORMAL"
    volatility = max(prices[-10:]) - min(prices[-10:])
    if volatility > 20:
        return "HIGH"
    return "LOW"

def smart_money(spread, pcr):
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

# -----------------------------
# SIGNAL UPDATE (your logic)
# -----------------------------
def update_all_analysis():
    ce = latest_data["ce_price"]
    pe = latest_data["pe_price"]
    if ce == 0 or pe == 0:
        return
    spread = ce - pe
    latest_data["spread"] = spread
    price_history.append(ce)
    pcr = get_pcr()
    latest_data["pcr"] = pcr

    # Simple signal for latest_data
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
    oi = oi_buildup(pcr)
    iv = iv_state(spread)
    candle = candle_structure()
    breadth = market_breadth(pcr)
    vp = volume_profile()
    sm = smart_money(spread, pcr)
    delta, gamma, theta, vega = estimate_greeks(ce, pe)

    # Confidence and action (your institutional logic)
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
    if multi_tf_confirmation(rsi) == "BULLISH CONFIRMATION":
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
        "alert": "BUY" if action in ["BUY CE", "STRONG BUY CE"] else ("EXIT" if action == "EXIT" else "HOLD"),
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
        "oi_buildup": oi,
        "iv_state": iv,
        "candle_structure": candle,
        "market_breadth": breadth,
        "volume_profile": vp,
        "banknifty_correlation": "NEUTRAL",
        "multi_tf": multi_tf_confirmation(rsi),
        "smart_money_flow": sm,
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "institutional_signal": action,
        "institutional_confidence": confidence
    })

# -----------------------------
# WEBSOCKET CALLBACKS
# -----------------------------
def on_message(instance, tick):
    try:
        sid = tick.get('security_id')
        ltp = tick.get('ltp', 0)
        if sid == SELECTED_CE:
            latest_data["ce_price"] = float(ltp)
        elif sid == SELECTED_PE:
            latest_data["pe_price"] = float(ltp)
        update_all_analysis()
    except Exception as e:
        logger.error(f"on_message error: {e}")

def on_connect(instance):
    logger.info("✅ WebSocket connected")

def on_error(instance, error):
    logger.error(f"WebSocket error: {error}")

def on_close(instance):
    logger.warning("WebSocket closed, will reconnect")

# -----------------------------
# RUN FEED (with auto-reconnect)
# -----------------------------
def run_feed():
    global SELECTED_CE, SELECTED_PE
    while True:
        try:
            spot = get_nifty_spot()
            if not select_contracts(spot):
                logger.error("Contract selection failed, retrying in 30s")
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
                on_connect=on_connect,
                on_error=on_error,
                on_close=on_close
            )
            feed.run_forever()
        except Exception as e:
            logger.error(f"Feed crashed: {e}, reconnecting in 10s")
            time.sleep(10)

# -----------------------------
# ROUTES
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
# START BACKGROUND THREAD
# -----------------------------
thread = threading.Thread(target=run_feed, daemon=True)
thread.start()
logger.info("Background feed thread started")

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", 10000)))
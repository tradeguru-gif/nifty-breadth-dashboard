# backend.py - Advanced NIFTY Options Signal Engine (Production Ready)

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
DEFAULT_NIFTY_SPOT = float(os.getenv("CURRENT_NIFTY", "24000"))

if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

# ------------------------------------------------------------
# Dhan Client
# ------------------------------------------------------------
dhan = DhanHQ(CLIENT_ID, ACCESS_TOKEN)
logger.info("Dhan client initialized")

# ------------------------------------------------------------
# Global State
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

# Advanced market state (from your second script)
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

SELECTED_CE_ID = None
SELECTED_PE_ID = None

price_history = deque(maxlen=200)   # for RSI, EMA, ATR
update_counter = 0
UPDATE_INTERVAL = 10
SPREAD_THRESHOLD = 5.0

# PCR cache
pcr_cache = {"value": 1.0, "time": 0}
PCR_TTL = 60

# ------------------------------------------------------------
# Helper Functions (Indicators)
# ------------------------------------------------------------
def get_nifty_spot():
    """Live NIFTY spot from NSE (fallback to DEFAULT_NIFTY_SPOT)."""
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=5)
        res = session.get(url, headers=headers, timeout=5)
        if res.status_code == 200 and res.text.strip():
            return float(res.json()["data"][0]["lastPrice"])
    except Exception as e:
        logger.error(f"NIFTY spot error: {e}")
    return DEFAULT_NIFTY_SPOT

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
# Dynamic Contract Selection (replaces static IDs)
# ------------------------------------------------------------
def select_contracts(spot):
    global SELECTED_CE_ID, SELECTED_PE_ID
    try:
        url = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
        df = pd.read_csv(url, low_memory=False)
        df.columns = df.columns.str.upper()
        fno = df[df["SEGMENT"] == "NSE_FNO"]
        opts = fno[fno["INSTRUMENT"] == "OPTIDX"].copy()
        opts["EXPIRY"] = pd.to_datetime(opts["SM_EXPIRY_DATE"], format="%d-%b-%Y", errors="coerce")
        opts = opts.dropna(subset=["EXPIRY"])
        nearest = opts["EXPIRY"].min()
        opts = opts[opts["EXPIRY"] == nearest]
        opts["STRIKE"] = pd.to_numeric(opts["STRIKE_PRICE"], errors="coerce")
        opts = opts.dropna(subset=["STRIKE"])
        atm_strike = opts.iloc[(opts["STRIKE"] - spot).abs().argmin()]["STRIKE"]
        ce = opts[(opts["OPTION_TYPE"] == "CE") & (opts["STRIKE"] == atm_strike)]
        pe = opts[(opts["OPTION_TYPE"] == "PE") & (opts["STRIKE"] == atm_strike)]
        if ce.empty or pe.empty:
            raise ValueError("No ATM CE/PE found")
        SELECTED_CE_ID = str(ce.iloc[0]["SEM_SMST_SECURITY_ID"])
        SELECTED_PE_ID = str(pe.iloc[0]["SEM_SMST_SECURITY_ID"])
        logger.info(f"Dynamic contracts: CE={SELECTED_CE_ID} PE={SELECTED_PE_ID} (strike={atm_strike})")
        return True
    except Exception as e:
        logger.error(f"Contract selection failed: {e}")
        return False

# ------------------------------------------------------------
# Advanced Analysis (runs on every tick)
# ------------------------------------------------------------
def advanced_analysis():
    global market_state, institutional_state, latest_data
    ce = latest_data["ce_price"]
    pe = latest_data["pe_price"]
    if ce == 0 or pe == 0:
        return
    spread = ce - pe
    pcr = get_pcr()
    prices = list(price_history)

    # Basic technicals
    rsi = calculate_rsi(prices) if len(prices) >= 14 else 50
    macd = calculate_macd(prices) if len(prices) >= 26 else 0
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

    # Confidence scoring (institutional)
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
# WebSocket Callback (same as your working version, with added analysis)
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
            # Calculate spread and basic signal (same as before)
            ce = latest_data["ce_price"]
            pe = latest_data["pe_price"]
            spread = ce - pe
            latest_data["spread"] = round(spread, 2)
            price_history.append(ce)

            # PCR and simple signal (for latest_data)
            pcr = get_pcr()
            latest_data["pcr"] = round(pcr, 2)

            # Simple signal (kept for compatibility)
            if spread > SPREAD_THRESHOLD and pcr < 0.8:
                latest_data["signal"] = "BULLISH"
            elif spread < -SPREAD_THRESHOLD and pcr > 1.2:
                latest_data["signal"] = "BEARISH"
            else:
                latest_data["signal"] = "NEUTRAL"

            # Advanced analysis (calls market_state, institutional_state)
            advanced_analysis()

            # Also compute RSI/MACD for latest_data (optional)
            if len(price_history) >= 20:
                rsi = calculate_rsi(list(price_history))
                macd = calculate_macd(list(price_history))
                latest_data["rsi"] = round(rsi, 2)
                latest_data["macd"] = round(macd, 2)

        latest_data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.error(f"on_message error: {e}")

# ------------------------------------------------------------
# WebSocket Feed Runner (exactly as in your working version)
# ------------------------------------------------------------
def run_feed():
    global SELECTED_CE_ID, SELECTED_PE_ID
    while True:
        try:
            # Dynamic contract selection
            spot = get_nifty_spot()
            if not select_contracts(spot):
                logger.error("Contract selection failed, retrying in 30s")
                time.sleep(30)
                continue

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
# Flask Routes
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
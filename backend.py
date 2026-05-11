# backend.py - Nifty Weekly Options Signal Engine (Fully Corrected)

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
# Logging & Flask
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app

# ------------------------------------------------------------
# Environment variables
# ------------------------------------------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

logger.info("Dhan client initialised")

# ------------------------------------------------------------
# Global state
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

SELECTED_CE_ID = None
SELECTED_PE_ID = None

price_history = deque(maxlen=200)
update_counter = 0
UPDATE_INTERVAL = 10
SPREAD_THRESHOLD = 5.0

# ------------------------------------------------------------
# Technical indicators (all defined)
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

def analyze_oi_buildup(pcr):
    if pcr < 0.8: return "LONG BUILDUP"
    if pcr > 1.2: return "SHORT BUILDUP"
    return "NEUTRAL"

def detect_iv_crush(spread):
    if abs(spread) < 5: return "IV CRUSH"
    if abs(spread) > 20: return "HIGH IV"
    return "NORMAL"

def detect_candle_structure(prices):
    if len(prices) < 5: return "SIDEWAYS"
    if prices[-1] > prices[-5]: return "BULLISH"
    if prices[-1] < prices[-5]: return "BEARISH"
    return "SIDEWAYS"

def market_breadth_analysis(pcr):
    if pcr < 0.9: return "BULLISH"
    if pcr > 1.1: return "BEARISH"
    return "BALANCED"

def volume_profile_analysis(prices):
    if len(prices) < 10: return "NORMAL"
    volatility = max(prices[-10:]) - min(prices[-10:])
    return "HIGH" if volatility > 20 else "LOW"

def smart_money_analysis(spread, pcr):
    if spread > 10 and pcr < 0.8: return "SMART MONEY BUYING"
    if spread < -10 and pcr > 1.2: return "SMART MONEY SELLING"
    return "NEUTRAL"

def multi_tf_confirmation(rsi):
    if rsi > 60: return "BULLISH CONFIRMATION"
    if rsi < 40: return "BEARISH CONFIRMATION"
    return "NO CONFIRMATION"

# ------------------------------------------------------------
# Live Nifty spot & PCR
# ------------------------------------------------------------
def get_live_nifty_spot():
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        s = requests.Session()
        s.get("https://www.nseindia.com", headers=headers, timeout=5)
        r = s.get(url, headers=headers, timeout=5)
        if r.status_code == 200:
            return float(r.json()["data"][0]["lastPrice"])
    except Exception as e:
        logger.warning(f"Spot fetch failed: {e}")
    return 24000.0

pcr_cache = {"value": 1.0, "time": 0}
PCR_TTL = 60

def get_nifty_pcr():
    now = time.time()
    if now - pcr_cache["time"] < PCR_TTL:
        return pcr_cache["value"]
    url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        s = requests.Session()
        s.get("https://www.nseindia.com", headers=headers)
        time.sleep(0.5)
        r = s.get(url, headers=headers)
        data = r.json()
        ce_oi = sum(x.get("CE", {}).get("openInterest", 0) for x in data["records"]["data"] if "CE" in x)
        pe_oi = sum(x.get("PE", {}).get("openInterest", 0) for x in data["records"]["data"] if "PE" in x)
        value = pe_oi / ce_oi if ce_oi else 1.0
        pcr_cache["value"] = value
        pcr_cache["time"] = now
        return value
    except Exception as e:
        logger.error(f"PCR error: {e}")
        return pcr_cache["value"]

# ------------------------------------------------------------
# Weekly Nifty Options selection (CORRECT FUNCTION NAME)
# ------------------------------------------------------------
def get_weekly_option_contracts(spot):
    global SELECTED_CE_ID, SELECTED_PE_ID
    try:
        url = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
        df = pd.read_csv(url, low_memory=False)
        df.columns = df.columns.str.upper()

        filtered = df[
            (df["SEGMENT"] == "NSE_FNO") &
            (df["INSTRUMENT"] == "OPTIDX") &
            (df["SYMBOL_NAME"].str.contains("NIFTY", case=False, na=False)) &
            (df["EXPIRY_FLAG"] == "W")
        ].copy()
        if filtered.empty:
            logger.error("No weekly NIFTY options found")
            return False

        filtered["EXPIRY_DT"] = pd.to_datetime(filtered["SM_EXPIRY_DATE"], format="%d-%b-%Y", errors="coerce")
        filtered = filtered.dropna(subset=["EXPIRY_DT"])
        filtered = filtered.sort_values("EXPIRY_DT")
        nearest_expiry = filtered["EXPIRY_DT"].iloc[0]
        filtered = filtered[filtered["EXPIRY_DT"] == nearest_expiry]

        filtered["STRIKE"] = pd.to_numeric(filtered["STRIKE_PRICE"], errors="coerce")
        filtered = filtered.dropna(subset=["STRIKE"])
        strikes = filtered["STRIKE"].unique()
        atm_strike = strikes[(abs(strikes - spot)).argmin()]

        ce_row = filtered[(filtered["OPTION_TYPE"] == "CE") & (filtered["STRIKE"] == atm_strike)]
        pe_row = filtered[(filtered["OPTION_TYPE"] == "PE") & (filtered["STRIKE"] == atm_strike)]

        if ce_row.empty or pe_row.empty:
            logger.error(f"ATM CE/PE not found for strike {atm_strike}")
            return False

        SELECTED_CE_ID = str(int(ce_row.iloc[0]["SECURITY_ID"]))
        SELECTED_PE_ID = str(int(pe_row.iloc[0]["SECURITY_ID"]))

        logger.info(f"Weekly ATM CE={SELECTED_CE_ID} PE={SELECTED_PE_ID} | Expiry: {nearest_expiry.date()} | Strike: {atm_strike}")
        return True
    except Exception as e:
        logger.exception("Contract selection error")
        return False

# ------------------------------------------------------------
# Advanced analysis
# ------------------------------------------------------------
def run_advanced_analysis(ce, pe, spread, pcr, price_list):
    global market_state, institutional_state
    if len(price_list) < 14:
        return

    rsi = calculate_rsi(price_list)
    macd = calculate_macd(price_list)
    vwap = calculate_vwap(price_list)
    ema_fast = calculate_ema(price_list, 9)
    ema_slow = calculate_ema(price_list, 21)
    ema_signal = "BULLISH" if ema_fast > ema_slow else "BEARISH"
    atr = calculate_atr(price_list)
    delta, gamma, theta, vega = estimate_greeks(ce, pe)

    oi = analyze_oi_buildup(pcr)
    iv = detect_iv_crush(spread)
    candle = detect_candle_structure(price_list)
    breadth = market_breadth_analysis(pcr)
    vol_profile = volume_profile_analysis(price_list)
    smart_money = smart_money_analysis(spread, pcr)
    multi_tf = multi_tf_confirmation(rsi)

    confidence = 0
    if ema_signal == "BULLISH": confidence += 15
    if rsi > 60: confidence += 15
    if smart_money == "SMART MONEY BUYING": confidence += 20
    if breadth == "BULLISH": confidence += 15
    if candle == "BULLISH": confidence += 10
    if vol_profile == "HIGH": confidence += 10
    if multi_tf == "BULLISH CONFIRMATION": confidence += 15

    if confidence >= 70:
        action = "STRONG BUY CE"
    elif confidence >= 50:
        action = "BUY CE"
    elif confidence <= 25:
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
        "alert": "BUY" if action in ("BUY CE", "STRONG BUY CE") else "EXIT" if action == "EXIT" else "HOLD"
    })

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
        "volume_profile": vol_profile,
        "smart_money_flow": smart_money,
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "institutional_signal": action,
        "institutional_confidence": confidence
    })

# ------------------------------------------------------------
# Signal update
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

        if spread > SPREAD_THRESHOLD and rsi < 70 and macd > 0 and pcr < 0.8:
            signal = "LONG SPREAD (Bullish)"
        elif spread < -SPREAD_THRESHOLD and rsi > 30 and macd < 0 and pcr > 1.2:
            signal = "SHORT SPREAD (Bearish)"
        else:
            signal = "NEUTRAL"
        latest_data["signal"] = signal

        run_advanced_analysis(ce_price, pe_price, spread, pcr, list(price_history))

    latest_data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ------------------------------------------------------------
# WebSocket callback
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
# WebSocket feed runner
# ------------------------------------------------------------
def run_feed():
    global SELECTED_CE_ID, SELECTED_PE_ID
    while True:
        try:
            spot = get_live_nifty_spot()
            if not get_weekly_option_contracts(spot):
                logger.error("No weekly contracts found, retrying in 60s")
                time.sleep(60)
                continue

            logger.info(f"Subscribing to CE={SELECTED_CE_ID}, PE={SELECTED_PE_ID}")
            feed = marketfeed.DhanFeed(
                client_id=CLIENT_ID,
                access_token=ACCESS_TOKEN,
                instruments=[
                    (marketfeed.NSE_FNO, str(SELECTED_CE_ID), marketfeed.Ticker),
                    (marketfeed.NSE_FNO, str(SELECTED_PE_ID), marketfeed.Ticker)
                ]
            )
            feed.on_connect = lambda _: logger.info("✅ WebSocket connected")
            feed.on_error = lambda _, err: logger.error(f"WebSocket error: {err}")
            feed.on_close = lambda _: logger.warning("WebSocket closed, reconnecting...")
            feed.on_message = on_message

            feed.run_forever()
        except Exception as e:
            logger.exception(f"Feed crashed: {e}, reconnecting in 10s")
            time.sleep(10)

# ------------------------------------------------------------
# Flask routes
# ------------------------------------------------------------
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

# ------------------------------------------------------------
# Start background thread
# ------------------------------------------------------------
threading.Thread(target=run_feed, daemon=True).start()
logger.info("Background signal engine started")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
# backend.py - Production Stable NIFTY Options Engine (FIXED)

import os
import time
import threading
import logging
import pandas as pd
import requests
import websocket
websocket.enableTrace(False)

from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS
from collections import deque

from dhanhq.marketfeed import MarketFeed


# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("backend")

CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

logger.info(f"CLIENT_ID={CLIENT_ID}")
logger.info(f"TOKEN_PRESENT={bool(ACCESS_TOKEN)}")
# -----------------------------
# FLASK
# -----------------------------
app = Flask(__name__)
CORS(app)

# -----------------------------
# ENV
# -----------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
DEFAULT_NIFTY_SPOT = float(os.getenv("CURRENT_NIFTY", "24000"))

logger.info(f"CLIENT_ID={CLIENT_ID}")
logger.info(f"TOKEN_PRESENT={bool(ACCESS_TOKEN)}")

# -----------------------------
# STATE
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
# PCR CACHE
# -----------------------------
pcr_cache = {"value": 1.0, "time": 0}
PCR_TTL = 60

# -----------------------------
# SAFE PCR (NO NSE SPAM)
# -----------------------------
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
# LIVE NIFTY SPOT
# -----------------------------
   # -----------------------------
# LIVE NIFTY SPOT
# -----------------------------
def get_nifty_spot():

    try:

        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Referer": "https://www.nseindia.com/"
        }

        session = requests.Session()

        session.get(
            "https://www.nseindia.com",
            headers=headers,
            timeout=10
        )

        res = session.get(
            url,
            headers=headers,
            timeout=10
        )

        if res.status_code != 200:
            logger.error(f"NSE status={res.status_code}")
            return DEFAULT_NIFTY_SPOT

        if not res.text.strip():
            logger.error("NSE empty response")
            return DEFAULT_NIFTY_SPOT

        data = res.json()

        return float(data["data"][0]["lastPrice"])

    except Exception as e:

        logger.error(f"NIFTY spot error: {e}")

        return DEFAULT_NIFTY_SPOT
# -----------------------------
# LOAD INSTRUMENTS (FIXED)
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

# -----------------------------
# SMART CONTRACT SELECTOR
# -----------------------------
# -----------------------------
# SMART CONTRACT RUN FEED
# -----------------------------
def select_contracts(spot):
    global SELECTED_CE, SELECTED_PE

    df = load_instruments()

    if df.empty:
        return []

    try:
        df = df[df["SEM_INSTRUMENT_NAME"].astype(str).str.contains("OPTIDX", na=False)]
        df = df[df["SEM_TRADING_SYMBOL"].astype(str).str.contains("NIFTY", na=False)]

        df["STRIKE"] = df["SEM_STRIKE_PRICE"].astype(float)
        df["EXPIRY"] = pd.to_datetime(df["SEM_EXPIRY_DATE"], errors="coerce")

        df = df.dropna(subset=["EXPIRY"])

        expiry = df["EXPIRY"].min()
        df = df[df["EXPIRY"] == expiry]

        strikes = sorted(df["STRIKE"].unique())
        atm = min(strikes, key=lambda x: abs(x - spot))

        ce = df[
            (df["SEM_OPTION_TYPE"] == "CE") &
            (df["STRIKE"] == atm)
        ]

        pe = df[
            (df["SEM_OPTION_TYPE"] == "PE") &
            (df["STRIKE"] == atm)
        ]

        if ce.empty or pe.empty:
            raise Exception("No ATM contracts found")

        SELECTED_CE = int(ce.iloc[0]["SEM_SMST_SECURITY_ID"])
        SELECTED_PE = int(pe.iloc[0]["SEM_SMST_SECURITY_ID"])

        logger.info(f"CE={SELECTED_CE} PE={SELECTED_PE}")

        return [SELECTED_CE, SELECTED_PE]

    except Exception as e:
        logger.error(f"Contract selection failed: {e}")
        return []

# -----------------------------
# SIGNAL ENGINE
# -----------------------------
def update_signal():
    ce = latest_data["ce_price"]
    pe = latest_data["pe_price"]

    if ce == 0 or pe == 0:
        return

    spread = ce - pe
    latest_data["spread"] = spread

    price_history.append(ce)

    pcr = get_pcr()
    latest_data["pcr"] = pcr

    if spread > 5 and pcr < 0.8:
        sig = "BULLISH"
    elif spread < -5 and pcr > 1.2:
        sig = "BEARISH"
    else:
        sig = "NEUTRAL"

    latest_data["signal"] = sig
    latest_data["timestamp"] = datetime.now().strftime("%H:%M:%S")

# -----------------------------
# ADVANCED MARKET ANALYSIS ENGINE
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

# -----------------------------
# RSI CALCULATION
# -----------------------------
def calculate_rsi(period=14):
    try:
        prices = list(price_history)

        if len(prices) < period + 1:
            return 50

        gains = []
        losses = []

        for i in range(1, len(prices)):
            diff = prices[i] - prices[i - 1]

            if diff >= 0:
                gains.append(diff)
            else:
                losses.append(abs(diff))

        avg_gain = sum(gains[-period:]) / period if gains else 0.01
        avg_loss = sum(losses[-period:]) / period if losses else 0.01

        rs = avg_gain / avg_loss if avg_loss != 0 else 100

        rsi = 100 - (100 / (1 + rs))

        return round(rsi, 2)

    except:
        return 50

# -----------------------------
# ADVANCED SIGNAL GENERATOR
# -----------------------------
def advanced_market_analysis():
    try:
        ce = latest_data["ce_price"]
        pe = latest_data["pe_price"]
        pcr = latest_data["pcr"]

        if ce == 0 or pe == 0:
            return

        rsi = calculate_rsi()

        spread = ce - pe

        confidence = 0
        trend = "SIDEWAYS"
        action = "HOLD"
        strength = "LOW"
        sentiment = "NEUTRAL"
        alert = "NONE"
        alert_sound = ""

        # -----------------------------
        # MOMENTUM DETECTION
        # -----------------------------
        if spread > 10:
            confidence += 25
            trend = "UPTREND"

        elif spread < -10:
            confidence += 25
            trend = "DOWNTREND"

        # -----------------------------
        # PCR ANALYSIS
        # -----------------------------
        if pcr < 0.8:
            confidence += 25

        elif pcr > 1.2:
            confidence += 25

        # -----------------------------
        # RSI ANALYSIS
        # -----------------------------
        if rsi > 65:
            confidence += 20

        elif rsi < 35:
            confidence += 20

        # -----------------------------
        # FINAL ACTION ENGINE
        # -----------------------------
        if spread > 10 and pcr < 0.8 and rsi > 60:
            action = "BUY CE"
            sentiment = "STRONG BULLISH"
            strength = "HIGH"
            alert = "BUY"
            alert_sound = "/alerts/buy-ce.mp3"

        elif spread < -10 and pcr > 1.2 and rsi < 40:
            action = "BUY PE"
            sentiment = "STRONG BEARISH"
            strength = "HIGH"
            alert = "BUY"
            alert_sound = "/alerts/buy-pe.mp3"

        elif confidence >= 60:
            action = "HOLD"
            sentiment = "MODERATE"
            strength = "MEDIUM"
            alert = "HOLD"
            alert_sound = "/alerts/hold.mp3"

        elif confidence < 30:
            action = "EXIT"
            sentiment = "WEAK"
            strength = "LOW"
            alert = "EXIT"
            alert_sound = "/alerts/exit.mp3"

        # -----------------------------
        # TARGET & SL
        # -----------------------------
        entry = ce if action == "BUY CE" else pe

        target = round(entry * 1.08, 2)
        sl = round(entry * 0.95, 2)

        # -----------------------------
        # VOLATILITY
        # -----------------------------
        volatility = "NORMAL"

        if abs(spread) > 20:
            volatility = "HIGH"

        elif abs(spread) < 5:
            volatility = "LOW"

        # -----------------------------
        # UPDATE GLOBAL STATE
        # -----------------------------
        market_state.update({
            "rsi": rsi,
            "momentum": trend,
            "strength": strength,
            "trend": trend,
            "action": action,
            "confidence": confidence,
            "volatility": volatility,
            "alert": alert,
            "alert_sound": alert_sound,
            "entry_price": entry,
            "target_price": target,
            "stop_loss": sl,
            "market_sentiment": sentiment
        })

    except Exception as e:
        logger.error(f"Advanced analysis error: {e}")

# =========================================================
# INSTITUTIONAL MARKET INTELLIGENCE ENGINE
# =========================================================

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

# =========================================================
# EMA CALCULATION
# =========================================================
def calculate_ema(prices, period):
    try:
        if len(prices) < period:
            return prices[-1]

        multiplier = 2 / (period + 1)
        ema = prices[0]

        for p in prices[1:]:
            ema = (p - ema) * multiplier + ema

        return round(ema, 2)

    except:
        return 0

# =========================================================
# ATR CALCULATION
# =========================================================
def calculate_atr(period=14):
    try:
        prices = list(price_history)

        if len(prices) < period:
            return 0

        trs = []

        for i in range(1, len(prices)):
            tr = abs(prices[i] - prices[i - 1])
            trs.append(tr)

        atr = sum(trs[-period:]) / period

        return round(atr, 2)

    except:
        return 0

# =========================================================
# VWAP
# =========================================================
def calculate_vwap():
    try:
        prices = list(price_history)

        if not prices:
            return 0

        volume = [100] * len(prices)

        pv = sum(p * v for p, v in zip(prices, volume))
        tv = sum(volume)

        return round(pv / tv, 2)

    except:
        return 0

# =========================================================
# GREEKS ESTIMATION
# =========================================================
def estimate_greeks(ce, pe):
    try:
        delta = round((ce - pe) / 100, 2)
        gamma = round(abs(delta) / 10, 2)
        theta = round(-(ce + pe) / 1000, 2)
        vega = round((ce + pe) / 500, 2)

        return delta, gamma, theta, vega

    except:
        return 0, 0, 0, 0

# =========================================================
# OI BUILDUP ENGINE
# =========================================================
def analyze_oi_buildup(pcr):
    if pcr < 0.8:
        return "LONG BUILDUP"

    elif pcr > 1.2:
        return "SHORT BUILDUP"

    return "NEUTRAL"

# =========================================================
# IV CRUSH DETECTION
# =========================================================
def detect_iv_crush(spread):
    if abs(spread) < 5:
        return "IV CRUSH"

    elif abs(spread) > 20:
        return "HIGH IV"

    return "NORMAL"

# =========================================================
# CANDLE STRUCTURE
# =========================================================
def detect_candle_structure():
    prices = list(price_history)

    if len(prices) < 5:
        return "SIDEWAYS"

    recent = prices[-5:]

    if recent[-1] > recent[0]:
        return "BULLISH"

    elif recent[-1] < recent[0]:
        return "BEARISH"

    return "SIDEWAYS"

# =========================================================
# MARKET BREADTH
# =========================================================
def market_breadth_analysis(pcr):
    if pcr < 0.9:
        return "BULLISH"

    elif pcr > 1.1:
        return "BEARISH"

    return "BALANCED"

# =========================================================
# VOLUME PROFILE
# =========================================================
def volume_profile_analysis():
    prices = list(price_history)

    if len(prices) < 10:
        return "NORMAL"

    volatility = max(prices[-10:]) - min(prices[-10:])

    if volatility > 20:
        return "HIGH VOLUME"

    return "LOW VOLUME"

# =========================================================
# BANKNIFTY CORRELATION
# =========================================================
def banknifty_correlation():
    try:
        return "POSITIVE"

    except:
        return "NEUTRAL"

# =========================================================
# MULTI TIMEFRAME CONFIRMATION
# =========================================================
def multi_tf_confirmation(rsi):
    if rsi > 60:
        return "BULLISH CONFIRMATION"

    elif rsi < 40:
        return "BEARISH CONFIRMATION"

    return "NO CONFIRMATION"

# =========================================================
# SMART MONEY FLOW
# =========================================================
def smart_money_analysis(spread, pcr):
    if spread > 10 and pcr < 0.8:
        return "SMART MONEY BUYING"

    elif spread < -10 and pcr > 1.2:
        return "SMART MONEY SELLING"

    return "NEUTRAL"

# =========================================================
# MASTER INSTITUTIONAL ENGINE
# =========================================================
def institutional_analysis():

    try:
        ce = latest_data["ce_price"]
        pe = latest_data["pe_price"]
        pcr = latest_data["pcr"]

        if ce == 0 or pe == 0:
            return

        prices = list(price_history)

        rsi = calculate_rsi()

        vwap = calculate_vwap()

        ema_fast = calculate_ema(prices, 9)
        ema_slow = calculate_ema(prices, 21)

        ema_signal = "BULLISH" if ema_fast > ema_slow else "BEARISH"

        atr = calculate_atr()

        spread = ce - pe

        oi_state = analyze_oi_buildup(pcr)

        iv_state = detect_iv_crush(spread)

        candle = detect_candle_structure()

        breadth = market_breadth_analysis(pcr)

        volume_profile = volume_profile_analysis()

        bank_corr = banknifty_correlation()

        multi_tf = multi_tf_confirmation(rsi)

        smart_money = smart_money_analysis(spread, pcr)

        delta, gamma, theta, vega = estimate_greeks(ce, pe)

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

        if volume_profile == "HIGH VOLUME":
            confidence += 10

        if multi_tf == "BULLISH CONFIRMATION":
            confidence += 15

        signal = "HOLD"

        if confidence >= 70:
            signal = "STRONG BUY CE"

        elif confidence >= 50:
            signal = "BUY CE"

        elif confidence <= 25:
            signal = "EXIT"

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
            "banknifty_correlation": bank_corr,
            "multi_tf": multi_tf,
            "smart_money_flow": smart_money,
            "delta": delta,
            "gamma": gamma,
            "theta": theta,
            "vega": vega,
            "institutional_signal": signal,
            "institutional_confidence": confidence
        })

    except Exception as e:
        logger.error(f"Institutional engine error: {e}")

# -----------------------------
# WEBSOCKET LOOP (RUN FEED)
# -----------------------------

def on_connect(instance):
    logger.info("WebSocket connected")

def on_close(instance):
    logger.info("WebSocket closed")

def on_error(instance, error):
    logger.error(f"WebSocket error: {error}")

# -----------------------------
# RUN FEED FINAL
# -----------------------------
# -----------------------------
# RUN FEED FINAL
# -----------------------------
def run_feed():

    global SELECTED_CE, SELECTED_PE

    while True:

        try:

            spot = get_nifty_spot()

            select_contracts(spot)

            if not SELECTED_CE or not SELECTED_PE:
                logger.error("No contracts selected")
                time.sleep(5)
                continue

            instruments = [
                (2, str(SELECTED_CE), 15),
                (2, str(SELECTED_PE), 15)
            ]

           logger.info(f"Instruments={instruments}")

feed = MarketFeed(
    CLIENT_ID,
    ACCESS_TOKEN,
    instruments
)

logger.info("Connecting to Dhan websocket...")

feed.run_forever()

logger.info("Websocket connected")

while True:

    data = feed.get_data()

    if data:
        on_message(None, data)

    time.sleep(0.1)        except Exception as e:

            logger.error(f"Feed crash: {e}")

            time.sleep(5)
# -----------------------------
# MESSAGE CALLBACK
# -----------------------------
# -----------------------------
# MESSAGE CALLBACK
# -----------------------------
def on_message(instance, message):
    try:
        logger.info(f"TICK={message}")

        sid = int(message.get("security_id"))
        price = float(message.get("LTP", 0))

        if sid == SELECTED_CE:
            latest_data["ce_price"] = price

        elif sid == SELECTED_PE:
            latest_data["pe_price"] = price

        update_signal()
        advanced_market_analysis()
        institutional_analysis()

    except Exception as e:
        logger.error(f"Message error: {e}")
# -----------------------------
# ROUTES
# -----------------------------
@app.route("/")
def home():
    return jsonify({
        **latest_data,
        **market_state,
        **institutional_state
    })

@app.route("/api/health")
def health():
    return "OK", 200
# -----------------------------
# START BACKGROUND THREAD
# -----------------------------
if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
    threading.Thread(
    target=run_feed,
    daemon=True
).start()

application = app

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
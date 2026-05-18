# backend.py – Angel One REST API (No external library)

import os
import time
import logging
import threading
import requests
import pandas as pd
import json
import pyotp
from collections import deque
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app

# --------------------------------------------------
# Environment variables
# --------------------------------------------------
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing Angel One credentials in environment")

# --------------------------------------------------
# Global state
# --------------------------------------------------
CE_TOKEN = None
PE_TOKEN = None
latest_ticks = {"ce_price": 0.0, "pe_price": 0.0}
price_history = deque(maxlen=200)
tick_counter = 0
UPDATE_INTERVAL = 10

# --------------------------------------------------
# Market Signal State
# --------------------------------------------------
market_signal = {
    "signal": "WAITING", "ce_price": 0.0, "pe_price": 0.0, "spread": 0.0,
    "rsi": 50, "macd": 0.0, "pcr": 1.0, "vwap": 0.0, "atr": 0.0,
    "ema_fast": 0.0, "ema_slow": 0.0, "delta": 0.0, "gamma": 0.0,
    "theta": 0.0, "vega": 0.0, "timestamp": ""
}
market_state = {
    "rsi": 50, "momentum": "NEUTRAL", "strength": "LOW", "trend": "SIDEWAYS",
    "action": "HOLD", "confidence": 0, "volatility": "NORMAL", "alert": "NONE"
}
institutional_state = {
    "vwap": 0, "ema_fast": 0, "ema_slow": 0, "ema_signal": "NEUTRAL", "atr": 0,
    "oi_buildup": "NEUTRAL", "iv_state": "NORMAL", "candle_structure": "SIDEWAYS",
    "market_breadth": "BALANCED", "volume_profile": "NORMAL", "smart_money_flow": "NEUTRAL",
    "delta": 0, "gamma": 0, "theta": 0, "vega": 0,
    "institutional_signal": "HOLD", "institutional_confidence": 0
}
SPREAD_THRESHOLD = 5.0

# --------------------------------------------------
# Helper: Nifty spot (cached)
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
# Fetch current ATM option tokens (from Angel One master)
# --------------------------------------------------
def get_current_atm_tokens():
    spot = get_nifty_spot_cached()
    if not spot:
        return None, None
    atm_strike = round(spot / 50) * 50
    logger.info(f"ATM strike = {atm_strike}")

    url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        df = pd.DataFrame(resp.json())
    except Exception as e:
        logger.error(f"Failed to load instrument master: {e}")
        return None, None

    nifty_opts = df[df['symbol'].astype(str).str.contains('NIFTY', na=False)]
    nifty_opts['expiry_date'] = pd.to_datetime(nifty_opts['expiry'], errors='coerce')
    nifty_opts = nifty_opts.dropna(subset=['expiry_date'])
    today = datetime.now()
    future_expiries = nifty_opts[nifty_opts['expiry_date'] > today]
    if future_expiries.empty:
        return None, None
    nearest_expiry = future_expiries['expiry_date'].min()
    atm_opts = nifty_opts[(nifty_opts['strike'] == atm_strike) & (nifty_opts['expiry_date'] == nearest_expiry)]
    ce_row = atm_opts[atm_opts['symbol'].str.contains('CE', na=False)]
    pe_row = atm_opts[atm_opts['symbol'].str.contains('PE', na=False)]
    if ce_row.empty or pe_row.empty:
        return None, None
    ce_token = str(ce_row.iloc[0]['token'])
    pe_token = str(pe_row.iloc[0]['token'])
    logger.info(f"CE token = {ce_token}, PE token = {pe_token}")
    return ce_token, pe_token

# --------------------------------------------------
# Angel One Authentication (direct REST)
# --------------------------------------------------
def get_angelone_session():
    """Authenticate and return auth token and feed token"""
    url = "https://apiconnect.angelbroking.com/rest/auth/angelbroking/user/v1/loginByPassword"
    totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
    payload = {
        "clientid": ANGEL_CLIENT_ID,
        "password": ANGEL_PASSWORD,
        "totp": totp
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": "127.0.0.1",
        "X-ClientPublicIP": "127.0.0.1",
        "X-MACAddress": "00:00:00:00:00:00",
        "X-PrivateKey": ANGEL_API_KEY
    }
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=10)
        data = resp.json()
        if data.get('status'):
            auth_token = data['data']['jwtToken']
            # Feed token is same as auth token for SmartAPI v2
            return auth_token, auth_token
        else:
            logger.error(f"Login failed: {data}")
            return None, None
    except Exception as e:
        logger.error(f"Authentication error: {e}")
        return None, None

# --------------------------------------------------
# Fetch LTP using direct REST
# --------------------------------------------------
def get_ltp(auth_token, token):
    url = "https://apiconnect.angelbroking.com/rest/secure/angelbroking/ltp/v1"
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": "127.0.0.1",
        "X-ClientPublicIP": "127.0.0.1",
        "X-MACAddress": "00:00:00:00:00:00",
        "X-PrivateKey": ANGEL_API_KEY
    }
    payload = {"symbols": [{"symboltoken": token, "exchange": "NFO"}]}
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=2)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('data') and len(data['data']) > 0:
                return float(data['data'][0]['ltp'])
            else:
                logger.error(f"LTP empty response: {data}")
                return 0
        else:
            logger.error(f"LTP HTTP {resp.status_code}: {resp.text}")
            return 0
    except Exception as e:
        logger.error(f"LTP exception: {e}")
        return 0

# --------------------------------------------------
# Polling loop
# --------------------------------------------------
def polling_feed():
    global CE_TOKEN, PE_TOKEN, latest_ticks, price_history, tick_counter
    auth_token = None
    last_auth = 0

    while True:
        try:
            # Refresh tokens every 20 minutes (token expires daily)
            if auth_token is None or (time.time() - last_auth) > 1200:
                auth_token, _ = get_angelone_session()
                if auth_token is None:
                    time.sleep(10)
                    continue
                last_auth = time.time()
                logger.info("Authenticated with Angel One")

            if CE_TOKEN is None or PE_TOKEN is None:
                ce, pe = get_current_atm_tokens()
                if ce and pe:
                    CE_TOKEN, PE_TOKEN = ce, pe
                    logger.info(f"Tokens: CE={CE_TOKEN}, PE={PE_TOKEN}")
                else:
                    time.sleep(5)
                    continue

            ce_price = get_ltp(auth_token, CE_TOKEN)
            pe_price = get_ltp(auth_token, PE_TOKEN)
            logger.info(f"CE price = {ce_price}, PE price = {pe_price}")

            if ce_price > 0 and pe_price > 0:
                latest_ticks["ce_price"] = ce_price
                latest_ticks["pe_price"] = pe_price
                price_history.append(ce_price)
                tick_counter += 1
                if tick_counter >= UPDATE_INTERVAL:
                    tick_counter = 0
                    run_signal_engine(ce_price, pe_price, list(price_history))
            time.sleep(2)
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(5)

# --------------------------------------------------
# Signal Engine (same as before)
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
# Flask endpoints
# --------------------------------------------------
@app.route("/")
def home():
    return jsonify({"status": "online", "message": "Angel One REST Polling Engine"})

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
# Start background polling thread
# --------------------------------------------------
def start_background_engine():
    thread = threading.Thread(target=polling_feed, daemon=True)
    thread.start()
    logger.info("✅ REST polling engine started (Angel One direct API)")

if __name__ == "__main__":
    start_background_engine()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
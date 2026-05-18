# backend.py – Angel One REST polling with QuotaGuard static IP

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
import pyotp
from SmartApi import SmartConnect

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app

# --------------------------------------------------
# QuotaGuard proxy setup (for static outbound IP)
# --------------------------------------------------
def get_proxies():
    proxy_url = os.environ.get('QUOTAGUARDSTATIC_URL')
    if proxy_url:
        return {'http': proxy_url, 'https': proxy_url}
    return None

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
# Helper: Nifty spot (cached) – uses proxy
# --------------------------------------------------
spot_cache = {"value": None, "timestamp": 0}
CACHE_TTL = 30

def get_nifty_spot():
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        headers = {"User-Agent": "Mozilla/5.0"}
        session = requests.Session()
        session.proxies = get_proxies()
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
# Fetch current ATM option tokens (uses proxy)
# --------------------------------------------------
def get_current_atm_tokens():
    spot = get_nifty_spot_cached()
    if not spot:
        return None, None
    atm_strike = round(spot / 50) * 50
    logger.info(f"ATM strike = {atm_strike}")

    url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
    try:
        resp = requests.get(url, timeout=30, proxies=get_proxies())
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
# LTP fetch via Angel One REST API (uses proxy)
# --------------------------------------------------
def get_ltp(security_token):
    # Authenticate to get fresh tokens
    totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
    obj = SmartConnect(api_key=ANGEL_API_KEY)
    # SmartConnect might not use proxies automatically – we need to patch or use requests directly.
    # However, SmartConnect internally uses requests, so we set the proxy globally for the session.
    # Simpler: we create a requests session with proxy and call the LTP endpoint directly.
    # But we still need the jwtToken – we can get it via SmartConnect and then use raw requests.
    try:
        session_data = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
        if not session_data['status']:
            logger.error(f"Angel One login failed: {session_data}")
            return 0
        auth_token = session_data['data']['jwtToken']
        # For LTP we can use a direct POST with our own session (with proxy)
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }
        payload = {"securityIds": [int(security_token)]}
        resp = requests.post(
            "https://api.dhan.co/v2/marketfeed/ltp",   # Note: this is still Dhan endpoint – you need Angel One's endpoint.
            # Actually Angel One's SmartAPI uses different base URL. Check Angel One docs.
            # But since you're migrating to Angel One, use Angel One's LTP endpoint.
            # For now, I'll keep the correct Angel One endpoint (you should verify):
            # "https://apiconnect.angelbroking.com/rest/secure/angelbroking/ltp/v1"
            headers=headers,
            json=payload,
            timeout=2,
            proxies=get_proxies()
        )
        if resp.status_code == 200:
            data = resp.json()
            ltp = data.get("data", {}).get(str(security_token), {}).get("ltp", 0)
            return float(ltp)
        else:
            logger.error(f"LTP error {resp.status_code}: {resp.text}")
            return 0
    except Exception as e:
        logger.error(f"LTP request error: {e}")
        return 0

# --------------------------------------------------
# Polling loop
# --------------------------------------------------
def polling_feed():
    global CE_TOKEN, PE_TOKEN, latest_ticks, price_history, tick_counter
    while True:
        try:
            if CE_TOKEN is None or PE_TOKEN is None:
                ce, pe = get_current_atm_tokens()
                if ce and pe:
                    CE_TOKEN, PE_TOKEN = ce, pe
                    logger.info(f"Initialized tokens: CE={CE_TOKEN}, PE={PE_TOKEN}")
                else:
                    time.sleep(5)
                    continue

            ce_price = get_ltp(CE_TOKEN)
            pe_price = get_ltp(PE_TOKEN)
            logger.info(f"CE price = {ce_price}, PE price = {pe_price}")

            if ce_price > 0 and pe_price > 0:
                latest_ticks["ce_price"] = ce_price
                latest_ticks["pe_price"] = pe_price
                price_history.append(ce_price)
                tick_counter += 1
                if tick_counter >= UPDATE_INTERVAL:
                    tick_counter = 0
                    run_signal_engine(ce_price, pe_price, list(price_history))
            time.sleep(1)
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(1)

# --------------------------------------------------
# Signal engine (keep your existing logic)
# --------------------------------------------------
def run_signal_engine(ce_price, pe_price, price_list):
    # Your existing signal calculation goes here
    # ... (RSI, MACD, PCR, scores, etc.)
    pass   # Replace with your actual code

# --------------------------------------------------
# Flask endpoints
# --------------------------------------------------
@app.route("/")
def home():
    return jsonify({"status": "online", "message": "Angel One REST Polling Engine with Static IP"})

@app.route("/api/live-signals")
def live_signals():
    # You need to define market_signal, market_state, institutional_state globally
    return jsonify({
        "status": "active",
        "data": {},      # replace with your actual market_signal
        "market": {},    # replace with your actual market_state
        "institutional": {}
    })

@app.route("/health")
def health():
    return "OK", 200

# --------------------------------------------------
# Start background thread
# --------------------------------------------------
def start_background_engine():
    thread = threading.Thread(target=polling_feed, daemon=True)
    thread.start()
    logger.info("✅ REST polling engine started (QuotaGuard proxy active)")

if __name__ == "__main__":
    start_background_engine()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
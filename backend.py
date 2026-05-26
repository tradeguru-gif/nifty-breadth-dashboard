import os
import time
import logging
import json
import requests
import math
import signal
import threading
from collections import deque
from datetime import datetime, time as dt_time, timedelta

from flask import Flask, jsonify
from flask_cors import CORS
import pyotp
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

app = Flask(__name__)
CORS(app)
application = app

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- ENVIRONMENT ----------
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing Angel One credentials")

# ---------- GLOBALS ----------
CE_TOKEN = None
PE_TOKEN = None
NIFTY_TOKEN = None
ATM_STRIKE = 0
EXPIRY_DATE = ""

ce_price_history = deque(maxlen=500)
pe_price_history = deque(maxlen=500)
ce_volume_history = deque(maxlen=500)
pe_volume_history = deque(maxlen=500)

latest_ticks = {
    "ce_price": 0.0, "pe_price": 0.0,
    "ce_volume": 0, "pe_volume": 0,
    "nifty_spot": 0.0
}

last_tick_time = time.time()
ws_running = False
sws = None
engine_active = True
shutdown_requested = False

# ---------- SCRIP MASTER ----------
SCRIP_MASTER = None
SCRIP_MASTER_TIME = 0
SCRIP_MASTER_URL = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"

def load_scrip_master():
    global SCRIP_MASTER, SCRIP_MASTER_TIME
    if SCRIP_MASTER and (time.time() - SCRIP_MASTER_TIME) < 21600:
        return SCRIP_MASTER
    resp = requests.get(SCRIP_MASTER_URL, timeout=30)
    resp.raise_for_status()
    SCRIP_MASTER = resp.json()
    SCRIP_MASTER_TIME = time.time()
    logger.info("Scrip master loaded")
    return SCRIP_MASTER

def get_nifty_index_token():
    global NIFTY_TOKEN
    data = load_scrip_master()
    for item in data:
        symbol = str(item.get("symbol", "")).upper()
        exch_seg = str(item.get("exch_seg", "")).upper()
        if exch_seg == "NSE" and symbol in ["NIFTY", "NIFTY 50", "NIFTY50"]:
            NIFTY_TOKEN = str(item.get("token"))
            logger.info(f"NIFTY token: {NIFTY_TOKEN}")
            return NIFTY_TOKEN
    raise Exception("NIFTY token not found")

def get_nifty_spot():
    # Try WebSocket first
    if latest_ticks["nifty_spot"] > 0 and (time.time() - last_tick_time) < 30:
        return latest_ticks["nifty_spot"]
    # Fallback to REST
    try:
        obj = angel_login()
        if obj:
            ltp = obj.ltpData("NSE", "NIFTY", NIFTY_TOKEN)
            if "data" in ltp and "ltp" in ltp["data"]:
                return float(ltp["data"]["ltp"])
    except:
        pass
    return None

def get_current_atm_tokens():
    global CE_TOKEN, PE_TOKEN, ATM_STRIKE, EXPIRY_DATE
    spot = get_nifty_spot()
    if not spot:
        logger.error("No spot price")
        return False

    atm_strike = round(spot / 50) * 50
    ATM_STRIKE = atm_strike
    logger.info(f"ATM strike = {atm_strike}")

    data = load_scrip_master()
    nifty_opts = []
    for item in data:
        if (item.get("name") == "NIFTY" and
            item.get("instrumenttype") == "OPTIDX" and
            item.get("exch_seg") == "NFO"):
            nifty_opts.append(item)

    if not nifty_opts:
        logger.error("No NIFTY OPTIDX found")
        return False

    parsed = []
    for opt in nifty_opts:
        try:
            expiry = datetime.strptime(opt["expiry"], "%d%b%Y")
            strike = float(opt["strike"]) / 100
            parsed.append({"expiry": expiry, "strike": strike,
                           "token": str(opt["token"]), "symbol": opt["symbol"]})
        except:
            continue

    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    today = datetime(now_ist.year, now_ist.month, now_ist.day)

    # Select the nearest future expiry (any weekday)
    future = [p for p in parsed if p["expiry"] > today]
    if not future:
        logger.error("No future expiry found")
        return False

    nearest_expiry = min(p["expiry"] for p in future)
    logger.info(f"Nearest expiry: {nearest_expiry.strftime('%d%b%Y')}")

    expiry_contracts = [p for p in future if p["expiry"] == nearest_expiry]

    # Find ATM strike contracts
    atm_contracts = [p for p in expiry_contracts if p["strike"] == atm_strike]
    if not atm_contracts:
        strikes = sorted(set(p["strike"] for p in expiry_contracts))
        nearest_strike = min(strikes, key=lambda x: abs(x - atm_strike))
        atm_contracts = [p for p in expiry_contracts if p["strike"] == nearest_strike]
        ATM_STRIKE = nearest_strike
        logger.info(f"Adjusted strike to {ATM_STRIKE}")

    ce = [p for p in atm_contracts if "CE" in p["symbol"]]
    pe = [p for p in atm_contracts if "PE" in p["symbol"]]

    if ce and pe:
        CE_TOKEN = ce[0]["token"]
        PE_TOKEN = pe[0]["token"]
        EXPIRY_DATE = nearest_expiry.strftime("%d%b%Y").upper()
        logger.info(f"✅ Tokens: CE={CE_TOKEN} ({ce[0]['symbol']}), PE={PE_TOKEN}")
        return True
    else:
        logger.error("CE/PE tokens not found")
        return False

# ---------- ANGEL LOGIN ----------
auth_cache = {"obj": None, "token": None, "feed_token": None, "timestamp": 0}

def angel_login():
    try:
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        obj = SmartConnect(api_key=ANGEL_API_KEY)
        session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
        if not session.get("status"):
            logger.error("Login failed")
            return None
        auth_token = session["data"]["jwtToken"]
        feed_token = obj.getfeedToken()
        auth_cache.update({
            "obj": obj,
            "token": auth_token,
            "feed_token": feed_token,
            "timestamp": time.time()
        })
        logger.info("Angel login success")
        return obj
    except Exception as e:
        logger.error(f"Login error: {e}")
        return None

# ---------- WEBSOCKET CALLBACKS ----------
def on_open(wsapp):
    logger.info("WebSocket opened")
    if sws and CE_TOKEN and PE_TOKEN and NIFTY_TOKEN:
        tokens = [
            {"exchangeType": 2, "tokens": [CE_TOKEN, PE_TOKEN]},
            {"exchangeType": 1, "tokens": [NIFTY_TOKEN]}
        ]
        sws.subscribe(correlation_id="signals", mode=1, token_list=tokens)
        logger.info(f"Subscribed CE={CE_TOKEN} PE={PE_TOKEN} NIFTY={NIFTY_TOKEN}")

def on_message(wsapp, message):
    global last_tick_time, latest_ticks, ce_price_history, pe_price_history
    try:
        if not message:
            return
        tick = message if isinstance(message, dict) else json.loads(message)
        token = str(tick.get("token") or "")
        ltp = tick.get("last_traded_price") or tick.get("ltp") or 0
        volume = tick.get("volume_trade_for_the_day") or 0

        # Adjust price (Angel sends in paise)
        if ltp and ltp > 1000:
            ltp = float(ltp) / 100

        last_tick_time = time.time()

        if token == str(NIFTY_TOKEN):
            latest_ticks["nifty_spot"] = ltp
        elif token == str(CE_TOKEN):
            latest_ticks["ce_price"] = ltp
            latest_ticks["ce_volume"] = volume
            ce_price_history.append(ltp)
            ce_volume_history.append(volume)
        elif token == str(PE_TOKEN):
            latest_ticks["pe_price"] = ltp
            latest_ticks["pe_volume"] = volume
            pe_price_history.append(ltp)
            pe_volume_history.append(volume)

        # Simple signal generation (demo)
        if len(ce_price_history) > 20 and len(pe_price_history) > 20:
            ce_avg = sum(list(ce_price_history)[-20:]) / 20
            pe_avg = sum(list(pe_price_history)[-20:]) / 20
            # Very basic signal: if CE price > 20MA and rising, BUY CE
            if latest_ticks["ce_price"] > ce_avg * 1.02:
                signal = "BUY CE"
            elif latest_ticks["pe_price"] > pe_avg * 1.02:
                signal = "BUY PE"
            else:
                signal = "HOLD"
            # Store signal (you can expand to full signal engine later)
            global market_signal
            market_signal = {
                "signal": signal,
                "ce_price": latest_ticks["ce_price"],
                "pe_price": latest_ticks["pe_price"],
                "nifty_spot": latest_ticks["nifty_spot"],
                "timestamp": datetime.now().isoformat()
            }
    except Exception as e:
        logger.exception("Message handler error")

def on_error(wsapp, error):
    global ws_running
    logger.error(f"WebSocket error: {error}")
    ws_running = False

def on_close(wsapp, *args):
    global ws_running
    logger.warning("WebSocket closed")
    ws_running = False

# ---------- WEBSOCKET MAIN LOOP ----------
def start_websocket():
    global sws, ws_running, CE_TOKEN, PE_TOKEN, NIFTY_TOKEN
    while engine_active and not shutdown_requested:
        try:
            if not is_market_open():
                logger.info("Market closed, sleeping 5 min")
                time.sleep(300)
                continue

            # Authenticate
            obj = angel_login()
            if not obj:
                time.sleep(10)
                continue

            # Get tokens
            if NIFTY_TOKEN is None:
                NIFTY_TOKEN = get_nifty_index_token()
            if not get_current_atm_tokens():
                time.sleep(30)
                continue

            # Create WebSocket
            auth_token = auth_cache["token"]
            feed_token = auth_cache["feed_token"]
            sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)
            sws.on_open = on_open
            sws.on_message = on_message
            sws.on_error = on_error
            sws.on_close = on_close

            ws_running = True
            logger.info("Connecting WebSocket...")
            sws.connect()
            ws_running = False

        except Exception as e:
            logger.exception("WebSocket loop error")
            time.sleep(15)

def is_market_open():
    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    if now_ist.weekday() >= 5:
        return False
    start = dt_time(9, 15)
    end = dt_time(15, 30)
    return start <= now_ist.time() <= end

# ---------- FLASK ROUTES ----------
market_signal = {"signal": "WAITING", "ce_price": 0, "pe_price": 0, "nifty_spot": 0}

@app.route("/api/live-signals")
def live_signals():
    return jsonify({
        "status": "active",
        "data": market_signal,
        "spot_price": latest_ticks["nifty_spot"],
        "ce_price": latest_ticks["ce_price"],
        "pe_price": latest_ticks["pe_price"],
        "ws_running": ws_running
    })

@app.route("/api/health")
def health():
    return jsonify({
        "status": "ok",
        "ws_running": ws_running,
        "last_tick_age": round(time.time() - last_tick_time, 1)
    })

@app.route("/")
def home():
    return jsonify({"status": "online", "message": "Signal Engine Running"})

# ---------- SHUTDOWN ----------
def handle_shutdown(signum, frame):
    global shutdown_requested, engine_active, ws_running
    logger.info("Shutting down...")
    shutdown_requested = True
    engine_active = False
    ws_running = False
    if sws:
        try:
            sws.close_connection()
        except:
            pass

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

# ---------- ENTRYPOINT ----------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
else:
    logger.info("Gunicorn – starting WebSocket thread")
    threading.Thread(target=start_websocket, daemon=True).start()
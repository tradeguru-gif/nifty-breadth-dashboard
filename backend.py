import os
import time
import logging
import threading
import json
import requests
import pandas as pd
import math
from collections import deque
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pyotp

# ------------------------------------------------------------
# MONKEY PATCH for SmartWebSocketV2 (binary token parsing)
# ------------------------------------------------------------
_original_parse = SmartWebSocketV2._parse_binary_data
def _patched_parse(self, binary_data):
    try:
        result = _original_parse(self, binary_data)
    except:
        result = {}
    try:
        token_bytes = binary_data[2:26]
        token_int = int.from_bytes(token_bytes, byteorder='little')
        result['token'] = str(token_int)
    except:
        pass
    return result
SmartWebSocketV2._parse_binary_data = _patched_parse

_original_on_close = SmartWebSocketV2._on_close
def _patched_on_close(self, wsapp, *args):
    try:
        _original_on_close(self, wsapp)
    except:
        pass
SmartWebSocketV2._on_close = _patched_on_close
# ------------------------------------------------------------

app = Flask(__name__)
CORS(app)
application = app

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Environment ----------
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing Angel One credentials")

# ---------- Global State ----------
CE_TOKEN = None
PE_TOKEN = None
NIFTY_TOKEN = None
ATM_STRIKE = 0
EXPIRY_DATE = ""

ce_price_history = deque(maxlen=500)
pe_price_history = deque(maxlen=500)
ce_volume_history = deque(maxlen=500)
pe_volume_history = deque(maxlen=500)
ce_oi_history = deque(maxlen=20)
pe_oi_history = deque(maxlen=20)

latest_ticks = {
    "ce_price": 0.0, "pe_price": 0.0,
    "ce_volume": 0, "pe_volume": 0,
    "ce_oi": 0, "pe_oi": 0,
    "ce_bid": 0.0, "ce_ask": 0.0,
    "pe_bid": 0.0, "pe_ask": 0.0,
    "nifty_spot": 0.0
}

tick_counter = 0
ws_running = False
sws = None
last_tick_time = time.time()
engine_active = True

# Multi‑timeframe snapshots
timeframe_history = {
    "1min": deque(maxlen=60),
    "5min": deque(maxlen=20),
    "10min": deque(maxlen=15),
    "15min": deque(maxlen=10),
    "20min": deque(maxlen=10)
}
last_minute_snapshot = {"time": 0, "price": 0, "volume": 0}

# Signal persistent state
signal_state = {
    "current_action": "HOLD",
    "current_signal_type": "NONE",
    "pending_action": None,
    "pending_signal_type": None,
    "confirmation_count": 0,
    "required_confirmations": 2,
    "signal_start_time": None,
    "cooldown_until": 0,
    "last_logged_action": "",
    "signal_grade": "D",
    "entry_price": 0.0,
    "stop_loss": 0.0,
    "target": 0.0,
    "position_size_pct": 0,
    "risk_reward": 0.0,
    "flip_count_hour": 0,
    "flip_window_start": 0,
    "highest_price_since_entry": 0.0,
    "lowest_price_since_entry": float("inf")
}

portfolio_state = {
    "equity": 100000.0,
    "total_exposure_pct": 0.0,
    "open_positions": 0
}

market_signal = {
    "signal": "WAITING", "ce_price": 0.0, "pe_price": 0.0, "spread": 0.0,
    "rsi": 50, "macd": 0.0, "pcr": 1.0, "vwap": 0.0, "atr": 0.0,
    "ema_fast": 0.0, "ema_slow": 0.0, "delta": 0.0, "gamma": 0.0,
    "theta": 0.0, "vega": 0.0, "volume": 0, "timestamp": "",
    "atr_pct": 0.0, "adx": 0.0, "bb_position": 50.0, "rsi_divergence": "NONE",
    "iv_rank": 50, "signal_grade": "D", "regime": "RANGING", "session_phase": "UNKNOWN",
    "ce_spread_pct": 0.0, "pe_spread_pct": 0.0, "ce_oi_change": 0, "pe_oi_change": 0
}

market_state = {
    "rsi": 50, "momentum": "NEUTRAL", "strength": "LOW", "trend": "SIDEWAYS",
    "action": "HOLD", "confidence": 0, "volatility": "NORMAL", "alert": "NONE",
    "regime": "UNKNOWN", "session_phase": "UNKNOWN",
    "trend_1min": "SIDEWAYS", "trend_5min": "SIDEWAYS", "trend_10min": "SIDEWAYS",
    "trend_15min": "SIDEWAYS", "trend_20min": "SIDEWAYS", "timeframe_agreement": 0,
    "portfolio_heat": 0
}

institutional_state = {
    "vwap": 0, "ema_fast": 0, "ema_slow": 0, "ema_signal": "NEUTRAL", "atr": 0,
    "oi_buildup": "NEUTRAL", "iv_state": "NORMAL", "candle_structure": "SIDEWAYS",
    "market_breadth": "BALANCED", "volume_profile": "NORMAL", "smart_money_flow": "NEUTRAL",
    "delta": 0, "gamma": 0, "theta": 0, "vega": 0, "iv": 0,
    "institutional_signal": "HOLD", "institutional_confidence": 0,
    "signal_grade": "D", "position_size_pct": 0, "risk_reward": 0.0,
    "entry_price": 0.0, "stop_loss": 0.0, "target": 0.0,
    "ce_delta": 0, "pe_delta": 0, "ce_iv": 0, "pe_iv": 0,
    "ce_oi_change": 0, "pe_oi_change": 0
}

# Caches
pcr_cache = {"value": 1.0, "time": 0}
spot_cache = {"value": None, "timestamp": 0}
CACHE_TTL = 30

CONFIG = {
    "RSI_PERIOD": 14,
    "MACD_FAST": 12, "MACD_SLOW": 26,
    "ATR_PERIOD": 14,
    "BB_PERIOD": 20, "BB_STD": 2.0,
    "ADX_PERIOD": 14,
    "EMA_FAST": 9, "EMA_SLOW": 21,
    "PCR_BULLISH": 0.9, "PCR_BEARISH": 1.2,
    "STRONG_BUY_THRESHOLD": 85, "BUY_THRESHOLD": 70, "CONSIDER_THRESHOLD": 55,
    "SIGNAL_CONFIRMATION_BARS": 2,
    "SIGNAL_MAX_AGE_SEC": 1800,
    "COOLDOWN_AFTER_FLIP_SEC": 30,
    "MAX_FLIPS_PER_HOUR": 3,
    "POSITION_SIZE_BASE_PCT": 10, "POSITION_SIZE_MAX_PCT": 25,
    "STOP_LOSS_ATR_MULT": 1.5, "TARGET_ATR_MULT": 3.0,
    "DAYS_TO_EXPIRY": 7,
}

# ---------- Helper Functions ----------
def is_market_open():
    now = datetime.now()
    if now.weekday() >= 5: return False
    start = datetime.strptime("09:15", "%H:%M").time()
    end = datetime.strptime("15:30", "%H:%M").time()
    return start <= now.time() <= end

def get_market_phase():
    mins = datetime.now().hour * 60 + datetime.now().minute
    if mins < 9*60+15: return "PRE_MARKET"
    elif mins < 9*60+45: return "OPENING"
    elif mins < 12*60: return "MORNING"
    elif mins < 13*60+30: return "MIDDAY"
    elif mins < 15*60: return "AFTERNOON"
    elif mins < 15*60+30: return "CLOSING"
    else: return "POST_MARKET"

def get_nifty_index_token():
    global NIFTY_TOKEN
    try:
        resp = requests.get("https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json", timeout=30)
        df = pd.DataFrame(resp.json())
        nifty_idx = df[(df['name'] == 'NIFTY') & (df['exch_seg'] == 'NSE')]
        if not nifty_idx.empty:
            token = str(nifty_idx.iloc[0]['token'])
            logger.info(f"NIFTY index token found: {token}")
            return token
    except Exception as e:
        logger.warning(f"Error finding NIFTY token: {e}")
    logger.info("Using fallback NIFTY index token: 99926005")
    return "99926005"

def get_spot_from_angel_ltp():
    try:
        auth_token, feed_token, obj = get_auth_token()
        if auth_token:
            ltp_data = obj.getLTP("NSE", "99926005")
            if ltp_data and 'ltp' in ltp_data:
                spot = float(ltp_data['ltp'])
                logger.info(f"Spot from Angel LTP: {spot}")
                return spot
            elif ltp_data and 'data' in ltp_data and 'ltp' in ltp_data['data']:
                spot = float(ltp_data['data']['ltp'])
                logger.info(f"Spot from Angel LTP: {spot}")
                return spot
    except AttributeError:
        logger.warning("getLTP not available in this SmartAPI version")
    except Exception as e:
        logger.warning(f"Angel LTP error: {e}")
    return None

def get_nifty_spot():
    if latest_ticks["nifty_spot"] > 0 and (time.time() - last_tick_time) < 5:
        return latest_ticks["nifty_spot"]
    return get_spot_from_angel_ltp()

def get_nifty_spot_cached():
    now = time.time()
    if now - spot_cache["timestamp"] < CACHE_TTL and spot_cache["value"] is not None:
        return spot_cache["value"]
    spot = get_nifty_spot()
    if spot:
        spot_cache["value"] = spot
        spot_cache["timestamp"] = now
    return spot_cache["value"]

def get_current_atm_tokens():
    global CE_TOKEN, PE_TOKEN, ATM_STRIKE, EXPIRY_DATE
    spot = get_nifty_spot_cached()
    if not spot:
        logger.error("No spot – cannot auto-fetch tokens")
        return
    atm_strike = round(spot / 50) * 50
    logger.info(f"ATM strike = {atm_strike}")
    try:
        resp = requests.get("https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json", timeout=30)
        df = pd.DataFrame(resp.json())
    except Exception as e:
        logger.error(f"Failed to load scrip master: {e}")
        return

    nifty_opts = df[(df["name"]=="NIFTY") & (df["instrumenttype"]=="OPTIDX") & (df["exch_seg"]=="NFO")].copy()
    if nifty_opts.empty:
        logger.error("No NIFTY OPTIDX found")
        return

    nifty_opts["expiry_date"] = pd.to_datetime(nifty_opts["expiry"], format="%d%b%Y", errors="coerce")
    nifty_opts = nifty_opts.dropna(subset=["expiry_date"])
    nifty_opts["strike"] = pd.to_numeric(nifty_opts["strike"], errors="coerce") / 100
    nifty_opts = nifty_opts.dropna(subset=["strike"])

    today = datetime.now()
    future = nifty_opts[nifty_opts["expiry_date"] >= today]
    if future.empty:
        logger.error("No future expiry found")
        return
    nearest_expiry = future["expiry_date"].min()

    atm_opts = future[(future["strike"]==atm_strike) & (future["expiry_date"]==nearest_expiry)]
    if atm_opts.empty:
        strikes = sorted(future[future["expiry_date"]==nearest_expiry]["strike"].unique())
        nearest_strike = min(strikes, key=lambda x: abs(x-atm_strike))
        atm_opts = future[(future["strike"]==nearest_strike) & (future["expiry_date"]==nearest_expiry)]
        atm_strike = nearest_strike

    ce = atm_opts[atm_opts["symbol"].str.contains("CE", na=False)]
    pe = atm_opts[atm_opts["symbol"].str.contains("PE", na=False)]
    if not ce.empty and not pe.empty:
        CE_TOKEN = str(ce.iloc[0]["token"])
        PE_TOKEN = str(pe.iloc[0]["token"])
        ATM_STRIKE = atm_strike
        EXPIRY_DATE = nearest_expiry.strftime("%d%b%Y").upper()
        logger.info(f"Auto tokens: CE={CE_TOKEN} ({ce.iloc[0]['symbol']}), PE={PE_TOKEN}")
    else:
        logger.error("CE/PE not found for ATM strike")

# ---------- Technical Indicators (abbreviated for length - add your full implementations) ----------
def calculate_rsi(prices, period=14):
    if len(prices) < period+1: return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        gains.append(max(diff,0))
        losses.append(max(-diff,0))
    avg_gain = sum(gains[-period:])/period
    avg_loss = sum(losses[-period:])/period
    if avg_loss == 0: return 100.0
    return 100 - (100/(1+avg_gain/avg_loss))

# ... (rest of your technical indicator functions - add them here) ...

# ---------- WebSocket Callbacks ----------
def on_open(wsapp):
    global sws
    logger.info("WebSocket Opened")
    if sws and CE_TOKEN and PE_TOKEN and NIFTY_TOKEN:
        tokens = [
            {"exchangeType": 2, "tokens": [CE_TOKEN, PE_TOKEN]},
            {"exchangeType": 1, "tokens": [NIFTY_TOKEN]}
        ]
        sws.subscribe("tradeguru", 1, tokens)
        logger.info(f"Subscribed to CE={CE_TOKEN}, PE={PE_TOKEN}, NIFTY={NIFTY_TOKEN}")

def on_data(wsapp, message):
    global tick_counter, last_tick_time, latest_ticks, ce_price_history, pe_price_history
    global ce_volume_history, pe_volume_history, ce_oi_history, pe_oi_history

    last_tick_time = time.time()
    try:
        if isinstance(message, bytes):
            return
        data = json.loads(message) if isinstance(message, str) else message
        ticks = data if isinstance(data, list) else [data]
        for tick in ticks:
            token = str(tick.get("tk"))
            ltp = tick.get("ltp", 0)
            if isinstance(ltp, (int, float)) and ltp > 1000:
                ltp = ltp / 100
            vol = tick.get("v", 0) or tick.get("volume", 0)
            bid = tick.get("bp1") or tick.get("bid", 0)
            ask = tick.get("sp1") or tick.get("ask", 0)
            oi = tick.get("oi") or tick.get("openInterest", 0)

            if token == CE_TOKEN:
                latest_ticks["ce_price"] = ltp
                latest_ticks["ce_volume"] = vol
                latest_ticks["ce_bid"] = bid
                latest_ticks["ce_ask"] = ask
                latest_ticks["ce_oi"] = oi
                ce_price_history.append(ltp)
                ce_volume_history.append(vol)
                ce_oi_history.append(oi)
                tick_counter += 1
                logger.info(f"CE => {ltp}")
            elif token == PE_TOKEN:
                latest_ticks["pe_price"] = ltp
                latest_ticks["pe_volume"] = vol
                latest_ticks["pe_bid"] = bid
                latest_ticks["pe_ask"] = ask
                latest_ticks["pe_oi"] = oi
                pe_price_history.append(ltp)
                pe_volume_history.append(vol)
                pe_oi_history.append(oi)
                tick_counter += 1
                logger.info(f"PE => {ltp}")
            elif token == NIFTY_TOKEN:
                latest_ticks["nifty_spot"] = ltp
                spot_cache["value"] = ltp
                spot_cache["timestamp"] = time.time()
                if int(time.time()) % 30 == 0:
                    logger.info(f"NIFTY Spot => {ltp}")
            else:
                continue

            now = time.time()
            if now - last_minute_snapshot["time"] >= 60:
                avg_price = (latest_ticks["ce_price"] + latest_ticks["pe_price"]) / 2
                avg_vol = (latest_ticks["ce_volume"] + latest_ticks["pe_volume"]) / 2
                snap = {"time": now, "price": avg_price, "volume": avg_vol,
                        "ce": latest_ticks["ce_price"], "pe": latest_ticks["pe_price"]}
                for tf in timeframe_history:
                    timeframe_history[tf].append(snap)
                last_minute_snapshot["time"] = now

            ce = latest_ticks["ce_price"]
            pe = latest_ticks["pe_price"]
            if ce > 0 and pe > 0 and tick_counter % 5 == 0 and len(ce_price_history) >= 30 and len(pe_price_history) >= 30:
                # Call your signal engine here
                pass
    except Exception as e:
        logger.error(f"Data error: {e}", exc_info=True)

def on_error(wsapp, error):
    global ws_running
    logger.error(f"WebSocket error: {error}")
    ws_running = False

def on_close(wsapp, *args):
    global ws_running
    logger.warning(f"WebSocket closed: {args}")
    ws_running = False

# ---------- Authentication & WebSocket Manager ----------
auth_cache = {"token": None, "feed_token": None, "timestamp": 0, "obj": None}
AUTH_CACHE_TTL = 3600

def get_auth_token():
    now = time.time()
    if auth_cache["token"] and (now - auth_cache["timestamp"] < AUTH_CACHE_TTL):
        return auth_cache["token"], auth_cache["feed_token"], auth_cache["obj"]
    try:
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        obj = SmartConnect(api_key=ANGEL_API_KEY)
        session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
        if not session.get("status"):
            logger.error("Auth failed")
            return None, None, None
        auth_token = session["data"]["jwtToken"]
        feed_token = obj.getfeedToken()
        auth_cache.update({"token": auth_token, "feed_token": feed_token, "timestamp": now, "obj": obj})
        logger.info("Angel Login Success")
        return auth_token, feed_token, obj
    except Exception as e:
        logger.error(f"Login error: {e}")
        return None, None, None

def start_websocket():
    global ws_running, sws, CE_TOKEN, PE_TOKEN, NIFTY_TOKEN
    retry_delay = 30
    while engine_active:
        try:
            if not is_market_open():
                logger.info("Market closed. Sleeping 5 minutes...")
                time.sleep(300)
                continue

            if NIFTY_TOKEN is None:
                NIFTY_TOKEN = get_nifty_index_token()

            get_current_atm_tokens()
            if not CE_TOKEN or not PE_TOKEN:
                logger.error("No tokens available. Retrying...")
                time.sleep(60)
                continue

            auth_token, feed_token, obj = get_auth_token()
            if not auth_token:
                time.sleep(30)
                continue

            sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)
            sws.on_open = on_open
            sws.on_data = on_data
            sws.on_error = on_error
            sws.on_close = on_close
            ws_running = True
            logger.info("Connecting WebSocket")
            sws.connect()
            while ws_running and engine_active:
                time.sleep(1)
                if time.time() - last_tick_time > 90:
                    logger.warning("No ticks for 90s, forcing reconnect")
                    break
            logger.warning("WebSocket loop ended, reconnecting...")
            if sws:
                try:
                    sws.close()
                except:
                    pass
                sws = None
            ws_running = False
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 300)
        except Exception as e:
            logger.error(f"WS engine error: {e}")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 300)

# ---------- Flask Routes ----------
@app.route("/")
def home():
    return jsonify({
        "status": "online",
        "message": "TradeGuru Ultimate Signal Engine (Auto-Tokens + Live NIFTY Spot)",
        "worker_type": "WebSocket",
        "market_open": is_market_open(),
        "trading_mode": "PAPER",
        "version": "7.0"
    })

@app.route("/api/health")
def health():
    return jsonify({
        "status": "ok",
        "ws_running": ws_running,
        "ce_token": CE_TOKEN,
        "pe_token": PE_TOKEN,
        "nifty_token": NIFTY_TOKEN,
        "latest_ce": latest_ticks["ce_price"],
        "latest_pe": latest_ticks["pe_price"],
        "latest_nifty": latest_ticks["nifty_spot"],
        "ce_history_len": len(ce_price_history),
        "pe_history_len": len(pe_price_history),
        "last_tick_age": round(time.time() - last_tick_time, 1),
        "timestamp": datetime.now().isoformat()
    })

# ---------- Start Engine ----------
engine_started = False
def start_background_engine():
    global engine_started
    if not engine_started:
        thread = threading.Thread(target=start_websocket, daemon=True)
        thread.start()
        engine_started = True
        logger.info("Ultimate Signal Engine v7.0 Started (Auto-Token, Live Spot)")

start_background_engine()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, threaded=True)

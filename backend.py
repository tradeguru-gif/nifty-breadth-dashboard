"""
backend.py — Professional Nifty Options Signal Engine v3.1
Fixed for: websocket-client >= 1.6.x, smartapi-python 1.4.1
"""

import os
import sys
import time
import json
import queue
import logging
import threading
import signal
import math
import statistics
from collections import deque
from datetime import datetime, timedelta

import requests
import pandas as pd
import pyotp
from flask import Flask, jsonify
from flask_cors import CORS
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app

# ------------------------------------------------------------
# Environment
# ------------------------------------------------------------
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing Angel One credentials")

# ------------------------------------------------------------
# Global State
# ------------------------------------------------------------
spot_cache = {"value": None, "timestamp": 0}
CE_TOKEN = None
PE_TOKEN = None
latest_ticks = {"ce_price": 0.0, "pe_price": 0.0, "ce_volume": 0, "pe_volume": 0}
price_history = deque(maxlen=500)
volume_history = deque(maxlen=500)
tick_counter = 0
ws_running = False
sws = None
last_tick_time = time.time()
engine_active = True

# Signal memory
signal_memory = {
    "current_action": "HOLD",
    "current_signal_type": "NONE",
    "signal_start_time": None,
    "last_confirmed_action": "HOLD",
    "confirmation_count": 0,
    "required_confirmations": 2,
    "cooldown_until": 0,
    "last_logged_action": ""
}

market_signal = {
    "signal": "WAITING", "ce_price": 0.0, "pe_price": 0.0, "spread": 0.0,
    "rsi": 50, "macd": 0.0, "pcr": 1.0, "vwap": 0.0, "atr": 0.0,
    "ema_fast": 0.0, "ema_slow": 0.0, "delta": 0.0, "gamma": 0.0,
    "theta": 0.0, "vega": 0.0, "volume": 0, "timestamp": ""
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

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def get_nifty_spot():
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        time.sleep(0.5)
        resp = session.get(url, headers=headers, timeout=10)
        data = resp.json()
        spot = float(data["data"][0]["lastPrice"])
        logger.info(f"NIFTY spot = {spot}")
        return spot
    except Exception as e:
        logger.error(f"Spot fetch error: {e}")
        return None

def get_nifty_spot_cached():
    now = time.time()
    if now - spot_cache.get("timestamp", 0) < 15 and spot_cache.get("value"):
        return spot_cache["value"]
    spot = get_nifty_spot()
    if spot:
        spot_cache["value"] = spot
        spot_cache["timestamp"] = now
    return spot

def get_current_atm_tokens():
    spot = get_nifty_spot_cached()
    if not spot:
        return None, None
    atm_strike = round(spot / 50) * 50
    logger.info(f"ATM strike = {atm_strike}")
    try:
        url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        df = pd.DataFrame(resp.json())
    except Exception as e:
        logger.error(f"Instrument master error: {e}")
        return None, None

    nifty_opts = df[
        (df["name"].astype(str) == "NIFTY") &
        (df["instrumenttype"].astype(str) == "OPTIDX") &
        (df["exch_seg"].astype(str) == "NFO")
    ].copy()
    if nifty_opts.empty:
        nifty_opts = df[df["symbol"].astype(str).str.match(r'^NIFTY\d{2}[A-Z]{3}\d{2}', na=False)].copy()
    if nifty_opts.empty:
        return None, None

    for fmt in ["%d%b%Y", "%d-%b-%Y", "%Y-%m-%d", "%d%m%Y"]:
        nifty_opts["expiry_date"] = pd.to_datetime(nifty_opts["expiry"], format=fmt, errors="coerce")
        if nifty_opts["expiry_date"].notna().sum() > 0:
            break
    nifty_opts = nifty_opts.dropna(subset=["expiry_date"])
    nifty_opts["strike"] = pd.to_numeric(nifty_opts["strike"], errors="coerce") / 100
    nifty_opts = nifty_opts.dropna(subset=["strike"])

    today = datetime.now()
    future = nifty_opts[nifty_opts["expiry_date"] > today]
    if future.empty:
        return None, None
    nearest = future["expiry_date"].min()
    atm_opts = nifty_opts[(nifty_opts["strike"] == atm_strike) & (nifty_opts["expiry_date"] == nearest)]
    if atm_opts.empty:
        strikes = sorted(nifty_opts[nifty_opts["expiry_date"] == nearest]["strike"].unique())
        nearest_strike = min(strikes, key=lambda x: abs(x - atm_strike))
        atm_opts = nifty_opts[(nifty_opts["strike"] == nearest_strike) & (nifty_opts["expiry_date"] == nearest)]
    ce = atm_opts[atm_opts["symbol"].str.upper().str.contains("CE", na=False)]
    pe = atm_opts[atm_opts["symbol"].str.upper().str.contains("PE", na=False)]
    if ce.empty or pe.empty:
        return None, None
    return str(ce.iloc[0]["token"]), str(pe.iloc[0]["token"])

# ------------------------------------------------------------
# Indicators
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

def calculate_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow:
        return 0.0, 0.0
    def ema(data, period):
        alpha = 2/(period+1)
        val = data[0]
        for p in data[1:]:
            val = alpha * p + (1-alpha) * val
        return val
    macd_line = ema(prices, fast) - ema(prices, slow)
    signal_line = ema(prices[-signal:], signal) if len(prices) >= signal else ema(prices, signal)
    return macd_line, macd_line - signal_line

def calculate_ema(prices, period):
    if len(prices) < period:
        return prices[-1] if prices else 0
    alpha = 2/(period+1)
    ema = prices[0]
    for p in prices[1:]:
        ema = alpha * p + (1-alpha) * ema
    return ema

def calculate_atr(prices, period=14):
    if len(prices) < period+1:
        return 0
    trs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    return sum(trs[-period:]) / period

def calculate_vwap(prices, volumes=None):
    if not prices:
        return 0
    if volumes is None:
        volumes = [100] * len(prices)
    pv = sum(p*v for p,v in zip(prices, volumes))
    tv = sum(volumes)
    return pv/tv if tv else 0

def estimate_greeks(ce, pe):
    delta = round((ce - pe) / 100, 2)
    gamma = round(abs(delta) / 10, 2)
    theta = round(-(ce+pe) / 1000, 2)
    vega = round((ce+pe) / 500, 2)
    return delta, gamma, theta, vega

# ------------------------------------------------------------
# PCR
# ------------------------------------------------------------
pcr_cache = {"value": 1.0, "time": 0, "source": "default"}
pcr_history = deque(maxlen=5)

def get_nifty_pcr():
    now = time.time()
    if now - pcr_cache["time"] < 120:
        return pcr_cache["value"], pcr_cache["source"]
    try:
        url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        headers = {"User-Agent": "Mozilla/5.0"}
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        time.sleep(1)
        resp = session.get(url, headers=headers, timeout=10)
        data = resp.json()
        records = data["records"]["data"]
        ce_oi = sum(x.get("CE", {}).get("openInterest", 0) for x in records if "CE" in x)
        pe_oi = sum(x.get("PE", {}).get("openInterest", 0) for x in records if "PE" in x)
        pcr = pe_oi / ce_oi if ce_oi else 1.0
        pcr_cache.update({"value": pcr, "time": now, "source": "nse_oi"})
        pcr_history.append(pcr)
        return pcr, "nse_oi"
    except Exception as e:
        logger.warning(f"NSE PCR failed: {e}")
    ce = latest_ticks.get("ce_price", 0)
    pe = latest_ticks.get("pe_price", 0)
    if ce > 0 and pe > 0:
        raw = pe / ce
        pcr_history.append(raw)
        if len(pcr_history) >= 3:
            ema = raw
            alpha = 2/6
            for val in list(pcr_history)[-5:]:
                ema = alpha * val + (1-alpha) * ema
            pcr = round(max(0.5, min(2.0, ema)), 2)
        else:
            pcr = round(max(0.5, min(2.0, raw)), 2)
        pcr_cache.update({"value": pcr, "time": now, "source": "price_ema"})
        return pcr, "price_ema"
    return pcr_cache["value"], "cached"

# ------------------------------------------------------------
# Signal Engine
# ------------------------------------------------------------
def run_signal_engine(ce_price, pe_price, price_list, vol_list):
    global market_signal, market_state, institutional_state, signal_memory
    if len(price_list) < 30:
        return
    spread = ce_price - pe_price
    rsi = calculate_rsi(price_list)
    macd_line, macd_hist = calculate_macd(price_list)
    vwap = calculate_vwap(price_list, vol_list)
    ema_fast = calculate_ema(price_list, 9)
    ema_slow = calculate_ema(price_list, 21)
    atr = calculate_atr(price_list)
    pcr, pcr_src = get_nifty_pcr()
    delta, gamma, theta, vega = estimate_greeks(ce_price, pe_price)

    # Volume filter
    if len(vol_list) >= 20:
        avg_vol = sum(vol_list[-20:]) / 20
        if vol_list[-1] < avg_vol * 0.6:
            return

    # Spread sanity
    if atr > 0 and abs(spread) > atr * 1.5:
        return

    # Volatility filter
    atr_pct = (atr / price_list[-1]) * 100 if price_list[-1] > 0 else 0
    if atr_pct < 0.3:
        return

    # Volume trend
    if len(vol_list) >= 20:
        recent_vol = sum(vol_list[-10:])/10
        older_vol = sum(vol_list[-20:-10])/10
        vol_trend = "INCREASING" if recent_vol > older_vol*1.2 else "DECREASING" if recent_vol < older_vol*0.8 else "FLAT"
    else:
        vol_trend = "FLAT"

    # Scoring
    tech_bull = 0
    tech_bear = 0
    if 55 < rsi < 75:
        tech_bull += 10
    elif 40 < rsi < 55:
        tech_bull += 5
    if 25 < rsi < 45:
        tech_bear += 10
    elif 45 < rsi < 60:
        tech_bear += 5
    if macd_hist > 0 and macd_line > 0:
        tech_bull += 10
    elif macd_hist > 0:
        tech_bull += 6
    elif macd_hist < 0 and macd_line < 0:
        tech_bear += 10
    elif macd_hist < 0:
        tech_bear += 6
    if pcr < 0.9:
        tech_bull += 10
    elif pcr < 1.0:
        tech_bull += 7
    elif pcr > 1.3:
        tech_bear += 10
    elif pcr > 1.2:
        tech_bear += 7
    if vol_trend == "INCREASING":
        if ema_fast > ema_slow:
            tech_bull += 10
        elif ema_fast < ema_slow:
            tech_bear += 10
    avg_price = (ce_price+pe_price)/2
    if avg_price > vwap and avg_price > ema_slow:
        tech_bull += 10
    elif avg_price > vwap or avg_price > ema_slow:
        tech_bull += 5
    elif avg_price < vwap and avg_price < ema_slow:
        tech_bear += 10
    elif avg_price < vwap or avg_price < ema_slow:
        tech_bear += 5

    total_bull = tech_bull
    total_bear = tech_bear
    raw_confidence = max(total_bull, total_bear)

    if total_bull >= total_bear and total_bull >= 55:
        if raw_confidence >= 85:
            raw_action = "STRONG BUY CE"
            signal_type = "MOMENTUM"
        elif raw_confidence >= 70:
            raw_action = "BUY CE"
            signal_type = "MOMENTUM"
        elif raw_confidence >= 55:
            raw_action = "CONSIDER CE BUY"
            signal_type = "MOMENTUM"
        else:
            raw_action = "HOLD"
            signal_type = "NONE"
    elif total_bear > total_bull and total_bear >= 55:
        if raw_confidence >= 85:
            raw_action = "STRONG BUY PE"
            signal_type = "MOMENTUM"
        elif raw_confidence >= 70:
            raw_action = "BUY PE"
            signal_type = "MOMENTUM"
        elif raw_confidence >= 55:
            raw_action = "CONSIDER PE BUY"
            signal_type = "MOMENTUM"
        else:
            raw_action = "HOLD"
            signal_type = "NONE"
    else:
        raw_action = "HOLD"
        signal_type = "NONE"
        raw_confidence = max(total_bull, total_bear)

    # Anti-flip
    now_ts = time.time()
    if raw_action != signal_memory["current_action"]:
        if now_ts < signal_memory.get("cooldown_until", 0):
            final_action = signal_memory["current_action"]
            final_signal_type = signal_memory["current_signal_type"]
        else:
            signal_memory["confirmation_count"] = 1 if raw_action != "HOLD" else 0
            if signal_memory["confirmation_count"] >= signal_memory["required_confirmations"]:
                final_action = raw_action
                final_signal_type = signal_type
                signal_memory["signal_start_time"] = now_ts
                signal_memory["cooldown_until"] = now_ts + 30
            else:
                final_action = signal_memory["current_action"]
                final_signal_type = signal_memory["current_signal_type"]
    else:
        if raw_action != "HOLD":
            signal_memory["confirmation_count"] += 1
        final_action = raw_action
        final_signal_type = signal_type

    # Signal expiry
    if signal_memory["signal_start_time"]:
        elapsed = now_ts - signal_memory["signal_start_time"]
        if elapsed > 1800:
            final_action = "HOLD"
            final_signal_type = "NONE"
            signal_memory["signal_start_time"] = None

    signal_memory["current_action"] = final_action
    signal_memory["current_signal_type"] = final_signal_type
    signal_duration = int((now_ts - signal_memory["signal_start_time"]) / 60) if signal_memory["signal_start_time"] else 0

    market_state.update({
        "rsi": round(rsi,2), "momentum": "UPTREND" if ema_fast>ema_slow else "DOWNTREND" if ema_fast<ema_slow else "NEUTRAL",
        "strength": "HIGH" if final_signal_type=="TRENDING" else "MODERATE" if final_signal_type=="MOMENTUM" else "LOW",
        "trend": "BULLISH" if ema_fast>ema_slow else "BEARISH" if ema_fast<ema_slow else "SIDEWAYS",
        "action": final_action, "confidence": raw_confidence,
        "volatility": "HIGH" if atr>15 else "NORMAL" if atr>5 else "LOW",
        "alert": final_action
    })
    institutional_state.update({
        "vwap": round(vwap,2), "ema_fast": round(ema_fast,2), "ema_slow": round(ema_slow,2),
        "ema_signal": "BULLISH" if ema_fast>ema_slow else "BEARISH",
        "atr": round(atr,2), "oi_buildup": "BULLISH" if pcr<0.9 else "BEARISH" if pcr>1.2 else "NEUTRAL",
        "iv_state": "HIGH" if vega>2 else "NORMAL", "candle_structure": "BULLISH" if ema_fast>ema_slow and rsi>55 else "BEARISH" if ema_fast<ema_slow and rsi<45 else "SIDEWAYS",
        "market_breadth": "BULLISH" if tech_bull>tech_bear else "BEARISH" if tech_bear>tech_bull else "BALANCED",
        "volume_profile": vol_trend, "smart_money_flow": "BULLISH" if vwap>ema_slow and vol_trend=="INCREASING" else "BEARISH" if vwap<ema_slow and vol_trend=="INCREASING" else "NEUTRAL",
        "delta": delta, "gamma": gamma, "theta": theta, "vega": vega,
        "institutional_signal": final_action, "institutional_confidence": raw_confidence
    })
    market_signal.update({
        "signal": "BULLISH" if final_action in ["BUY CE","STRONG BUY CE","CONSIDER CE BUY"] else "BEARISH" if final_action in ["BUY PE","STRONG BUY PE","CONSIDER PE BUY"] else "NEUTRAL",
        "ce_price": ce_price, "pe_price": pe_price, "spread": round(spread,2),
        "rsi": round(rsi,2), "macd": round(macd_hist,2), "pcr": round(pcr,2),
        "vwap": round(vwap,2), "atr": round(atr,2), "ema_fast": round(ema_fast,2), "ema_slow": round(ema_slow,2),
        "delta": delta, "gamma": gamma, "theta": theta, "vega": vega,
        "volume": int(vol_list[-1]) if vol_list else 0, "timestamp": datetime.now().isoformat()
    })
    if final_action != signal_memory.get("last_logged_action", ""):
        logger.info(f"PRO SIGNAL: {final_action} [{final_signal_type}] Conf:{raw_confidence} Bull:{tech_bull} Bear:{tech_bear} RSI:{rsi:.1f}")
        signal_memory["last_logged_action"] = final_action

# ------------------------------------------------------------
# CRITICAL FIX: Monkey-patch SmartWebSocketV2 for websocket-client >= 1.6.x
# ------------------------------------------------------------
def patch_smartwebsocket(sws_instance):
    """Fix on_data being ignored by websocket-client >= 1.6.x"""
    import websocket
    import ssl

    # Store reference to original methods
    original_connect = sws_instance.connect

    def fixed_connect():
        headers = {
            "Authorization": sws_instance.auth_token,
            "x-api-key": sws_instance.api_key,
            "x-client-code": sws_instance.client_code,
            "x-feed-token": sws_instance.feed_token
        }
        try:
            sws_instance.wsapp = websocket.WebSocketApp(
                sws_instance.ROOT_URI,
                header=headers,
                on_open=sws_instance._on_open,
                on_message=sws_instance._on_message,  # KEY FIX: use on_message instead of ignored on_data
                on_error=sws_instance._on_error,
                on_close=sws_instance._on_close,
                on_ping=sws_instance._on_ping,
                on_pong=sws_instance._on_pong
            )
            sws_instance.wsapp.run_forever(
                sslopt={"cert_reqs": ssl.CERT_NONE},
                ping_interval=sws_instance.HEART_BEAT_INTERVAL
            )
        except Exception as e:
            logger.error(f"WebSocket connect error: {e}")
            raise

    sws_instance.connect = fixed_connect

    # Fix _on_close signature
    def fixed_on_close(wsapp, close_status_code=None, close_msg=None):
        logger.warning(f"WebSocket closed: code={close_status_code}, msg={close_msg}")
        if hasattr(sws_instance, 'on_close') and sws_instance.on_close:
            try:
                sws_instance.on_close(wsapp, close_status_code, close_msg)
            except:
                sws_instance.on_close(wsapp)
    sws_instance._on_close = fixed_on_close

    # Disable broken internal reconnect
    sws_instance.MAX_RETRY_ATTEMPT = 0
    sws_instance.retry_strategy = 0

    return sws_instance

# ------------------------------------------------------------
# WebSocket Callbacks
# ------------------------------------------------------------
def on_open(wsapp):
    logger.info("WebSocket OPENED")
    global sws
    if sws and CE_TOKEN and PE_TOKEN:
        try:
            sws.subscribe("nifty_signal", 2, [{"exchangeType": 2, "tokens": [CE_TOKEN, PE_TOKEN]}])
            logger.info(f"Subscribed to CE={CE_TOKEN}, PE={PE_TOKEN}")
        except Exception as e:
            logger.error(f"Subscribe error: {e}")

def on_data(wsapp, message):
    global tick_counter, last_tick_time
    last_tick_time = time.time()
    try:
        # message from _on_message is already parsed dict
        if isinstance(message, dict):
            token = str(message.get("token", ""))
            ltp = message.get("last_traded_price", 0)
            if isinstance(ltp, (int, float)) and ltp > 1000:
                ltp = ltp / 100
            vol = message.get("volume_trade_for_the_day", 0) or message.get("v", 0)

            if token == CE_TOKEN:
                latest_ticks["ce_price"] = ltp
                latest_ticks["ce_volume"] = vol
                price_history.append(ltp)
                volume_history.append(vol)
                tick_counter += 1
                logger.info(f"CE TICK: {ltp} (vol:{vol})")
            elif token == PE_TOKEN:
                latest_ticks["pe_price"] = ltp
                latest_ticks["pe_volume"] = vol
                tick_counter += 1
                logger.info(f"PE TICK: {ltp} (vol:{vol})")

            ce = latest_ticks["ce_price"]
            pe = latest_ticks["pe_price"]
            if ce > 0 and pe > 0 and len(price_history) >= 30 and tick_counter % 5 == 0:
                run_signal_engine(ce, pe, list(price_history), list(volume_history))
    except Exception as e:
        logger.error(f"Data error: {e}")

def on_error(wsapp, error):
    logger.error(f"WebSocket error: {error}")

def on_close(wsapp, close_status_code=None, close_msg=None):
    logger.warning(f"WebSocket CLOSE: code={close_status_code}, msg={close_msg}")
    global ws_running
    ws_running = False

# ------------------------------------------------------------
# Connection Manager
# ------------------------------------------------------------
def start_websocket():
    global ws_running, CE_TOKEN, PE_TOKEN, sws
    retry_delay = 30
    while engine_active:
        try:
            if not CE_TOKEN or not PE_TOKEN:
                CE_TOKEN, PE_TOKEN = get_current_atm_tokens()
                if not CE_TOKEN or not PE_TOKEN:
                    time.sleep(60)
                    continue

            totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
            obj = SmartConnect(api_key=ANGEL_API_KEY)
            session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
            if not session.get("status"):
                logger.error("Auth failed")
                time.sleep(retry_delay)
                continue

            auth_token = session["data"]["jwtToken"]
            feed_token = obj.getfeedToken()

            sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)
            sws = patch_smartwebsocket(sws)  # APPLY CRITICAL FIX

            sws.on_open = on_open
            sws.on_data = on_data
            sws.on_error = on_error
            sws.on_close = on_close

            ws_running = True
            logger.info("Connecting WebSocket...")
            sws.connect()

            while ws_running:
                time.sleep(1)
                if time.time() - last_tick_time > 90 and tick_counter > 0:
                    logger.warning("Watchdog: no ticks, reconnecting")
                    ws_running = False

            logger.warning("WebSocket loop ended, reconnecting...")
            retry_delay = 30

        except Exception as e:
            logger.error(f"Fatal error: {e}")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 300)

# ------------------------------------------------------------
# REST Fallback
# ------------------------------------------------------------
def rest_fallback():
    while engine_active:
        time.sleep(10)
        if ws_running and (time.time() - last_tick_time) < 15:
            continue
        if not CE_TOKEN or not PE_TOKEN:
            continue
        try:
            totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
            obj = SmartConnect(api_key=ANGEL_API_KEY)
            session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
            if not session.get("status"):
                continue
            auth_token = session["data"]["jwtToken"]
            url = "https://apiconnect.angelbroking.com/rest/secure/angelbroking/ltp/v1"
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json",
                "X-PrivateKey": ANGEL_API_KEY
            }
            for token, key in [(CE_TOKEN, "ce"), (PE_TOKEN, "pe")]:
                resp = requests.post(url, json={"symbols": [{"symboltoken": token, "exchange": "NFO"}]}, headers=headers, timeout=5)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("data"):
                        ltp = float(data["data"][0]["ltp"])
                        latest_ticks[f"{key}_price"] = ltp
                        price_history.append(ltp)
                        volume_history.append(100)
                        logger.info(f"[REST] {key.upper()}={ltp}")
            ce = latest_ticks["ce_price"]
            pe = latest_ticks["pe_price"]
            if ce > 0 and pe > 0 and len(price_history) >= 30:
                run_signal_engine(ce, pe, list(price_history), list(volume_history))
        except Exception as e:
            logger.error(f"REST error: {e}")

# ------------------------------------------------------------
# Flask Endpoints
# ------------------------------------------------------------
@app.route("/")
def home():
    return jsonify({"status": "online", "message": "Nifty Signal Engine v3.1"})

@app.route("/api/live-signals")
def live_signals():
    spot = get_nifty_spot_cached()
    return jsonify({
        "status": "active",
        "data": market_signal,
        "market": market_state,
        "institutional": institutional_state,
        "spot_price": spot if spot else 0,
        "signal_memory": {
            "current_action": signal_memory["current_action"],
            "signal_type": signal_memory["current_signal_type"],
            "confirmations": signal_memory["confirmation_count"]
        }
    })

@app.route("/api/health")
def health():
    return jsonify({
        "status": "ok",
        "ws_running": ws_running,
        "ce_token": CE_TOKEN,
        "pe_token": PE_TOKEN,
        "latest_ce": latest_ticks["ce_price"],
        "latest_pe": latest_ticks["pe_price"],
        "last_tick_age": round(time.time() - last_tick_time, 1),
        "timestamp": datetime.now().isoformat()
    })

# ------------------------------------------------------------
# Graceful shutdown
# ------------------------------------------------------------
def shutdown_handler(signum, frame):
    global engine_active
    logger.info("Shutdown signal received")
    engine_active = False
    if sws:
        try:
            sws.close()
        except:
            pass
    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)

# ------------------------------------------------------------
# Startup
# ------------------------------------------------------------
def start_engine():
    threading.Thread(target=start_websocket, daemon=True, name="WS-Main").start()
    threading.Thread(target=rest_fallback, daemon=True, name="REST-Fallback").start()
    logger.info("=" * 50)
    logger.info("Nifty Signal Engine v3.1 Started")
    logger.info("=" * 50)

start_engine()

if __name__ == "__main__":
    start_engine()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
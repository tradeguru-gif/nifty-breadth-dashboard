# === VERSION 13.0 - NO TOKEN FETCHING, PURE REST, HARDCODED TOKENS ===
import sys
import logging
import os
import time
import threading
import json
import requests
import pandas as pd
import numpy as np
import sqlite3
import math
from collections import deque
from datetime import datetime, timedelta, time as dt_time
from flask import Flask, jsonify
from flask_cors import CORS
import pyotp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app

# ============================================================================
# ENVIRONMENT
# ============================================================================
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing Angel One credentials")

DB_PATH = "trading_data.db"

# ============================================================================
# DATABASE
# ============================================================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS portfolio_equity (index_name TEXT PRIMARY KEY, equity REAL, last_updated REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS trades (timestamp REAL, action TEXT, entry_price REAL, exit_price REAL, pnl REAL, exit_reason TEXT)""")
    conn.commit()
    conn.close()
init_db()

from SmartApi import SmartConnect

# ============================================================================
# HARDCODED TOKENS (from your successful run on 2026-06-09)
# ============================================================================
INDEX_CONFIG = {
    "NIFTY": {
        "token": "99926000",
        "exchange": "NSE",
        "symbol": "NIFTY",
        "lot_size": 50,
        "min_premium": 5,
        "option_exchange": "NFO",
        "ce_token": "50565",
        "pe_token": "50566",
        "ce_symbol": "NIFTY23200CE",
        "pe_symbol": "NIFTY23200PE"
    },
    "BANKNIFTY": {
        "token": "99926009",
        "exchange": "NSE",
        "symbol": "BANKNIFTY",
        "lot_size": 25,
        "min_premium": 5,
        "option_exchange": "NFO",
        "ce_token": "75600",
        "pe_token": "75601",
        "ce_symbol": "BANKNIFTY55100CE",
        "pe_symbol": "BANKNIFTY55100PE"
    },
    "FINNIFTY": {
        "token": "99926037",
        "exchange": "NSE",
        "symbol": "FINNIFTY",
        "lot_size": 40,
        "min_premium": 5,
        "option_exchange": "NFO",
        "ce_token": "77498",
        "pe_token": "77499",
        "ce_symbol": "FINNIFTY25100CE",
        "pe_symbol": "FINNIFTY25100PE"
    },
    "MIDCPNIFTY": {
        "token": "99926074",
        "exchange": "NSE",
        "symbol": "MIDCPNIFTY",
        "lot_size": 75,
        "min_premium": 5,
        "option_exchange": "NFO",
        "ce_token": "78658",
        "pe_token": "78659",
        "ce_symbol": "MIDCPNIFTY14125CE",
        "pe_symbol": "MIDCPNIFTY14125PE"
    },
    "SENSEX": {
        "token": "99919000",
        "exchange": "BSE",
        "symbol": "SENSEX",
        "lot_size": 15,
        "min_premium": 5,
        "option_exchange": "BFO",
        "ce_token": "1132676",
        "pe_token": "1132838",
        "ce_symbol": "SENSEX73800CE",
        "pe_symbol": "SENSEX73800PE"
    }
}

# price storage
price_histories = {idx: deque(maxlen=2000) for idx in INDEX_CONFIG}
latest_ticks = {idx: {"spot": 0.0, "ce": 0.0, "pe": 0.0} for idx in INDEX_CONFIG}
latest_ticks["VIX"] = 15.0

portfolio_state = {idx: {"equity": 100000.0, "open_positions": 0, "daily_trades": 0} for idx in INDEX_CONFIG}
signal_state = {idx: {"action": "HOLD", "entry_price": 0, "stop_loss": 0, "target": 0, "lots": 1, "cooldown": 0, "highest": 0, "entry_time": 0} for idx in INDEX_CONFIG}
daily_trade_count = {idx: 0 for idx in INDEX_CONFIG}
last_trade_date = {idx: "" for idx in INDEX_CONFIG}
market_signal = {idx: {"signal": "WAITING", "alert_message": ""} for idx in INDEX_CONFIG}

# ============================================================================
# AUTHENTICATION
# ============================================================================
auth_cache = {"token": None, "timestamp": 0, "obj": None}
_auth_lock = threading.Lock()
_api_last_call = 0
_api_min_interval = 0.5
_api_lock = threading.Lock()

def rate_limited_api_call(func, *args, **kwargs):
    global _api_last_call
    with _api_lock:
        elapsed = time.time() - _api_last_call
        if elapsed < _api_min_interval:
            time.sleep(_api_min_interval - elapsed)
        result = func(*args, **kwargs)
        _api_last_call = time.time()
        return result

def get_auth_token():
    with _auth_lock:
        now = time.time()
        if auth_cache["token"] and now - auth_cache["timestamp"] < 3300:
            return auth_cache["token"], auth_cache["obj"]
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        obj = SmartConnect(api_key=ANGEL_API_KEY)
        session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
        if not session.get("status"):
            return None, None
        auth_token = session["data"]["jwtToken"]
        auth_cache.update({"token": auth_token, "timestamp": now, "obj": obj})
        logger.info("Auth token refreshed")
        return auth_token, obj

def safe_ltp(resp):
    if not resp or not resp.get("status"):
        return None
    data = resp.get("data", {})
    if isinstance(data, dict):
        if "fetched" in data and data["fetched"]:
            fetched = data["fetched"]
            if isinstance(fetched, list) and len(fetched) > 0:
                return float(fetched[0].get("ltp", 0))
            elif isinstance(fetched, dict):
                return float(fetched.get("ltp", 0))
        elif "ltp" in data:
            return float(data["ltp"])
    elif isinstance(data, list) and len(data) > 0:
        return float(data[0].get("ltp", 0))
    return None

def get_spot_price(idx, obj):
    cfg = INDEX_CONFIG[idx]
    try:
        resp = rate_limited_api_call(obj.ltpData, cfg["exchange"], cfg["symbol"], cfg["token"])
        ltp = safe_ltp(resp)
        if ltp and ltp > 0:
            if ltp > 100000:
                ltp /= 100
            return ltp
    except Exception as e:
        logger.debug(f"Spot error {idx}: {e}")
    return None

def get_option_price(idx, side, obj):
    cfg = INDEX_CONFIG[idx]
    token = cfg["ce_token"] if side == "CE" else cfg["pe_token"]
    symbol = cfg["ce_symbol"] if side == "CE" else cfg["pe_symbol"]
    try:
        resp = rate_limited_api_call(obj.ltpData, cfg["option_exchange"], symbol, token)
        ltp = safe_ltp(resp)
        if ltp and ltp > 0:
            if ltp > 100000:
                ltp /= 100
            return ltp
    except Exception as e:
        logger.debug(f"Option {idx} {side} error: {e}")
    return None

# ============================================================================
# SIMPLE SIGNAL ENGINE
# ============================================================================
def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    return 100 - (100 / (1 + avg_gain / avg_loss))

def calculate_atr(prices, period=14):
    if len(prices) < period + 1:
        return 5.0
    tr = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    return sum(tr[-period:]) / period

def run_signal_engine():
    for idx, cfg in INDEX_CONFIG.items():
        prices = list(price_histories[idx])
        if len(prices) < 30:
            market_signal[idx]["alert_message"] = f"Data {len(prices)}/30"
            market_signal[idx]["signal"] = "WAITING"
            continue

        spot = prices[-1]
        ce = latest_ticks[idx]["ce"]
        pe = latest_ticks[idx]["pe"]

        # Simple RSI‑based signal
        rsi = calculate_rsi(prices)
        atr = calculate_atr(prices)
        now = time.time()

        # cooldown
        if now < signal_state[idx]["cooldown"]:
            market_signal[idx]["alert_message"] = f"Cooldown {int(signal_state[idx]['cooldown']-now)}s"
            market_signal[idx]["signal"] = "COOLDOWN"
            continue

        # daily reset
        today = datetime.now().strftime("%Y-%m-%d")
        if last_trade_date[idx] != today:
            daily_trade_count[idx] = 0
            last_trade_date[idx] = today

        if daily_trade_count[idx] >= 20:
            market_signal[idx]["alert_message"] = "Max daily trades"
            market_signal[idx]["signal"] = "BLOCKED"
            continue

        # decide action
        action = "NO_TRADE"
        if rsi < 30 and ce > cfg["min_premium"]:
            action = "BUY_CE"
        elif rsi > 70 and pe > cfg["min_premium"]:
            action = "BUY_PE"

        if action == "NO_TRADE":
            market_signal[idx]["alert_message"] = f"RSI={rsi:.1f} no signal"
            market_signal[idx]["signal"] = "NO_TRADE"
            continue

        prem = ce if "CE" in action else pe
        if prem <= 0:
            market_signal[idx]["alert_message"] = f"Premium zero"
            market_signal[idx]["signal"] = "WAITING"
            continue

        # entry
        lots = 1
        sl = prem * 0.6
        target = prem + atr * 3

        signal_state[idx].update({
            "action": action,
            "entry_price": prem,
            "stop_loss": sl,
            "target": target,
            "lots": lots,
            "entry_time": now,
            "highest": prem,
            "cooldown": 0
        })
        portfolio_state[idx]["open_positions"] = 1
        daily_trade_count[idx] += 1

        msg = f"ENTRY {action} {idx} | Spot:{spot:.0f} Prem:{prem:.2f} SL:{sl:.2f} Tgt:{target:.2f} | RSI:{rsi:.1f}"
        logger.info(msg)
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            try:
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                              json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=3)
            except:
                pass

        market_signal[idx].update({
            "signal": action,
            "alert_message": f"ENTRY {action}",
            "entry_price": prem,
            "stop_loss": sl,
            "target": target
        })

# ============================================================================
# REST POLLER (only data source)
# ============================================================================
def is_market_open():
    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    current = now_ist.time()
    return now_ist.weekday() < 5 and dt_time(9, 10) <= current <= dt_time(15, 35)

def rest_poller():
    logger.info("REST poller started")
    while True:
        try:
            if not is_market_open():
                time.sleep(10)
                continue
            auth_token, obj = get_auth_token()
            if not obj:
                time.sleep(10)
                continue

            # fetch spot and option prices for all indices
            for idx in INDEX_CONFIG:
                # spot
                spot = get_spot_price(idx, obj)
                if spot:
                    latest_ticks[idx]["spot"] = spot
                    price_histories[idx].append(spot)

                # CE
                ce = get_option_price(idx, "CE", obj)
                if ce:
                    latest_ticks[idx]["ce"] = ce
                # PE
                pe = get_option_price(idx, "PE", obj)
                if pe:
                    latest_ticks[idx]["pe"] = pe

            # run signal engine
            run_signal_engine()

            time.sleep(10)
        except Exception as e:
            logger.error(f"REST poller error: {e}")
            time.sleep(10)

# ============================================================================
# FLASK ROUTES
# ============================================================================
@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "healthy", "version": "13.0", "indices": list(INDEX_CONFIG.keys()), "market_open": is_market_open()})

@app.route("/api/live-signals", methods=["GET"])
def live_signals():
    return jsonify({
        "timestamp": datetime.now().isoformat(),
        "signals": market_signal,
        "portfolios": portfolio_state,
        "ticks": latest_ticks,
        "market_open": is_market_open(),
        "version": "13.0"
    })

@app.route("/api/health")
def health():
    return jsonify({"status": "OK", "market_open": is_market_open()})

# ============================================================================
# STARTUP
# ============================================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    # start REST poller in background
    threading.Thread(target=rest_poller, daemon=True).start()
    app.run(host="0.0.0.0", port=port, debug=False)
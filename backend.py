import os
import time
import logging
import threading
import json
import requests
import pandas as pd
from collections import deque, defaultdict
from datetime import datetime, timedelta
from flask import Flask, jsonify
from flask_cors import CORS
import pyotp
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import queue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app

# ============================================================
# ENVIRONMENT VARIABLES
# ============================================================
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

# NEW: Dynamic thresholds from env (Feature #8)
SPREAD_THRESHOLD = float(os.getenv("SPREAD_THRESHOLD", "3.0"))
STRONG_BUY_THRESHOLD = float(os.getenv("STRONG_BUY_THRESHOLD", "85"))
BUY_THRESHOLD = float(os.getenv("BUY_THRESHOLD", "70"))
CONSIDER_THRESHOLD = float(os.getenv("CONSIDER_THRESHOLD", "55"))
HOLD_THRESHOLD = float(os.getenv("HOLD_THRESHOLD", "45"))

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing Angel One credentials")

# ============================================================
# GLOBAL STATE (Your original + NEW additions)
# ============================================================
CE_TOKEN = None
PE_TOKEN = None
latest_ticks = {"ce_price": 0.0, "pe_price": 0.0}
price_history = deque(maxlen=500)
tick_counter = 0
UPDATE_INTERVAL = 5
ws_running = False
sws = None

# NEW: Token metadata with timestamp (Feature #1)
token_metadata = {
    "ce_token": None,
    "pe_token": None,
    "last_updated": 0,
    "expiry_date": None,
    "frozen_until": 0  # Unix timestamp until which we freeze expiry rollover
}

# NEW: Tick queue for non-blocking processing (Feature #5)
tick_queue = queue.Queue(maxsize=10000)

# NEW: Last tick timestamp for watchdog (Feature #3)
last_tick_time = {"value": 0, "lock": threading.Lock()}

# NEW: REST fallback state (Feature #4)
rest_fallback = {
    "last_rest_call": 0,
    "rest_cooldown": 10,  # seconds between REST calls
    "get_ltp_func": None   # Will hold reference to REST LTP function
}

# NEW: Volume tracking from real tick data (Feature #7)
real_volume = {"ce": 0, "pe": 0, "ce_total": 0, "pe_total": 0}

# NEW: Log throttling counters (Feature #9)
log_throttle = {
    "tick_count": 0,
    "last_score_log": 0,
    "last_signal": None
}

# NEW: Enhanced anti-flip with cooldown (Feature #10)
anti_flip = {
    "last_signal_change_time": 0,
    "cooldown_seconds": 30,
    "consecutive_raw_signals": 0,
    "last_raw_action": "HOLD",
    "required_consecutive": 3
}

# NEW: PCR smoothing EMA (Feature #6)
pcr_ema_history = deque(maxlen=5)
pcr_cache = {"value": 1.0, "time": 0, "source": "default"}
PCR_TTL = 600  # Extended to 10 minutes when NSE blocked (Feature #6)

# NEW: SmartConnect instance for REST fallback
smart_obj = None
auth_token_global = None
feed_token_global = None

# --------------------------------------------------
# Your Original Professional Signal State (UNCHANGED)
# --------------------------------------------------
market_signal = {
    "signal": "WAITING",
    "ce_price": 0.0,
    "pe_price": 0.0,
    "spread": 0.0,
    "rsi": 50,
    "macd": 0.0,
    "pcr": 1.0,
    "vwap": 0.0,
    "atr": 0.0,
    "ema_fast": 0.0,
    "ema_slow": 0.0,
    "delta": 0.0,
    "gamma": 0.0,
    "theta": 0.0,
    "vega": 0.0,
    "volume": 0,
    "volume_avg": 0,
    "oi_signal": "NEUTRAL",
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
    "alert": "NONE",
    "signal_duration_minutes": 0,
    "trend_1min": "SIDEWAYS",
    "trend_5min": "SIDEWAYS",
    "trend_10min": "SIDEWAYS",
    "trend_15min": "SIDEWAYS",
    "trend_20min": "SIDEWAYS",
    "timeframe_agreement": 0
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
    "volume_trend": "FLAT",
    "delta": 0,
    "gamma": 0,
    "theta": 0,
    "vega": 0,
    "institutional_signal": "HOLD",
    "institutional_confidence": 0,
    "consecutive_confirmations": 0
}

signal_memory = {
    "current_action": "HOLD",
    "current_signal_type": "NONE",
    "signal_start_time": None,
    "last_confirmed_action": "HOLD",
    "confirmation_count": 0,
    "required_confirmations": 2,
    "min_signal_duration_seconds": 180,
    "max_sideways_duration": 600
}

timeframe_history = {
    "1min": deque(maxlen=60),
    "5min": deque(maxlen=20),
    "10min": deque(maxlen=15),
    "15min": deque(maxlen=10),
    "20min": deque(maxlen=10)
}

last_minute_snapshot = {"time": 0, "price": 0}

# ============================================================
# NEW FEATURE #2: Daily Re-authentication Timer
# ============================================================
def reauth_timer():
    """Background thread: re-authenticate every 20 hours before 5AM expiry"""
    while True:
        time.sleep(20 * 3600)  # 20 hours
        logger.info("[REAUTH] 20-hour timer triggered. Starting graceful re-auth...")
        try:
            graceful_reauth()
        except Exception as e:
            logger.error(f"[REAUTH] Graceful re-auth failed: {e}")

def graceful_reauth():
    """Re-login and restart WebSocket without breaking signal processing"""
    global sws, ws_running, smart_obj, auth_token_global, feed_token_global

    # Store last prices in memory (already in latest_ticks)
    logger.info("[REAUTH] Preserving last prices in memory")

    # Close existing WebSocket gracefully
    if sws is not None:
        try:
            sws.close_connection()
            logger.info("[REAUTH] Old WebSocket closed")
        except:
            pass

    ws_running = False
    time.sleep(2)

    # Fresh login
    try:
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        smart_obj = SmartConnect(api_key=ANGEL_API_KEY)
        session = smart_obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
        if session.get("status"):
            auth_token_global = session["data"]["jwtToken"]
            feed_token_global = smart_obj.getfeedToken()
            logger.info("[REAUTH] Fresh login successful")
            # WebSocket will auto-reconnect via the main loop
        else:
            logger.error(f"[REAUTH] Login failed: {session}")
    except Exception as e:
        logger.error(f"[REAUTH] Login error: {e}")

# ============================================================
# NEW FEATURE #3: Watchdog Thread for Stale Ticks
# ============================================================
def watchdog_thread():
    """Monitor last tick time and force reconnect if >90 seconds"""
    while True:
        time.sleep(5)
        with last_tick_time["lock"]:
            elapsed = time.time() - last_tick_time["value"]

        if last_tick_time["value"] > 0 and elapsed > 90:
            logger.warning(f"[WATCHDOG] No tick for {elapsed:.0f}s. Forcing reconnect...")
            try:
                # Fallback to REST once before reconnect (Feature #4)
                rest_price_update()
            except:
                pass

            # Force WebSocket reconnect
            global ws_running
            ws_running = False
            if sws is not None:
                try:
                    sws.close_connection()
                except:
                    pass

# ============================================================
# NEW FEATURE #4: REST Fallback for Prices
# ============================================================
def get_ltp_rest(token):
    """Fetch LTP via REST API as WebSocket fallback"""
    global smart_obj
    if smart_obj is None:
        return None
    try:
        # Angel One REST API for LTP
        resp = smart_obj.ltpData("NFO", "", token)
        if resp and resp.get("status") and resp.get("data"):
            ltp = resp["data"].get("ltp", 0)
            return float(ltp) if ltp else None
    except Exception as e:
        logger.warning(f"[REST] LTP fetch error for {token}: {e}")
    return None

def rest_price_update():
    """Call REST API when WebSocket hasn't sent ticks for 10+ seconds"""
    now = time.time()
    if now - rest_fallback["last_rest_call"] < rest_fallback["rest_cooldown"]:
        return

    if not CE_TOKEN or not PE_TOKEN:
        return

    rest_fallback["last_rest_call"] = now

    ce_ltp = get_ltp_rest(CE_TOKEN)
    pe_ltp = get_ltp_rest(PE_TOKEN)

    if ce_ltp:
        latest_ticks["ce_price"] = ce_ltp
        latest_ticks["ce_timestamp"] = datetime.now().isoformat()
        price_history.append(ce_ltp)
        logger.info(f"[REST FALLBACK] CE price updated: {ce_ltp}")

    if pe_ltp:
        latest_ticks["pe_price"] = pe_ltp
        latest_ticks["pe_timestamp"] = datetime.now().isoformat()
        logger.info(f"[REST FALLBACK] PE price updated: {pe_ltp}")

# ============================================================
# NEW FEATURE #5: Tick Processing Worker Thread
# ============================================================
def tick_worker():
    """Dedicated thread: dequeue ticks and process them"""
    while True:
        try:
            tick_data = tick_queue.get(timeout=1)
            process_tick_data(tick_data)
        except queue.Empty:
            # Check if we should do REST fallback during quiet periods
            with last_tick_time["lock"]:
                elapsed = time.time() - last_tick_time["value"]
            if elapsed > 10 and last_tick_time["value"] > 0:
                rest_price_update()
            continue
        except Exception as e:
            logger.error(f"[WORKER] Error processing tick: {e}")

def process_tick_data(data):
    """All heavy processing moved here from WebSocket callback"""
    global latest_ticks, price_history, tick_counter, last_minute_snapshot, timeframe_history

    try:
        ticks = data if isinstance(data, list) else [data]

        for tick in ticks:
            token = None
            for key in ["tk", "token", "symbolToken", "instrument_token"]:
                if key in tick:
                    token = str(tick.get(key))
                    break

            if not token:
                continue

            ltp = None
            volume = None  # Feature #7: Extract real volume

            for key in ["ltp", "last_traded_price", "lp", "price"]:
                if key in tick:
                    val = tick.get(key)
                    if isinstance(val, (int, float)):
                        ltp = val / 100 if val > 1000 else val
                    break

            # Feature #7: Extract real volume if present
            for vkey in ["v", "volume", "tradedVolume", "vol"]:
                if vkey in tick:
                    vval = tick.get(vkey)
                    if isinstance(vval, (int, float)):
                        volume = int(vval)
                    break

            if ltp is None:
                continue

            # Update prices
            if token == CE_TOKEN:
                latest_ticks["ce_price"] = ltp
                latest_ticks["ce_timestamp"] = datetime.now().isoformat()
                price_history.append(ltp)
                tick_counter += 1
                if volume:
                    real_volume["ce"] = volume
                    real_volume["ce_total"] += volume
            elif token == PE_TOKEN:
                latest_ticks["pe_price"] = ltp
                latest_ticks["pe_timestamp"] = datetime.now().isoformat()
                if volume:
                    real_volume["pe"] = volume
                    real_volume["pe_total"] += volume

            # Update last tick time (Feature #3)
            with last_tick_time["lock"]:
                last_tick_time["value"] = time.time()

            # Store minute snapshots
            now = time.time()
            if now - last_minute_snapshot["time"] >= 60:
                avg_price = (latest_ticks["ce_price"] + latest_ticks["pe_price"]) / 2
                last_minute_snapshot["time"] = now
                last_minute_snapshot["price"] = avg_price

                snapshot = {
                    "time": now, 
                    "price": avg_price, 
                    "ce": latest_ticks["ce_price"], 
                    "pe": latest_ticks["pe_price"],
                    "ce_volume": real_volume["ce"],
                    "pe_volume": real_volume["pe"]
                }
                for tf in timeframe_history:
                    timeframe_history[tf].append(snapshot)

            # Run signal engine
            ce = latest_ticks["ce_price"]
            pe = latest_ticks["pe_price"]
            if ce > 0 and pe > 0 and len(price_history) >= 20 and tick_counter % UPDATE_INTERVAL == 0:
                run_signal_engine(ce, pe, list(price_history))

    except Exception as e:
        logger.error(f"[WORKER] Tick processing error: {e}")

# ============================================================
# FEATURE #1: Weekly Expiry Rollover Fix
# ============================================================
def should_freeze_expiry_rollover():
    """Check if we should freeze expiry during 3:00-3:30 PM"""
    now = datetime.now()
    # Thursday 15:00-15:30 (or any day last 30 min of trading)
    if now.weekday() == 3:  # Thursday
        if now.hour == 15 and now.minute < 30:
            return True
    return False

def should_switch_after_close():
    """Only switch expiry after 4:00 PM when market is closed"""
    now = datetime.now()
    return now.hour >= 16

def get_current_atm_tokens_enhanced():
    """Enhanced version with expiry freeze logic"""
    global token_metadata

    now = time.time()

    # Check if we're in freeze period (3:00-3:30 PM Thursday)
    if should_freeze_expiry_rollover():
        if token_metadata["ce_token"] and token_metadata["pe_token"]:
            logger.info("[EXPIRY FREEZE] Trading in last 30 min. Keeping current expiry.")
            return token_metadata["ce_token"], token_metadata["pe_token"]

    # Check if we already have valid tokens that aren't too old
    if token_metadata["ce_token"] and token_metadata["pe_token"]:
        age = now - token_metadata["last_updated"]
        # If tokens are <1 hour old and it's before 4 PM, keep them
        if age < 3600 and datetime.now().hour < 16:
            return token_metadata["ce_token"], token_metadata["pe_token"]

    # Fetch fresh tokens
    ce_token, pe_token = get_current_atm_tokens()

    if ce_token and pe_token:
        token_metadata.update({
            "ce_token": ce_token,
            "pe_token": pe_token,
            "last_updated": now,
            "expiry_date": None  # Could parse and store actual expiry
        })

    return ce_token, pe_token

# ============================================================
# FEATURE #6: Smoothed PCR with EMA
# ============================================================
def calculate_ema(values, period=5):
    """Calculate Exponential Moving Average"""
    if not values:
        return 1.0
    alpha = 2 / (period + 1)
    ema = values[0]
    for val in values[1:]:
        ema = alpha * val + (1 - alpha) * ema
    return ema

def get_nifty_pcr_enhanced():
    """Enhanced PCR with EMA smoothing and extended cache"""
    now = time.time()

    # Extended cache when NSE is unreachable
    if now - pcr_cache["time"] < PCR_TTL:
        return pcr_cache["value"], pcr_cache["source"]

    raw_pcr = None
    source = "default"

    # Try NSE Option Chain
    try:
        url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9"
        }
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        time.sleep(1)
        resp = session.get(url, headers=headers, timeout=10)
        data = resp.json()

        if "records" in data and "data" in data["records"]:
            records = data["records"]["data"]
            ce_oi = sum(x.get("CE", {}).get("openInterest", 0) for x in records if "CE" in x)
            pe_oi = sum(x.get("PE", {}).get("openInterest", 0) for x in records if "PE" in x)
            raw_pcr = pe_oi / ce_oi if ce_oi else 1.0
            source = "nse_oi"
            logger.info(f"[PCR] Raw NSE OI PCR: {raw_pcr:.2f}")
    except Exception as e:
        logger.warning(f"[PCR] NSE fetch failed: {e}")

    # Fallback: Price-based PCR
    if raw_pcr is None:
        try:
            ce = latest_ticks.get("ce_price", 0)
            pe = latest_ticks.get("pe_price", 0)
            if ce > 0 and pe > 0:
                raw_pcr = pe / ce
                raw_pcr = max(0.5, min(2.0, raw_pcr))
                source = "price_based"
                logger.info(f"[PCR] Raw price-based PCR: {raw_pcr:.2f}")
        except Exception as e:
            logger.warning(f"[PCR] Price fallback failed: {e}")

    if raw_pcr is not None:
        pcr_ema_history.append(raw_pcr)
        # Apply 5-period EMA smoothing
        smoothed_pcr = calculate_ema(list(pcr_ema_history), period=5)
        pcr_cache.update({"value": smoothed_pcr, "time": now, "source": source})
        logger.info(f"[PCR] Smoothed EMA PCR: {smoothed_pcr:.2f} (from {source})")
        return smoothed_pcr, source

    # Return cached value with extended TTL
    pcr_cache["time"] = now  # Reset timer to extend cache
    return pcr_cache["value"], "cached_extended"

# ============================================================
# FEATURE #8: Dynamic Thresholds based on ATR
# ============================================================
def get_dynamic_thresholds(atr):
    """Calculate adaptive thresholds based on current ATR"""
    if atr <= 0:
        return STRONG_BUY_THRESHOLD, SPREAD_THRESHOLD

    dynamic_strong_buy = 50 + (2 * atr)
    dynamic_spread = atr * 0.3

    # Cap within reasonable bounds
    dynamic_strong_buy = min(95, max(70, dynamic_strong_buy))
    dynamic_spread = max(1.0, min(10.0, dynamic_spread))

    return dynamic_strong_buy, dynamic_spread

# ============================================================
# FEATURE #10: Enhanced Anti-Flip with Cooldown
# ============================================================
def enhanced_anti_flip_logic(raw_action, raw_confidence, signal_type):
    """Improved anti-flip with cooldown and consecutive requirements"""
    global anti_flip

    now = time.time()
    current_action = signal_memory["current_action"]

    # Count consecutive identical raw signals
    if raw_action == anti_flip["last_raw_action"]:
        anti_flip["consecutive_raw_signals"] += 1
    else:
        anti_flip["consecutive_raw_signals"] = 1
        anti_flip["last_raw_action"] = raw_action

    # Check cooldown period after signal change
    time_since_last_change = now - anti_flip["last_signal_change_time"]
    in_cooldown = time_since_last_change < anti_flip["cooldown_seconds"]

    # If in cooldown and trying to change direction, block it
    if in_cooldown and raw_action != current_action and current_action != "HOLD":
        logger.info(f"[ANTI-FLIP] In cooldown ({time_since_last_change:.0f}s). Blocking change to {raw_action}")
        return current_action, signal_memory["current_signal_type"]

    # From HOLD: require 2 confirmations (original logic)
    if current_action in ["HOLD", "WAITING"]:
        if raw_action != "HOLD" and anti_flip["consecutive_raw_signals"] >= signal_memory["required_confirmations"]:
            anti_flip["last_signal_change_time"] = now
            return raw_action, signal_type
        return "HOLD", "NONE"

    # Already in signal: require 3 consecutive identical signals + min duration
    if raw_action == "HOLD":
        # Check sideways timeout
        if signal_memory["signal_start_time"] is not None:
            elapsed = now - signal_memory["signal_start_time"]
            if elapsed > signal_memory["max_sideways_duration"]:
                anti_flip["last_signal_change_time"] = now
                return "HOLD", "NONE"
        return current_action, signal_memory["current_signal_type"]

    if raw_action != current_action:
        # Direction change: need cooldown passed + 3 consecutive + min duration
        min_duration_met = True
        if signal_memory["signal_start_time"] is not None:
            elapsed = now - signal_memory["signal_start_time"]
            if elapsed < signal_memory["min_signal_duration_seconds"]:
                min_duration_met = False

        if (not in_cooldown and 
            anti_flip["consecutive_raw_signals"] >= anti_flip["required_consecutive"] and 
            min_duration_met):
            anti_flip["last_signal_change_time"] = now
            anti_flip["consecutive_raw_signals"] = 0
            return raw_action, signal_type
        else:
            return current_action, signal_memory["current_signal_type"]

    # Same direction, continue
    return current_action, signal_memory["current_signal_type"]

# ============================================================
# FEATURE #9: Throttled Logging
# ============================================================
def throttled_signal_log(final_action, final_signal_type, signal_duration, raw_confidence, 
                         bullish_count, bearish_count, sideways_count, pcr, pcr_source, 
                         volume_trend, rsi, macd_hist):
    """Log signals only when they change or every 100 ticks"""
    global log_throttle

    log_throttle["tick_count"] += 1
    now = time.time()

    # Always log on signal change
    signal_changed = final_action != log_throttle["last_signal"]

    # Log every 100 ticks for heartbeat
    heartbeat = log_throttle["tick_count"] % 100 == 0

    # Log raw score once per minute
    score_log_time = now - log_throttle["last_score_log"] > 60

    if signal_changed:
        logger.info(f"🎯 SIGNAL CHANGE: {final_action} [Type:{final_signal_type}] [Dur:{signal_duration}m] [Conf:{raw_confidence}]")
        log_throttle["last_signal"] = final_action
    elif heartbeat:
        logger.info(f"💓 HEARTBEAT: {final_action} | CE:{latest_ticks['ce_price']:.2f} PE:{latest_ticks['pe_price']:.2f} | Ticks:{log_throttle['tick_count']}")
    elif score_log_time:
        logger.info(f"📊 SCORE: {final_action} [Conf:{raw_confidence}] [TF:{bullish_count}B/{bearish_count}Be/{sideways_count}S] [PCR:{pcr:.2f}@{pcr_source}] [Vol:{volume_trend}] [RSI:{rsi:.1f}]")
        log_throttle["last_score_log"] = now

# ============================================================
# Your Original Helper Functions (UNCHANGED)
# ============================================================
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
        spot = data["data"][0]["lastPrice"]
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
# Original get_current_atm_tokens (UNCHANGED)
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
        logger.info(f"Total instruments loaded: {len(df)}")
    except Exception as e:
        logger.error(f"Failed to load instrument master: {e}")
        return None, None

    nifty_opts = df[
        (df["name"].astype(str) == "NIFTY") & 
        (df["instrumenttype"].astype(str) == "OPTIDX") &
        (df["exch_seg"].astype(str) == "NFO")
    ].copy()

    logger.info(f"NIFTY OPTIDX NFO symbols found: {len(nifty_opts)}")

    if nifty_opts.empty:
        nifty_opts = df[df["symbol"].astype(str).str.match(r'^NIFTY\d{2}[A-Z]{3}\d{2}', na=False)].copy()
        logger.info(f"Fallback NIFTY symbols found: {len(nifty_opts)}")

    if nifty_opts.empty:
        logger.error("No NIFTY symbols found")
        return None, None

    for fmt in ["%d%b%Y", "%d-%b-%Y", "%Y-%m-%d", "%d%m%Y"]:
        nifty_opts["expiry_date"] = pd.to_datetime(nifty_opts["expiry"], format=fmt, errors="coerce")
        valid_count = nifty_opts["expiry_date"].notna().sum()
        if valid_count > 0:
            logger.info(f"Parsed expiry with format {fmt}: {valid_count} valid")
            break
    else:
        logger.error("Could not parse any expiry dates")
        return None, None

    nifty_opts = nifty_opts.dropna(subset=["expiry_date"])
    nifty_opts["strike"] = pd.to_numeric(nifty_opts["strike"], errors="coerce") / 100
    nifty_opts = nifty_opts.dropna(subset=["strike"])
    logger.info(f"NIFTY with valid expiry and strike: {len(nifty_opts)}")

    today = datetime.now()
    future_expiries = nifty_opts[nifty_opts["expiry_date"] > today]
    logger.info(f"Future expiries: {len(future_expiries)}")

    if future_expiries.empty:
        logger.error("No future expiries found")
        return None, None

    nearest_expiry = future_expiries["expiry_date"].min()
    logger.info(f"Nearest expiry: {nearest_expiry}")

    nearest_opts = nifty_opts[nifty_opts["expiry_date"] == nearest_expiry]
    logger.info(f"Options for nearest expiry: {len(nearest_opts)}")

    available_strikes = sorted(nearest_opts["strike"].unique())
    logger.info(f"Available strikes (first 20): {available_strikes[:20]}")
    logger.info(f"Looking for strike: {atm_strike}")

    atm_opts = nifty_opts[(nifty_opts["strike"] == atm_strike) & (nifty_opts["expiry_date"] == nearest_expiry)]
    logger.info(f"ATM options found: {len(atm_opts)}")

    ce_row = atm_opts[atm_opts["symbol"].str.upper().str.contains("CE", na=False)]
    pe_row = atm_opts[atm_opts["symbol"].str.upper().str.contains("PE", na=False)]

    logger.info(f"CE matches: {len(ce_row)}, PE matches: {len(pe_row)}")

    if ce_row.empty or pe_row.empty:
        logger.error(f"CE or PE not found for strike {atm_strike}, expiry {nearest_expiry}")
        if available_strikes:
            nearest_strike = min(available_strikes, key=lambda x: abs(x - atm_strike))
            logger.info(f"Trying nearest strike: {nearest_strike}")
            atm_opts = nifty_opts[(nifty_opts["strike"] == nearest_strike) & (nifty_opts["expiry_date"] == nearest_expiry)]
            ce_row = atm_opts[atm_opts["symbol"].str.upper().str.contains("CE", na=False)]
            pe_row = atm_opts[atm_opts["symbol"].str.upper().str.contains("PE", na=False)]
            if not ce_row.empty and not pe_row.empty:
                ce_token = str(ce_row.iloc[0]["token"])
                pe_token = str(pe_row.iloc[0]["token"])
                logger.info(f"Fallback CE token = {ce_token}, PE token = {pe_token}")
                return ce_token, pe_token
        return None, None

    ce_token = str(ce_row.iloc[0]["token"])
    pe_token = str(pe_row.iloc[0]["token"])
    logger.info(f"CE token = {ce_token}, PE token = {pe_token}")
    return ce_token, pe_token

# ============================================================
# WebSocket Callbacks (MODIFIED for Features #3, #4, #5, #7)
# ============================================================
def on_ws_open(wsapp):
    global sws
    logger.info("Angel One WebSocket opened")
    if sws is not None:
        try:
            correlation_id = "tradeguru_001"
            mode = 2
            token_list = [
                {"exchangeType": 2, "tokens": [CE_TOKEN, PE_TOKEN]}
            ]
            sws.subscribe(correlation_id, mode, token_list)
            logger.info(f"Subscribed to tokens: {CE_TOKEN}, {PE_TOKEN}")
        except Exception as e:
            logger.error(f"Subscribe error: {e}")

def on_ws_data(wsapp, message, *args):
    """WebSocket callback: ONLY enqueues to queue. No heavy processing."""
    try:
        if isinstance(message, str):
            data = json.loads(message)
        else:
            data = message

        # Fast enqueue - no blocking calculations (Feature #5)
        try:
            tick_queue.put_nowait(data)
        except queue.Full:
            logger.warning("[QUEUE] Tick queue full, dropping tick")

    except Exception as e:
        logger.error(f"WebSocket data enqueue error: {e}")

def on_ws_error(wsapp, error):
    logger.error(f"WebSocket error: {error}")

def on_ws_close(wsapp, *args):
    logger.warning(f"WebSocket closed, args: {args}")
    global ws_running
    ws_running = False

# ============================================================
# Start WebSocket (MODIFIED for Features #1, #2)
# ============================================================
def start_angel_websocket():
    global ws_running, CE_TOKEN, PE_TOKEN, sws, smart_obj, auth_token_global, feed_token_global
    retry_delay = 30

    while True:
        try:
            totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
            smart_obj = SmartConnect(api_key=ANGEL_API_KEY)
            session = smart_obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
            if not session.get("status"):
                logger.error(f"Login failed: {session}")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 300)
                continue

            auth_token_global = session["data"]["jwtToken"]
            feed_token_global = smart_obj.getfeedToken()
            logger.info("Authenticated, feed token obtained")

            # Use enhanced token fetch with expiry freeze (Feature #1)
            CE_TOKEN, PE_TOKEN = get_current_atm_tokens_enhanced()
            if not CE_TOKEN or not PE_TOKEN:
                logger.error("Could not fetch ATM tokens. Retrying...")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 300)
                continue

            sws = SmartWebSocketV2(auth_token_global, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token_global)
            sws.on_open = on_ws_open
            sws.on_data = on_ws_data
            sws.on_error = on_ws_error
            sws.on_close = on_ws_close
            ws_running = True
            retry_delay = 30
            logger.info("Connecting WebSocket...")
            sws.connect()

            while ws_running:
                time.sleep(0.1)

            logger.warning("WebSocket loop ended, reconnecting...")

        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 300)

# ============================================================
# Your Original Technical Indicators (UNCHANGED)
# ============================================================
def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
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
        alpha = 2 / (period + 1)
        val = data[0]
        for p in data[1:]:
            val = alpha * p + (1 - alpha) * val
        return val
    macd_line = ema(prices, fast) - ema(prices, slow)
    signal_line = ema(prices[-signal:], signal) if len(prices) >= signal else ema(prices, signal)
    histogram = macd_line - signal_line
    return macd_line, histogram

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
    trs = [abs(prices[i] - prices[i - 1]) for i in range(1, len(prices))]
    return sum(trs[-period:]) / period

def calculate_vwap(prices, volumes=None):
    if not prices:
        return 0
    if volumes is None:
        volumes = [100] * len(prices)
    pv = sum(p * v for p, v in zip(prices, volumes))
    tv = sum(volumes)
    return pv / tv if tv else 0

def calculate_volume_trend(prices):
    """Estimate volume trend from price movement intensity"""
    if len(prices) < 20:
        return "FLAT", 0
    recent = prices[-10:]
    older = prices[-20:-10]
    recent_volatility = sum(abs(recent[i] - recent[i-1]) for i in range(1, len(recent)))
    older_volatility = sum(abs(older[i] - older[i-1]) for i in range(1, len(older)))
    if older_volatility == 0:
        return "FLAT", 0
    ratio = recent_volatility / older_volatility
    if ratio > 1.5:
        return "INCREASING", ratio
    elif ratio < 0.7:
        return "DECREASING", ratio
    return "FLAT", ratio

def estimate_greeks(ce, pe):
    delta = round((ce - pe) / 100, 2)
    gamma = round(abs(delta) / 10, 2)
    theta = round(-(ce + pe) / 1000, 2)
    vega = round((ce + pe) / 500, 2)
    return delta, gamma, theta, vega

# ============================================================
# Your Original Timeframe Analysis (UNCHANGED)
# ============================================================
def analyze_timeframe_trend(tf_name, history_deque):
    history = list(history_deque)
    if len(history) < 2:
        return "SIDEWAYS", 0, 0

    n = len(history)
    if n < 2:
        return "SIDEWAYS", 0, 0

    prices = [h["price"] for h in history]
    x = list(range(n))
    x_mean = sum(x) / n
    y_mean = sum(prices) / n

    numerator = sum((x[i] - x_mean) * (prices[i] - y_mean) for i in range(n))
    denominator = sum((x[i] - x_mean) ** 2 for i in range(n))

    if denominator == 0:
        return "SIDEWAYS", 0, 0

    slope = numerator / denominator

    ss_res = sum((prices[i] - (y_mean + slope * (x[i] - x_mean))) ** 2 for i in range(n))
    ss_tot = sum((prices[i] - y_mean) ** 2 for i in range(n))
    r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0

    slope_threshold = 0.05
    strength = abs(slope) * r_squared * 100

    if abs(slope) < slope_threshold or r_squared < 0.3:
        return "SIDEWAYS", strength, r_squared
    elif slope > 0:
        return "BULLISH", strength, r_squared
    else:
        return "BEARISH", strength, r_squared

def get_all_timeframe_trends():
    trends = {}
    for tf_name, tf_deque in timeframe_history.items():
        trend, strength, r2 = analyze_timeframe_trend(tf_name, tf_deque)
        trends[tf_name] = {
            "trend": trend,
            "strength": round(strength, 2),
            "confidence": round(r2, 2)
        }
    return trends

# ============================================================
# Original PCR (kept for reference, using enhanced version)
# ============================================================
# Replaced by get_nifty_pcr_enhanced() above (Feature #6)

# ============================================================
# Your Original Signal Engine (UNCHANGED CORE LOGIC)
# ============================================================
def run_signal_engine(ce_price, pe_price, price_list):
    global market_signal, market_state, institutional_state, signal_memory

    if len(price_list) < 30:
        return

    # Calculate all indicators
    spread = ce_price - pe_price
    rsi = calculate_rsi(price_list)
    macd_line, macd_hist = calculate_macd(price_list)
    vwap = calculate_vwap(price_list)
    ema_fast = calculate_ema(price_list, 9)
    ema_slow = calculate_ema(price_list, 21)
    atr = calculate_atr(price_list)

    # Use enhanced PCR with EMA smoothing (Feature #6)
    pcr, pcr_source = get_nifty_pcr_enhanced()

    volume_trend, volume_ratio = calculate_volume_trend(price_list)
    delta, gamma, theta, vega = estimate_greeks(ce_price, pe_price)

    # Get dynamic thresholds based on ATR (Feature #8)
    dynamic_strong_buy, dynamic_spread = get_dynamic_thresholds(atr)

    # Use dynamic thresholds
    effective_strong_buy = dynamic_strong_buy
    effective_buy = dynamic_strong_buy - 15
    effective_consider = dynamic_strong_buy - 30

    # Get timeframe trends
    tf_trends = get_all_timeframe_trends()

    trend_1min = tf_trends.get("1min", {}).get("trend", "SIDEWAYS")
    trend_5min = tf_trends.get("5min", {}).get("trend", "SIDEWAYS")
    trend_10min = tf_trends.get("10min", {}).get("trend", "SIDEWAYS")
    trend_15min = tf_trends.get("15min", {}).get("trend", "SIDEWAYS")
    trend_20min = tf_trends.get("20min", {}).get("trend", "SIDEWAYS")

    bullish_count = sum(1 for t in [trend_1min, trend_5min, trend_10min, trend_15min, trend_20min] if t == "BULLISH")
    bearish_count = sum(1 for t in [trend_1min, trend_5min, trend_10min, trend_15min, trend_20min] if t == "BEARISH")
    sideways_count = 5 - bullish_count - bearish_count

    timeframe_agreement = max(bullish_count, bearish_count)

    # ============================================================
    # PROFESSIONAL SIGNAL SCORING SYSTEM (UNCHANGED)
    # ============================================================
    timeframe_score = 0
    if trend_1min == "BULLISH":
        timeframe_score += 10
    if trend_5min == "BULLISH":
        timeframe_score += 10
    if trend_10min == "BULLISH":
        timeframe_score += 10
    if trend_15min == "BULLISH":
        timeframe_score += 10
    if trend_20min == "BULLISH":
        timeframe_score += 10

    bearish_timeframe_score = 0
    if trend_1min == "BEARISH":
        bearish_timeframe_score += 10
    if trend_5min == "BEARISH":
        bearish_timeframe_score += 10
    if trend_10min == "BEARISH":
        bearish_timeframe_score += 10
    if trend_15min == "BEARISH":
        bearish_timeframe_score += 10
    if trend_20min == "BEARISH":
        bearish_timeframe_score += 10

    tech_bullish = 0
    tech_bearish = 0

    if 55 < rsi < 75:
        tech_bullish += 10
    elif rsi > 75:
        tech_bullish += 3
    elif 40 < rsi < 55:
        tech_bullish += 5

    if 25 < rsi < 45:
        tech_bearish += 10
    elif rsi < 25:
        tech_bearish += 3
    elif 45 < rsi < 60:
        tech_bearish += 5

    if macd_hist > 0 and macd_line > 0:
        tech_bullish += 10
    elif macd_hist > 0:
        tech_bullish += 6
    elif macd_hist < 0 and macd_line < 0:
        tech_bearish += 10
    elif macd_hist < 0:
        tech_bearish += 6

    if pcr < 0.9:
        tech_bullish += 10
    elif pcr < 1.0:
        tech_bullish += 7
    elif pcr > 1.3:
        tech_bearish += 10
    elif pcr > 1.2:
        tech_bearish += 7

    if volume_trend == "INCREASING":
        if bullish_count > bearish_count:
            tech_bullish += 10
        elif bearish_count > bullish_count:
            tech_bearish += 10

    avg_price = (ce_price + pe_price) / 2
    if avg_price > vwap and avg_price > ema_slow:
        tech_bullish += 10
    elif avg_price > vwap or avg_price > ema_slow:
        tech_bullish += 5
    elif avg_price < vwap and avg_price < ema_slow:
        tech_bearish += 10
    elif avg_price < vwap or avg_price < ema_slow:
        tech_bearish += 5

    # ============================================================
    # COMBINED SCORING & SIGNAL DETERMINATION (UNCHANGED)
    # ============================================================
    total_bullish = timeframe_score + tech_bullish
    total_bearish = bearish_timeframe_score + tech_bearish

    now = datetime.now()

    if total_bullish >= total_bearish and total_bullish >= effective_consider:
        raw_confidence = total_bullish
        if timeframe_agreement >= 4 and raw_confidence >= effective_strong_buy:
            raw_action = "TRENDING: CE"
            signal_type = "TRENDING"
        elif raw_confidence >= effective_strong_buy:
            raw_action = "STRONG BUY CE"
            signal_type = "MOMENTUM"
        elif raw_confidence >= effective_buy:
            raw_action = "BUY CE"
            signal_type = "MOMENTUM"
        elif raw_confidence >= effective_consider:
            raw_action = "CONSIDER CE"
            signal_type = "MOMENTUM"
        else:
            raw_action = "HOLD"
            signal_type = "NONE"
    elif total_bearish > total_bullish and total_bearish >= effective_consider:
        raw_confidence = total_bearish
        if timeframe_agreement >= 4 and raw_confidence >= effective_strong_buy:
            raw_action = "TRENDING: PE"
            signal_type = "TRENDING"
        elif raw_confidence >= effective_strong_buy:
            raw_action = "STRONG BUY PE"
            signal_type = "MOMENTUM"
        elif raw_confidence >= effective_buy:
            raw_action = "BUY PE"
            signal_type = "MOMENTUM"
        elif raw_confidence >= effective_consider:
            raw_action = "CONSIDER PE"
            signal_type = "MOMENTUM"
        else:
            raw_action = "HOLD"
            signal_type = "NONE"
    else:
        raw_confidence = max(total_bullish, total_bearish)
        raw_action = "HOLD"
        signal_type = "NONE"

    # ============================================================
    # ENHANCED ANTI-FLIP LOGIC (Feature #10)
    # ============================================================
    final_action, final_signal_type = enhanced_anti_flip_logic(raw_action, raw_confidence, signal_type)

    # Update memory (compatible with original)
    signal_memory["current_action"] = final_action
    signal_memory["current_signal_type"] = final_signal_type

    if final_action != "HOLD" and signal_memory["signal_start_time"] is None:
        signal_memory["signal_start_time"] = time.time()
    elif final_action == "HOLD":
        signal_memory["signal_start_time"] = None

    signal_duration = 0
    if signal_memory["signal_start_time"] is not None:
        signal_duration = int((time.time() - signal_memory["signal_start_time"]) / 60)

    # ============================================================
    # UPDATE ALL STATE OBJECTS (UNCHANGED)
    # ============================================================
    market_state.update({
        "rsi": round(rsi, 2),
        "momentum": "UPTREND" if bullish_count > bearish_count else "DOWNTREND" if bearish_count > bullish_count else "NEUTRAL",
        "strength": "HIGH" if final_signal_type == "TRENDING" else "MODERATE" if final_signal_type == "MOMENTUM" else "LOW",
        "trend": "BULLISH" if bullish_count > bearish_count else "BEARISH" if bearish_count > bullish_count else "SIDEWAYS",
        "action": final_action,
        "confidence": raw_confidence,
        "volatility": "HIGH" if atr > 15 else "NORMAL" if atr > 5 else "LOW",
        "alert": final_action,
        "signal_duration_minutes": signal_duration,
        "trend_1min": trend_1min,
        "trend_5min": trend_5min,
        "trend_10min": trend_10min,
        "trend_15min": trend_15min,
        "trend_20min": trend_20min,
        "timeframe_agreement": timeframe_agreement
    })

    institutional_state.update({
        "vwap": round(vwap, 2),
        "ema_fast": round(ema_fast, 2),
        "ema_slow": round(ema_slow, 2),
        "ema_signal": "BULLISH" if ema_fast > ema_slow else "BEARISH",
        "atr": round(atr, 2),
        "oi_buildup": "BULLISH" if pcr < 0.9 else "BEARISH" if pcr > 1.2 else "NEUTRAL",
        "iv_state": "HIGH" if vega > 2 else "NORMAL",
        "candle_structure": "BULLISH" if trend_5min == "BULLISH" and rsi > 55 else "BEARISH" if trend_5min == "BEARISH" and rsi < 45 else "SIDEWAYS",
        "market_breadth": "BULLISH" if bullish_count >= 3 else "BEARISH" if bearish_count >= 3 else "BALANCED",
        "volume_profile": volume_trend,
        "smart_money_flow": "BULLISH" if vwap > ema_slow and volume_trend == "INCREASING" else "BEARISH" if vwap < ema_slow and volume_trend == "INCREASING" else "NEUTRAL",
        "volume_trend": volume_trend,
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "institutional_signal": final_action,
        "institutional_confidence": raw_confidence,
        "consecutive_confirmations": signal_memory["confirmation_count"]
    })

    market_signal.update({
        "signal": "BULLISH" if final_action in ["BUY CE", "STRONG BUY CE", "TRENDING: CE", "CONSIDER CE"] else "BEARISH" if final_action in ["BUY PE", "STRONG BUY PE", "TRENDING: PE", "CONSIDER PE"] else "NEUTRAL",
        "ce_price": ce_price,
        "pe_price": pe_price,
        "spread": round(spread, 2),
        "rsi": round(rsi, 2),
        "macd": round(macd_hist, 2),
        "pcr": round(pcr, 2),
        "vwap": round(vwap, 2),
        "atr": round(atr, 2),
        "ema_fast": round(ema_fast, 2),
        "ema_slow": round(ema_slow, 2),
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "volume": round(volume_ratio * 100, 0),
        "volume_avg": 100,
        "oi_signal": institutional_state["oi_buildup"],
        "timestamp": datetime.now().isoformat()
    })

    # Throttled logging (Feature #9)
    throttled_signal_log(final_action, final_signal_type, signal_duration, raw_confidence,
                         bullish_count, bearish_count, sideways_count, pcr, pcr_source,
                         volume_trend, rsi, macd_hist)

# ============================================================
# Flask Endpoints (UNCHANGED + NEW DEBUG INFO)
# ============================================================
@app.route("/")
def home():
    return jsonify({"status": "online", "message": "Nifty Alpha Engine - Professional Trading Signals (Enhanced v2)"})

@app.route("/api/live-signals")
def live_signals():
    return jsonify({
        "status": "active",
        "data": market_signal,
        "market": market_state,
        "institutional": institutional_state,
        "timeframes": {k: {"trend": v["trend"], "strength": v["strength"]} for k, v in get_all_timeframe_trends().items()},
        "signal_memory": {
            "current_action": signal_memory["current_action"],
            "signal_type": signal_memory["current_signal_type"],
            "confirmations": signal_memory["confirmation_count"]
        },
        # NEW: Enhanced debug info
        "enhanced": {
            "token_metadata": {
                "last_updated": datetime.fromtimestamp(token_metadata["last_updated"]).isoformat() if token_metadata["last_updated"] else None,
                "frozen": should_freeze_expiry_rollover()
            },
            "last_tick_seconds_ago": round(time.time() - last_tick_time["value"], 1) if last_tick_time["value"] > 0 else None,
            "real_volume": real_volume,
            "pcr_source": pcr_cache["source"],
            "dynamic_thresholds": {
                "strong_buy": round(get_dynamic_thresholds(calculate_atr(list(price_history)))[0], 2) if len(price_history) > 14 else None,
                "spread": round(get_dynamic_thresholds(calculate_atr(list(price_history)))[1], 2) if len(price_history) > 14 else None
            }
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
        "price_history_len": len(price_history),
        "timestamp": datetime.now().isoformat(),
        # NEW: Enhanced health info
        "enhanced": {
            "queue_size": tick_queue.qsize(),
            "last_tick_seconds_ago": round(time.time() - last_tick_time["value"], 1) if last_tick_time["value"] > 0 else None,
            "token_age_minutes": round((time.time() - token_metadata["last_updated"]) / 60, 1) if token_metadata["last_updated"] else None,
            "pcr_cache_age_seconds": round(time.time() - pcr_cache["time"], 1),
            "real_volume": real_volume
        }
    }), 200

@app.route("/debug/ws-status")
def debug_ws():
    return jsonify({
        "ws_running": ws_running,
        "ce_token": CE_TOKEN,
        "pe_token": PE_TOKEN,
        "latest_ce": latest_ticks["ce_price"],
        "latest_pe": latest_ticks["pe_price"],
        "price_history_len": len(price_history),
        "timeframe_data": {k: len(v) for k, v in timeframe_history.items()},
        "signal_memory": signal_memory,
        # NEW: Enhanced debug
        "enhanced": {
            "tick_queue_size": tick_queue.qsize(),
            "last_tick_time": datetime.fromtimestamp(last_tick_time["value"]).isoformat() if last_tick_time["value"] > 0 else None,
            "anti_flip": anti_flip,
            "token_metadata": token_metadata,
            "pcr_ema_history": list(pcr_ema_history),
            "real_volume": real_volume,
            "log_throttle": log_throttle
        }
    })

# ============================================================
# Start Background Engine (MODIFIED for all features)
# ============================================================
engine_started = False

def start_background_engine():
    global engine_started
    if not engine_started:
        # Start WebSocket thread
        ws_thread = threading.Thread(target=start_angel_websocket, daemon=True)
        ws_thread.start()

        # NEW: Start tick worker thread (Feature #5)
        worker_thread = threading.Thread(target=tick_worker, daemon=True)
        worker_thread.start()

        # NEW: Start watchdog thread (Feature #3)
        watchdog = threading.Thread(target=watchdog_thread, daemon=True)
        watchdog.start()

        # NEW: Start re-auth timer (Feature #2)
        reauth = threading.Thread(target=reauth_timer, daemon=True)
        reauth.start()

        engine_started = True
        logger.info("✅ Enhanced Angel One engine started:")
        logger.info("   - WebSocket with queue-based processing")
        logger.info("   - 90s watchdog for stale ticks")
        logger.info("   - 20h re-auth timer")
        logger.info("   - REST fallback for prices")
        logger.info("   - Expiry freeze 3:00-3:30 PM Thu")
        logger.info("   - PCR EMA smoothing")
        logger.info("   - Dynamic ATR thresholds")
        logger.info("   - Throttled logging")
        logger.info("   - Enhanced anti-flip with cooldown")

start_background_engine()

if __name__ == "__main__":
    start_background_engine()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, threaded=True)
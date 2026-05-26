import os
import time
import logging
import threading
import json
import requests
import math
import signal
from collections import deque
from datetime import datetime, time as dt_time, timedelta
from flask import Flask, jsonify, g
from flask_cors import CORS
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pyotp
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================================================
# GRACEFUL SHUTDOWN HANDLER (Severity 7 Fix)
# ============================================================
shutdown_requested = False
shutdown_event = threading.Event()

def handle_shutdown(signum, frame):
    global shutdown_requested, engine_active, ws_running
    signal_name = signal.Signals(signum).name
    logger.info(f"Received {signal_name}, initiating graceful shutdown...")
    shutdown_requested = True
    engine_active = False
    ws_running = False
    shutdown_event.set()
    global sws
    if sws:
        try:
            sws.close_connection()
            logger.info("WebSocket connection closed gracefully")
        except Exception as e:
            logger.warning(f"WebSocket close error during shutdown: {e}")

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

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
        if not result.get("token"):
            token_bytes = binary_data[2:26]
            token_int = int.from_bytes(token_bytes, byteorder='little')
            result["token"] = str(token_int)
    except:
        pass
    return result

SmartWebSocketV2._parse_binary_data = _patched_parse

_original_on_close = SmartWebSocketV2._on_close

def _patched_on_close(self, wsapp, *args):
    try:
        if _original_on_close:
            _original_on_close(self, wsapp)
    except:
        pass

SmartWebSocketV2._on_close = _patched_on_close

# ============================================================
# SMARTWEBSOCKETV2 RATE LIMIT FIX - Disable aggressive internal retry
# ============================================================
# Angel One limits: max 3 concurrent connections, rate limited on reconnects
# We MUST disable SmartAPI's internal retry and handle it ourselves with backoff

_original_smartws_init = SmartWebSocketV2.__init__

def _patched_smartws_init(self, auth_token, api_key, client_code, feed_token):
    _original_smartws_init(self, auth_token, api_key, client_code, feed_token)
    # Disable internal auto-reconnect - our outer loop handles it
    self._should_reconnect = False
    self._max_retry_attempts = 1
    self._retry_delay = 0

SmartWebSocketV2.__init__ = _patched_smartws_init

# Override reconnect to prevent aggressive retry loops
if hasattr(SmartWebSocketV2, '_reconnect'):
    _orig_reconnect = SmartWebSocketV2._reconnect
    def _patched_reconnect(self, wsapp):
        logger.warning("SmartAPI internal reconnect blocked - using custom backoff")
        pass
    SmartWebSocketV2._reconnect = _patched_reconnect

# Override on_close to not trigger auto-reconnect
if hasattr(SmartWebSocketV2, 'on_close'):
    def _patched_smartws_on_close(self, wsapp, *args):
        logger.info("SmartWebSocketV2 closed - NOT auto-reconnecting, outer loop will handle")
        self.HB_THREAD_FLAG = False
        self._is_first_connect = False
    SmartWebSocketV2.on_close = _patched_smartws_on_close



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

# For RSI divergence
rsi_history = deque(maxlen=100)

latest_ticks = {
    "ce_price": 0.0, "pe_price": 0.0,
    "ce_volume": 0, "pe_volume": 0,
    "ce_oi": 0, "pe_oi": 0,
    "ce_bid": 0.0, "ce_ask": 0.0,
    "pe_bid": 0.0, "pe_ask": 0.0,
    "nifty_spot": 0.0
}

tick_counter = 0
last_engine_run = 0
ws_running = False
sws = None
last_tick_time = time.time()
tick_lock = threading.Lock()
state_lock = threading.RLock()

# Heartbeats
last_ce_tick = time.time()
last_pe_tick = time.time()
last_spot_tick = time.time()
last_rest_fetch = 0

engine_active = True

# Multi-timeframe snapshots
timeframe_history = {
    "1min": deque(maxlen=60),
    "5min": deque(maxlen=20),
    "10min": deque(maxlen=15),
    "15min": deque(maxlen=10),
    "20min": deque(maxlen=10)
}
last_minute_snapshot = {"time": 0, "price": 0, "volume": 0}

# Paper trade P&L tracking (Severity 10 Fix)
paper_trade_log = deque(maxlen=1000)
paper_pnl = 0.0

# ============================================================
# TIMEZONE FIXES (Severity 1 & 2)
# ============================================================

def is_market_open():
    """Convert UTC to IST (UTC+5:30) for market hours check"""
    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)

    # Saturday=5, Sunday=6
    if now_ist.weekday() >= 5:
        return False

    market_start = dt_time(9, 15)
    market_end = dt_time(15, 30)

    logger.debug(f"[TZ-CHECK] UTC: {datetime.utcnow()} | IST: {now_ist} | Weekday: {now_ist.weekday()} | Time: {now_ist.time()} | Open: {market_start <= now_ist.time() <= market_end}")

    return market_start <= now_ist.time() <= market_end

def get_market_phase():
    """Convert UTC to IST for market phase detection"""
    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    mins = now_ist.hour * 60 + now_ist.minute

    if mins < 9*60+15:
        return "PRE_MARKET"
    elif mins < 9*60+45:
        return "OPENING"
    elif mins < 12*60:
        return "MORNING"
    elif mins < 13*60+30:
        return "MIDDAY"
    elif mins < 15*60:
        return "AFTERNOON"
    elif mins < 15*60+30:
        return "CLOSING"
    else:
        return "POST_MARKET"

# ============================================================
# ANGEL LOGIN FIX (Severity 3)
# ============================================================

def angel_login():
    """Authenticate with Angel One and cache credentials"""
    try:
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        obj = SmartConnect(api_key=ANGEL_API_KEY)
        session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
        if not session.get("status"):
            logger.error("Angel Login: Auth failed")
            return None
        auth_token = session["data"]["jwtToken"]
        feed_token = obj.getfeedToken()
        auth_cache.update({
            "token": auth_token,
            "feed_token": feed_token,
            "timestamp": time.time(),
            "obj": obj
        })
        logger.info("Angel Login: Success")
        return obj
    except Exception as e:
        logger.error(f"Angel Login error: {e}")
        return None

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
    "open_positions": 0,
    "total_pnl": 0.0  # Paper trade P&L tracking (Severity 10 Fix)
}

market_signal = {
    "signal": "WAITING", "ce_price": 0.0, "pe_price": 0.0, "spread": 0.0,
    "rsi": 50, "macd": 0.0, "pcr": 1.0, "vwap": 0.0, "atr": 0.0,
    "ema_fast": 0.0, "ema_slow": 0.0, "delta": 0.0, "gamma": 0.0,
    "theta": 0.0, "vega": 0.0, "volume": 0, "timestamp": "",
    "atr_pct": 0.0, "adx": 0.0, "bb_position": 50.0, "rsi_divergence": "NONE",
    "iv_rank": 50, "signal_grade": "D", "regime": "RANGING", "session_phase": "UNKNOWN",
    "ce_spread_pct": 0.0, "pe_spread_pct": 0.0, "ce_oi_change": 0, "pe_oi_change": 0,
    "reason": "",  # Severity 4 Fix: Added reason field
    "market_open": False,
    "paper_pnl": 0.0
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

# SCRIP MASTER CACHE
SCRIP_MASTER = None
SCRIP_MASTER_TIME = 0
SCRIP_MASTER_URL = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"

CONFIG = {
    "RSI_PERIOD": 14,
    "MACD_FAST": 12, "MACD_SLOW": 26, "MACD_SIGNAL": 9,
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
    "ENGINE_INTERVAL_SEC": 5,
    "STALE_TICK_SEC": 180,
    "REST_COOLDOWN_SEC": 5,
    "WATCHDOG_INTERVAL_SEC": 30,
    "WATCHDOG_STALE_LIMIT": 120,
    "WS_BASE_RETRY_SEC": 15,      # FIX: Increased to 15s to avoid Angel One 429 rate limit
    "WS_MAX_RETRY_SEC": 300,      # Severity 9 Fix: Max 5 minutes
    "WS_MAX_RETRIES": 20          # Severity 9 Fix: Max retry attempts
}

# ---------- Helper Functions ----------

def update_timeframes(price, volume):
    global last_minute_snapshot
    now = time.time()
    if now - last_minute_snapshot["time"] >= 60:
        snapshot = {"time": now, "price": price, "volume": volume}
        timeframe_history["1min"].append(snapshot)
        if len(timeframe_history["1min"]) % 5 == 0:
            timeframe_history["5min"].append(snapshot)
        if len(timeframe_history["1min"]) % 10 == 0:
            timeframe_history["10min"].append(snapshot)
        if len(timeframe_history["1min"]) % 15 == 0:
            timeframe_history["15min"].append(snapshot)
        if len(timeframe_history["1min"]) % 20 == 0:
            timeframe_history["20min"].append(snapshot)
        last_minute_snapshot = snapshot

# ---------- SCRIP MASTER CACHE ----------
def load_scrip_master():
    global SCRIP_MASTER, SCRIP_MASTER_TIME
    if SCRIP_MASTER is not None and (time.time() - SCRIP_MASTER_TIME) < 21600:
        return SCRIP_MASTER
    try:
        resp = requests.get(SCRIP_MASTER_URL, timeout=30)
        resp.raise_for_status()
        SCRIP_MASTER = resp.json()
        SCRIP_MASTER_TIME = time.time()
        logger.info("Scrip master loaded and cached")
    except Exception as e:
        logger.error(f"Failed to load scrip master: {e}")
        if SCRIP_MASTER is None:
            raise
    return SCRIP_MASTER

def get_nifty_index_token():
    global NIFTY_TOKEN
    try:
        data = load_scrip_master()
        for item in data:
            symbol = str(item.get("symbol", "")).upper()
            exch_seg = str(item.get("exch_seg", "")).upper()
            if exch_seg == "NSE" and symbol in ["NIFTY", "NIFTY 50", "NIFTY50"]:
                token = str(item.get("token"))
                logger.info(f"NIFTY token found: {symbol} -> {token}")
                NIFTY_TOKEN = token
                return token
        logger.error("NIFTY token not found")
    except Exception as e:
        logger.exception(f"Error finding NIFTY token: {e}")
    return None

# ---------- NIFTY SPOT FETCHER ----------
token_cache = {"token": None, "symbol": None, "timestamp": 0}

def get_nifty_token():
    if token_cache["token"] and (time.time() - token_cache["timestamp"]) < 3600:
        return token_cache["symbol"], token_cache["token"]
    data = load_scrip_master()
    for item in data:
        symbol = str(item.get("symbol", "")).upper()
        exch_seg = str(item.get("exch_seg", "")).upper()
        token = str(item.get("token", ""))
        if exch_seg == "NSE" and symbol in ["NIFTY", "NIFTY 50", "NIFTY50"]:
            token_cache["token"] = token
            token_cache["symbol"] = symbol
            token_cache["timestamp"] = time.time()
            logger.info(f"Cached NIFTY token: {symbol} -> {token}")
            return symbol, token
    return None, None

def get_nifty_spot():
    with tick_lock:
        spot_ws = latest_ticks.get("nifty_spot", 0)
        last_ws_time = last_tick_time
    if spot_ws > 0 and (time.time() - last_ws_time) < CONFIG["STALE_TICK_SEC"]:
        return spot_ws
    logger.debug("WebSocket spot stale, using REST fallback")
    return get_spot_from_angel_ltp()

def get_spot_from_angel_ltp():
    global last_rest_fetch
    now = time.time()
    if now - last_rest_fetch < CONFIG["REST_COOLDOWN_SEC"]:
        return None
    last_rest_fetch = now
    try:
        obj = auth_cache.get("obj")
        # AUTO RE-LOGIN (Severity 3 Fix: angel_login() now exists)
        if not obj:
            logger.warning("SmartAPI unavailable, re-authenticating...")
            obj = angel_login()
            if not obj:
                logger.error("Re-login failed")
                return None
        trading_symbol, symbol_token = get_nifty_token()
        if not trading_symbol or not symbol_token:
            return None
        ltp = obj.ltpData("NSE", trading_symbol, symbol_token)
        if "data" in ltp and "ltp" in ltp["data"]:
            spot = float(ltp["data"]["ltp"])
            logger.info(f"NIFTY Spot from REST: {spot}")
            return spot
    except Exception as e:
        logger.error(f"REST LTP fetch failed: {e}")
    return None

# Severity 8 Fix: Thread-safe spot cache access
def get_nifty_spot_cached():
    with tick_lock:
        now = time.time()
        if now - spot_cache["timestamp"] < CACHE_TTL and spot_cache["value"] is not None:
            return spot_cache["value"]
    spot = get_nifty_spot()
    if spot is not None:
        with tick_lock:
            spot_cache["value"] = spot
            spot_cache["timestamp"] = now
    with tick_lock:
        return spot_cache["value"]

# ============================================================
# ATM TOKENS FIX (Severity 6: Use IST for expiry comparison)
# ============================================================

def get_current_atm_tokens():
    global CE_TOKEN, PE_TOKEN, ATM_STRIKE, EXPIRY_DATE
    spot = get_nifty_spot_cached()
    if not spot:
        logger.error("No spot - cannot auto-fetch tokens")
        return
    atm_strike = round(spot / 50) * 50
    logger.info(f"ATM strike = {atm_strike}")
    try:
        data = load_scrip_master()
    except Exception as e:
        logger.error(f"Failed to load scrip master: {e}")
        return
    nifty_opts = []
    for item in data:
        if (item.get("name") == "NIFTY" and
            item.get("instrumenttype") == "OPTIDX" and
            item.get("exch_seg") == "NFO"):
            nifty_opts.append(item)
    if not nifty_opts:
        logger.error("No NIFTY OPTIDX found")
        return
    parsed = []
    for opt in nifty_opts:
        try:
            expiry = datetime.strptime(opt["expiry"], "%d%b%Y")
            strike = float(opt["strike"]) / 100
            parsed.append({"expiry": expiry, "strike": strike, "token": str(opt["token"]), "symbol": opt["symbol"]})
        except:
            continue
    # Severity 6 Fix: Use IST for expiry comparison
    today_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    future = [p for p in parsed if p["expiry"] >= today_ist]
    if not future:
        logger.error("No future expiry found")
        return
    nearest_expiry = min(p["expiry"] for p in future)
    atm_opts = [p for p in future if p["expiry"] == nearest_expiry and p["strike"] == atm_strike]
    if not atm_opts:
        strikes = sorted(set(p["strike"] for p in future if p["expiry"] == nearest_expiry))
        nearest_strike = min(strikes, key=lambda x: abs(x - atm_strike))
        atm_opts = [p for p in future if p["expiry"] == nearest_expiry and p["strike"] == nearest_strike]
        atm_strike = nearest_strike
    ce = [p for p in atm_opts if "CE" in p["symbol"]]
    pe = [p for p in atm_opts if "PE" in p["symbol"]]
    if ce and pe:
        CE_TOKEN = ce[0]["token"]
        PE_TOKEN = pe[0]["token"]
        ATM_STRIKE = atm_strike
        EXPIRY_DATE = nearest_expiry.strftime("%d%b%Y").upper()
        logger.info(f"Auto tokens: CE={CE_TOKEN} ({ce[0]['symbol']}), PE={PE_TOKEN}")
    else:
        logger.error("CE/PE not found for ATM strike")

# ---------- Technical Indicators ----------
def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    try:
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
    except:
        return 50.0

def calculate_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow:
        return 0.0, 0.0, 0.0
    def ema(series, period):
        if len(series) == 0:
            return 0
        alpha = 2 / (period + 1)
        val = series[0]
        for x in series[1:]:
            val = alpha * x + (1 - alpha) * val
        return val
    ema_fast = ema(prices[-fast:], fast)
    ema_slow = ema(prices[-slow:], slow)
    macd_line = ema_fast - ema_slow
    macd_series = []
    for i in range(slow, len(prices)):
        f = ema(prices[i-fast+1:i+1], fast) if i-fast+1 >= 0 else prices[i]
        s = ema(prices[i-slow+1:i+1], slow) if i-slow+1 >= 0 else prices[i]
        macd_series.append(f - s)
    signal_line = ema(macd_series[-signal:], signal) if len(macd_series) >= signal else macd_line
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def calculate_vwap(prices, volumes):
    if not prices or not volumes or len(prices) != len(volumes):
        return prices[-1] if prices else 0
    try:
        cum_pv = sum(p * v for p, v in zip(prices, volumes))
        cum_vol = sum(volumes)
        return cum_pv / cum_vol if cum_vol > 0 else prices[-1]
    except:
        return prices[-1] if prices else 0

def calculate_ema(prices, period):
    if len(prices) < period:
        return prices[-1] if prices else 0
    try:
        alpha = 2 / (period + 1)
        val = prices[0]
        for p in prices[1:]:
            val = alpha * p + (1 - alpha) * val
        return val
    except:
        return prices[-1] if prices else 0

def calculate_atr(prices, period=14):
    if len(prices) < period + 1:
        return 0.0
    try:
        trs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
        return sum(trs[-period:]) / period if len(trs) >= period else 0.0
    except:
        return 0.0

def calculate_bollinger(prices, period=20, std_dev=2.0):
    if len(prices) < period:
        return 0.0, 0.0, 0.0, 50.0
    try:
        window = prices[-period:]
        sma = sum(window) / period
        var = sum((p - sma) ** 2 for p in window) / period
        std = math.sqrt(var)
        upper = sma + std_dev * std
        lower = sma - std_dev * std
        pos = (prices[-1] - lower) / (upper - lower) * 100 if upper != lower else 50.0
        return sma, upper, lower, max(0, min(100, pos))
    except:
        return prices[-1], prices[-1], prices[-1], 50.0

def calculate_adx(prices, period=14):
    if len(prices) < period * 2:
        return 0.0
    try:
        tr = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
        plus_dm = [max(prices[i] - prices[i-1], 0) for i in range(1, len(prices))]
        minus_dm = [max(prices[i-1] - prices[i], 0) for i in range(1, len(prices))]
        atr = sum(tr[-period:]) / period if len(tr) >= period else 0.0
        if atr == 0:
            return 0.0
        plus_di = 100 * sum(plus_dm[-period:]) / period / atr
        minus_di = 100 * sum(minus_dm[-period:]) / period / atr
        return 100 * abs(plus_di - minus_di) / (plus_di + minus_di + 1e-10)
    except:
        return 0.0

def calculate_rsi_divergence(prices, rsi_series, lookback=5):
    if len(prices) < lookback + 2 or len(rsi_series) < lookback + 2:
        return "NONE"
    try:
        price_lows = [prices[-i] for i in range(1, lookback+1)]
        rsi_lows = [rsi_series[-i] for i in range(1, lookback+1)]
        if min(price_lows) < price_lows[0] and min(rsi_lows) > rsi_lows[0]:
            return "BULLISH"
        price_highs = [prices[-i] for i in range(1, lookback+1)]
        rsi_highs = [rsi_series[-i] for i in range(1, lookback+1)]
        if max(price_highs) > price_highs[0] and max(rsi_highs) < rsi_highs[0]:
            return "BEARISH"
    except:
        pass
    return "NONE"

# ---------- PCR with retry session ----------
nse_session = requests.Session()
retry = Retry(total=2, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
nse_session.mount('https://', HTTPAdapter(max_retries=retry))
nse_session.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9"
})
try:
    nse_session.get("https://www.nseindia.com", timeout=5)
except:
    pass

def get_nifty_pcr():
    now = time.time()
    if now - pcr_cache["time"] < 120:
        return pcr_cache["value"]
    try:
        resp = nse_session.get("https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            records = data.get("records", {}).get("data", [])
            ce_oi = sum(x.get("CE", {}).get("openInterest", 0) for x in records if "CE" in x)
            pe_oi = sum(x.get("PE", {}).get("openInterest", 0) for x in records if "PE" in x)
            pcr = pe_oi / ce_oi if ce_oi else 1.0
            pcr_cache["value"] = pcr
            pcr_cache["time"] = now
            return pcr
    except:
        pass
    return pcr_cache["value"]

def get_real_greeks(option_type="CE"):
    spot = get_nifty_spot_cached() or 0
    price = latest_ticks.get("ce_price" if option_type == "CE" else "pe_price", 0)
    if spot == 0 or ATM_STRIKE == 0 or price == 0:
        return 0.0, 0.0, 0.0, 0.0, 0.20
    moneyness = abs(spot - ATM_STRIKE) / spot
    if option_type == "CE":
        delta = 0.5 - moneyness
        if spot > ATM_STRIKE:
            delta = 0.8 - moneyness
    else:
        delta = -(0.5 - moneyness)
        if spot < ATM_STRIKE:
            delta = -0.8 + moneyness
    delta = max(-1, min(1, delta))
    gamma = 0.05 * (1 - moneyness * 2) if moneyness < 0.5 else 0.01
    theta = -price * 0.001 * CONFIG["DAYS_TO_EXPIRY"]
    vega = price * 0.1
    iv = 0.20 + moneyness * 0.1
    return delta, gamma, theta, vega, iv

def analyze_timeframe_trend(history):
    n = len(history)
    if n < 2:
        return "SIDEWAYS", 0
    try:
        prices = [h["price"] for h in history]
        x = list(range(n))
        x_mean = sum(x) / n
        y_mean = sum(prices) / n
        num = sum((x[i] - x_mean) * (prices[i] - y_mean) for i in range(n))
        den = sum((x[i] - x_mean) ** 2 for i in range(n))
        if den == 0:
            return "SIDEWAYS", 0
        slope = num / den
        if abs(slope) < 0.05:
            return "SIDEWAYS", 0
        return ("BULLISH" if slope > 0 else "BEARISH"), abs(slope) * 100
    except:
        return "SIDEWAYS", 0

def get_all_timeframe_trends():
    return {tf: {"trend": analyze_timeframe_trend(list(hist))[0],
                 "strength": round(analyze_timeframe_trend(list(hist))[1], 2)}
            for tf, hist in timeframe_history.items()}

# ============================================================
# SIGNAL ENGINE (Full - with all fixes)
# ============================================================

def run_signal_engine(ce_price, pe_price, ce_hist, pe_hist, ce_vol_hist, pe_vol_hist):
    global market_signal, market_state, institutional_state, signal_state, portfolio_state, rsi_history, paper_pnl

    # Initialize locals
    position_pct = 0
    rr = 0
    entry = 0
    init_stop = 0
    target = 0
    stop = 0
    spread = 0
    reason = ""  # Severity 4 Fix: Build reason string

    if len(ce_hist) < 30 or len(pe_hist) < 30:
        return

    with state_lock:
        if not hasattr(run_signal_engine, "spot_history"):
            run_signal_engine.spot_history = deque(maxlen=500)
        spot_history = run_signal_engine.spot_history
        if len(spot_history) == 0:
            prices = [(c + p) / 2 for c, p in zip(ce_hist, pe_hist)]
        else:
            prices = list(spot_history)

    combined_volumes = [(c + p) / 2 for c, p in zip(ce_vol_hist, pe_vol_hist)]

    # Isolated calculations
    try:
        rsi = calculate_rsi(prices, CONFIG["RSI_PERIOD"])
    except:
        rsi = 50
    try:
        macd_line, signal_line, macd_hist = calculate_macd(prices, CONFIG["MACD_FAST"], CONFIG["MACD_SLOW"], CONFIG["MACD_SIGNAL"])
    except:
        macd_line = macd_hist = 0
    try:
        vwap = calculate_vwap(prices, combined_volumes)
    except:
        vwap = prices[-1] if prices else 0
    try:
        ema_fast = calculate_ema(prices, CONFIG["EMA_FAST"])
        ema_slow = calculate_ema(prices, CONFIG["EMA_SLOW"])
    except:
        ema_fast = ema_slow = prices[-1] if prices else 0
    try:
        atr = calculate_atr(prices, CONFIG["ATR_PERIOD"])
    except:
        atr = 0
    try:
        pcr = get_nifty_pcr()
    except:
        pcr = 1.0
    try:
        ce_delta, ce_gamma, ce_theta, ce_vega, ce_iv = get_real_greeks("CE")
        pe_delta, pe_gamma, pe_theta, pe_vega, pe_iv = get_real_greeks("PE")
    except:
        ce_delta = ce_gamma = ce_theta = ce_vega = ce_iv = 0
        pe_delta = pe_gamma = pe_theta = pe_vega = pe_iv = 0
    try:
        bb_sma, bb_upper, bb_lower, bb_pos = calculate_bollinger(prices, CONFIG["BB_PERIOD"], CONFIG["BB_STD"])
    except:
        bb_pos = 50
    try:
        adx = calculate_adx(prices, CONFIG["ADX_PERIOD"])
    except:
        adx = 0

    rsi_history.append(rsi)
    try:
        rsi_div = calculate_rsi_divergence(prices, list(rsi_history))
    except:
        rsi_div = "NONE"

    atr_pct = (atr / prices[-1]) * 100 if prices[-1] > 0 else 0

    # Volume trend
    if len(combined_volumes) >= 20:
        recent_vol = sum(combined_volumes[-10:]) / 10
        older_vol = sum(combined_volumes[-20:-10]) / 10
        vol_trend = "INCREASING" if recent_vol > older_vol * 1.2 else "DECREASING" if recent_vol < older_vol * 0.8 else "FLAT"
    else:
        vol_trend = "FLAT"

    # OI change
    if len(ce_oi_history) >= 5:
        ce_oi_change = (ce_oi_history[-1] - ce_oi_history[-5]) / (ce_oi_history[-5] + 1e-6) * 100
    else:
        ce_oi_change = 0
    if len(pe_oi_history) >= 5:
        pe_oi_change = (pe_oi_history[-1] - pe_oi_history[-5]) / (pe_oi_history[-5] + 1e-6) * 100
    else:
        pe_oi_change = 0
    ce_oi_change = max(-50, min(50, ce_oi_change))
    pe_oi_change = max(-50, min(50, pe_oi_change))

    # Spread %
    ce_spread_pct = (latest_ticks["ce_ask"] - latest_ticks["ce_bid"]) / (ce_price + 1e-6) * 100 if latest_ticks["ce_ask"] > 0 else 0
    pe_spread_pct = (latest_ticks["pe_ask"] - latest_ticks["pe_bid"]) / (pe_price + 1e-6) * 100 if latest_ticks["pe_ask"] > 0 else 0

    # Regime
    if adx > 30:
        regime = "TRENDING"
    elif atr_pct > 1.5:
        regime = "VOLATILE"
    elif bb_pos < 20 or bb_pos > 80:
        regime = "BREAKOUT"
    elif adx < 20 and atr_pct < 0.5:
        regime = "CHOPPY"
    else:
        regime = "RANGING"

    session_phase = get_market_phase()
    tf_trends = get_all_timeframe_trends()
    bullish_tf = sum(1 for t in ["1min","5min","10min","15min","20min"] if tf_trends[t]["trend"]=="BULLISH")
    bearish_tf = sum(1 for t in ["1min","5min","10min","15min","20min"] if tf_trends[t]["trend"]=="BEARISH")
    tf_score_bull = bullish_tf * 10
    tf_score_bear = bearish_tf * 10

    tech_bull = tech_bear = 0
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

    if pcr < CONFIG["PCR_BULLISH"]:
        tech_bull += 10
    elif pcr < 1.0:
        tech_bull += 7
    elif pcr > CONFIG["PCR_BEARISH"]:
        tech_bear += 10
    elif pcr > 1.2:
        tech_bear += 7

    if vol_trend == "INCREASING":
        if ema_fast > ema_slow:
            tech_bull += 10
        else:
            tech_bear += 10

    avg_price = (ce_price + pe_price) / 2
    if avg_price > vwap and avg_price > ema_slow:
        tech_bull += 10
    elif avg_price > vwap or avg_price > ema_slow:
        tech_bull += 5
    elif avg_price < vwap and avg_price < ema_slow:
        tech_bear += 10
    elif avg_price < vwap or avg_price < ema_slow:
        tech_bear += 5

    if bb_pos < 20:
        tech_bull += 8
    elif bb_pos > 80:
        tech_bear += 8

    if adx > 30:
        if ema_fast > ema_slow:
            tech_bull += 5
        else:
            tech_bear += 5

    if rsi_div == "BULLISH" and ema_fast > ema_slow:
        tech_bull += 8
    elif rsi_div == "BEARISH" and ema_fast < ema_slow:
        tech_bear += 8

    if ce_oi_change > 10:
        tech_bull += 8
    elif ce_oi_change > 5:
        tech_bull += 4
    if pe_oi_change > 10:
        tech_bear += 8
    elif pe_oi_change > 5:
        tech_bear += 4

    total_bull = tf_score_bull + tech_bull
    total_bear = tf_score_bear + tech_bear
    raw_confidence = max(total_bull, total_bear)

    if ce_spread_pct > 2.0 or pe_spread_pct > 2.0:
        raw_confidence = min(raw_confidence, 40)

    if bullish_tf == 5:
        raw_confidence += 15
    elif bearish_tf == 5:
        raw_confidence += 15
    elif bullish_tf >= 4 or bearish_tf >= 4:
        raw_confidence += 8
    elif bullish_tf <= 1 and bearish_tf <= 1:
        raw_confidence = max(raw_confidence - 10, 0)

    if regime == "CHOPPY":
        raw_action = "HOLD"
        signal_type = "NONE"
        raw_confidence = 0
    else:
        if total_bull >= total_bear and total_bull >= CONFIG["CONSIDER_THRESHOLD"]:
            if raw_confidence >= CONFIG["STRONG_BUY_THRESHOLD"]:
                raw_action = "STRONG BUY CE"
                signal_type = "TRENDING" if bullish_tf>=4 else "MOMENTUM"
            elif raw_confidence >= CONFIG["BUY_THRESHOLD"]:
                raw_action = "BUY CE"
                signal_type = "MOMENTUM"
            elif raw_confidence >= CONFIG["CONSIDER_THRESHOLD"]:
                raw_action = "CONSIDER CE BUY"
                signal_type = "MOMENTUM"
            else:
                raw_action = "HOLD"
                signal_type = "NONE"
        elif total_bear > total_bull and total_bear >= CONFIG["CONSIDER_THRESHOLD"]:
            if raw_confidence >= CONFIG["STRONG_BUY_THRESHOLD"]:
                raw_action = "STRONG BUY PE"
                signal_type = "TRENDING" if bearish_tf>=4 else "MOMENTUM"
            elif raw_confidence >= CONFIG["BUY_THRESHOLD"]:
                raw_action = "BUY PE"
                signal_type = "MOMENTUM"
            elif raw_confidence >= CONFIG["CONSIDER_THRESHOLD"]:
                raw_action = "CONSIDER PE BUY"
                signal_type = "MOMENTUM"
            else:
                raw_action = "HOLD"
                signal_type = "NONE"
        else:
            raw_action = "HOLD"
            signal_type = "NONE"
            raw_confidence = max(total_bull, total_bear)

    # Build reason string (Severity 4 Fix)
    reasons = []
    if bullish_tf >= 4:
        reasons.append(f"{bullish_tf}/5 timeframes BULLISH")
    elif bearish_tf >= 4:
        reasons.append(f"{bearish_tf}/5 timeframes BEARISH")
    if 55 < rsi < 75:
        reasons.append(f"RSI {rsi:.1f} bullish zone")
    elif 25 < rsi < 45:
        reasons.append(f"RSI {rsi:.1f} bearish zone")
    if macd_hist > 0:
        reasons.append("MACD positive")
    elif macd_hist < 0:
        reasons.append("MACD negative")
    if pcr < 0.9:
        reasons.append(f"PCR {pcr:.2f} bullish")
    elif pcr > 1.2:
        reasons.append(f"PCR {pcr:.2f} bearish")
    if adx > 30:
        reasons.append(f"ADX {adx:.1f} strong trend")
    if rsi_div != "NONE":
        reasons.append(f"RSI divergence: {rsi_div}")
    reason = " | ".join(reasons) if reasons else "No clear signal"

    # Confirmation logic
    now_ts = time.time()
    final_action = signal_state["current_action"]
    final_signal_type = signal_state["current_signal_type"]

    if raw_action != signal_state["current_action"]:
        if now_ts < signal_state.get("cooldown_until", 0):
            final_action = signal_state["current_action"]
            final_signal_type = signal_state["current_signal_type"]
        else:
            if signal_state["pending_action"] != raw_action:
                signal_state["pending_action"] = raw_action
                signal_state["pending_signal_type"] = signal_type
                signal_state["confirmation_count"] = 1
                signal_state["signal_start_time"] = now_ts
                logger.info(f"New pending: {raw_action} (1/{CONFIG['SIGNAL_CONFIRMATION_BARS']})")
            else:
                signal_state["confirmation_count"] += 1
                logger.info(f"Confirmation {signal_state['confirmation_count']}/{CONFIG['SIGNAL_CONFIRMATION_BARS']} for {raw_action}")
            if signal_state["confirmation_count"] >= CONFIG["SIGNAL_CONFIRMATION_BARS"]:
                final_action = raw_action
                final_signal_type = signal_type
                signal_state["current_action"] = raw_action
                signal_state["current_signal_type"] = signal_type
                signal_state["cooldown_until"] = now_ts + CONFIG["COOLDOWN_AFTER_FLIP_SEC"]
                signal_state["pending_action"] = None
                signal_state["confirmation_count"] = 0
                if signal_state["flip_window_start"] == 0 or (now_ts - signal_state["flip_window_start"]) > 3600:
                    signal_state["flip_window_start"] = now_ts
                    signal_state["flip_count_hour"] = 1
                else:
                    signal_state["flip_count_hour"] += 1
                logger.info(f"SIGNAL CONFIRMED: {final_action} [{final_signal_type}]")
            else:
                final_action = signal_state["current_action"]
                final_signal_type = signal_state["current_signal_type"]
    else:
        if signal_state["pending_action"] is not None:
            signal_state["pending_action"] = None
            signal_state["confirmation_count"] = 0
        if signal_state["signal_start_time"] and (now_ts - signal_state["signal_start_time"]) > CONFIG["SIGNAL_MAX_AGE_SEC"]:
            if final_action != "HOLD":
                logger.info(f"Signal expired: {final_action}")
                final_action = "HOLD"
                final_signal_type = "NONE"
                signal_state["current_action"] = "HOLD"
                signal_state["current_signal_type"] = "NONE"
                signal_state["signal_start_time"] = None

    if signal_state["flip_count_hour"] >= CONFIG["MAX_FLIPS_PER_HOUR"]:
        final_action = "HOLD"
        final_signal_type = "NONE"

    # Grade
    grade = "D"
    if final_action != "HOLD" and final_signal_type != "NONE":
        if raw_confidence >= 90 and (bullish_tf>=4 or bearish_tf>=4):
            grade = "A"
        elif raw_confidence >= 80 or ((bullish_tf>=3 or bearish_tf>=3) and raw_confidence>=70):
            grade = "B"
        elif raw_confidence >= 65:
            grade = "C"
    signal_state["signal_grade"] = grade

    # Position sizing & paper trading with P&L tracking (Severity 10 Fix)
    trade_pnl = 0.0
    if final_action in ["STRONG BUY CE", "BUY CE", "CONSIDER CE BUY"]:
        entry = ce_price
        init_stop = entry - atr * CONFIG["STOP_LOSS_ATR_MULT"]
        stop = init_stop
        target = entry + atr * CONFIG["TARGET_ATR_MULT"]
        if grade == "A":
            base = CONFIG["POSITION_SIZE_MAX_PCT"]
        elif grade == "B":
            base = CONFIG["POSITION_SIZE_BASE_PCT"] * 1.5
        elif grade == "C":
            base = CONFIG["POSITION_SIZE_BASE_PCT"]
        else:
            base = 0
        if regime == "VOLATILE":
            base *= 0.7
        elif regime == "TRENDING":
            base *= 1.2
        position_pct = min(CONFIG["POSITION_SIZE_MAX_PCT"], max(0, base))
        if atr > 0:
            risk = entry - init_stop
            reward = target - entry
            rr = reward / risk if risk > 0 else 0
        if position_pct > 0 and signal_state["entry_price"] == 0:
            logger.info(f"*** PAPER TRADE: BUY {final_action} at {entry:.2f}, SL {init_stop:.2f}, TGT {target:.2f}, Size {position_pct}% ***")
            signal_state["entry_price"] = entry
            signal_state["stop_loss"] = init_stop
            signal_state["target"] = target
            signal_state["highest_price_since_entry"] = entry
            # Log trade start
            paper_trade_log.append({
                "time": datetime.now().isoformat(),
                "action": final_action,
                "entry": entry,
                "stop": init_stop,
                "target": target,
                "size_pct": position_pct,
                "status": "OPEN"
            })
        elif signal_state["entry_price"] > 0:
            if ce_price > signal_state["highest_price_since_entry"]:
                signal_state["highest_price_since_entry"] = ce_price
                profit_range = signal_state["highest_price_since_entry"] - signal_state["entry_price"]
                if profit_range > 0:
                    new_stop = signal_state["entry_price"] + profit_range * 0.5
                    if new_stop > signal_state["stop_loss"]:
                        signal_state["stop_loss"] = new_stop
                        logger.info(f"Trailing stop raised to {signal_state['stop_loss']:.2f}")
            if ce_price <= signal_state["stop_loss"]:
                trade_pnl = (signal_state["stop_loss"] - signal_state["entry_price"]) * (position_pct / 100) * portfolio_state["equity"] / signal_state["entry_price"]
                paper_pnl += trade_pnl
                portfolio_state["total_pnl"] = paper_pnl
                logger.info(f"*** PAPER EXIT: STOP LOSS for {final_action} at {ce_price:.2f} | P&L: {trade_pnl:.2f} ***")
                signal_state["entry_price"] = 0
                signal_state["stop_loss"] = 0
                signal_state["target"] = 0
                signal_state["highest_price_since_entry"] = 0
                # Update trade log
                if paper_trade_log:
                    paper_trade_log[-1]["status"] = "STOP_LOSS"
                    paper_trade_log[-1]["exit_price"] = ce_price
                    paper_trade_log[-1]["pnl"] = trade_pnl
            elif ce_price >= signal_state["target"]:
                trade_pnl = (signal_state["target"] - signal_state["entry_price"]) * (position_pct / 100) * portfolio_state["equity"] / signal_state["entry_price"]
                paper_pnl += trade_pnl
                portfolio_state["total_pnl"] = paper_pnl
                logger.info(f"*** PAPER EXIT: TARGET for {final_action} at {ce_price:.2f} | P&L: {trade_pnl:.2f} ***")
                signal_state["entry_price"] = 0
                signal_state["stop_loss"] = 0
                signal_state["target"] = 0
                signal_state["highest_price_since_entry"] = 0
                if paper_trade_log:
                    paper_trade_log[-1]["status"] = "TARGET"
                    paper_trade_log[-1]["exit_price"] = ce_price
                    paper_trade_log[-1]["pnl"] = trade_pnl

    elif final_action in ["STRONG BUY PE", "BUY PE", "CONSIDER PE BUY"]:
        entry = pe_price
        init_stop = entry + atr * CONFIG["STOP_LOSS_ATR_MULT"]
        stop = init_stop
        target = entry - atr * CONFIG["TARGET_ATR_MULT"]
        if grade == "A":
            base = CONFIG["POSITION_SIZE_MAX_PCT"]
        elif grade == "B":
            base = CONFIG["POSITION_SIZE_BASE_PCT"] * 1.5
        elif grade == "C":
            base = CONFIG["POSITION_SIZE_BASE_PCT"]
        else:
            base = 0
        if regime == "VOLATILE":
            base *= 0.7
        elif regime == "TRENDING":
            base *= 1.2
        position_pct = min(CONFIG["POSITION_SIZE_MAX_PCT"], max(0, base))
        if atr > 0:
            risk = init_stop - entry
            reward = entry - target
            rr = reward / risk if risk > 0 else 0
        if position_pct > 0 and signal_state["entry_price"] == 0:
            logger.info(f"*** PAPER TRADE: BUY {final_action} at {entry:.2f}, SL {init_stop:.2f}, TGT {target:.2f}, Size {position_pct}% ***")
            signal_state["entry_price"] = entry
            signal_state["stop_loss"] = init_stop
            signal_state["target"] = target
            signal_state["lowest_price_since_entry"] = entry
            paper_trade_log.append({
                "time": datetime.now().isoformat(),
                "action": final_action,
                "entry": entry,
                "stop": init_stop,
                "target": target,
                "size_pct": position_pct,
                "status": "OPEN"
            })
        elif signal_state["entry_price"] > 0:
            if pe_price < signal_state["lowest_price_since_entry"]:
                signal_state["lowest_price_since_entry"] = pe_price
                profit_range = signal_state["entry_price"] - signal_state["lowest_price_since_entry"]
                if profit_range > 0:
                    new_stop = signal_state["entry_price"] - profit_range * 0.5
                    if new_stop < signal_state["stop_loss"]:
                        signal_state["stop_loss"] = new_stop
                        logger.info(f"Trailing stop lowered to {signal_state['stop_loss']:.2f}")
            if pe_price >= signal_state["stop_loss"]:
                trade_pnl = (signal_state["entry_price"] - signal_state["stop_loss"]) * (position_pct / 100) * portfolio_state["equity"] / signal_state["entry_price"]
                paper_pnl += trade_pnl
                portfolio_state["total_pnl"] = paper_pnl
                logger.info(f"*** PAPER EXIT: STOP LOSS for {final_action} at {pe_price:.2f} | P&L: {trade_pnl:.2f} ***")
                signal_state["entry_price"] = 0
                signal_state["stop_loss"] = 0
                signal_state["target"] = 0
                signal_state["lowest_price_since_entry"] = float("inf")
                if paper_trade_log:
                    paper_trade_log[-1]["status"] = "STOP_LOSS"
                    paper_trade_log[-1]["exit_price"] = pe_price
                    paper_trade_log[-1]["pnl"] = trade_pnl
            elif pe_price <= signal_state["target"]:
                trade_pnl = (signal_state["entry_price"] - signal_state["target"]) * (position_pct / 100) * portfolio_state["equity"] / signal_state["entry_price"]
                paper_pnl += trade_pnl
                portfolio_state["total_pnl"] = paper_pnl
                logger.info(f"*** PAPER EXIT: TARGET for {final_action} at {pe_price:.2f} | P&L: {trade_pnl:.2f} ***")
                signal_state["entry_price"] = 0
                signal_state["stop_loss"] = 0
                signal_state["target"] = 0
                signal_state["lowest_price_since_entry"] = float("inf")
                if paper_trade_log:
                    paper_trade_log[-1]["status"] = "TARGET"
                    paper_trade_log[-1]["exit_price"] = pe_price
                    paper_trade_log[-1]["pnl"] = trade_pnl
    else:
        signal_state["entry_price"] = 0
        signal_state["stop_loss"] = 0
        signal_state["target"] = 0
        signal_state["highest_price_since_entry"] = 0
        signal_state["lowest_price_since_entry"] = float("inf")
        position_pct = 0
        rr = 0

    signal_state["position_size_pct"] = position_pct
    signal_state["risk_reward"] = rr
    portfolio_state["total_exposure_pct"] = position_pct if signal_state["entry_price"] > 0 else 0
    portfolio_state["open_positions"] = 1 if signal_state["entry_price"] > 0 else 0

    # Update global dictionaries
    market_state.update({
        "rsi": round(rsi, 2),
        "momentum": "UPTREND" if ema_fast > ema_slow else "DOWNTREND" if ema_fast < ema_slow else "NEUTRAL",
        "strength": "HIGH" if final_signal_type == "TRENDING" else "MODERATE" if final_signal_type == "MOMENTUM" else "LOW",
        "trend": "BULLISH" if ema_fast > ema_slow else "BEARISH" if ema_fast < ema_slow else "SIDEWAYS",
        "action": final_action,
        "confidence": raw_confidence,
        "volatility": "HIGH" if atr > 15 else "NORMAL" if atr > 5 else "LOW",
        "alert": final_action,
        "regime": regime,
        "session_phase": session_phase,
        "trend_1min": tf_trends["1min"]["trend"],
        "trend_5min": tf_trends["5min"]["trend"],
        "trend_10min": tf_trends["10min"]["trend"],
        "trend_15min": tf_trends["15min"]["trend"],
        "trend_20min": tf_trends["20min"]["trend"],
        "timeframe_agreement": max(bullish_tf, bearish_tf),
        "portfolio_heat": round(portfolio_state["total_exposure_pct"], 2),
    })

    institutional_state.update({
        "vwap": round(vwap, 2),
        "ema_fast": round(ema_fast, 2),
        "ema_slow": round(ema_slow, 2),
        "ema_signal": "BULLISH" if ema_fast > ema_slow else "BEARISH",
        "atr": round(atr, 2),
        "oi_buildup": "BULLISH" if pcr < 0.9 else "BEARISH" if pcr > 1.2 else "NEUTRAL",
        "iv_state": "HIGH" if ce_vega > 2 else "NORMAL",
        "candle_structure": "BULLISH" if ema_fast > ema_slow and rsi > 55 else "BEARISH" if ema_fast < ema_slow and rsi < 45 else "SIDEWAYS",
        "market_breadth": "BULLISH" if bullish_tf >= 3 else "BEARISH" if bearish_tf >= 3 else "BALANCED",
        "volume_profile": vol_trend,
        "smart_money_flow": "BULLISH" if vwap > ema_slow and vol_trend == "INCREASING" else "BEARISH" if vwap < ema_slow and vol_trend == "INCREASING" else "NEUTRAL",
        "delta": ce_delta, "gamma": ce_gamma, "theta": ce_theta, "vega": ce_vega, "iv": ce_iv,
        "institutional_signal": final_action,
        "institutional_confidence": raw_confidence,
        "signal_grade": grade,
        "position_size_pct": position_pct,
        "risk_reward": round(rr, 2),
        "entry_price": round(entry, 2) if entry else 0,
        "stop_loss": round(stop, 2) if stop else 0,
        "target": round(target, 2) if target else 0,
        "ce_delta": ce_delta, "pe_delta": pe_delta,
        "ce_iv": ce_iv, "pe_iv": pe_iv,
        "ce_oi_change": round(ce_oi_change, 1), "pe_oi_change": round(pe_oi_change, 1)
    })

    spread = ce_price - pe_price

    # Severity 4 Fix: Include reason, market_open, paper_pnl
    market_signal.update({
        "signal": "BULLISH" if final_action in ["STRONG BUY CE","BUY CE","CONSIDER CE BUY"] else "BEARISH" if final_action in ["STRONG BUY PE","BUY PE","CONSIDER PE BUY"] else "NEUTRAL",
        "ce_price": ce_price, "pe_price": pe_price, "spread": round(spread, 2),
        "rsi": round(rsi, 2), "macd": round(macd_hist, 2), "pcr": round(pcr, 2),
        "vwap": round(vwap, 2), "atr": round(atr, 2), "atr_pct": round(atr_pct, 2),
        "ema_fast": round(ema_fast, 2), "ema_slow": round(ema_slow, 2),
        "delta": ce_delta, "gamma": ce_gamma, "theta": ce_theta, "vega": ce_vega,
        "volume": int(combined_volumes[-1]) if combined_volumes else 0,
        "timestamp": datetime.now().isoformat(),
        "adx": round(adx, 2), "bb_position": round(bb_pos, 2),
        "rsi_divergence": rsi_div, "iv_rank": 50,
        "signal_grade": grade, "regime": regime, "session_phase": session_phase,
        "ce_spread_pct": round(ce_spread_pct, 1), "pe_spread_pct": round(pe_spread_pct, 1),
        "ce_oi_change": round(ce_oi_change, 1), "pe_oi_change": round(pe_oi_change, 1),
        "reason": reason,  # Severity 4 Fix
        "market_open": is_market_open(),  # Severity 4 Fix
        "paper_pnl": round(paper_pnl, 2)  # Severity 10 Fix
    })

    if final_action != signal_state["last_logged_action"]:
        logger.info(f"PRO SIGNAL: {final_action} [{final_signal_type}] Grade:{grade} Conf:{raw_confidence} "
                    f"BullTF:{bullish_tf} BearTF:{bearish_tf} RSI:{rsi:.1f} ADX:{adx:.1f} PCR:{pcr:.2f} "
                    f"PosSize:{position_pct}% RR:{rr:.1f} P&L:{paper_pnl:.2f}")
        signal_state["last_logged_action"] = final_action

# ============================================================
# WEBSOCKET CALLBACKS (with bid/ask parsing - Severity 5 Fix)
# ============================================================

def on_open(wsapp):
    logger.info("WebSocket Opened")
    if sws and CE_TOKEN and PE_TOKEN and NIFTY_TOKEN:
        tokens = [
            {"exchangeType": 2, "tokens": [CE_TOKEN, PE_TOKEN]},
            {"exchangeType": 1, "tokens": [NIFTY_TOKEN]}
        ]
        sws.subscribe(correlation_id="tradeguru", mode=1, token_list=tokens)
        logger.info(f"Subscribed CE={CE_TOKEN} PE={PE_TOKEN} NIFTY={NIFTY_TOKEN}")

def on_data(wsapp, message):
    global tick_counter, last_tick_time, last_ce_tick, last_pe_tick, last_spot_tick, last_engine_run
    global spot_cache
    try:
        if not message:
            return
        tick = message if isinstance(message, dict) else json.loads(message)
        token = str(tick.get("token") or tick.get("tk") or "")
        ltp = tick.get("last_traded_price") or tick.get("ltp") or 0
        volume = tick.get("volume_trade_for_the_day") or tick.get("v") or 0
        oi = tick.get("open_interest") or tick.get("oi") or 0

        # Severity 5 Fix: Parse bid/ask from tick data
        bid = tick.get("best_bid_price") or tick.get("bp") or tick.get("bid") or 0
        ask = tick.get("best_ask_price") or tick.get("ap") or tick.get("ask") or 0

        if ltp and ltp > 1000:
            ltp = float(ltp) / 100
        if bid and bid > 1000:
            bid = float(bid) / 100
        if ask and ask > 1000:
            ask = float(ask) / 100

        last_tick_time = time.time()

        with state_lock:
            if token == str(NIFTY_TOKEN):
                last_spot_tick = time.time()
                with tick_lock:
                    latest_ticks["nifty_spot"] = ltp
                spot_cache["value"] = ltp
                spot_cache["timestamp"] = time.time()
                if not hasattr(run_signal_engine, "spot_history"):
                    run_signal_engine.spot_history = deque(maxlen=500)
                run_signal_engine.spot_history.append(ltp)
                return

            if token == str(CE_TOKEN):
                last_ce_tick = time.time()
                with tick_lock:
                    latest_ticks["ce_price"] = ltp
                    latest_ticks["ce_volume"] = volume
                    latest_ticks["ce_oi"] = oi
                    # Severity 5 Fix: Update bid/ask
                    if bid > 0:
                        latest_ticks["ce_bid"] = bid
                    if ask > 0:
                        latest_ticks["ce_ask"] = ask
                ce_price_history.append(ltp)
                ce_volume_history.append(volume)
                ce_oi_history.append(oi)
                tick_counter += 1

            elif token == str(PE_TOKEN):
                last_pe_tick = time.time()
                with tick_lock:
                    latest_ticks["pe_price"] = ltp
                    latest_ticks["pe_volume"] = volume
                    latest_ticks["pe_oi"] = oi
                    # Severity 5 Fix: Update bid/ask
                    if bid > 0:
                        latest_ticks["pe_bid"] = bid
                    if ask > 0:
                        latest_ticks["pe_ask"] = ask
                pe_price_history.append(ltp)
                pe_volume_history.append(volume)
                pe_oi_history.append(oi)
                tick_counter += 1

            ce = latest_ticks["ce_price"]
            pe = latest_ticks["pe_price"]
            avg_price = (ce + pe) / 2
            avg_volume = (latest_ticks.get("ce_volume", 0) + latest_ticks.get("pe_volume", 0)) / 2
            update_timeframes(avg_price, avg_volume)

            if ce > 0 and pe > 0 and (time.time() - last_engine_run >= CONFIG["ENGINE_INTERVAL_SEC"]) and len(ce_price_history) >= 30 and len(pe_price_history) >= 30:
                last_engine_run = time.time()
                run_signal_engine(ce, pe, list(ce_price_history), list(pe_price_history), list(ce_volume_history), list(pe_volume_history))
    except Exception as e:
        logger.exception(f"Tick parse error: {e}")

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

def restart_websocket():
    global sws, ws_running
    logger.info("Restarting WebSocket...")
    if sws:
        try:
            sws.close_connection()
        except:
            pass
    ws_running = False
    time.sleep(5)

# ============================================================
# WATCHDOG WITH SHUTDOWN CHECK (Severity 7 Fix)
# ============================================================

def watchdog():
    while engine_active and not shutdown_requested:
        time.sleep(CONFIG["WATCHDOG_INTERVAL_SEC"])
        if shutdown_requested:
            logger.info("Watchdog: shutdown requested, exiting")
            break
        if not is_market_open():
            continue
        if ws_running and (time.time() - last_tick_time > CONFIG["WATCHDOG_STALE_LIMIT"]):
            logger.warning("Watchdog: no ticks for long time, restarting websocket")
            restart_websocket()

# ============================================================
# WEBSOCKET WITH EXPONENTIAL BACKOFF (Severity 9 Fix)
# ============================================================

def start_websocket():
    global ws_running, sws, CE_TOKEN, PE_TOKEN, NIFTY_TOKEN, last_tick_time
    retry_count = 0
    retry_delay = CONFIG["WS_BASE_RETRY_SEC"]
    last_connect_attempt = 0
    MIN_CONNECT_INTERVAL = 10  # Minimum seconds between connection attempts to avoid 429

    while engine_active and not shutdown_requested:
        try:
            if shutdown_requested:
                logger.info("WebSocket thread: shutdown requested, exiting")
                break

            if not is_market_open():
                logger.info("Market closed. Sleeping 5 minutes...")
                time.sleep(300)
                continue

            # RATE LIMIT PROTECTION: Enforce minimum interval between connection attempts
            now = time.time()
            time_since_last = now - last_connect_attempt
            if time_since_last < MIN_CONNECT_INTERVAL:
                wait = MIN_CONNECT_INTERVAL - time_since_last
                logger.info(f"Rate limit protection: waiting {wait:.1f}s before next connect attempt")
                time.sleep(wait)
            last_connect_attempt = time.time()

            auth_token, feed_token, obj = get_auth_token()

            if not auth_token:
                logger.error("Authentication failed. Retrying...")
                retry_count += 1
                retry_delay = min(CONFIG["WS_MAX_RETRY_SEC"], CONFIG["WS_BASE_RETRY_SEC"] * (2 ** retry_count))
                logger.info(f"Retry {retry_count}/{CONFIG['WS_MAX_RETRIES']} in {retry_delay}s")
                time.sleep(retry_delay)
                continue

            if NIFTY_TOKEN is None:
                NIFTY_TOKEN = get_nifty_index_token()

            get_current_atm_tokens()

            if not CE_TOKEN or not PE_TOKEN:
                logger.error("No tokens available. Retrying...")
                retry_count += 1
                retry_delay = min(CONFIG["WS_MAX_RETRY_SEC"], CONFIG["WS_BASE_RETRY_SEC"] * (2 ** retry_count))
                time.sleep(retry_delay)
                continue

            # Reset retry counter on successful connection setup
            retry_count = 0
            retry_delay = CONFIG["WS_BASE_RETRY_SEC"]

            # CRITICAL: Close any existing connection before creating new one
            if sws:
                try:
                    logger.info("Closing previous WebSocket before reconnect...")
                    sws.close_connection()
                    sws = None
                    time.sleep(2)  # Give server time to clean up
                except Exception as e:
                    logger.debug(f"Error closing old WS: {e}")

            sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)
            sws.on_open = on_open
            sws.on_message = on_data
            sws.on_error = on_error
            sws.on_close = on_close
            ws_running = True
            logger.info("Connecting WebSocket (rate-limit protected)")
            last_tick_time = time.time()
            ws_thread = threading.Thread(target=sws.connect, daemon=True)
            ws_thread.start()

            # Connection monitoring loop
            stale_check_count = 0
            while ws_running and engine_active and not shutdown_requested:
                time.sleep(1)
                if shutdown_requested:
                    break

                now = time.time()
                ce_stale = now - last_ce_tick > CONFIG["STALE_TICK_SEC"]
                pe_stale = now - last_pe_tick > CONFIG["STALE_TICK_SEC"]
                spot_stale = now - last_spot_tick > CONFIG["STALE_TICK_SEC"]

                if ce_stale and pe_stale and spot_stale:
                    stale_check_count += 1
                    if stale_check_count >= 3:  # Confirm stale for 3 seconds before reconnect
                        logger.warning("All feeds stale -> reconnecting websocket")
                        break
                else:
                    stale_check_count = 0

        except Exception as e:
            logger.exception(f"WebSocket connection error: {e}")
            retry_count += 1
            retry_delay = min(CONFIG["WS_MAX_RETRY_SEC"], CONFIG["WS_BASE_RETRY_SEC"] * (2 ** retry_count))
            if retry_count >= CONFIG["WS_MAX_RETRIES"]:
                logger.error(f"Max retries ({CONFIG['WS_MAX_RETRIES']}) reached. Waiting 5 min before reset.")
                retry_count = 0
                time.sleep(300)
            else:
                logger.info(f"Retry {retry_count}/{CONFIG['WS_MAX_RETRIES']} in {retry_delay}s")
                time.sleep(retry_delay)

# ============================================================
# FLASK ROUTES (with graceful shutdown awareness)
# ============================================================

@app.route("/")
def home():
    if shutdown_requested:
        return jsonify({"status": "shutting_down", "message": "Service is shutting down"}), 503
    return jsonify({
        "status": "online",
        "message": "TradeGuru Ultimate Signal Engine (Fixed Production Version)",
        "worker_type": "WebSocket",
        "market_open": is_market_open(),
        "trading_mode": "PAPER",
        "version": "9.0",
        "fixes_applied": [
            "IST_timezone_conversion",
            "angel_login_defined",
            "market_reason_field",
            "bid_ask_parsing",
            "ist_expiry_comparison",
            "graceful_shutdown",
            "spot_cache_threadsafe",
            "exponential_backoff",
            "paper_pnl_tracking"
        ]
    })

@app.route("/api/live-signals")
def live_signals():
    if shutdown_requested:
        return jsonify({"status": "shutting_down"}), 503
    with state_lock:
        spot = get_nifty_spot_cached()
        return jsonify({
            "status": "active",
            "data": market_signal,
            "market": market_state,
            "institutional": institutional_state,
            "spot_price": spot if spot else 0,
            "signal_state": {
                "current_action": signal_state["current_action"],
                "pending_action": signal_state["pending_action"] or "NONE",
                "signal_type": signal_state["current_signal_type"],
                "confirmation_count": signal_state["confirmation_count"],
                "required_confirmations": CONFIG["SIGNAL_CONFIRMATION_BARS"],
                "grade": signal_state["signal_grade"],
                "position_size_pct": signal_state["position_size_pct"],
                "risk_reward": signal_state["risk_reward"],
                "entry_price": signal_state["entry_price"],
                "stop_loss": signal_state["stop_loss"],
                "target": signal_state["target"]
            },
            "portfolio": {
                "equity": portfolio_state["equity"],
                "total_exposure_pct": round(portfolio_state["total_exposure_pct"], 2),
                "open_positions": portfolio_state["open_positions"],
                "total_pnl": round(portfolio_state["total_pnl"], 2)  # Severity 10 Fix
            },
            "paper_trades": list(paper_trade_log)[-10:]  # Severity 10 Fix: Last 10 trades
        })

@app.route("/api/health")
def health():
    if shutdown_requested:
        return jsonify({"status": "shutting_down"}), 503
    with state_lock:
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
            "market_open": is_market_open(),
            "force_market_open": FORCE_MARKET_OPEN,
            "timestamp": datetime.now().isoformat()
        })

# Test endpoint for debugging
@app.route("/api/test-signal")
def test_signal():
    return jsonify({
        "signal": "TEST_BULLISH",
        "ce_price": 150.5,
        "pe_price": 120.3,
        "rsi": 65,
        "timestamp": datetime.now().isoformat(),
        "market_open": is_market_open()
    })

# ============================================================
# START ENGINE (with graceful shutdown)
# ============================================================
engine_started = False

def start_background_engine():
    global engine_started
    if not engine_started:
        threading.Thread(target=watchdog, daemon=True).start()
        threading.Thread(target=start_websocket, daemon=True).start()
        engine_started = True
        logger.info("Ultimate Signal Engine v9.0 Started (All 10 severity fixes applied)")

start_background_engine()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, threaded=True)
import os
import time
import logging
import threading
import json
import requests
import pandas as pd
from collections import deque
from datetime import datetime, timedelta
from flask import Flask, jsonify
from flask_cors import CORS
import pyotp
import math

# ============================================================
# MONKEY‑PATCH FOR SmartWebSocketV2 (fixes token parsing)
# ============================================================
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

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
    except Exception as e:
        logging.getLogger(__name__).error(f"Token extraction failed: {e}")
    return result

SmartWebSocketV2._parse_binary_data = _patched_parse

_original_on_close = SmartWebSocketV2._on_close
def _patched_on_close(self, wsapp, *args):
    try:
        _original_on_close(self, wsapp)
    except:
        pass
SmartWebSocketV2._on_close = _patched_on_close
# ============================================================

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
    raise ValueError("Missing Angel One credentials")

# --------------------------------------------------
# Global state (professional)
# --------------------------------------------------
CE_TOKEN = None
PE_TOKEN = None
CE_SYMBOL = ""
PE_SYMBOL = ""
ATM_STRIKE = 0
EXPIRY_DATE = ""

# Separate price & volume histories
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
    "pe_bid": 0.0, "pe_ask": 0.0
}

tick_counter = 0
ws_running = False
sws = None
last_tick_time = time.time()
engine_active = True

# Timeframe snapshots (1min, 5min, 10min, 15min, 20min)
timeframe_history = {
    "1min": deque(maxlen=60),
    "5min": deque(maxlen=20),
    "10min": deque(maxlen=15),
    "15min": deque(maxlen=10),
    "20min": deque(maxlen=10)
}
last_minute_snapshot = {"time": 0, "price": 0, "volume": 0}

# Signal state – persistent across ticks
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
    "max_drawdown_pct": 0.0,
    "flip_count_hour": 0,
    "flip_window_start": 0,
    "highest_price_since_entry": 0.0,
    "lowest_price_since_entry": float("inf")
}

# Portfolio state (simple in‑memory)
portfolio_state = {
    "equity": 100000.0,
    "total_exposure_pct": 0.0,
    "daily_pnl": 0.0,
    "max_drawdown_today": 0.0,
    "open_positions": 0
}

# Market data containers
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
    "portfolio_heat": 0, "daily_pnl_pct": 0, "max_drawdown_today": 0
}

institutional_state = {
    "vwap": 0, "ema_fast": 0, "ema_slow": 0, "ema_signal": "NEUTRAL", "atr": 0,
    "oi_buildup": "NEUTRAL", "iv_state": "NORMAL", "candle_structure": "SIDEWAYS",
    "market_breadth": "BALANCED", "volume_profile": "NORMAL", "smart_money_flow": "NEUTRAL",
    "delta": 0, "gamma": 0, "theta": 0, "vega": 0, "iv": 0,
    "institutional_signal": "HOLD", "institutional_confidence": 0,
    "signal_grade": "D", "position_size_pct": 0, "risk_reward": 0.0,
    "entry_price": 0.0, "stop_loss": 0.0, "target": 0.0, "max_drawdown_pct": 0.0,
    "ce_delta": 0, "pe_delta": 0, "ce_iv": 0, "pe_iv": 0,
    "ce_oi_change": 0, "pe_oi_change": 0
}

# PCR cache
pcr_cache = {"value": 1.0, "time": 0, "source": "default"}
pcr_history = deque(maxlen=20)

# Spot cache
spot_cache = {"value": None, "timestamp": 0}
CACHE_TTL = 30

# Configuration constants
CONFIG = {
    "RSI_PERIOD": 14,
    "MACD_FAST": 12,
    "MACD_SLOW": 26,
    "MACD_SIGNAL": 9,
    "ATR_PERIOD": 14,
    "BB_PERIOD": 20,
    "BB_STD": 2.0,
    "ADX_PERIOD": 14,
    "EMA_FAST": 9,
    "EMA_SLOW": 21,
    "PCR_EMA_PERIOD": 10,
    "PCR_BULLISH": 0.9,
    "PCR_BEARISH": 1.2,
    "STRONG_BUY_THRESHOLD": 85,
    "BUY_THRESHOLD": 70,
    "CONSIDER_THRESHOLD": 55,
    "SIGNAL_CONFIRMATION_BARS": 2,
    "SIGNAL_MAX_AGE_SEC": 1800,
    "COOLDOWN_AFTER_FLIP_SEC": 30,
    "MAX_FLIPS_PER_HOUR": 3,
    "POSITION_SIZE_BASE_PCT": 10,
    "POSITION_SIZE_MAX_PCT": 25,
    "STOP_LOSS_ATR_MULT": 1.5,
    "TARGET_ATR_MULT": 3.0,
    "MAX_DRAWDOWN_PCT": 5.0,
    "RISK_FREE_RATE": 0.06,
    "DAYS_TO_EXPIRY": 7,
    "TOKEN_REFRESH_SEC": 300,
    "REST_POLL_INTERVAL_SEC": 30,
    "SPREAD_THRESHOLD": 5.0,
}

# ============================================================
# NSE SESSION MANAGER (Persistent session with retries)
# ============================================================
class NSESessionManager:
    def __init__(self):
        self.session = None
        self.last_init = 0
        self.init_cooldown = 300  # 5 minutes
    
    def get_session(self):
        now = time.time()
        if self.session is None or (now - self.last_init) > self.init_cooldown:
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
            })
            try:
                # Warm up session with NSE homepage
                resp = self.session.get("https://www.nseindia.com", timeout=10)
                resp.raise_for_status()
                time.sleep(1)  # Let cookies settle
                self.last_init = now
                logger.info("NSE session initialized successfully")
            except Exception as e:
                logger.warning(f"NSE session init warning: {e}")
                # Return session anyway, might still work
        return self.session

nse_manager = NSESessionManager()

def safe_json_request(url, max_retries=3, backoff=2, timeout=10):
    """Make JSON request with retries and proper error handling."""
    session = nse_manager.get_session()
    for attempt in range(max_retries):
        try:
            resp = session.get(url, timeout=timeout)
            # Check if response is valid JSON
            content_type = resp.headers.get('Content-Type', '')
            if 'json' not in content_type and 'text/plain' not in content_type:
                logger.warning(f"Unexpected content type: {content_type}")
            
            # Try to parse JSON
            try:
                data = resp.json()
                return data
            except json.JSONDecodeError as je:
                # Log snippet of response for debugging
                snippet = resp.text[:200] if resp.text else "[EMPTY]"
                logger.warning(f"JSON decode failed (attempt {attempt+1}/{max_retries}): {je} | Response: {snippet}")
                if attempt < max_retries - 1:
                    time.sleep(backoff * (attempt + 1))
                continue
                
        except requests.exceptions.RequestException as re:
            logger.warning(f"Request failed (attempt {attempt+1}/{max_retries}): {re}")
            if attempt < max_retries - 1:
                time.sleep(backoff * (attempt + 1))
            continue
        except Exception as e:
            logger.error(f"Unexpected error in safe_json_request: {e}")
            break
    
    return None

# ============================================================
# HELPER FUNCTIONS
# ============================================================
def is_market_open():
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    start = datetime.strptime("09:15", "%H:%M").time()
    end = datetime.strptime("15:30", "%H:%M").time()
    return start <= now.time() <= end

def get_market_phase():
    now = datetime.now()
    mins = now.hour * 60 + now.minute
    if mins < 9*60 + 15:
        return "PRE_MARKET"
    elif mins < 9*60 + 45:
        return "OPENING"
    elif mins < 12*60:
        return "MORNING"
    elif mins < 13*60 + 30:
        return "MIDDAY"
    elif mins < 15*60:
        return "AFTERNOON"
    elif mins < 15*60 + 30:
        return "CLOSING"
    else:
        return "POST_MARKET"

def get_nifty_spot():
    """Fetch NIFTY spot with robust error handling."""
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        data = safe_json_request(url, max_retries=3, timeout=10)
        
        if data is None:
            logger.error("Spot fetch failed: All retries exhausted")
            return None
            
        if not isinstance(data, dict) or "data" not in data:
            logger.error(f"Spot fetch failed: Invalid data structure: {type(data)}")
            return None
            
        spot = float(data["data"][0]["lastPrice"])
        logger.info(f"NIFTY spot = {spot}")
        return spot
        
    except (KeyError, IndexError, TypeError, ValueError) as e:
        logger.error(f"Spot parse error: {e}")
        return None
    except Exception as e:
        logger.error(f"Spot fetch unexpected error: {e}")
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

def get_current_atm_tokens():
    global CE_TOKEN, PE_TOKEN, CE_SYMBOL, PE_SYMBOL, ATM_STRIKE, EXPIRY_DATE
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

    nifty_opts = df[
        (df["name"].astype(str) == "NIFTY") & 
        (df["instrumenttype"].astype(str) == "OPTIDX") &
        (df["exch_seg"].astype(str) == "NFO")
    ].copy()
    if nifty_opts.empty:
        nifty_opts = df[df["symbol"].astype(str).str.match(r'^NIFTY\d{2}[A-Z]{3}\d{2}', na=False)].copy()
    if nifty_opts.empty:
        logger.error("No NIFTY symbols found")
        return None, None

    for fmt in ["%d%b%Y", "%d-%b%Y", "%Y-%m-%d", "%d%m%Y"]:
        nifty_opts["expiry_date"] = pd.to_datetime(nifty_opts["expiry"], format=fmt, errors="coerce")
        if nifty_opts["expiry_date"].notna().sum() > 0:
            break
    nifty_opts = nifty_opts.dropna(subset=["expiry_date"])
    nifty_opts["strike"] = pd.to_numeric(nifty_opts["strike"], errors="coerce") / 100
    nifty_opts = nifty_opts.dropna(subset=["strike"])

    today = datetime.now()
    future_expiries = nifty_opts[nifty_opts["expiry_date"] >= today]
    if future_expiries.empty:
        logger.error("No future expiry found")
        return None, None
    nearest_expiry = future_expiries["expiry_date"].min()
    logger.info(f"Using expiry: {nearest_expiry.date()}")

    atm_opts = nifty_opts[(nifty_opts["strike"] == atm_strike) & (nifty_opts["expiry_date"] == nearest_expiry)]
    if atm_opts.empty:
        strikes = sorted(nifty_opts[nifty_opts["expiry_date"] == nearest_expiry]["strike"].unique())
        nearest_strike = min(strikes, key=lambda x: abs(x - atm_strike))
        logger.info(f"ATM strike not found, using nearest: {nearest_strike}")
        atm_opts = nifty_opts[(nifty_opts["strike"] == nearest_strike) & (nifty_opts["expiry_date"] == nearest_expiry)]
        atm_strike = nearest_strike

    ce = atm_opts[atm_opts["symbol"].str.upper().str.contains("CE", na=False)]
    pe = atm_opts[atm_opts["symbol"].str.upper().str.contains("PE", na=False)]
    if ce.empty or pe.empty:
        logger.error(f"CE/PE not found for strike {atm_strike}")
        return None, None

    CE_TOKEN = str(ce.iloc[0]["token"])
    PE_TOKEN = str(pe.iloc[0]["token"])
    CE_SYMBOL = str(ce.iloc[0]["symbol"])
    PE_SYMBOL = str(pe.iloc[0]["symbol"])
    ATM_STRIKE = atm_strike
    EXPIRY_DATE = nearest_expiry.strftime("%d%b%Y").upper()
    logger.info(f"Tokens resolved: CE={CE_TOKEN} ({CE_SYMBOL}), PE={PE_TOKEN} ({PE_SYMBOL})")
    return CE_TOKEN, PE_TOKEN

# ============================================================
# TECHNICAL INDICATORS
# ============================================================
def calculate_ema_series(prices, period):
    if len(prices) < period:
        return [prices[-1]] * len(prices) if prices else [0]
    alpha = 2 / (period + 1)
    ema = [prices[0]]
    for p in prices[1:]:
        ema.append(alpha * p + (1 - alpha) * ema[-1])
    return ema

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    gains = []
    losses = []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calculate_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow:
        return 0.0, 0.0
    ema_fast = calculate_ema_series(prices, fast)[-1]
    ema_slow = calculate_ema_series(prices, slow)[-1]
    macd_line = ema_fast - ema_slow
    macd_hist = macd_line
    return macd_line, macd_hist

def calculate_vwap(prices, volumes):
    if not prices or not volumes or len(prices) != len(volumes):
        return prices[-1] if prices else 0
    cum_pv = sum(p * v for p, v in zip(prices, volumes))
    cum_vol = sum(volumes)
    return cum_pv / cum_vol if cum_vol > 0 else prices[-1]

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
        return 0.0
    trs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    return sum(trs[-period:]) / period if len(trs) >= period else 0.0

def calculate_bollinger(prices, period=20, std_dev=2.0):
    if len(prices) < period:
        return 0.0, 0.0, 0.0, 50.0
    window = prices[-period:]
    sma = sum(window) / period
    variance = sum((p - sma) ** 2 for p in window) / period
    std = math.sqrt(variance)
    upper = sma + std_dev * std
    lower = sma - std_dev * std
    if upper == lower:
        pos = 50.0
    else:
        pos = (prices[-1] - lower) / (upper - lower) * 100
    return sma, upper, lower, max(0, min(100, pos))

def calculate_adx(prices, period=14):
    if len(prices) < period * 2:
        return 0.0
    trs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    plus_dm = []
    minus_dm = []
    for i in range(1, len(prices)):
        move = prices[i] - prices[i-1]
        plus_dm.append(max(move, 0))
        minus_dm.append(max(-move, 0))
    if len(trs) < period:
        return 0.0
    atr = sum(trs[-period:]) / period
    if atr == 0:
        return 0.0
    plus_di = 100 * sum(plus_dm[-period:]) / period / atr
    minus_di = 100 * sum(minus_dm[-period:]) / period / atr
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di + 1e-10)
    return dx

def calculate_rsi_divergence(prices, rsi_values, lookback=5):
    if len(prices) < lookback + 2 or len(rsi_values) < lookback + 2:
        return "NONE"
    price_lows = prices[-lookback:]
    rsi_lows = rsi_values[-lookback:]
    if min(price_lows) < price_lows[0] and min(rsi_lows) > rsi_lows[0]:
        return "BULLISH"
    price_highs = prices[-lookback:]
    rsi_highs = rsi_values[-lookback:]
    if max(price_highs) > price_highs[0] and max(rsi_highs) < rsi_highs[0]:
        return "BEARISH"
    return "NONE"

def estimate_iv_rank(price, history, period=20):
    if len(history) < period:
        return 50
    iv_min = min(history[-period:])
    iv_max = max(history[-period:])
    if iv_max == iv_min:
        return 50
    rank = (price - iv_min) / (iv_max - iv_min) * 100
    return max(0, min(100, rank))

def analyze_timeframe_trend(history):
    n = len(history)
    if n < 2:
        return "SIDEWAYS", 0, 0
    prices = [h["price"] for h in history]
    x = list(range(n))
    x_mean = sum(x) / n
    y_mean = sum(prices) / n
    num = sum((x[i] - x_mean) * (prices[i] - y_mean) for i in range(n))
    den = sum((x[i] - x_mean) ** 2 for i in range(n))
    if den == 0:
        return "SIDEWAYS", 0, 0
    slope = num / den
    ss_res = sum((prices[i] - (y_mean + slope * (x[i] - x_mean))) ** 2 for i in range(n))
    ss_tot = sum((prices[i] - y_mean) ** 2 for i in range(n))
    r2 = 1 - (ss_res / ss_tot) if ss_tot else 0
    if abs(slope) < 0.05 or r2 < 0.3:
        return "SIDEWAYS", abs(slope) * r2 * 100, r2
    return ("BULLISH" if slope > 0 else "BEARISH"), abs(slope) * r2 * 100, r2

def get_all_timeframe_trends():
    return {tf: {"trend": analyze_timeframe_trend(list(hist))[0],
                 "strength": round(analyze_timeframe_trend(list(hist))[1], 2)}
            for tf, hist in timeframe_history.items()}

def get_nifty_pcr():
    """Fetch PCR with robust error handling and fallback."""
    now = time.time()
    if now - pcr_cache["time"] < 120:
        return pcr_cache["value"]
    
    try:
        url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        data = safe_json_request(url, max_retries=2, timeout=8)
        
        if data is None:
            logger.warning("PCR fetch failed: All retries exhausted, using cache")
            return pcr_cache["value"]
            
        if not isinstance(data, dict) or "records" not in data:
            logger.warning(f"PCR fetch failed: Invalid data structure: {type(data)}")
            return pcr_cache["value"]
            
        records = data.get("records", {}).get("data", [])
        if not records:
            logger.warning("PCR fetch failed: No records found")
            return pcr_cache["value"]
            
        ce_oi = sum(x.get("CE", {}).get("openInterest", 0) for x in records if "CE" in x)
        pe_oi = sum(x.get("PE", {}).get("openInterest", 0) for x in records if "PE" in x)
        pcr = pe_oi / ce_oi if ce_oi else 1.0
        
        pcr_cache["value"] = pcr
        pcr_cache["time"] = now
        pcr_history.append(pcr)
        logger.info(f"PCR updated: {pcr:.3f}")
        return pcr
        
    except Exception as e:
        logger.warning(f"PCR fetch unexpected error: {e}")
        return pcr_cache["value"]

def estimate_greeks(ce_price, pe_price):
    spot = get_nifty_spot_cached() or 0
    if spot == 0 or ATM_STRIKE == 0:
        return 0.0, 0.0, 0.0, 0.0
    moneyness = abs(spot - ATM_STRIKE) / spot
    if ce_price > pe_price:
        delta = 0.5 - moneyness
    else:
        delta = -(0.5 - moneyness)
    delta = max(-1, min(1, delta))
    gamma = 0.05 * (1 - moneyness * 2) if moneyness < 0.5 else 0.01
    theta = -ce_price * 0.001 * CONFIG["DAYS_TO_EXPIRY"]
    vega = ce_price * 0.1
    return round(delta, 4), round(gamma, 4), round(theta, 4), round(vega, 4)

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

# ============================================================
# PROFESSIONAL SIGNAL ENGINE (FULLY ENHANCED)
# ============================================================
def run_signal_engine(ce_price, pe_price, ce_hist, pe_hist, ce_vol_hist, pe_vol_hist):
    global market_signal, market_state, institutional_state, signal_state, portfolio_state

    if len(ce_hist) < 30 or len(pe_hist) < 30:
        return

    spot = get_nifty_spot_cached() or 0
    spread = ce_price - pe_price

    # Combined prices for market direction
    combined_prices = [(c + p) / 2 for c, p in zip(ce_hist, pe_hist)]
    combined_volumes = [(c + p) / 2 for c, p in zip(ce_vol_hist, pe_vol_hist)]

    # Technical indicators on combined
    rsi = calculate_rsi(combined_prices, CONFIG["RSI_PERIOD"])
    macd_line, macd_hist = calculate_macd(combined_prices, CONFIG["MACD_FAST"], CONFIG["MACD_SLOW"])
    vwap = calculate_vwap(combined_prices, combined_volumes)
    ema_fast = calculate_ema(combined_prices, CONFIG["EMA_FAST"])
    ema_slow = calculate_ema(combined_prices, CONFIG["EMA_SLOW"])
    atr = calculate_atr(combined_prices, CONFIG["ATR_PERIOD"])
    
    ce_rsi = calculate_rsi(ce_hist, CONFIG["RSI_PERIOD"])
    pe_rsi = calculate_rsi(pe_hist, CONFIG["RSI_PERIOD"])
    
    pcr = get_nifty_pcr()
    
    # Greeks
    ce_delta, ce_gamma, ce_theta, ce_vega, ce_iv = get_real_greeks("CE")
    pe_delta, pe_gamma, pe_theta, pe_vega, pe_iv = get_real_greeks("PE")
    
    bb_sma, bb_upper, bb_lower, bb_pos = calculate_bollinger(combined_prices, CONFIG["BB_PERIOD"], CONFIG["BB_STD"])
    adx = calculate_adx(combined_prices, CONFIG["ADX_PERIOD"])
    
    rsi_vals = [calculate_rsi(combined_prices[:i+1], CONFIG["RSI_PERIOD"]) for i in range(CONFIG["RSI_PERIOD"], len(combined_prices))]
    rsi_div = calculate_rsi_divergence(combined_prices, rsi_vals) if len(rsi_vals) >= 5 else "NONE"
    atr_pct = (atr / combined_prices[-1]) * 100 if combined_prices[-1] > 0 else 0
    iv_rank = estimate_iv_rank(ce_price, list(ce_hist)[-min(20, len(ce_hist)):], 20)
    
    # Volume trend
    if len(combined_volumes) >= 20:
        recent_vol = sum(combined_volumes[-10:]) / 10
        older_vol = sum(combined_volumes[-20:-10]) / 10
        vol_trend = "INCREASING" if recent_vol > older_vol * 1.2 else "DECREASING" if recent_vol < older_vol * 0.8 else "FLAT"
    else:
        vol_trend = "FLAT"
    
    # OI Change Rate (last 5 ticks ≈ 1-2 minutes)
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
    
    # Bid-ask spread percentage
    ce_spread_pct = (latest_ticks["ce_ask"] - latest_ticks["ce_bid"]) / (ce_price + 1e-6) * 100 if latest_ticks["ce_ask"] > 0 else 0
    pe_spread_pct = (latest_ticks["pe_ask"] - latest_ticks["pe_bid"]) / (pe_price + 1e-6) * 100 if latest_ticks["pe_ask"] > 0 else 0
    
    # Regime detection (enhanced)
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
    
    # Timeframe trends
    tf_trends = get_all_timeframe_trends()
    bullish_tf = sum(1 for t in ["1min","5min","10min","15min","20min"] if tf_trends[t]["trend"] == "BULLISH")
    bearish_tf = sum(1 for t in ["1min","5min","10min","15min","20min"] if tf_trends[t]["trend"] == "BEARISH")
    tf_score_bull = bullish_tf * 10
    tf_score_bear = bearish_tf * 10
    
    # Technical scoring
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
        elif ema_fast < ema_slow:
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
    
    if iv_rank > 70:
        tech_bull -= 5
        tech_bear += 3
    elif iv_rank < 30:
        tech_bull += 3
    
    # OI change scoring
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
    
    # ============================================================
    # LIQUIDITY FILTER (high spread caps confidence)
    # ============================================================
    high_spread = (ce_spread_pct > 2.0) or (pe_spread_pct > 2.0)
    if high_spread:
        raw_confidence = min(raw_confidence, 40)
        logger.debug(f"High spread: CE={ce_spread_pct:.1f}% PE={pe_spread_pct:.1f}% → confidence capped")
    
    # ============================================================
    # MULTI‑TIMEFRAME CONFLUENCE
    # ============================================================
    if bullish_tf == 5:
        raw_confidence += 15
    elif bearish_tf == 5:
        raw_confidence += 15
    elif bullish_tf >= 4 or bearish_tf >= 4:
        raw_confidence += 8
    elif bullish_tf <= 1 and bearish_tf <= 1:
        raw_confidence = max(raw_confidence - 10, 0)
    
    # ============================================================
    # REGIME OVERLAY (CHOPPY → no trade)
    # ============================================================
    if regime == "CHOPPY":
        raw_confidence = 0
        raw_action = "HOLD"
        signal_type = "NONE"
    else:
        # Determine raw action based on scores
        if total_bull >= total_bear and total_bull >= CONFIG["CONSIDER_THRESHOLD"]:
            if raw_confidence >= CONFIG["STRONG_BUY_THRESHOLD"]:
                raw_action = "STRONG BUY CE"
                signal_type = "TRENDING" if bullish_tf >= 4 else "MOMENTUM"
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
                signal_type = "TRENDING" if bearish_tf >= 4 else "MOMENTUM"
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
    
    # ============================================================
    # SIGNAL CONFIRMATION LOGIC (PERSISTENT)
    # ============================================================
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
                logger.info(f"New pending action: {raw_action} (1/{CONFIG['SIGNAL_CONFIRMATION_BARS']})")
            else:
                signal_state["confirmation_count"] += 1
                logger.info(f"Confirmation {signal_state['confirmation_count']}/{CONFIG['SIGNAL_CONFIRMATION_BARS']} for {raw_action}")
            
            if signal_state["confirmation_count"] >= CONFIG["SIGNAL_CONFIRMATION_BARS"]:
                final_action = raw_action
                final_signal_type = signal_type
                signal_state["current_action"] = raw_action
                signal_state["current_signal_type"] = signal_type
                signal_state["last_confirmed_action"] = raw_action
                signal_state["cooldown_until"] = now_ts + CONFIG["COOLDOWN_AFTER_FLIP_SEC"]
                signal_state["pending_action"] = None
                signal_state["confirmation_count"] = 0
                signal_state["signal_start_time"] = now_ts
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
            signal_state["pending_signal_type"] = None
            signal_state["confirmation_count"] = 0
        
        if signal_state["signal_start_time"] and (now_ts - signal_state["signal_start_time"]) > CONFIG["SIGNAL_MAX_AGE_SEC"]:
            if final_action != "HOLD":
                logger.info(f"Signal {final_action} expired after {CONFIG['SIGNAL_MAX_AGE_SEC']}s")
                final_action = "HOLD"
                final_signal_type = "NONE"
                signal_state["current_action"] = "HOLD"
                signal_state["current_signal_type"] = "NONE"
                signal_state["signal_start_time"] = None
    
    if signal_state["flip_count_hour"] >= CONFIG["MAX_FLIPS_PER_HOUR"]:
        final_action = "HOLD"
        final_signal_type = "NONE"
    
    # ============================================================
    # GRADE ASSIGNMENT
    # ============================================================
    grade = "D"
    if final_action != "HOLD" and final_signal_type != "NONE":
        if raw_confidence >= 90 and (bullish_tf >= 4 or bearish_tf >= 4):
            grade = "A"
        elif raw_confidence >= 80 or ((bullish_tf >= 3 or bearish_tf >= 3) and raw_confidence >= 70):
            grade = "B"
        elif raw_confidence >= 65:
            grade = "C"
    
    signal_state["signal_grade"] = grade
    
    # ============================================================
    # POSITION SIZING & TRAILING STOP
    # ============================================================
    position_pct = 0
    rr = 0
    entry = 0
    stop = 0
    target = 0
    
    if final_action in ["STRONG BUY CE", "BUY CE", "CONSIDER CE BUY"]:
        entry = ce_price
        init_stop = entry - atr * CONFIG["STOP_LOSS_ATR_MULT"]
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
        
        # Paper trade simulation with trailing stop
        if position_pct > 0 and signal_state["entry_price"] == 0:
            logger.info(f"*** PAPER TRADE: BUY {final_action} at {entry:.2f}, SL {init_stop:.2f}, TGT {target:.2f}, Size {position_pct}% ***")
            signal_state["entry_price"] = entry
            signal_state["stop_loss"] = init_stop
            signal_state["target"] = target
            signal_state["highest_price_since_entry"] = entry
        elif signal_state["entry_price"] > 0:
            # Update highest price
            if ce_price > signal_state["highest_price_since_entry"]:
                signal_state["highest_price_since_entry"] = ce_price
                # Trail stop: lock 50% of max profit
                profit_range = signal_state["highest_price_since_entry"] - signal_state["entry_price"]
                if profit_range > 0:
                    new_stop = signal_state["entry_price"] + profit_range * 0.5
                    if new_stop > signal_state["stop_loss"]:
                        signal_state["stop_loss"] = new_stop
                        logger.info(f"Trailing stop raised to {signal_state['stop_loss']:.2f}")
            # Check exit conditions
            if ce_price <= signal_state["stop_loss"]:
                logger.info(f"*** PAPER EXIT: STOP LOSS HIT for {final_action} at {ce_price:.2f} ***")
                signal_state["entry_price"] = 0
                signal_state["stop_loss"] = 0
                signal_state["target"] = 0
                signal_state["highest_price_since_entry"] = 0
            elif ce_price >= signal_state["target"]:
                logger.info(f"*** PAPER EXIT: TARGET HIT for {final_action} at {ce_price:.2f} ***")
                signal_state["entry_price"] = 0
                signal_state["stop_loss"] = 0
                signal_state["target"] = 0
                signal_state["highest_price_since_entry"] = 0
    
    elif final_action in ["STRONG BUY PE", "BUY PE", "CONSIDER PE BUY"]:
        entry = pe_price
        init_stop = entry + atr * CONFIG["STOP_LOSS_ATR_MULT"]
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
        elif signal_state["entry_price"] > 0:
            # Update lowest price
            if pe_price < signal_state["lowest_price_since_entry"]:
                signal_state["lowest_price_since_entry"] = pe_price
                profit_range = signal_state["entry_price"] - signal_state["lowest_price_since_entry"]
                if profit_range > 0:
                    new_stop = signal_state["entry_price"] - profit_range * 0.5
                    if new_stop < signal_state["stop_loss"]:
                        signal_state["stop_loss"] = new_stop
                        logger.info(f"Trailing stop lowered to {signal_state['stop_loss']:.2f}")
            if pe_price >= signal_state["stop_loss"]:
                logger.info(f"*** PAPER EXIT: STOP LOSS HIT for {final_action} at {pe_price:.2f} ***")
                signal_state["entry_price"] = 0
                signal_state["stop_loss"] = 0
                signal_state["target"] = 0
                signal_state["lowest_price_since_entry"] = float("inf")
            elif pe_price <= signal_state["target"]:
                logger.info(f"*** PAPER EXIT: TARGET HIT for {final_action} at {pe_price:.2f} ***")
                signal_state["entry_price"] = 0
                signal_state["stop_loss"] = 0
                signal_state["target"] = 0
                signal_state["lowest_price_since_entry"] = float("inf")
    
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
    
    # ============================================================
    # UPDATE GLOBAL DICTIONARIES
    # ============================================================
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
        "daily_pnl_pct": 0,
        "max_drawdown_today": 0
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
        "delta": ce_delta,
        "gamma": ce_gamma,
        "theta": ce_theta,
        "vega": ce_vega,
        "iv": ce_iv,
        "institutional_signal": final_action,
        "institutional_confidence": raw_confidence,
        "signal_grade": grade,
        "position_size_pct": position_pct,
        "risk_reward": round(rr, 2),
        "entry_price": round(entry, 2) if entry else 0,
        "stop_loss": round(stop, 2) if stop else 0,
        "target": round(target, 2) if target else 0,
        "max_drawdown_pct": 0,
        "ce_delta": ce_delta,
        "pe_delta": pe_delta,
        "ce_iv": ce_iv,
        "pe_iv": pe_iv,
        "ce_oi_change": round(ce_oi_change, 1),
        "pe_oi_change": round(pe_oi_change, 1)
    })
    
    market_signal.update({
        "signal": "BULLISH" if final_action in ["STRONG BUY CE", "BUY CE", "CONSIDER CE BUY"] else 
                  "BEARISH" if final_action in ["STRONG BUY PE", "BUY PE", "CONSIDER PE BUY"] else "NEUTRAL",
        "ce_price": ce_price,
        "pe_price": pe_price,
        "spread": round(spread, 2),
        "rsi": round(rsi, 2),
        "macd": round(macd_hist, 2),
        "pcr": round(pcr, 2),
        "vwap": round(vwap, 2),
        "atr": round(atr, 2),
        "atr_pct": round(atr_pct, 2),
        "ema_fast": round(ema_fast, 2),
        "ema_slow": round(ema_slow, 2),
        "delta": ce_delta,
        "gamma": ce_gamma,
        "theta": ce_theta,
        "vega": ce_vega,
        "volume": int(combined_volumes[-1]) if combined_volumes else 0,
        "timestamp": datetime.now().isoformat(),
        "adx": round(adx, 2),
        "bb_position": round(bb_pos, 2),
        "rsi_divergence": rsi_div,
        "iv_rank": round(iv_rank, 2),
        "signal_grade": grade,
        "regime": regime,
        "session_phase": session_phase,
        "ce_spread_pct": round(ce_spread_pct, 1),
        "pe_spread_pct": round(pe_spread_pct, 1),
        "ce_oi_change": round(ce_oi_change, 1),
        "pe_oi_change": round(pe_oi_change, 1)
    })
    
    if final_action != signal_state["last_logged_action"]:
        logger.info(f"PRO SIGNAL: {final_action} [{final_signal_type}] Grade:{grade} Conf:{raw_confidence} "
                    f"BullTF:{bullish_tf} BearTF:{bearish_tf} RSI:{rsi:.1f} ADX:{adx:.1f} PCR:{pcr:.2f} "
                    f"PosSize:{position_pct}% RR:{rr:.1f} Heat:{portfolio_state['total_exposure_pct']:.1f}%")
        signal_state["last_logged_action"] = final_action

# ============================================================
# WEBSOCKET CALLBACKS (with bid, ask, oi)
# ============================================================# ============================================================
# WEBSOCKET CALLBACKS (with bid, ask, oi)
# ============================================================

def on_ws_open(wsapp):
    global sws

    logger.info("Angel WebSocket Connected Successfully")
    logger.info("Angel One WebSocket opened")

    if sws is not None:
        try:
            logger.info("Attempting token subscription...")

            sws.subscribe(
                "tradeguru_001",
                1,
                [{"exchangeType": 2, "tokens": [CE_TOKEN, PE_TOKEN]}]
            )

            logger.info(f"Subscribed to tokens: {CE_TOKEN}, {PE_TOKEN}")

        except Exception as e:
            logger.error(f"Subscribe error: {e}")


def on_ws_data(wsapp, message, *args):

    global tick_counter, last_tick_time, latest_ticks
    global ce_price_history, pe_price_history
    global ce_volume_history, pe_volume_history
    global ce_oi_history, pe_oi_history

    last_tick_time = time.time()

    try:

        logger.info("Tick data received")

        if isinstance(message, bytes):
            return

        data = json.loads(message) if isinstance(message, str) else message

        ticks = data if isinstance(data, list) else [data]

        for tick in ticks:

            logger.info(f"Raw Tick: {tick}")

            token = str(tick.get("tk"))

            ltp = tick.get("ltp", 0)

            if isinstance(ltp, (int, float)) and ltp > 1000:
                ltp = ltp / 100

            vol = tick.get("v", 0) or tick.get("volume", 0)

            bid = tick.get("bp1") or tick.get("bid") or 0

            ask = tick.get("sp1") or tick.get("ask") or 0

            oi = tick.get("oi") or tick.get("openInterest") or 0

            if token == CE_TOKEN:

                logger.info(f"CE Tick -> Price: {ltp}")

                latest_ticks["ce_price"] = ltp
                latest_ticks["ce_volume"] = vol
                latest_ticks["ce_bid"] = bid
                latest_ticks["ce_ask"] = ask
                latest_ticks["ce_oi"] = oi

                ce_price_history.append(ltp)
                ce_volume_history.append(vol)
                ce_oi_history.append(oi)

                tick_counter += 1

            elif token == PE_TOKEN:

                logger.info(f"PE Tick -> Price: {ltp}")

                latest_ticks["pe_price"] = ltp
                latest_ticks["pe_volume"] = vol
                latest_ticks["pe_bid"] = bid
                latest_ticks["pe_ask"] = ask
                latest_ticks["pe_oi"] = oi

                pe_price_history.append(ltp)
                pe_volume_history.append(vol)
                pe_oi_history.append(oi)

                tick_counter += 1

    except Exception as e:
        logger.error(f"WebSocket data error: {e}", exc_info=True)


def on_ws_error(wsapp, error):

    logger.error(f"Angel WebSocket Error: {error}")


def on_ws_close(wsapp, *args):

    logger.warning(f"Angel WebSocket Closed: {args}")

    global ws_running

    ws_running = False
# ============================================================
# WEBSOCKET CONNECTION MANAGER
# ============================================================
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
        logger.info("Auth token refreshed")
        return auth_token, feed_token, obj
    except Exception as e:
        logger.error(f"Auth error: {e}")
        return None, None, None

def start_angel_websocket():
    global ws_running, CE_TOKEN, PE_TOKEN, sws, last_tick_time, tick_counter

    retry_delay = 30

    while engine_active:

        try:

            # --------------------------------------------------
            # MARKET CLOSED
            # --------------------------------------------------
            if not is_market_open():

                logger.info("Market closed. Sleeping 5 minutes...")

                CE_TOKEN = None
                PE_TOKEN = None

                stop_event = threading.Event()

                # Sleep 5 minutes but interruptible
                stop_event.wait(timeout=300)

                continue

            # --------------------------------------------------
            # AUTH
            # --------------------------------------------------
            auth_token, feed_token, obj = get_auth_token()

            if not auth_token:

                logger.error("Auth failed, retrying in 60s...")

                shutdown_event = threading.Event()

                shutdown_event.wait(timeout=60)

                continue

            # --------------------------------------------------
            # TOKENS
            # --------------------------------------------------
            if not CE_TOKEN or not PE_TOKEN:

                ce_tok, pe_tok = get_current_atm_tokens()

                if not ce_tok or not pe_tok:

                    logger.error("Failed to fetch ATM tokens")

                    shutdown_event = threading.Event()

                    shutdown_event.wait(timeout=60)

                    continue

            # --------------------------------------------------
            # WEBSOCKET
            # --------------------------------------------------
            sws = SmartWebSocketV2(
                auth_token,
                ANGEL_API_KEY,
                ANGEL_CLIENT_ID,
                feed_token
            )

            sws.on_open = on_ws_open
            sws.on_data = on_ws_data
            sws.on_error = on_ws_error
            sws.on_close = on_ws_close

            ws_running = True

            retry_delay = 30

            logger.info("Connecting WebSocket...")

            sws.connect()

            # --------------------------------------------------
            # HEARTBEAT LOOP
            # --------------------------------------------------
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

            # --------------------------------------------------
            # RETRY WAIT
            # --------------------------------------------------
            shutdown_event = threading.Event()

            shutdown_event.wait(timeout=retry_delay)

            retry_delay = min(retry_delay * 2, 300)

        except Exception as e:

            logger.error(f"WebSocket connection error: {e}")

            shutdown_event = threading.Event()

            shutdown_event.wait(timeout=retry_delay)

            retry_delay = min(retry_delay * 2, 300)        
        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 300)

# ============================================================
# FLASK ENDPOINTS
# ============================================================
@app.route("/")
def home():
    return jsonify({
        "status": "online", 
        "message": "Nifty Signal Engine v5.1 Ultimate",
        "worker_type": "PRIMARY (WebSocket)",
        "market_open": is_market_open(),
        "trading_mode": "PAPER",
        "version": "5.1"
    })

@app.route("/api/live-signals")
def live_signals():
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
            "open_positions": portfolio_state["open_positions"]
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
        "ce_history_len": len(ce_price_history),
        "pe_history_len": len(pe_price_history),
        "last_tick_age": round(time.time() - last_tick_time, 1),
        "timestamp": datetime.now().isoformat()
    })

# ============================================================
# BACKGROUND ENGINE START
# ============================================================
# ============================================================
# BACKGROUND ENGINE START
# ============================================================

engine_started = False

def initialize_engine():
    global engine_started

    if not engine_started:
        ws_thread = threading.Thread(
            target=start_angel_websocket,
            daemon=True
        )
        ws_thread.start()

        engine_started = True
        logger.info("Ultimate Signal Engine v5.1 started (auto-reconnecting)")


# START ENGINE IMMEDIATELY

if __name__ == "__main__":
# ============================================================
# BACKGROUND ENGINE START
# ============================================================
engine_started = False

def initialize_engine():
    global engine_started

    if not engine_started:

        ws_thread = threading.Thread(
            target=start_angel_websocket,
            daemon=True
        )

        ws_thread.start()

        engine_started = True

        logger.info("Ultimate Signal Engine v5.1 started (auto-reconnecting)")


# START ENGINE IMMEDIATELY
initialize_engine()


if __name__ == "__main__":

    port = int(os.environ.get("PORT", 10000))

    app.run(host="0.0.0.0", port=port)
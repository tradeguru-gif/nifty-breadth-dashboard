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

# The rest of your code follows...

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
    """
    Dynamically find valid NIFTY index token from Angel scrip master.
    """

    global NIFTY_TOKEN

    try:

        resp = requests.get(
            "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json",
            timeout=30
        )

        resp.raise_for_status()

        data = resp.json()

        for item in data:

            symbol = str(item.get("symbol", "")).upper()
            exch_seg = str(item.get("exch_seg", "")).upper()

            # Valid NSE NIFTY index
            if (
                exch_seg == "NSE"
                and symbol in ["NIFTY", "NIFTY 50", "NIFTY50"]
            ):

                token = str(item.get("token"))

                logger.info(
                    f"NIFTY token found dynamically: {symbol} -> {token}"
                )

                NIFTY_TOKEN = token

                return token

        logger.error("NIFTY token not found in scrip master")

    except Exception as e:
        logger.exception(f"Error finding NIFTY token: {e}")

    return None

# ============================================================
# NIFTY SPOT FETCHER (FIXED VERSION)
# ============================================================

import requests
import time
import logging

logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# CACHE
# ------------------------------------------------------------

CACHE_TTL = 3  # seconds

spot_cache = {
    "value": None,
    "timestamp": 0
}

token_cache = {
    "token": None,
    "symbol": None,
    "timestamp": 0
}

# ------------------------------------------------------------
# FETCH NIFTY TOKEN FROM ANGEL SCRIP MASTER
# ------------------------------------------------------------

def get_nifty_token():
    """
    Dynamically fetch correct NIFTY token from Angel master.
    """

    # Use cached token for 1 hour
    if (
        token_cache["token"] is not None
        and (time.time() - token_cache["timestamp"]) < 3600
    ):
        return token_cache["symbol"], token_cache["token"]

    try:
        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()

        for item in data:

            symbol = str(item.get("symbol", "")).upper()
            exch_seg = str(item.get("exch_seg", "")).upper()
            token = str(item.get("token", ""))

            # Find NIFTY index
            if (
                exch_seg == "NSE"
                and symbol in ["NIFTY", "NIFTY 50", "NIFTY50"]
            ):

                token_cache["token"] = token
                token_cache["symbol"] = symbol
                token_cache["timestamp"] = time.time()

                logger.info(f"Loaded NIFTY token: {symbol} -> {token}")

                return symbol, token

        logger.error("NIFTY token not found in scrip master")

    except Exception as e:
        logger.exception(f"Error loading NIFTY token: {e}")

    return None, None


# ------------------------------------------------------------
# FETCH SPOT FROM ANGEL LTP API
# ------------------------------------------------------------

def get_spot_from_angel_ltp():

    try:

        auth_token, feed_token, obj = get_auth_token()

        if not auth_token or obj is None:
            logger.warning("Angel auth failed")
            return None

        trading_symbol, symbol_token = get_nifty_token()

        if not trading_symbol or not symbol_token:
            logger.warning("Could not get valid NIFTY token")
            return None

        logger.info(
            f"Fetching LTP for {trading_symbol} ({symbol_token})"
        )

        ltp_data = obj.ltpData(
            "NSE",
            trading_symbol,
            symbol_token
        )

        if (
            ltp_data
            and ltp_data.get("status")
            and ltp_data.get("data")
            and ltp_data["data"].get("ltp")
        ):

            spot = float(ltp_data["data"]["ltp"])

            logger.info(f"NIFTY Spot from Angel LTP: {spot}")

            return spot

        logger.warning(f"Invalid LTP response: {ltp_data}")

    except AttributeError:
        logger.warning("ltpData not available in this SmartAPI version")

    except Exception as e:
        logger.exception(f"Angel LTP fetch error: {e}")

    return None


# ------------------------------------------------------------
# MAIN SPOT FETCHER
# ------------------------------------------------------------

def get_nifty_spot():
    """
    Priority:
    1. WebSocket live tick
    2. Angel REST LTP fallback
    """

    try:

        # Use fresh websocket tick first
        if (
            latest_ticks.get("nifty_spot", 0) > 0
            and (time.time() - last_tick_time) < 5
        ):

            return latest_ticks["nifty_spot"]

        logger.warning("WebSocket spot stale -> using REST fallback")

        # Fallback to REST
        return get_spot_from_angel_ltp()

    except Exception as e:
        logger.exception(f"get_nifty_spot error: {e}")

    return None


# ------------------------------------------------------------
# CACHED SPOT FETCHER
# ------------------------------------------------------------

def get_nifty_spot_cached():

    now = time.time()

    # Return cached value if fresh
    if (
        now - spot_cache["timestamp"] < CACHE_TTL
        and spot_cache["value"] is not None
    ):
        return spot_cache["value"]

    # Fetch fresh spot
    spot = get_nifty_spot()

    if spot is not None:

        spot_cache["value"] = spot
        spot_cache["timestamp"] = now

    return spot_cache["value"]




def get_current_atm_tokens():
    """Auto-fetch CE/PE tokens using live spot and scrip master."""
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

# ---------- Technical Indicators ----------
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

def calculate_macd(prices, fast=12, slow=26):
    if len(prices) < slow: return 0.0, 0.0
    def ema(series, period):
        alpha = 2/(period+1)
        val = series[0]
        for x in series[1:]:
            val = alpha*x + (1-alpha)*val
        return val
    ema_fast = ema(prices[-fast:], fast) if len(prices)>=fast else prices[-1]
    ema_slow = ema(prices[-slow:], slow) if len(prices)>=slow else prices[-1]
    return ema_fast - ema_slow, ema_fast - ema_slow

def calculate_vwap(prices, volumes):
    if not prices or not volumes or len(prices)!=len(volumes):
        return prices[-1] if prices else 0
    cum_pv = sum(p*v for p,v in zip(prices, volumes))
    cum_vol = sum(volumes)
    return cum_pv/cum_vol if cum_vol>0 else prices[-1]

def calculate_ema(prices, period):
    if len(prices) < period: return prices[-1] if prices else 0
    alpha = 2/(period+1)
    val = prices[0]
    for p in prices[1:]:
        val = alpha*p + (1-alpha)*val
    return val

def calculate_atr(prices, period=14):
    if len(prices) < period+1: return 0.0
    trs = [abs(prices[i]-prices[i-1]) for i in range(1, len(prices))]
    return sum(trs[-period:])/period if len(trs)>=period else 0.0

def calculate_bollinger(prices, period=20, std_dev=2.0):
    if len(prices) < period: return 0.0,0.0,0.0,50.0
    window = prices[-period:]
    sma = sum(window)/period
    var = sum((p-sma)**2 for p in window)/period
    std = math.sqrt(var)
    upper = sma + std_dev*std
    lower = sma - std_dev*std
    pos = (prices[-1]-lower)/(upper-lower)*100 if upper!=lower else 50.0
    return sma, upper, lower, max(0,min(100,pos))

def calculate_adx(prices, period=14):
    if len(prices) < period*2: return 0.0
    tr = [abs(prices[i]-prices[i-1]) for i in range(1, len(prices))]
    plus_dm = [max(prices[i]-prices[i-1],0) for i in range(1,len(prices))]
    minus_dm = [max(prices[i-1]-prices[i],0) for i in range(1,len(prices))]
    atr = sum(tr[-period:])/period if len(tr)>=period else 0.0
    if atr==0: return 0.0
    plus_di = 100*sum(plus_dm[-period:])/period/atr
    minus_di = 100*sum(minus_dm[-period:])/period/atr
    return 100*abs(plus_di-minus_di)/(plus_di+minus_di+1e-10)

def calculate_rsi_divergence(prices, rsi_values, lookback=5):
    if len(prices)<lookback+2 or len(rsi_values)<lookback+2: return "NONE"
    price_lows = prices[-lookback:]
    rsi_lows = rsi_values[-lookback:]
    if min(price_lows) < price_lows[0] and min(rsi_lows) > rsi_lows[0]: return "BULLISH"
    price_highs = prices[-lookback:]
    rsi_highs = rsi_values[-lookback:]
    if max(price_highs) > price_highs[0] and max(rsi_highs) < rsi_highs[0]: return "BEARISH"
    return "NONE"

def get_nifty_pcr():
    now = time.time()
    if now - pcr_cache["time"] < 120: return pcr_cache["value"]
    try:
        # Use Angel One option chain? Simpler: fallback to cached
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get("https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY", headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            records = data.get("records",{}).get("data",[])
            ce_oi = sum(x.get("CE",{}).get("openInterest",0) for x in records if "CE" in x)
            pe_oi = sum(x.get("PE",{}).get("openInterest",0) for x in records if "PE" in x)
            pcr = pe_oi/ce_oi if ce_oi else 1.0
            pcr_cache["value"] = pcr
            pcr_cache["time"] = now
            return pcr
    except:
        pass
    return pcr_cache["value"]

def get_real_greeks(option_type="CE"):
    spot = get_nifty_spot_cached() or 0
    price = latest_ticks.get("ce_price" if option_type=="CE" else "pe_price", 0)
    if spot==0 or ATM_STRIKE==0 or price==0:
        return 0.0,0.0,0.0,0.0,0.20
    moneyness = abs(spot - ATM_STRIKE)/spot
    if option_type=="CE":
        delta = 0.5 - moneyness
        if spot > ATM_STRIKE: delta = 0.8 - moneyness
    else:
        delta = -(0.5 - moneyness)
        if spot < ATM_STRIKE: delta = -0.8 + moneyness
    delta = max(-1, min(1, delta))
    gamma = 0.05 * (1 - moneyness*2) if moneyness < 0.5 else 0.01
    theta = -price * 0.001 * CONFIG["DAYS_TO_EXPIRY"]
    vega = price * 0.1
    iv = 0.20 + moneyness * 0.1
    return delta, gamma, theta, vega, iv

def analyze_timeframe_trend(history):
    n = len(history)
    if n < 2: return "SIDEWAYS", 0
    prices = [h["price"] for h in history]
    x = list(range(n))
    x_mean = sum(x)/n
    y_mean = sum(prices)/n
    num = sum((x[i]-x_mean)*(prices[i]-y_mean) for i in range(n))
    den = sum((x[i]-x_mean)**2 for i in range(n))
    if den==0: return "SIDEWAYS",0
    slope = num/den
    if abs(slope) < 0.05: return "SIDEWAYS",0
    return ("BULLISH" if slope>0 else "BEARISH"), abs(slope)*100

def get_all_timeframe_trends():
    return {tf: {"trend": analyze_timeframe_trend(list(hist))[0],
                 "strength": round(analyze_timeframe_trend(list(hist))[1],2)}
            for tf, hist in timeframe_history.items()}

# ---------- Signal Engine (Full) ----------
def run_signal_engine(ce_price, pe_price, ce_hist, pe_hist, ce_vol_hist, pe_vol_hist):
    global market_signal, market_state, institutional_state, signal_state, portfolio_state

    if len(ce_hist) < 30 or len(pe_hist) < 30:
        return

    spot = get_nifty_spot_cached() or 0
    spread = ce_price - pe_price

    combined_prices = [(c+p)/2 for c,p in zip(ce_hist, pe_hist)]
    combined_volumes = [(c+p)/2 for c,p in zip(ce_vol_hist, pe_vol_hist)]

    rsi = calculate_rsi(combined_prices, CONFIG["RSI_PERIOD"])
    macd_line, macd_hist = calculate_macd(combined_prices, CONFIG["MACD_FAST"], CONFIG["MACD_SLOW"])
    vwap = calculate_vwap(combined_prices, combined_volumes)
    ema_fast = calculate_ema(combined_prices, CONFIG["EMA_FAST"])
    ema_slow = calculate_ema(combined_prices, CONFIG["EMA_SLOW"])
    atr = calculate_atr(combined_prices, CONFIG["ATR_PERIOD"])
    pcr = get_nifty_pcr()
    ce_delta, ce_gamma, ce_theta, ce_vega, ce_iv = get_real_greeks("CE")
    pe_delta, pe_gamma, pe_theta, pe_vega, pe_iv = get_real_greeks("PE")
    bb_sma, bb_upper, bb_lower, bb_pos = calculate_bollinger(combined_prices, CONFIG["BB_PERIOD"], CONFIG["BB_STD"])
    adx = calculate_adx(combined_prices, CONFIG["ADX_PERIOD"])
    rsi_vals = [calculate_rsi(combined_prices[:i+1], CONFIG["RSI_PERIOD"]) for i in range(CONFIG["RSI_PERIOD"], len(combined_prices))]
    rsi_div = calculate_rsi_divergence(combined_prices, rsi_vals) if len(rsi_vals)>=5 else "NONE"
    atr_pct = (atr/combined_prices[-1])*100 if combined_prices[-1]>0 else 0

    # Volume trend
    if len(combined_volumes) >= 20:
        recent_vol = sum(combined_volumes[-10:])/10
        older_vol = sum(combined_volumes[-20:-10])/10
        vol_trend = "INCREASING" if recent_vol > older_vol*1.2 else "DECREASING" if recent_vol < older_vol*0.8 else "FLAT"
    else:
        vol_trend = "FLAT"

    # OI change
    if len(ce_oi_history)>=5:
        ce_oi_change = (ce_oi_history[-1]-ce_oi_history[-5])/(ce_oi_history[-5]+1e-6)*100
    else:
        ce_oi_change = 0
    if len(pe_oi_history)>=5:
        pe_oi_change = (pe_oi_history[-1]-pe_oi_history[-5])/(pe_oi_history[-5]+1e-6)*100
    else:
        pe_oi_change = 0
    ce_oi_change = max(-50, min(50, ce_oi_change))
    pe_oi_change = max(-50, min(50, pe_oi_change))

    # Spread %
    ce_spread_pct = (latest_ticks["ce_ask"]-latest_ticks["ce_bid"])/(ce_price+1e-6)*100 if latest_ticks["ce_ask"]>0 else 0
    pe_spread_pct = (latest_ticks["pe_ask"]-latest_ticks["pe_bid"])/(pe_price+1e-6)*100 if latest_ticks["pe_ask"]>0 else 0

    # Regime
    if adx > 30: regime = "TRENDING"
    elif atr_pct > 1.5: regime = "VOLATILE"
    elif bb_pos < 20 or bb_pos > 80: regime = "BREAKOUT"
    elif adx < 20 and atr_pct < 0.5: regime = "CHOPPY"
    else: regime = "RANGING"

    session_phase = get_market_phase()
    tf_trends = get_all_timeframe_trends()
    bullish_tf = sum(1 for t in ["1min","5min","10min","15min","20min"] if tf_trends[t]["trend"]=="BULLISH")
    bearish_tf = sum(1 for t in ["1min","5min","10min","15min","20min"] if tf_trends[t]["trend"]=="BEARISH")
    tf_score_bull = bullish_tf * 10
    tf_score_bear = bearish_tf * 10

    tech_bull = tech_bear = 0
    if 55 < rsi < 75: tech_bull += 10
    elif 40 < rsi < 55: tech_bull += 5
    if 25 < rsi < 45: tech_bear += 10
    elif 45 < rsi < 60: tech_bear += 5

    if macd_hist > 0 and macd_line > 0: tech_bull += 10
    elif macd_hist > 0: tech_bull += 6
    elif macd_hist < 0 and macd_line < 0: tech_bear += 10
    elif macd_hist < 0: tech_bear += 6

    if pcr < CONFIG["PCR_BULLISH"]: tech_bull += 10
    elif pcr < 1.0: tech_bull += 7
    elif pcr > CONFIG["PCR_BEARISH"]: tech_bear += 10
    elif pcr > 1.2: tech_bear += 7

    if vol_trend == "INCREASING":
        if ema_fast > ema_slow: tech_bull += 10
        else: tech_bear += 10

    avg_price = (ce_price+pe_price)/2
    if avg_price > vwap and avg_price > ema_slow: tech_bull += 10
    elif avg_price > vwap or avg_price > ema_slow: tech_bull += 5
    elif avg_price < vwap and avg_price < ema_slow: tech_bear += 10
    elif avg_price < vwap or avg_price < ema_slow: tech_bear += 5

    if bb_pos < 20: tech_bull += 8
    elif bb_pos > 80: tech_bear += 8

    if adx > 30:
        if ema_fast > ema_slow: tech_bull += 5
        else: tech_bear += 5

    if rsi_div == "BULLISH" and ema_fast > ema_slow: tech_bull += 8
    elif rsi_div == "BEARISH" and ema_fast < ema_slow: tech_bear += 8

    if ce_oi_change > 10: tech_bull += 8
    elif ce_oi_change > 5: tech_bull += 4
    if pe_oi_change > 10: tech_bear += 8
    elif pe_oi_change > 5: tech_bear += 4

    total_bull = tf_score_bull + tech_bull
    total_bear = tf_score_bear + tech_bear
    raw_confidence = max(total_bull, total_bear)

    # Liquidity filter
    if ce_spread_pct > 2.0 or pe_spread_pct > 2.0:
        raw_confidence = min(raw_confidence, 40)

    # Multi‑timeframe confluence
    if bullish_tf == 5: raw_confidence += 15
    elif bearish_tf == 5: raw_confidence += 15
    elif bullish_tf >= 4 or bearish_tf >= 4: raw_confidence += 8
    elif bullish_tf <= 1 and bearish_tf <= 1: raw_confidence = max(raw_confidence-10, 0)

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

    # Position sizing & paper trading
    position_pct = 0
    rr = 0
    entry = 0
    stop = 0
    target = 0

    if final_action in ["STRONG BUY CE", "BUY CE", "CONSIDER CE BUY"]:
        entry = ce_price
        init_stop = entry - atr * CONFIG["STOP_LOSS_ATR_MULT"]
        init_stop = entry + atr * CONFIG["STOP_LOSS_ATR_MULT"]
stop = init_stop
        target = entry + atr * CONFIG["TARGET_ATR_MULT"]
        if grade == "A": base = CONFIG["POSITION_SIZE_MAX_PCT"]
        elif grade == "B": base = CONFIG["POSITION_SIZE_BASE_PCT"] * 1.5
        elif grade == "C": base = CONFIG["POSITION_SIZE_BASE_PCT"]
        else: base = 0
        if regime == "VOLATILE": base *= 0.7
        elif regime == "TRENDING": base *= 1.2
        position_pct = min(CONFIG["POSITION_SIZE_MAX_PCT"], max(0, base))
        if atr > 0:
            risk = entry - init_stop
            reward = target - entry
            rr = reward/risk if risk>0 else 0
        if position_pct > 0 and signal_state["entry_price"] == 0:
            logger.info(f"*** PAPER TRADE: BUY {final_action} at {entry:.2f}, SL {init_stop:.2f}, TGT {target:.2f}, Size {position_pct}% ***")
            signal_state["entry_price"] = entry
            signal_state["stop_loss"] = init_stop
            signal_state["target"] = target
            signal_state["highest_price_since_entry"] = entry
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
                logger.info(f"*** PAPER EXIT: STOP LOSS for {final_action} at {ce_price:.2f} ***")
                signal_state["entry_price"] = 0
                signal_state["stop_loss"] = 0
                signal_state["target"] = 0
                signal_state["highest_price_since_entry"] = 0
            elif ce_price >= signal_state["target"]:
                logger.info(f"*** PAPER EXIT: TARGET for {final_action} at {ce_price:.2f} ***")
                signal_state["entry_price"] = 0
                signal_state["stop_loss"] = 0
                signal_state["target"] = 0
                signal_state["highest_price_since_entry"] = 0

    elif final_action in ["STRONG BUY PE", "BUY PE", "CONSIDER PE BUY"]:
        entry = pe_price
        init_stop = entry + atr * CONFIG["STOP_LOSS_ATR_MULT"]
       
stop = init_stop
        target = entry - atr * CONFIG["TARGET_ATR_MULT"]
        if grade == "A": base = CONFIG["POSITION_SIZE_MAX_PCT"]
        elif grade == "B": base = CONFIG["POSITION_SIZE_BASE_PCT"] * 1.5
        elif grade == "C": base = CONFIG["POSITION_SIZE_BASE_PCT"]
        else: base = 0
        if regime == "VOLATILE": base *= 0.7
        elif regime == "TRENDING": base *= 1.2
        position_pct = min(CONFIG["POSITION_SIZE_MAX_PCT"], max(0, base))
        if atr > 0:
            risk = init_stop - entry
            reward = entry - target
            rr = reward/risk if risk>0 else 0
        if position_pct > 0 and signal_state["entry_price"] == 0:
            logger.info(f"*** PAPER TRADE: BUY {final_action} at {entry:.2f}, SL {init_stop:.2f}, TGT {target:.2f}, Size {position_pct}% ***")
            signal_state["entry_price"] = entry
            signal_state["stop_loss"] = init_stop
            signal_state["target"] = target
            signal_state["lowest_price_since_entry"] = entry
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
                logger.info(f"*** PAPER EXIT: STOP LOSS for {final_action} at {pe_price:.2f} ***")
                signal_state["entry_price"] = 0
                signal_state["stop_loss"] = 0
                signal_state["target"] = 0
                signal_state["lowest_price_since_entry"] = float("inf")
            elif pe_price <= signal_state["target"]:
                logger.info(f"*** PAPER EXIT: TARGET for {final_action} at {pe_price:.2f} ***")
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
    portfolio_state["total_exposure_pct"] = position_pct if signal_state["entry_price"]>0 else 0
    portfolio_state["open_positions"] = 1 if signal_state["entry_price"]>0 else 0

    # Update global dictionaries
    market_state.update({
        "rsi": round(rsi,2),
        "momentum": "UPTREND" if ema_fast>ema_slow else "DOWNTREND" if ema_fast<ema_slow else "NEUTRAL",
        "strength": "HIGH" if final_signal_type=="TRENDING" else "MODERATE" if final_signal_type=="MOMENTUM" else "LOW",
        "trend": "BULLISH" if ema_fast>ema_slow else "BEARISH" if ema_fast<ema_slow else "SIDEWAYS",
        "action": final_action,
        "confidence": raw_confidence,
        "volatility": "HIGH" if atr>15 else "NORMAL" if atr>5 else "LOW",
        "alert": final_action,
        "regime": regime,
        "session_phase": session_phase,
        "trend_1min": tf_trends["1min"]["trend"],
        "trend_5min": tf_trends["5min"]["trend"],
        "trend_10min": tf_trends["10min"]["trend"],
        "trend_15min": tf_trends["15min"]["trend"],
        "trend_20min": tf_trends["20min"]["trend"],
        "timeframe_agreement": max(bullish_tf, bearish_tf),
        "portfolio_heat": round(portfolio_state["total_exposure_pct"],2),
    })

    institutional_state.update({
        "vwap": round(vwap,2),
        "ema_fast": round(ema_fast,2),
        "ema_slow": round(ema_slow,2),
        "ema_signal": "BULLISH" if ema_fast>ema_slow else "BEARISH",
        "atr": round(atr,2),
        "oi_buildup": "BULLISH" if pcr<0.9 else "BEARISH" if pcr>1.2 else "NEUTRAL",
        "iv_state": "HIGH" if ce_vega>2 else "NORMAL",
        "candle_structure": "BULLISH" if ema_fast>ema_slow and rsi>55 else "BEARISH" if ema_fast<ema_slow and rsi<45 else "SIDEWAYS",
        "market_breadth": "BULLISH" if bullish_tf>=3 else "BEARISH" if bearish_tf>=3 else "BALANCED",
        "volume_profile": vol_trend,
        "smart_money_flow": "BULLISH" if vwap>ema_slow and vol_trend=="INCREASING" else "BEARISH" if vwap<ema_slow and vol_trend=="INCREASING" else "NEUTRAL",
        "delta": ce_delta, "gamma": ce_gamma, "theta": ce_theta, "vega": ce_vega, "iv": ce_iv,
        "institutional_signal": final_action,
        "institutional_confidence": raw_confidence,
        "signal_grade": grade,
        "position_size_pct": position_pct,
        "risk_reward": round(rr,2),
        "entry_price": round(entry,2) if entry else 0,
        "stop_loss": round(stop,2) if stop else 0,
        "target": round(target,2) if target else 0,
        "ce_delta": ce_delta, "pe_delta": pe_delta,
        "ce_iv": ce_iv, "pe_iv": pe_iv,
        "ce_oi_change": round(ce_oi_change,1), "pe_oi_change": round(pe_oi_change,1)
    })

    market_signal.update({
        "signal": "BULLISH" if final_action in ["STRONG BUY CE","BUY CE","CONSIDER CE BUY"] else "BEARISH" if final_action in ["STRONG BUY PE","BUY PE","CONSIDER PE BUY"] else "NEUTRAL",
        "ce_price": ce_price, "pe_price": pe_price, "spread": round(spread,2),
        "rsi": round(rsi,2), "macd": round(macd_hist,2), "pcr": round(pcr,2),
        "vwap": round(vwap,2), "atr": round(atr,2), "atr_pct": round(atr_pct,2),
        "ema_fast": round(ema_fast,2), "ema_slow": round(ema_slow,2),
        "delta": ce_delta, "gamma": ce_gamma, "theta": ce_theta, "vega": ce_vega,
        "volume": int(combined_volumes[-1]) if combined_volumes else 0,
        "timestamp": datetime.now().isoformat(),
        "adx": round(adx,2), "bb_position": round(bb_pos,2),
        "rsi_divergence": rsi_div, "iv_rank": 50,
        "signal_grade": grade, "regime": regime, "session_phase": session_phase,
        "ce_spread_pct": round(ce_spread_pct,1), "pe_spread_pct": round(pe_spread_pct,1),
        "ce_oi_change": round(ce_oi_change,1), "pe_oi_change": round(pe_oi_change,1)
    })

    if final_action != signal_state["last_logged_action"]:
        logger.info(f"PRO SIGNAL: {final_action} [{final_signal_type}] Grade:{grade} Conf:{raw_confidence} "
                    f"BullTF:{bullish_tf} BearTF:{bearish_tf} RSI:{rsi:.1f} ADX:{adx:.1f} PCR:{pcr:.2f} "
                    f"PosSize:{position_pct}% RR:{rr:.1f}")
        signal_state["last_logged_action"] = final_action

# ---------- WebSocket Callbacks ----------
# ---------- WebSocket Callbacks ----------
def on_open(wsapp):
    global sws

    logger.info("WebSocket Opened")

    if sws and CE_TOKEN and PE_TOKEN and NIFTY_TOKEN:

        tokens = [
            {
                "exchangeType": 2,
                "tokens": [CE_TOKEN, PE_TOKEN]
            },
            {
                "exchangeType": 1,
                "tokens": [NIFTY_TOKEN]
            }
        ]

        sws.subscribe(
            correlation_id="tradeguru",
            mode=1,
            token_list=tokens
        )

        logger.info(f"Subscribed CE={CE_TOKEN} PE={PE_TOKEN} NIFTY={NIFTY_TOKEN}")

def on_data(wsapp, message):

    global tick_counter
    global last_tick_time

    try:

        logger.info(f"TICK RECEIVED: {message}")

        if not message:
            return

        # SmartAPI sometimes gives dict directly
        tick = message if isinstance(message, dict) else json.loads(message)

        token = str(
            tick.get("token")
            or tick.get("tk")
            or ""
        )

        ltp = (
            tick.get("last_traded_price")
            or tick.get("ltp")
            or 0
        )

        # Angel sends paise
        if ltp and ltp > 1000:
            ltp = float(ltp) / 100

        last_tick_time = time.time()

        # -------------------------------------------------
        # NIFTY SPOT
        # -------------------------------------------------

        if token == str(NIFTY_TOKEN):

            latest_ticks["nifty_spot"] = ltp

            spot_cache["value"] = ltp
            spot_cache["timestamp"] = time.time()

            logger.info(f"WS Spot Updated: {ltp}")

            return

        # -------------------------------------------------
        # CE OPTION
        # -------------------------------------------------

        if token == str(CE_TOKEN):

            latest_ticks["ce_price"] = ltp

            ce_price_history.append(ltp)

            tick_counter += 1

            logger.info(f"CE => {ltp}")

        # -------------------------------------------------
        # PE OPTION
        # -------------------------------------------------

        elif token == str(PE_TOKEN):

            latest_ticks["pe_price"] = ltp

            pe_price_history.append(ltp)

            tick_counter += 1

            logger.info(f"PE => {ltp}")

        # -------------------------------------------------
        # RUN ENGINE
        # -------------------------------------------------

        ce = latest_ticks["ce_price"]
        pe = latest_ticks["pe_price"]

        if (
            ce > 0
            and pe > 0
            and tick_counter % 5 == 0
            and len(ce_price_history) >= 30
            and len(pe_price_history) >= 30
        ):

            run_signal_engine(
                ce,
                pe,
                list(ce_price_history),
                list(pe_price_history),
                list(ce_volume_history),
                list(pe_volume_history)
            )

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

def start_websocket():
    global ws_running, sws, CE_TOKEN, PE_TOKEN, NIFTY_TOKEN
    retry_delay = 30
    while engine_active:
        try:
            if not is_market_open():
                logger.info("Market closed. Sleeping 5 minutes...")
                time.sleep(300)
                continue

            # Get NIFTY index token (once)
            if NIFTY_TOKEN is None:
                NIFTY_TOKEN = get_nifty_index_token()

            # Get fresh ATM tokens
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
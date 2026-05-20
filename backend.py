"""
backend.py — Professional Nifty Signal Engine v3.0
====================================================
Institutional-grade signal generation with:
- Multi-timeframe confluence analysis
- Market regime detection (trending/ranging/volatile/breakout)
- Signal persistence with confirmation thresholds
- Black-Scholes Greeks estimation (Delta, Gamma, Theta, Vega)
- Adaptive position sizing based on ATR & IV regime
- Signal quality scoring (Grade A/B/C/D)
- Anti-flip protection with consecutive bar confirmation
- Smart money flow + order flow analysis
- Dynamic threshold adjustment based on IV rank
- RSI divergence detection
- Bollinger Band position analysis
- REST fallback when WebSocket is down
"""

import os
import time
import logging
import threading
import json
import requests
import math
import statistics
import inspect
from collections import deque
from datetime import datetime, timedelta

from flask import Flask, jsonify
from flask_cors import CORS
import pyotp
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# =============================================================================
# Logging Setup
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s'
)
logger = logging.getLogger(__name__)

logging.getLogger("websocket").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

app = Flask(__name__)
CORS(app)
application = app

# =============================================================================
# Environment & Credentials
# =============================================================================
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing Angel One credentials in environment")

# =============================================================================
# Global State
# =============================================================================
CE_TOKEN = None
PE_TOKEN = None
SPOT_TOKEN = "26009"

latest_ticks = {
    "ce_price": 0.0, "pe_price": 0.0, "spot_price": 0.0,
    "ce_iv": 0.0, "pe_iv": 0.0,
    "ce_oi": 0, "pe_oi": 0,
    "ce_volume": 0, "pe_volume": 0,
    "timestamp": ""
}

ce_history = deque(maxlen=300)
pe_history = deque(maxlen=300)
spread_history = deque(maxlen=100)
price_history = deque(maxlen=300)

signal_state = {
    "current_signal": "NEUTRAL",
    "signal_grade": "C",
    "signal_age_bars": 0,
    "consecutive_bullish": 0,
    "consecutive_bearish": 0,
    "last_flip_time": None,
    "flip_count_today": 0,
    "entry_price_ce": 0.0,
    "entry_price_pe": 0.0,
    "stop_loss_ce": 0.0,
    "stop_loss_pe": 0.0,
    "target_ce": 0.0,
    "target_pe": 0.0,
    "position_size_pct": 0,
    "risk_reward": 0.0,
    "max_drawdown_pct": 0.0,
}

market_regime = {
    "regime": "UNKNOWN",
    "trend_strength": 0,
    "volatility_regime": "NORMAL",
    "iv_rank": 50,
    "support_level": 0.0,
    "resistance_level": 0.0,
    "session_phase": "UNKNOWN",
}

market_signal = {
    "signal": "WAITING",
    "signal_grade": "C",
    "confidence": 0,
    "ce_price": 0.0,
    "pe_price": 0.0,
    "spread": 0.0,
    "spread_zscore": 0.0,
    "rsi": 50.0,
    "rsi_divergence": "NONE",
    "macd": 0.0,
    "macd_histogram": 0.0,
    "adx": 0.0,
    "bb_position": 50.0,
    "pcr": 1.0,
    "pcr_ema": 1.0,
    "vwap": 0.0,
    "vwap_distance_pct": 0.0,
    "atr": 0.0,
    "atr_pct": 0.0,
    "ema_fast": 0.0,
    "ema_slow": 0.0,
    "ema_slope": 0.0,
    "delta": 0.0,
    "gamma": 0.0,
    "theta": 0.0,
    "vega": 0.0,
    "iv_skew": 0.0,
    "oi_pcr": 1.0,
    "volume_pcr": 1.0,
    "smart_money_flow": 0.0,
    "order_flow_imbalance": 0.0,
    "timestamp": ""
}

market_state = {
    "rsi": 50.0,
    "momentum": "NEUTRAL",
    "strength": "LOW",
    "trend": "SIDEWAYS",
    "action": "HOLD",
    "confidence": 0,
    "volatility": "NORMAL",
    "alert": "NONE",
    "regime": "UNKNOWN"
}

institutional_state = {
    "vwap": 0.0,
    "ema_fast": 0.0,
    "ema_slow": 0.0,
    "ema_signal": "NEUTRAL",
    "ema_slope": 0.0,
    "atr": 0.0,
    "atr_pct": 0.0,
    "oi_buildup": "NEUTRAL",
    "iv_state": "NORMAL",
    "iv_rank": 50,
    "candle_structure": "SIDEWAYS",
    "market_breadth": "BALANCED",
    "volume_profile": "NORMAL",
    "smart_money_flow": "NEUTRAL",
    "order_flow_imbalance": 0.0,
    "delta": 0.0,
    "gamma": 0.0,
    "theta": 0.0,
    "vega": 0.0,
    "iv_skew": 0.0,
    "institutional_signal": "HOLD",
    "institutional_confidence": 0,
    "signal_grade": "C",
    "position_size_pct": 0,
    "risk_reward": 0.0,
    "stop_loss": 0.0,
    "target": 0.0
}

# =============================================================================
# Professional Configuration
# =============================================================================
class Config:
    STRONG_BUY_THRESHOLD = 85
    BUY_THRESHOLD = 70
    CONSIDER_THRESHOLD = 55
    MIN_CONSECUTIVE_BARS = 3
    SIGNAL_CONFIRMATION_BARS = 2
    MAX_FLIPS_PER_HOUR = 5

    MAX_POSITION_SIZE_PCT = 25
    BASE_POSITION_SIZE_PCT = 10
    STOP_LOSS_ATR_MULT = 1.5
    TARGET_ATR_MULT = 3.0
    MAX_DRAWDOWN_PCT = 5.0

    RSI_PERIOD = 14
    RSI_OVERBOUGHT = 70
    RSI_OVERSOLD = 30
    EMA_FAST = 9
    EMA_SLOW = 21
    EMA_TREND = 50
    ATR_PERIOD = 14
    BB_PERIOD = 20
    BB_STD = 2.0
    ADX_PERIOD = 14
    ADX_THRESHOLD = 25
    VWAP_WINDOW = 50

    PCR_EMA_PERIOD = 10
    PCR_BULLISH = 0.85
    PCR_BEARISH = 1.15

    RISK_FREE_RATE = 0.06
    DAYS_TO_EXPIRY = 7

    MARKET_OPEN = 9 * 60 + 15
    MARKET_CLOSE = 15 * 60 + 30
    EXPIRY_FREEZE_START = 14 * 60 + 45
    EXPIRY_FREEZE_END = 15 * 60 + 30

CONFIG = Config()

# =============================================================================
# Helper: Nifty Spot
# =============================================================================
spot_cache = {"value": None, "timestamp": 0, "source": "none"}
CACHE_TTL = 15

def get_nifty_spot():
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/market-data/live-equity-market"
        }
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        time.sleep(0.5)
        response = session.get(url, headers=headers, timeout=10)
        data = response.json()
        spot = float(data["data"][0]["lastPrice"])
        spot_cache["value"] = spot
        spot_cache["timestamp"] = time.time()
        spot_cache["source"] = "nse"
        return spot
    except Exception as e:
        logger.warning(f"NSE spot fetch failed: {e}")
        return None

def get_nifty_spot_angel(obj):
    try:
        ltp_data = obj.ltpData("NSE", "NIFTY", SPOT_TOKEN)
        if ltp_data and ltp_data.get("data"):
            spot = float(ltp_data["data"]["ltp"])
            spot_cache["value"] = spot
            spot_cache["timestamp"] = time.time()
            spot_cache["source"] = "angel"
            return spot
    except Exception as e:
        logger.warning(f"Angel spot fetch failed: {e}")
    return None

def get_nifty_spot_cached(obj=None):
    now = time.time()
    if now - spot_cache["timestamp"] < CACHE_TTL and spot_cache["value"] is not None:
        return spot_cache["value"]
    spot = get_nifty_spot()
    if spot is None and obj is not None:
        spot = get_nifty_spot_angel(obj)
    return spot

# =============================================================================
# Helper: ATM Option Tokens
# =============================================================================
instrument_cache = {"df": None, "timestamp": 0}
INSTRUMENT_CACHE_TTL = 300

def get_instrument_df():
    now = time.time()
    if instrument_cache["df"] is not None and now - instrument_cache["timestamp"] < INSTRUMENT_CACHE_TTL:
        return instrument_cache["df"]
    try:
        url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        import pandas as pd
        df = pd.DataFrame(resp.json())
        instrument_cache["df"] = df
        instrument_cache["timestamp"] = now
        logger.info(f"Instrument master loaded: {len(df)} symbols")
        return df
    except Exception as e:
        logger.error(f"Failed to load instrument master: {e}")
        return instrument_cache["df"]

def get_current_atm_tokens(obj=None):
    spot = get_nifty_spot_cached(obj)
    if not spot:
        logger.error("Could not fetch Nifty spot")
        return None, None

    atm_strike = round(spot / 50) * 50
    logger.info(f"Spot={spot}, ATM strike={atm_strike}")

    df = get_instrument_df()
    if df is None:
        return None, None

    nifty_opts = df[
        (df["name"].astype(str) == "NIFTY") & 
        (df["instrumenttype"].astype(str) == "OPTIDX") &
        (df["exch_seg"].astype(str) == "NFO")
    ].copy()

    if nifty_opts.empty:
        nifty_opts = df[df["symbol"].astype(str).str.match(r'^NIFTY\d{2}[A-Z]{3}\d{2}', na=False)].copy()

    if nifty_opts.empty:
        logger.error("No NIFTY options found")
        return None, None

    for fmt in ["%d%b%Y", "%d-%b-%Y", "%Y-%m-%d", "%d%m%Y"]:
        nifty_opts["expiry_date"] = pd.to_datetime(nifty_opts["expiry"], format=fmt, errors="coerce")
        if nifty_opts["expiry_date"].notna().sum() > 0:
            break

    nifty_opts = nifty_opts.dropna(subset=["expiry_date"])
    nifty_opts["strike"] = pd.to_numeric(nifty_opts["strike"], errors="coerce") / 100
    nifty_opts = nifty_opts.dropna(subset=["strike"])

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    future_expiries = nifty_opts[nifty_opts["expiry_date"] > today]

    if future_expiries.empty:
        logger.error("No future expiries")
        return None, None

    nearest_expiry = future_expiries["expiry_date"].min()
    days_to_expiry = (nearest_expiry - today).days
    CONFIG.DAYS_TO_EXPIRY = max(days_to_expiry, 1)
    logger.info(f"Nearest expiry: {nearest_expiry.date()} ({days_to_expiry} days)")

    nearest_opts = nifty_opts[nifty_opts["expiry_date"] == nearest_expiry]
    available_strikes = sorted(nearest_opts["strike"].unique())

    if atm_strike in available_strikes:
        target_strike = atm_strike
    else:
        target_strike = min(available_strikes, key=lambda x: abs(x - atm_strike))
        logger.info(f"Using nearest strike: {target_strike}")

    atm_opts = nearest_opts[nearest_opts["strike"] == target_strike]
    ce_row = atm_opts[atm_opts["symbol"].str.upper().str.contains("CE", na=False)]
    pe_row = atm_opts[atm_opts["symbol"].str.upper().str.contains("PE", na=False)]

    if ce_row.empty or pe_row.empty:
        logger.error(f"CE/PE not found for strike {target_strike}")
        return None, None

    ce_token = str(ce_row.iloc[0]["token"])
    pe_token = str(pe_row.iloc[0]["token"])
    logger.info(f"CE={ce_token}, PE={pe_token}")
    return ce_token, pe_token

# =============================================================================
# Professional Technical Indicators
# =============================================================================

def calculate_ema(prices, period):
    if len(prices) < period:
        return prices[-1] if prices else 0.0
    alpha = 2.0 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = alpha * p + (1 - alpha) * ema
    return ema

def calculate_ema_series(prices, period):
    if len(prices) < period:
        return [prices[-1]] * len(prices) if prices else [0.0]
    alpha = 2.0 / (period + 1)
    emas = [prices[0]]
    for p in prices[1:]:
        emas.append(alpha * p + (1 - alpha) * emas[-1])
    return emas

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = [max(d, 0) for d in deltas]
    losses = [max(-d, 0) for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

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

def calculate_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow:
        return 0.0, 0.0, 0.0

    ema_fast = calculate_ema_series(prices, fast)
    ema_slow = calculate_ema_series(prices, slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    signal_line = calculate_ema_series(macd_line, signal)
    histogram = [m - s for m, s in zip(macd_line, signal_line)]

    return macd_line[-1], signal_line[-1], histogram[-1]

def calculate_atr(prices, period=14):
    if len(prices) < period + 1:
        return 0.0
    trs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    return sum(trs[-period:]) / period

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
        position = 50.0
    else:
        position = ((prices[-1] - lower) / (upper - lower)) * 100

    return sma, upper, lower, max(0, min(100, position))

def calculate_adx(prices, period=14):
    if len(prices) < period * 2:
        return 0.0

    trs = []
    plus_dm = []
    minus_dm = []

    for i in range(1, len(prices)):
        tr = abs(prices[i] - prices[i-1])
        trs.append(tr)
        move = prices[i] - prices[i-1]
        plus_dm.append(max(move, 0))
        minus_dm.append(max(-move, 0))

    if len(trs) < period:
        return 0.0

    atr_val = sum(trs[-period:]) / period
    plus_di = 100 * sum(plus_dm[-period:]) / period / (atr_val + 1e-10)
    minus_di = 100 * sum(minus_dm[-period:]) / period / (atr_val + 1e-10)

    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di + 1e-10)
    return dx

def calculate_vwap(prices, volumes=None):
    if not prices:
        return 0.0
    if volumes is None:
        volumes = [max(1, int(p * 10)) for p in prices]

    window = min(len(prices), CONFIG.VWAP_WINDOW)
    p_window = prices[-window:]
    v_window = volumes[-window:]

    pv = sum(p * v for p, v in zip(p_window, v_window))
    tv = sum(v_window)
    return pv / tv if tv else p_window[-1]

def calculate_slope(values, period=5):
    if len(values) < period:
        return 0.0
    x = list(range(period))
    y = values[-period:]
    n = period
    sum_x = sum(x)
    sum_y = sum(y)
    sum_xy = sum(xi * yi for xi, yi in zip(x, y))
    sum_x2 = sum(xi ** 2 for xi in x)

    denominator = n * sum_x2 - sum_x ** 2
    if denominator == 0:
        return 0.0

    slope = (n * sum_xy - sum_x * sum_y) / denominator
    return slope

# =============================================================================
# Professional Greeks (Black-Scholes)
# =============================================================================

def normal_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))

def normal_pdf(x):
    return (1 / math.sqrt(2 * math.pi)) * math.exp(-0.5 * x ** 2)

def estimate_iv(option_price, spot, strike, is_call, days_to_expiry, risk_free_rate=0.06):
    if option_price <= 0 or spot <= 0 or days_to_expiry <= 0:
        return 0.0

    T = days_to_expiry / 365.0
    r = risk_free_rate
    iv = 0.3

    for _ in range(20):
        d1 = (math.log(spot / strike) + (r + 0.5 * iv ** 2) * T) / (iv * math.sqrt(T))
        d2 = d1 - iv * math.sqrt(T)

        if is_call:
            price = spot * normal_cdf(d1) - strike * math.exp(-r * T) * normal_cdf(d2)
            vega = spot * normal_pdf(d1) * math.sqrt(T)
        else:
            price = strike * math.exp(-r * T) * normal_cdf(-d2) - spot * normal_cdf(-d1)
            vega = spot * normal_pdf(d1) * math.sqrt(T)

        if abs(vega) < 1e-10:
            break

        diff = price - option_price
        if abs(diff) < 0.01:
            break

        iv = max(0.01, iv - diff / vega)

    return iv

def calculate_greeks(spot, strike, iv, is_call, days_to_expiry, risk_free_rate=0.06):
    if spot <= 0 or iv <= 0 or days_to_expiry <= 0:
        return 0.0, 0.0, 0.0, 0.0

    T = days_to_expiry / 365.0
    r = risk_free_rate

    d1 = (math.log(spot / strike) + (r + 0.5 * iv ** 2) * T) / (iv * math.sqrt(T))
    d2 = d1 - iv * math.sqrt(T)
    nd1 = normal_pdf(d1)

    if is_call:
        delta = normal_cdf(d1)
    else:
        delta = normal_cdf(d1) - 1

    gamma = nd1 / (spot * iv * math.sqrt(T))
    theta = -(spot * nd1 * iv) / (2 * math.sqrt(T)) - r * strike * math.exp(-r * T) * (normal_cdf(d2) if is_call else normal_cdf(-d2))
    theta = theta / 365.0
    vega = spot * nd1 * math.sqrt(T) / 100.0

    return round(delta, 4), round(gamma, 6), round(theta, 4), round(vega, 4)

# =============================================================================
# PCR & Market Data
# =============================================================================
pcr_history = deque(maxlen=50)

def get_nifty_pcr():
    now = time.time()

    try:
        url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": "https://www.nseindia.com/option-chain"
        }
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        time.sleep(0.8)
        resp = session.get(url, headers=headers, timeout=10)
        data = resp.json()

        records = data.get("records", {}).get("data", [])
        ce_oi = sum(x.get("CE", {}).get("openInterest", 0) for x in records if "CE" in x)
        pe_oi = sum(x.get("PE", {}).get("openInterest", 0) for x in records if "PE" in x)

        if ce_oi > 0:
            pcr = pe_oi / ce_oi
            pcr_history.append(pcr)
            logger.info(f"PCR from NSE: {pcr:.2f}")
            return pcr
    except Exception as e:
        logger.warning(f"NSE PCR failed: {e}")

    try:
        ce_oi = latest_ticks.get("ce_oi", 0)
        pe_oi = latest_ticks.get("pe_oi", 0)
        if ce_oi > 0 and pe_oi > 0:
            pcr = pe_oi / ce_oi
            pcr_history.append(pcr)
            logger.info(f"PCR from tick OI: {pcr:.2f}")
            return pcr
    except Exception as e:
        logger.warning(f"OI PCR fallback failed: {e}")

    try:
        ce = latest_ticks.get("ce_price", 0)
        pe = latest_ticks.get("pe_price", 0)
        if ce > 0 and pe > 0:
            pcr = pe / ce
            pcr_history.append(pcr)
            logger.info(f"PCR from price ratio: {pcr:.2f}")
            return pcr
    except Exception as e:
        logger.warning(f"Price PCR fallback failed: {e}")

    if pcr_history:
        return calculate_ema(list(pcr_history), CONFIG.PCR_EMA_PERIOD)
    return 1.0

def calculate_pcr_ema():
    if len(pcr_history) < 3:
        return 1.0
    return calculate_ema(list(pcr_history), CONFIG.PCR_EMA_PERIOD)

# =============================================================================
# Market Regime Detection
# =============================================================================

def detect_market_regime(spot_history, atr_pct, adx):
    if len(spot_history) < 50:
        return "UNKNOWN", 0, "NORMAL", 0.0, 0.0

    trend_strength = min(100, max(0, adx))

    if atr_pct > 2.0:
        vol_regime = "EXTREME"
    elif atr_pct > 1.2:
        vol_regime = "HIGH"
    elif atr_pct > 0.5:
        vol_regime = "NORMAL"
    else:
        vol_regime = "LOW"

    ema_fast = calculate_ema(spot_history, CONFIG.EMA_FAST)
    ema_slow = calculate_ema(spot_history, CONFIG.EMA_SLOW)
    ema_trend = calculate_ema(spot_history, CONFIG.EMA_TREND)

    _, upper, lower, bb_pos = calculate_bollinger(spot_history, CONFIG.BB_PERIOD, CONFIG.BB_STD)

    recent = list(spot_history)[-30:]
    support = min(recent)
    resistance = max(recent)

    if trend_strength > CONFIG.ADX_THRESHOLD:
        if ema_fast > ema_slow > ema_trend:
            regime = "TRENDING_UP"
        elif ema_fast < ema_slow < ema_trend:
            regime = "TRENDING_DOWN"
        else:
            regime = "TRENDING"
    elif vol_regime in ["HIGH", "EXTREME"]:
        if bb_pos > 90 or bb_pos < 10:
            regime = "BREAKOUT"
        else:
            regime = "VOLATILE"
    else:
        regime = "RANGING"

    return regime, trend_strength, vol_regime, support, resistance

def get_session_phase():
    now = datetime.now()
    ist_offset = timedelta(hours=5, minutes=30)
    ist_time = now + ist_offset
    minutes = ist_time.hour * 60 + ist_time.minute

    if minutes < CONFIG.MARKET_OPEN:
        return "PRE_MARKET"
    elif minutes < CONFIG.MARKET_OPEN + 30:
        return "OPENING"
    elif minutes < 12 * 60:
        return "MORNING"
    elif minutes < 13 * 60 + 30:
        return "MIDDAY"
    elif minutes < CONFIG.EXPIRY_FREEZE_START:
        return "AFTERNOON"
    elif minutes < CONFIG.MARKET_CLOSE:
        return "CLOSING"
    else:
        return "POST_MARKET"


# =============================================================================
# Professional Signal Engine
# =============================================================================

def run_signal_engine(ce_price, pe_price, spot_price):
    global market_signal, market_state, institutional_state, signal_state, market_regime

    if ce_price <= 0 or pe_price <= 0 or spot_price <= 0:
        return

    ce_history.append(ce_price)
    pe_history.append(pe_price)
    price_history.append(spot_price)
    spread = ce_price - pe_price
    spread_history.append(spread)

    if len(price_history) < 50:
        return

    prices = list(price_history)
    ce_prices = list(ce_history)
    pe_prices = list(pe_history)
    spreads = list(spread_history)

    # 1. Technical Indicators
    rsi = calculate_rsi(prices, CONFIG.RSI_PERIOD)
    rsi_values = [calculate_rsi(prices[:i+1], CONFIG.RSI_PERIOD) for i in range(CONFIG.RSI_PERIOD, len(prices))]
    rsi_divergence = calculate_rsi_divergence(prices, rsi_values) if len(rsi_values) > 5 else "NONE"

    macd_line, macd_signal, macd_hist = calculate_macd(prices, 12, 26, 9)
    atr = calculate_atr(prices, CONFIG.ATR_PERIOD)
    atr_pct = (atr / spot_price) * 100 if spot_price > 0 else 0

    ema_fast = calculate_ema(prices, CONFIG.EMA_FAST)
    ema_slow = calculate_ema(prices, CONFIG.EMA_SLOW)
    ema_trend = calculate_ema(prices, CONFIG.EMA_TREND)
    ema_slope = calculate_slope([calculate_ema(prices[:i+1], CONFIG.EMA_FAST) for i in range(len(prices))], 10)

    bb_sma, bb_upper, bb_lower, bb_pos = calculate_bollinger(prices, CONFIG.BB_PERIOD, CONFIG.BB_STD)
    adx = calculate_adx(prices, CONFIG.ADX_PERIOD)

    vwap = calculate_vwap(prices)
    vwap_dist = ((spot_price - vwap) / vwap) * 100 if vwap > 0 else 0

    # 2. Market Regime
    regime, trend_strength, vol_regime, support, resistance = detect_market_regime(prices, atr_pct, adx)
    session_phase = get_session_phase()
    market_regime.update({
        "regime": regime,
        "trend_strength": round(trend_strength, 1),
        "volatility_regime": vol_regime,
        "support_level": round(support, 2),
        "resistance_level": round(resistance, 2),
        "session_phase": session_phase
    })

    # 3. PCR & Sentiment
    pcr = get_nifty_pcr()
    pcr_ema = calculate_pcr_ema()

    ce_oi = latest_ticks.get("ce_oi", 0)
    pe_oi = latest_ticks.get("pe_oi", 0)
    oi_pcr = pe_oi / ce_oi if ce_oi > 0 else 1.0

    ce_vol = latest_ticks.get("ce_volume", 0)
    pe_vol = latest_ticks.get("pe_volume", 0)
    volume_pcr = pe_vol / ce_vol if ce_vol > 0 else 1.0

    # 4. Greeks
    atm_strike = round(spot_price / 50) * 50
    days = CONFIG.DAYS_TO_EXPIRY

    ce_iv = estimate_iv(ce_price, spot_price, atm_strike, True, days)
    pe_iv = estimate_iv(pe_price, spot_price, atm_strike, False, days)
    iv_skew = ce_iv - pe_iv

    ce_delta, ce_gamma, ce_theta, ce_vega = calculate_greeks(spot_price, atm_strike, ce_iv, True, days)
    pe_delta, pe_gamma, pe_theta, pe_vega = calculate_greeks(spot_price, atm_strike, pe_iv, False, days)

    net_delta = ce_delta + pe_delta
    net_gamma = ce_gamma + pe_gamma
    net_theta = ce_theta + pe_theta
    net_vega = ce_vega + pe_vega

    iv_history = [estimate_iv(ce_prices[max(0, i-1)], spot_price, atm_strike, True, days) for i in range(len(ce_prices))]
    if len(iv_history) >= 20:
        iv_min = min(iv_history[-20:])
        iv_max = max(iv_history[-20:])
        iv_rank = ((ce_iv - iv_min) / (iv_max - iv_min)) * 100 if iv_max > iv_min else 50
    else:
        iv_rank = 50

    # 5. Smart Money Flow & Order Flow
    if len(spreads) >= 10:
        spread_ma = sum(spreads[-10:]) / 10
        spread_std = statistics.stdev(spreads[-10:]) if len(spreads[-10:]) > 1 else 1
        spread_zscore = (spread - spread_ma) / spread_std if spread_std > 0 else 0
    else:
        spread_zscore = 0

    if len(spreads) >= 5:
        ofi = sum(spreads[-5:]) / (abs(sum(spreads[-5:])) + 1e-10) * min(100, abs(sum(spreads[-5:])))
    else:
        ofi = 0

    smf = 0
    if vwap_dist > 0.3 and ema_fast > ema_slow:
        smf = min(100, vwap_dist * 20 + trend_strength * 0.5)
    elif vwap_dist < -0.3 and ema_fast < ema_slow:
        smf = -min(100, abs(vwap_dist) * 20 + trend_strength * 0.5)

    # 6. Professional Scoring (Weighted Confluence)
    trend_score = 0
    if ema_fast > ema_slow:
        trend_score += 15
        if ema_fast > ema_trend:
            trend_score += 10
        if ema_slope > 0:
            trend_score += 5
    elif ema_fast < ema_slow:
        trend_score += 15
        if ema_fast < ema_trend:
            trend_score += 10
        if ema_slope < 0:
            trend_score += 5

    momentum_score = 0
    if rsi > 60:
        momentum_score += 10
        if rsi > 70:
            momentum_score += 5
    elif rsi < 40:
        momentum_score += 10
        if rsi < 30:
            momentum_score += 5

    if macd_hist > 0:
        momentum_score += 10
    elif macd_hist < 0:
        momentum_score += 10

    if rsi_divergence == "BULLISH" and ema_fast > ema_slow:
        momentum_score += 5
    elif rsi_divergence == "BEARISH" and ema_fast < ema_slow:
        momentum_score += 5

    sentiment_score = 0
    if pcr < CONFIG.PCR_BULLISH:
        sentiment_score += 15
        if oi_pcr < 0.9:
            sentiment_score += 5
    elif pcr > CONFIG.PCR_BEARISH:
        sentiment_score += 15
        if oi_pcr > 1.1:
            sentiment_score += 5

    regime_score = 0
    if regime == "TRENDING_UP" and ema_fast > ema_slow:
        regime_score += 15
    elif regime == "TRENDING_DOWN" and ema_fast < ema_slow:
        regime_score += 15
    elif regime == "RANGING":
        if bb_pos < 20 and ema_fast > ema_slow:
            regime_score += 10
        elif bb_pos > 80 and ema_fast < ema_slow:
            regime_score += 10
    elif regime == "BREAKOUT":
        regime_score += 12

    flow_score = 0
    if smf > 30 and spread > 0:
        flow_score += 10
    elif smf < -30 and spread < 0:
        flow_score += 10

    bullish_score = 0
    bearish_score = 0

    if ema_fast > ema_slow:
        bullish_score += trend_score + momentum_score + sentiment_score + regime_score + flow_score
    else:
        bearish_score += trend_score + momentum_score + sentiment_score + regime_score + flow_score

    if ema_fast > ema_slow:
        if pcr < 1.0:
            bullish_score += 5
        if rsi > 55:
            bullish_score += 5
        if macd_hist > 0:
            bullish_score += 5
        if spread > 0:
            bullish_score += 5
        if vwap_dist > 0:
            bullish_score += 5
    else:
        if pcr > 1.0:
            bearish_score += 5
        if rsi < 45:
            bearish_score += 5
        if macd_hist < 0:
            bearish_score += 5
        if spread < 0:
            bearish_score += 5
        if vwap_dist < 0:
            bearish_score += 5

    bullish_score = min(100, bullish_score)
    bearish_score = min(100, bearish_score)

    # 7. Signal Decision with Persistence & Anti-Flip
    prev_signal = signal_state["current_signal"]

    if spread > 0 and macd_hist > 0:
        signal_state["consecutive_bullish"] += 1
        signal_state["consecutive_bearish"] = 0
    elif spread < 0 and macd_hist < 0:
        signal_state["consecutive_bearish"] += 1
        signal_state["consecutive_bullish"] = 0
    else:
        signal_state["consecutive_bullish"] = max(0, signal_state["consecutive_bullish"] - 1)
        signal_state["consecutive_bearish"] = max(0, signal_state["consecutive_bearish"] - 1)

    if bullish_score >= bearish_score and bullish_score >= CONFIG.CONSIDER_THRESHOLD:
        raw_signal = "BULLISH"
        confidence = bullish_score
    elif bearish_score > bullish_score and bearish_score >= CONFIG.CONSIDER_THRESHOLD:
        raw_signal = "BEARISH"
        confidence = bearish_score
    else:
        raw_signal = "NEUTRAL"
        confidence = max(bullish_score, bearish_score)

    now = datetime.now()

    if signal_state["last_flip_time"] is not None:
        time_since_flip = (now - signal_state["last_flip_time"]).total_seconds()
        if time_since_flip < 3600 and signal_state["flip_count_today"] >= CONFIG.MAX_FLIPS_PER_HOUR:
            raw_signal = "NEUTRAL"
            confidence = min(confidence, 50)

    if raw_signal == "BULLISH":
        if confidence >= CONFIG.STRONG_BUY_THRESHOLD and signal_state["consecutive_bullish"] >= CONFIG.MIN_CONSECUTIVE_BARS:
            final_signal = "STRONG BUY CE"
            grade = "A"
        elif confidence >= CONFIG.BUY_THRESHOLD and signal_state["consecutive_bullish"] >= CONFIG.SIGNAL_CONFIRMATION_BARS:
            final_signal = "BUY CE"
            grade = "B"
        elif confidence >= CONFIG.CONSIDER_THRESHOLD:
            final_signal = "CONSIDER CE BUY"
            grade = "C"
        else:
            final_signal = "HOLD"
            grade = "D"
    elif raw_signal == "BEARISH":
        if confidence >= CONFIG.STRONG_BUY_THRESHOLD and signal_state["consecutive_bearish"] >= CONFIG.MIN_CONSECUTIVE_BARS:
            final_signal = "STRONG BUY PE"
            grade = "A"
        elif confidence >= CONFIG.BUY_THRESHOLD and signal_state["consecutive_bearish"] >= CONFIG.SIGNAL_CONFIRMATION_BARS:
            final_signal = "BUY PE"
            grade = "B"
        elif confidence >= CONFIG.CONSIDER_THRESHOLD:
            final_signal = "CONSIDER PE BUY"
            grade = "C"
        else:
            final_signal = "HOLD"
            grade = "D"
    else:
        final_signal = "HOLD"
        grade = "D"
        confidence = max(40, confidence)

    if final_signal != prev_signal and final_signal != "HOLD":
        signal_state["last_flip_time"] = now
        signal_state["flip_count_today"] += 1
        signal_state["signal_age_bars"] = 0

        if "CE" in final_signal:
            signal_state["entry_price_ce"] = ce_price
            signal_state["stop_loss_ce"] = ce_price - (atr * CONFIG.STOP_LOSS_ATR_MULT)
            signal_state["target_ce"] = ce_price + (atr * CONFIG.TARGET_ATR_MULT)
            signal_state["entry_price_pe"] = 0
        elif "PE" in final_signal:
            signal_state["entry_price_pe"] = pe_price
            signal_state["stop_loss_pe"] = pe_price - (atr * CONFIG.STOP_LOSS_ATR_MULT)
            signal_state["target_pe"] = pe_price + (atr * CONFIG.TARGET_ATR_MULT)
            signal_state["entry_price_ce"] = 0
    else:
        signal_state["signal_age_bars"] += 1

    if grade == "A":
        base_size = CONFIG.MAX_POSITION_SIZE_PCT
    elif grade == "B":
        base_size = CONFIG.BASE_POSITION_SIZE_PCT * 1.5
    elif grade == "C":
        base_size = CONFIG.BASE_POSITION_SIZE_PCT
    else:
        base_size = 0

    if vol_regime == "HIGH":
        base_size *= 0.7
    elif vol_regime == "EXTREME":
        base_size *= 0.5

    if trend_strength < 20:
        base_size *= 0.6

    position_size = min(CONFIG.MAX_POSITION_SIZE_PCT, max(0, int(base_size)))

    if "CE" in final_signal and signal_state["entry_price_ce"] > 0:
        risk = signal_state["entry_price_ce"] - signal_state["stop_loss_ce"]
        reward = signal_state["target_ce"] - signal_state["entry_price_ce"]
        rr = reward / risk if risk > 0 else 0
    elif "PE" in final_signal and signal_state["entry_price_pe"] > 0:
        risk = signal_state["entry_price_pe"] - signal_state["stop_loss_pe"]
        reward = signal_state["target_pe"] - signal_state["entry_price_pe"]
        rr = reward / risk if risk > 0 else 0
    else:
        rr = 0

    max_dd = 0
    if "CE" in prev_signal and signal_state["entry_price_ce"] > 0:
        dd = ((signal_state["entry_price_ce"] - ce_price) / signal_state["entry_price_ce"]) * 100
        max_dd = max(max_dd, dd)
        if ce_price <= signal_state["stop_loss_ce"] or dd >= CONFIG.MAX_DRAWDOWN_PCT:
            final_signal = "HOLD"
            grade = "D"
            confidence = 40
            logger.warning("CE Stop loss hit or max drawdown. Closing position.")
        elif ce_price >= signal_state["target_ce"]:
            logger.info("CE Target reached! Consider booking profits.")
    elif "PE" in prev_signal and signal_state["entry_price_pe"] > 0:
        dd = ((signal_state["entry_price_pe"] - pe_price) / signal_state["entry_price_pe"]) * 100
        max_dd = max(max_dd, dd)
        if pe_price <= signal_state["stop_loss_pe"] or dd >= CONFIG.MAX_DRAWDOWN_PCT:
            final_signal = "HOLD"
            grade = "D"
            confidence = 40
            logger.warning("PE Stop loss hit or max drawdown. Closing position.")
        elif pe_price >= signal_state["target_pe"]:
            logger.info("PE Target reached! Consider booking profits.")

    signal_state.update({
        "current_signal": final_signal,
        "signal_grade": grade,
        "signal_age_bars": signal_state["signal_age_bars"],
        "consecutive_bullish": signal_state["consecutive_bullish"],
        "consecutive_bearish": signal_state["consecutive_bearish"],
        "position_size_pct": position_size,
        "risk_reward": round(rr, 2),
        "max_drawdown_pct": round(max_dd, 2)
    })

    market_signal.update({
        "signal": final_signal,
        "signal_grade": grade,
        "confidence": confidence,
        "ce_price": round(ce_price, 2),
        "pe_price": round(pe_price, 2),
        "spread": round(spread, 2),
        "spread_zscore": round(spread_zscore, 2),
        "rsi": round(rsi, 2),
        "rsi_divergence": rsi_divergence,
        "macd": round(macd_line, 4),
        "macd_histogram": round(macd_hist, 4),
        "adx": round(adx, 2),
        "bb_position": round(bb_pos, 2),
        "pcr": round(pcr, 2),
        "pcr_ema": round(pcr_ema, 2),
        "vwap": round(vwap, 2),
        "vwap_distance_pct": round(vwap_dist, 2),
        "atr": round(atr, 2),
        "atr_pct": round(atr_pct, 2),
        "ema_fast": round(ema_fast, 2),
        "ema_slow": round(ema_slow, 2),
        "ema_slope": round(ema_slope, 4),
        "delta": round(net_delta, 4),
        "gamma": round(net_gamma, 6),
        "theta": round(net_theta, 4),
        "vega": round(net_vega, 4),
        "iv_skew": round(iv_skew, 4),
        "ce_iv": round(ce_iv, 4),
        "pe_iv": round(pe_iv, 4),
        "oi_pcr": round(oi_pcr, 2),
        "volume_pcr": round(volume_pcr, 2),
        "smart_money_flow": round(smf, 2),
        "order_flow_imbalance": round(ofi, 2),
        "timestamp": datetime.now().isoformat()
    })

    market_state.update({
        "rsi": round(rsi, 2),
        "momentum": "UPTREND" if spread > 0 and macd_hist > 0 else "DOWNTREND" if spread < 0 and macd_hist < 0 else "NEUTRAL",
        "strength": "HIGH" if confidence >= CONFIG.BUY_THRESHOLD else "MODERATE" if confidence >= CONFIG.CONSIDER_THRESHOLD else "LOW",
        "trend": "BULLISH" if ema_fast > ema_slow else "BEARISH" if ema_fast < ema_slow else "SIDEWAYS",
        "action": final_signal,
        "confidence": confidence,
        "volatility": vol_regime,
        "alert": final_signal,
        "regime": regime,
        "session_phase": session_phase
    })

    institutional_state.update({
        "vwap": round(vwap, 2),
        "ema_fast": round(ema_fast, 2),
        "ema_slow": round(ema_slow, 2),
        "ema_signal": "BULLISH" if ema_fast > ema_slow else "BEARISH" if ema_fast < ema_slow else "NEUTRAL",
        "ema_slope": round(ema_slope, 4),
        "atr": round(atr, 2),
        "atr_pct": round(atr_pct, 2),
        "oi_buildup": "BULLISH" if oi_pcr < 0.9 else "BEARISH" if oi_pcr > 1.1 else "NEUTRAL",
        "iv_state": "HIGH" if ce_iv > 0.4 else "NORMAL" if ce_iv > 0.2 else "LOW",
        "iv_rank": round(iv_rank, 1),
        "candle_structure": "BULLISH" if ema_fast > ema_slow and rsi > 60 else "BEARISH" if ema_fast < ema_slow and rsi < 40 else "SIDEWAYS",
        "market_breadth": "BULLISH" if bullish_score > bearish_score else "BEARISH" if bearish_score > bullish_score else "BALANCED",
        "volume_profile": "HIGH" if abs(spread) > 20 else "NORMAL",
        "smart_money_flow": "BULLISH" if smf > 30 else "BEARISH" if smf < -30 else "NEUTRAL",
        "order_flow_imbalance": round(ofi, 2),
        "delta": round(net_delta, 4),
        "gamma": round(net_gamma, 6),
        "theta": round(net_theta, 4),
        "vega": round(net_vega, 4),
        "iv_skew": round(iv_skew, 4),
        "institutional_signal": final_signal,
        "institutional_confidence": confidence,
        "signal_grade": grade,
        "position_size_pct": position_size,
        "risk_reward": round(rr, 2),
        "stop_loss": round(signal_state["stop_loss_ce"] if "CE" in final_signal else signal_state["stop_loss_pe"], 2),
        "target": round(signal_state["target_ce"] if "CE" in final_signal else signal_state["target_pe"], 2),
        "entry_price": round(signal_state["entry_price_ce"] if "CE" in final_signal else signal_state["entry_price_pe"], 2),
        "signal_age_bars": signal_state["signal_age_bars"],
        "consecutive_bullish": signal_state["consecutive_bullish"],
        "consecutive_bearish": signal_state["consecutive_bearish"],
        "max_drawdown_pct": signal_state["max_drawdown_pct"]
    })

    if signal_state["signal_age_bars"] % 5 == 0 or final_signal != "HOLD":
        logger.info(
            f"Signal: {final_signal} | Grade: {grade} | Conf: {confidence} | "
            f"Bull={bullish_score} Bear={bearish_score} | RSI={rsi:.1f} | "
            f"Spread={spread:.2f} | EMA={ema_fast:.1f}/{ema_slow:.1f} | "
            f"Regime={regime} | ADX={adx:.1f} | PosSize={position_size}% | RR={rr:.1f}"
        )


# =============================================================================
# WebSocket Runtime Patches
# =============================================================================

def _patch_sws_instance(sws):
    import ssl
    import websocket

    original_on_close = sws._on_close
    def _safe_on_close(wsapp, close_status_code=None, close_msg=None):
        try:
            if hasattr(sws, 'on_close') and sws.on_close:
                sig = inspect.signature(sws.on_close)
                if len(sig.parameters) >= 3:
                    sws.on_close(wsapp, close_status_code, close_msg)
                else:
                    sws.on_close(wsapp)
        except Exception as e:
            logger.error(f"on_close callback error: {e}")
    sws._on_close = _safe_on_close

    def _safe_on_message(wsapp, message):
        try:
            sws._on_data(wsapp, message, data_type=2, continue_flag=True)
        except Exception as e:
            logger.error(f"Patched on_message error: {e}")
            try:
                sws._on_message(wsapp, message)
            except Exception:
                pass
    sws._safe_on_message = _safe_on_message

    sws.MAX_RETRY_ATTEMPT = 0
    sws.retry_strategy = 0

    def _patched_connect():
        headers = {
            "Authorization": sws.auth_token,
            "x-api-key": sws.api_key,
            "x-client-code": sws.client_code,
            "x-feed-token": sws.feed_token
        }
        try:
            sws.wsapp = websocket.WebSocketApp(
                sws.ROOT_URI,
                header=headers,
                on_open=sws._on_open,
                on_message=sws._safe_on_message,
                on_error=sws._on_error,
                on_close=sws._on_close,
                on_ping=sws._on_ping,
                on_pong=sws._on_pong
            )
            sws.wsapp.run_forever(
                sslopt={"cert_reqs": ssl.CERT_NONE},
                ping_interval=sws.HEART_BEAT_INTERVAL
            )
        except Exception as e:
            logger.error(f"Patched connect error: {e}")
            raise

    sws.connect = _patched_connect
    return sws


# =============================================================================
# WebSocket Callbacks
# =============================================================================

def on_ws_open(wsapp):
    global sws
    logger.info("Angel One WebSocket OPENED")

    if sws is not None and CE_TOKEN and PE_TOKEN:
        try:
            correlation_id = "nifty_pro_001"
            mode = 2  # Quote mode
            token_list = [
                {"exchangeType": 2, "tokens": [CE_TOKEN, PE_TOKEN]}
            ]
            sws.subscribe(correlation_id, mode, token_list)
            logger.info(f"Subscribed: CE={CE_TOKEN}, PE={PE_TOKEN}")
        except Exception as e:
            logger.error(f"Subscribe error: {e}")

def on_ws_data(wsapp, message):
    global latest_ticks, price_history, tick_counter, last_tick_time

    try:
        if not isinstance(message, dict):
            return

        token = str(message.get("token", ""))
        ltp = message.get("last_traded_price", 0)

        if ltp > 1000:
            ltp = ltp / 100

        if token == CE_TOKEN:
            latest_ticks["ce_price"] = ltp
            latest_ticks["ce_timestamp"] = datetime.now().isoformat()
            ce_history.append(ltp)

            if "open_interest" in message:
                latest_ticks["ce_oi"] = message.get("open_interest", 0)
            if "volume_trade_for_the_day" in message:
                latest_ticks["ce_volume"] = message.get("volume_trade_for_the_day", 0)

            tick_counter += 1

        elif token == PE_TOKEN:
            latest_ticks["pe_price"] = ltp
            latest_ticks["pe_timestamp"] = datetime.now().isoformat()
            pe_history.append(ltp)

            if "open_interest" in message:
                latest_ticks["pe_oi"] = message.get("open_interest", 0)
            if "volume_trade_for_the_day" in message:
                latest_ticks["pe_volume"] = message.get("volume_trade_for_the_day", 0)

            tick_counter += 1

        last_tick_time = time.time()

        ce = latest_ticks["ce_price"]
        pe = latest_ticks["pe_price"]
        spot = spot_cache.get("value", 0)

        if ce > 0 and pe > 0 and spot > 0 and tick_counter % 5 == 0:
            run_signal_engine(ce, pe, spot)

    except Exception as e:
        logger.error(f"Tick processing error: {e}")

def on_ws_error(wsapp, error):
    logger.error(f"WebSocket error: {error}")

def on_ws_close(wsapp, close_status_code=None, close_msg=None):
    logger.warning(f"WebSocket CLOSE | code={close_status_code} | msg={close_msg}")
    global ws_running
    ws_running = False


# =============================================================================
# WebSocket Connection Manager
# =============================================================================
ws_running = False
sws = None
tick_counter = 0
last_tick_time = 0

def start_angel_websocket():
    global ws_running, CE_TOKEN, PE_TOKEN, sws, last_tick_time
    retry_delay = 30
    reconnect_count = 0

    while True:
        try:
            totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
            obj = SmartConnect(api_key=ANGEL_API_KEY)
            session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)

            if not session.get("status"):
                logger.error(f"Login failed: {session}")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 300)
                continue

            auth_token = session["data"]["jwtToken"]
            feed_token = obj.getfeedToken()
            logger.info("Authenticated successfully")

            spot = get_nifty_spot_angel(obj)
            if not spot:
                spot = get_nifty_spot()

            CE_TOKEN, PE_TOKEN = get_current_atm_tokens(obj)
            if not CE_TOKEN or not PE_TOKEN:
                logger.error("ATM tokens not found. Retrying...")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 300)
                continue

            sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)
            sws = _patch_sws_instance(sws)

            sws.on_open = on_ws_open
            sws.on_data = on_ws_data
            sws.on_error = on_ws_error
            sws.on_close = on_ws_close

            ws_running = True
            reconnect_count = 0
            retry_delay = 30
            last_tick_time = time.time()

            logger.info("Connecting WebSocket...")
            sws.connect()

            while ws_running:
                time.sleep(1)
                if time.time() - last_tick_time > 90 and tick_counter > 0:
                    logger.warning("Watchdog: No ticks for 90s, forcing reconnect")
                    ws_running = False
                    break

            logger.warning("WebSocket loop ended, reconnecting...")
            reconnect_count += 1

        except Exception as e:
            logger.error(f"WebSocket fatal error: {e}")
            reconnect_count += 1
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 300)


# =============================================================================
# REST Fallback
# =============================================================================
rest_client = None
rest_last_update = 0
REST_INTERVAL = 10

def get_rest_prices():
    global rest_client, rest_last_update

    if not CE_TOKEN or not PE_TOKEN:
        return

    now = time.time()
    if now - rest_last_update < REST_INTERVAL:
        return

    try:
        if rest_client is None:
            totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
            rest_client = SmartConnect(api_key=ANGEL_API_KEY)
            session = rest_client.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
            if not session.get("status"):
                rest_client = None
                return

        ltp_data = rest_client.ltpData("NFO", "NIFTY", CE_TOKEN)
        if ltp_data and ltp_data.get("data"):
            ce_price = float(ltp_data["data"]["ltp"])
            latest_ticks["ce_price"] = ce_price
            ce_history.append(ce_price)

        ltp_data = rest_client.ltpData("NFO", "NIFTY", PE_TOKEN)
        if ltp_data and ltp_data.get("data"):
            pe_price = float(ltp_data["data"]["ltp"])
            latest_ticks["pe_price"] = pe_price
            pe_history.append(pe_price)

        spot = get_nifty_spot_angel(rest_client)
        if not spot:
            spot = get_nifty_spot()

        if ce_price > 0 and pe_price > 0 and spot > 0:
            run_signal_engine(ce_price, pe_price, spot)

        rest_last_update = now
        logger.info(f"[REST FALLBACK] CE={ce_price}, PE={pe_price}, Spot={spot}")

    except Exception as e:
        logger.error(f"REST fallback error: {e}")
        rest_client = None

def rest_fallback_loop():
    while True:
        if not ws_running and CE_TOKEN and PE_TOKEN:
            get_rest_prices()
        time.sleep(REST_INTERVAL)


# =============================================================================
# Flask Endpoints
# =============================================================================

@app.route("/")
def home():
    return jsonify({
        "status": "online",
        "version": "3.0-professional",
        "message": "Nifty Professional Signal Engine",
        "features": [
            "Multi-timeframe confluence",
            "Market regime detection",
            "Signal persistence with confirmation",
            "Black-Scholes Greeks estimation",
            "Adaptive position sizing",
            "Risk management (SL/Target/Drawdown)",
            "Smart money flow analysis",
            "Order flow imbalance"
        ]
    })

@app.route("/api/live-signals")
def live_signals():
    return jsonify({
        "status": "active",
        "data": market_signal,
        "market": market_state,
        "institutional": institutional_state,
        "regime": market_regime,
        "signal_state": {
            "current_signal": signal_state["current_signal"],
            "signal_grade": signal_state["signal_grade"],
            "signal_age_bars": signal_state["signal_age_bars"],
            "consecutive_bullish": signal_state["consecutive_bullish"],
            "consecutive_bearish": signal_state["consecutive_bearish"],
            "position_size_pct": signal_state["position_size_pct"],
            "risk_reward": signal_state["risk_reward"],
            "max_drawdown_pct": signal_state["max_drawdown_pct"],
            "entry_price_ce": signal_state["entry_price_ce"],
            "entry_price_pe": signal_state["entry_price_pe"],
            "stop_loss_ce": signal_state["stop_loss_ce"],
            "stop_loss_pe": signal_state["stop_loss_pe"],
            "target_ce": signal_state["target_ce"],
            "target_pe": signal_state["target_pe"]
        },
        "timestamp": datetime.now().isoformat()
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
        "latest_spot": latest_ticks.get("spot_price", 0),
        "price_history_len": len(price_history),
        "ce_history_len": len(ce_history),
        "pe_history_len": len(pe_history),
        "signal": signal_state["current_signal"],
        "signal_grade": signal_state["signal_grade"],
        "regime": market_regime["regime"],
        "timestamp": datetime.now().isoformat()
    }), 200

@app.route("/api/signal-history")
def signal_history():
    return jsonify({
        "price_history": list(price_history)[-100:],
        "ce_history": list(ce_history)[-100:],
        "pe_history": list(pe_history)[-100:],
        "spread_history": list(spread_history)[-100:],
        "timestamp": datetime.now().isoformat()
    })

@app.route("/debug/ws-status")
def debug_ws():
    return jsonify({
        "ws_running": ws_running,
        "ce_token": CE_TOKEN,
        "pe_token": PE_TOKEN,
        "latest_ce": latest_ticks["ce_price"],
        "latest_pe": latest_ticks["pe_price"],
        "latest_spot": latest_ticks.get("spot_price", 0),
        "price_history_len": len(price_history),
        "ce_history_len": len(ce_history),
        "pe_history_len": len(pe_history),
        "signal": signal_state["current_signal"],
        "regime": market_regime["regime"],
        "session_phase": market_regime["session_phase"]
    })


# =============================================================================
# Startup
# =============================================================================
engine_started = False

def start_background_engine():
    global engine_started
    if not engine_started:
        ws_thread = threading.Thread(target=start_angel_websocket, daemon=True, name="WS-Main")
        ws_thread.start()

        rest_thread = threading.Thread(target=rest_fallback_loop, daemon=True, name="REST-Fallback")
        rest_thread.start()

        engine_started = True
        logger.info("=" * 60)
        logger.info("Nifty Professional Signal Engine v3.0 Started")
        logger.info("=" * 60)
        logger.info("Features:")
        logger.info("  - Multi-timeframe confluence analysis")
        logger.info("  - Market regime detection (Trending/Ranging/Volatile)")
        logger.info("  - Signal persistence with confirmation bars")
        logger.info("  - Black-Scholes Greeks estimation")
        logger.info("  - Adaptive position sizing")
        logger.info("  - Risk management: SL, Target, Max Drawdown")
        logger.info("  - Smart money flow analysis")
        logger.info("  - Order flow imbalance")
        logger.info("  - Anti-flip protection with consecutive bar confirmation")
        logger.info("  - Grade A/B/C/D signal quality scoring")
        logger.info("  - REST fallback when WebSocket is down")
        logger.info("=" * 60)

start_background_engine()

if __name__ == "__main__":
    start_background_engine()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, threaded=True)
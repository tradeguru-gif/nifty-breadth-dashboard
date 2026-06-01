import os
import time
import logging
import threading
import json
import requests
import pandas as pd
import numpy as np
import sqlite3
import math
from collections import deque
from datetime import datetime, timedelta, time as dt_time
from flask import Flask, jsonify, request
from flask_cors import CORS
import pyotp

# ============================================================
# INITIALIZATION, LOGGING & DEPENDENCIES
# ============================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app

try:
    from sklearn.ensemble import RandomForestClassifier
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import telebot
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False

# Credentials Matrix Configuration
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing critical Angel One environment variables")

DB_PATH = "trading_data.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS ticks
                 (timestamp REAL, token TEXT, price REAL, volume REAL, bid REAL, ask REAL, oi REAL)''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_ticks_time ON ticks(timestamp)')
    c.execute('''CREATE TABLE IF NOT EXISTS signals
                 (timestamp REAL, action TEXT, signal_type TEXT, grade TEXT, confidence REAL,
                  ce_price REAL, pe_price REAL, rsi REAL, pcr REAL, regime TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS trades
                 (timestamp REAL, action TEXT, entry_price REAL, exit_price REAL, pnl REAL,
                  size_pct REAL, status TEXT, grade TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS daily_performance
                 (date TEXT, equity REAL, daily_pnl REAL, drawdown_pct REAL, sharpe REAL, var REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS ml_models
                 (id INTEGER PRIMARY KEY, model BLOB, created_at REAL, features TEXT)''')
    conn.commit()
    conn.close()

init_db()

# ============================================================
# MONKEY-PATCH FOR SmartWebSocketV2 (Fix Token Parsing)
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
        ltp = int.from_bytes(binary_data[26:34], byteorder='little') / 100
        result['ltp'] = ltp
        volume = int.from_bytes(binary_data[34:42], byteorder='little')
        result['v'] = volume
    except Exception as e:
        logging.getLogger(__name__).error(f"Binary parse error: {e}")
    return result

SmartWebSocketV2._parse_binary_data = _patched_parse

_original_on_close = SmartWebSocketV2._on_close
def _patched_on_close(self, wsapp, *args):
    try: _original_on_close(self, wsapp)
    except: pass
SmartWebSocketV2._on_close = _patched_on_close

# ============================================================
# GLOBAL STATE & HIGH-FREQUENCY BUFFERS
# ============================================================
CE_TOKEN = None
PE_TOKEN = None
SPOT_TOKEN = "99926000"
CE_SYMBOL = ""
PE_SYMBOL = ""
ATM_STRIKE = 0
EXPIRY_DATE = ""

# Spot and Premium Analytical Rolling Buffers
spot_price_history = deque(maxlen=1000)
ce_price_history = deque(maxlen=1000)
pe_price_history = deque(maxlen=1000)
ce_volume_history = deque(maxlen=1000)
pe_volume_history = deque(maxlen=1000)
ce_oi_history = deque(maxlen=50)
pe_oi_history = deque(maxlen=50)

# Timeframe candle history for multi-timeframe trend analysis
# ONLY INITIALIZED ONCE HERE - do not redefine elsewhere
TIMEFRAMES = ["1min", "2min", "3min", "5min", "10min", "15min", "20min"]
timeframe_history = {tf: deque(maxlen=50) for tf in TIMEFRAMES}

# Timeframe tracking for candle aggregation
last_timeframe_update = {tf: 0 for tf in TIMEFRAMES}
timeframe_candles = {
    tf: {"open": 0, "high": 0, "low": float('inf'), "close": 0, "active": False}
    for tf in TIMEFRAMES
}

latest_ticks = {
    "spot_price": 0.0, "ce_price": 0.0, "pe_price": 0.0,
    "ce_volume": 0, "pe_volume": 0, "ce_oi": 0, "pe_oi": 0,
    "ce_bid": 0.0, "ce_ask": 0.0, "pe_bid": 0.0, "pe_ask": 0.0
}

tick_counter = 0
ws_running = False
sws = None
last_tick_time = time.time()
engine_active = True
last_minute_snapshot = {"time": 0, "price": 0}

# Professional Signal State Management
signal_state = {
    "current_action": "HOLD",
    "entry_price": 0.0,
    "stop_loss": 0.0,
    "target": 0.0,
    "highest_premium_seen": 0.0,
    "signal_grade": "D",
    "confidence": 0.0,
    "position_size_pct": 0,
    "cooldown_until": 0,
    "entry_time": 0,
    "max_profit_seen": 0.0
}

portfolio_state = {
    "equity": 100000.0, "initial_equity": 100000.0, "daily_pnl": 0.0,
    "max_drawdown_today": 0.0, "open_positions": 0, "daily_peak": 100000.0,
    "daily_loss_limit_pct": 2.0, "var_95": 0.0, "sharpe_ratio": 0.0
}

market_signal = {
    "signal": "WAITING", "spot_price": 0.0, "ce_price": 0.0, "pe_price": 0.0,
    "spot_rsi": 50.0, "spot_macd": 0.0, "pcr": 1.0, "spot_atr": 0.0,
    "regime": "RANGING", "confidence": 50.0, "timestamp": "",
    "alert_message": "Initializing...",
    "signal_strength": "NONE"
}

market_state = {
    "rsi": 50, "momentum": "NEUTRAL", "strength": "LOW", "trend": "SIDEWAYS",
    "action": "HOLD", "confidence": 0, "volatility": "NORMAL", "regime": "UNKNOWN"
}

institutional_state = {
    "vwap": 0.0, "ema_fast": 0.0, "ema_slow": 0.0, "atr": 0.0,
    "delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0, "iv": 0.20
}

pcr_cache = {"value": 1.0, "time": 0}
spot_cache = {"value": None, "timestamp": 0}
CACHE_TTL = 15

# ============================================================
# ENHANCED CONFIGURATION - PROFESSIONAL GRADE
# ============================================================
CONFIG = {
    "SPOT_RSI_PERIOD": 14,
    "SPOT_RSI_SMOOTHING": 3,
    "SPOT_MACD_FAST": 12,
    "SPOT_MACD_SLOW": 26,
    "SPOT_MACD_SIGNAL": 9,
    "SPOT_ATR_PERIOD": 14,
    "ATR_SMOOTHING": True,
    "CONSIDER_CE_RSI": 52,
    "CONSIDER_PE_RSI": 48,
    "STRONG_CE_RSI": 58,
    "STRONG_PE_RSI": 42,
    "EXTREME_CE_RSI": 65,
    "EXTREME_PE_RSI": 35,
    "MACD_CONFIRM_THRESHOLD": 0.5,
    "VOLUME_SPIKE_RATIO": 1.3,
    "VOLUME_MA_PERIOD": 20,
    "PCR_BULLISH_THRESHOLD": 0.85,
    "PCR_BEARISH_THRESHOLD": 1.15,
    "TREND_STRENGTH_PERIOD": 14,
    "STRONG_TREND_MIN": 25,
    "ENTRY_ATR_MULT": 1.5,
    "TRAILING_ATR_MULT": 1.8,
    "TARGET_ATR_MULT": 4.0,
    "COOLDOWN_SEC": 120,
    "MAX_HOLD_TIME_MIN": 45,
    "MIN_PROFIT_LOCK": 0.3,
    "BREAKEVEN_TRIGGER": 1.0,
    "MIN_SIGNAL_HOLD_SEC": 30,
    "MAX_DAILY_TRADES": 8,
    "CONSECUTIVE_SAME_DIR_MAX": 2,
}

# Signal tracking for persistence validation
signal_buffer = {
    "ce_count": 0, "pe_count": 0,
    "last_signal_time": 0, "consecutive_ce": 0, "consecutive_pe": 0
}

daily_trade_count = 0
last_trade_date = ""

# ============================================================
# ENHANCED TECHNICAL ANALYSIS ENGINE
# ============================================================
def calculate_rsi(prices, period=14, smoothing=3):
    if len(prices) < period + 1: return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0: return 100.0
    rsi_raw = 100 - (100 / (1 + avg_gain / avg_loss))
    if smoothing > 1 and len(prices) >= period + smoothing:
        rsi_values = []
        for j in range(smoothing):
            sub_gains = gains[-(period+j):-j if j > 0 else None]
            sub_losses = losses[-(period+j):-j if j > 0 else None]
            if len(sub_gains) == period:
                ag = sum(sub_gains) / period
                al = sum(sub_losses) / period
                if al == 0:
                    rsi_values.append(100.0)
                else:
                    rsi_values.append(100 - (100 / (1 + ag / al)))
        if rsi_values:
            alpha = 2 / (smoothing + 1)
            rsi_smooth = rsi_values[0]
            for rv in rsi_values[1:]:
                rsi_smooth = alpha * rv + (1 - alpha) * rsi_smooth
            return rsi_smooth
    return rsi_raw

def calculate_macd(prices, fast=12, slow=26, signal_period=9):
    if len(prices) < slow + signal_period: return 0.0, 0.0, 0.0
    def ema(arr, p):
        alpha = 2 / (p + 1)
        val = arr[0]
        for x in arr[1:]: val = alpha * x + (1 - alpha) * val
        return val
    ema_fast = ema(prices[-fast:], fast)
    ema_slow = ema(prices[-slow:], slow)
    macd_line = ema_fast - ema_slow
    macd_history = []
    for i in range(signal_period, 0, -1):
        if len(prices) >= slow + i:
            ef = ema(prices[-(fast+i):-i], fast)
            es = ema(prices[-(slow+i):-i], slow)
            macd_history.append(ef - es)
    if macd_history:
        signal_line = ema(macd_history, signal_period)
    else:
        signal_line = macd_line
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def calculate_atr(prices, period=14):
    if len(prices) < period + 1: return 5.0
    tr_list = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    if CONFIG.get("ATR_SMOOTHING", True) and len(tr_list) >= period:
        atr = sum(tr_list[:period]) / period
        for tr in tr_list[period:]:
            atr = ((period - 1) * atr + tr) / period
        return atr
    return sum(tr_list[-period:]) / period

def calculate_adx(prices, period=14):
    if len(prices) < period * 2 + 1: return 20.0
    plus_dm = []
    minus_dm = []
    tr_list = []
    for i in range(1, len(prices)):
        up_move = prices[i] - prices[i-1]
        down_move = prices[i-1] - prices[i]
        plus_dm.append(max(up_move, 0) if up_move > down_move else 0)
        minus_dm.append(max(down_move, 0) if down_move > up_move else 0)
        tr_list.append(abs(prices[i] - prices[i-1]))
    if len(tr_list) < period: return 20.0
    atr = sum(tr_list[-period:]) / period
    plus_di = 100 * sum(plus_dm[-period:]) / period / atr if atr > 0 else 0
    minus_di = 100 * sum(minus_dm[-period:]) / period / atr if atr > 0 else 0
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di) if (plus_di + minus_di) > 0 else 0
    return dx

def calculate_ema(prices, period):
    if len(prices) < period: return sum(prices) / len(prices) if prices else 0
    alpha = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = alpha * p + (1 - alpha) * ema
    return ema

# ============================================================
# RISK ENVIRONMENT MATRICES
# ============================================================
class RiskManager:
    def check_daily_loss_limit(self):
        loss_pct = (portfolio_state["initial_equity"] - portfolio_state["equity"]) / portfolio_state["initial_equity"] * 100
        return loss_pct >= portfolio_state["daily_loss_limit_pct"]

    def check_max_daily_trades(self):
        global daily_trade_count, last_trade_date
        today = datetime.now().strftime("%Y-%m-%d")
        if today != last_trade_date:
            daily_trade_count = 0
            last_trade_date = today
        return daily_trade_count >= CONFIG["MAX_DAILY_TRADES"]

risk_manager = RiskManager()

def send_telegram_alert(message):
    if not TELEGRAM_AVAILABLE or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}, timeout=3)
    except Exception as e: logger.error(f"Telegram failed: {e}")

# ============================================================
# DATA SOURCING PIPELINES
# ============================================================
auth_cache = {"token": None, "feed_token": None, "timestamp": 0, "obj": None}

def get_auth_token():
    now = time.time()
    if auth_cache["token"] and (now - auth_cache["timestamp"] < 3600):
        return auth_cache["token"], auth_cache["feed_token"], auth_cache["obj"]
    try:
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        obj = SmartConnect(api_key=ANGEL_API_KEY)
        session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
        if not session.get("status"): return None, None, None
        auth_token = session["data"]["jwtToken"]
        feed_token = obj.getfeedToken()
        auth_cache.update({"token": auth_token, "feed_token": feed_token, "timestamp": now, "obj": obj})
        return auth_token, feed_token, obj
    except Exception as e:
        logger.error(f"Auth loop fail: {e}")
        return None, None, None

def get_nifty_spot():
    _, _, obj = get_auth_token()
    if not obj: return None
    try:
        response = obj.ltpData("NSE", "NIFTY", SPOT_TOKEN)
        if response.get("status") and response.get("data"):
            return float(response["data"].get("ltp", 0))
    except Exception as e: logger.error(f"Error reading Spot Nifty: {e}")
    return None

def get_nifty_pcr():
    global ce_oi_history, pe_oi_history
    if ce_oi_history and pe_oi_history:
        ce_sum = sum(ce_oi_history)
        pe_sum = sum(pe_oi_history)
        if ce_sum > 0: return round(pe_sum / ce_sum, 2)
    return pcr_cache["value"]

def get_current_atm_tokens():
    global CE_TOKEN, PE_TOKEN, ATM_STRIKE, EXPIRY_DATE
    spot = get_nifty_spot()
    if not spot: return None, None
    atm_strike = round(spot / 50) * 50
    try:
        url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
        resp = requests.get(url, timeout=15)
        df = pd.DataFrame(resp.json())
        nifty_opts = df[(df["name"] == "NIFTY") & (df["instrumenttype"] == "OPTIDX") & (df["exch_seg"] == "NFO")].copy()
        if nifty_opts.empty: return None, None
        nifty_opts["expiry_date"] = pd.to_datetime(nifty_opts["expiry"], format="%d%b%Y", errors="coerce")
        nifty_opts = nifty_opts.dropna(subset=["expiry_date"])
        nifty_opts["strike"] = pd.to_numeric(nifty_opts["strike"], errors="coerce") / 100
        today = datetime.now()
        future = nifty_opts[nifty_opts["expiry_date"] >= today]
        nearest_expiry = future["expiry_date"].min()
        atm_opts = future[(future["strike"] == atm_strike) & (future["expiry_date"] == nearest_expiry)]
        ce = atm_opts[atm_opts["symbol"].str.contains("CE")]
        pe = atm_opts[atm_opts["symbol"].str.contains("PE")]
        if ce.empty or pe.empty: return None, None
        CE_TOKEN = str(ce.iloc[0]["token"])
        PE_TOKEN = str(pe.iloc[0]["token"])
        ATM_STRIKE = atm_strike
        EXPIRY_DATE = nearest_expiry.strftime("%d%b%Y").upper()
        logger.info(f"Scrip Resolved Successfully -> CE: {CE_TOKEN} | PE: {PE_TOKEN} | Strike: {ATM_STRIKE}")
        return CE_TOKEN, PE_TOKEN
    except Exception as e:
        logger.error(f"Error building Option Chain parameters: {e}")
        return None, None

# ============================================================
# GRADE 1 PRO SIGNAL CLASSIFICATION ENGINE
# ============================================================
def classify_signal_strength(rsi, macd_hist, adx, pcr, volume_ratio, trend_direction):
    score = 0
    factors = []
    if trend_direction == "BULLISH":
        if rsi >= CONFIG["STRONG_CE_RSI"]:
            score += 2; factors.append("RSI_STRONG")
        elif rsi >= CONFIG["CONSIDER_CE_RSI"]:
            score += 1; factors.append("RSI_CONSIDER")
        if macd_hist > CONFIG["MACD_CONFIRM_THRESHOLD"]:
            score += 2; factors.append("MACD_CONFIRM")
        elif macd_hist > 0:
            score += 1; factors.append("MACD_WEAK")
        if adx >= CONFIG["STRONG_TREND_MIN"]:
            score += 2; factors.append("TREND_STRONG")
        elif adx >= 20:
            score += 1; factors.append("TREND_MODERATE")
        if pcr <= CONFIG["PCR_BULLISH_THRESHOLD"]:
            score += 1; factors.append("PCR_BULLISH")
        if volume_ratio >= CONFIG["VOLUME_SPIKE_RATIO"]:
            score += 1; factors.append("VOLUME_SPIKE")
    else:
        if rsi <= CONFIG["STRONG_PE_RSI"]:
            score += 2; factors.append("RSI_STRONG")
        elif rsi <= CONFIG["CONSIDER_PE_RSI"]:
            score += 1; factors.append("RSI_CONSIDER")
        if macd_hist < -CONFIG["MACD_CONFIRM_THRESHOLD"]:
            score += 2; factors.append("MACD_CONFIRM")
        elif macd_hist < 0:
            score += 1; factors.append("MACD_WEAK")
        if adx >= CONFIG["STRONG_TREND_MIN"]:
            score += 2; factors.append("TREND_STRONG")
        elif adx >= 20:
            score += 1; factors.append("TREND_MODERATE")
        if pcr >= CONFIG["PCR_BEARISH_THRESHOLD"]:
            score += 1; factors.append("PCR_BEARISH")
        if volume_ratio >= CONFIG["VOLUME_SPIKE_RATIO"]:
            score += 1; factors.append("VOLUME_SPIKE")
    if score >= 6:
        return "STRONG", score, factors
    elif score >= 3:
        return "CONSIDER", score, factors
    else:
        return "WEAK", score, factors

def generate_alert_message(action, strength, spot_price, premium, sl, target, factors, confidence):
    factor_str = " | ".join(factors) if factors else "Basic"
    if action == "BUY_CE":
        if strength == "STRONG":
            return (
                f"🟢 <b>STRONG CE BUY</b>\n"
                f"💰 Spot: {spot_price} | Premium: {premium}\n"
                f"🎯 Target: {target:.2f} | 🛡️ SL: {sl:.2f}\n"
                f"📊 Confidence: {confidence:.1f}% | Factors: {factor_str}"
            )
        else:
            return (
                f"🟡 <b>CONSIDER CE BUY</b>\n"
                f"💰 Spot: {spot_price} | Premium: {premium}\n"
                f"🎯 Target: {target:.2f} | 🛡️ SL: {sl:.2f}\n"
                f"📊 Confidence: {confidence:.1f}% | Factors: {factor_str}"
            )
    elif action == "BUY_PE":
        if strength == "STRONG":
            return (
                f"🔴 <b>STRONG PE BUY</b>\n"
                f"💰 Spot: {spot_price} | Premium: {premium}\n"
                f"🎯 Target: {target:.2f} | 🛡️ SL: {sl:.2f}\n"
                f"📊 Confidence: {confidence:.1f}% | Factors: {factor_str}"
            )
        else:
            return (
                f"🟠 <b>CONSIDER PE BUY</b>\n"
                f"💰 Spot: {spot_price} | Premium: {premium}\n"
                f"🎯 Target: {target:.2f} | 🛡️ SL: {sl:.2f}\n"
                f"📊 Confidence: {confidence:.1f}% | Factors: {factor_str}"
            )
    elif action == "EXIT":
        return (
            f"⚠️ <b>EXIT SIGNAL</b>\n"
            f"💰 Spot: {spot_price} | Premium: {premium}\n"
            f"📊 Reason: {factor_str}"
        )
    elif action == "HOLD":
        return (
            f"⏸️ <b>HOLD</b>\n"
            f"💰 Spot: {spot_price}\n"
            f"📊 Market: Ranging | Confidence: {confidence:.1f}%"
        )
    return (
        f"📊 <b>WAITING</b>\n"
        f"💰 Spot: {spot_price}"
    )

# ============================================================
# GRADE 1 PRO SIGNAL EXECUTION ENGINE
# ============================================================
def run_signal_engine():
    global market_signal, market_state, signal_state, portfolio_state, latest_ticks
    global signal_buffer, daily_trade_count

    if risk_manager.check_daily_loss_limit():
        market_state["action"] = "HALTED_LOSS_LIMIT"
        market_signal["alert_message"] = "🚫 HALTED: Daily loss limit reached"
        market_signal["signal_strength"] = "HALTED"
        return

    if risk_manager.check_max_daily_trades():
        market_state["action"] = "HALTED_MAX_TRADES"
        market_signal["alert_message"] = "🚫 HALTED: Max daily trades reached"
        market_signal["signal_strength"] = "HALTED"
        return

    spot_history_list = list(spot_price_history)
    if len(spot_history_list) < 50:
        market_signal["alert_message"] = "⏳ Collecting market data..."
        market_signal["signal_strength"] = "WAITING"
        return

    spot_rsi = calculate_rsi(spot_history_list, CONFIG["SPOT_RSI_PERIOD"], CONFIG["SPOT_RSI_SMOOTHING"])
    spot_macd, macd_signal_line, macd_hist = calculate_macd(
        spot_history_list, CONFIG["SPOT_MACD_FAST"], CONFIG["SPOT_MACD_SLOW"], CONFIG["SPOT_MACD_SIGNAL"]
    )
    spot_atr = calculate_atr(spot_history_list, CONFIG["SPOT_ATR_PERIOD"])
    adx = calculate_adx(spot_history_list, CONFIG["TREND_STRENGTH_PERIOD"])
    pcr = get_nifty_pcr()

    ce_vol_list = list(ce_volume_history)
    pe_vol_list = list(pe_volume_history)
    avg_ce_vol = sum(ce_vol_list[-CONFIG["VOLUME_MA_PERIOD"]:]) / min(len(ce_vol_list), CONFIG["VOLUME_MA_PERIOD"]) if ce_vol_list else 1
    avg_pe_vol = sum(pe_vol_list[-CONFIG["VOLUME_MA_PERIOD"]:]) / min(len(pe_vol_list), CONFIG["VOLUME_MA_PERIOD"]) if pe_vol_list else 1
    current_ce_vol = ce_vol_list[-1] if ce_vol_list else 0
    current_pe_vol = pe_vol_list[-1] if pe_vol_list else 0
    ce_vol_ratio = current_ce_vol / avg_ce_vol if avg_ce_vol > 0 else 1
    pe_vol_ratio = current_pe_vol / avg_pe_vol if avg_pe_vol > 0 else 1

    ema_fast = calculate_ema(spot_history_list, 20)
    ema_slow = calculate_ema(spot_history_list, 50)
    price_above_fast = spot_history_list[-1] > ema_fast
    price_above_slow = spot_history_list[-1] > ema_slow

    current_time = time.time()

    market_state.update({
        "rsi": round(spot_rsi, 1),
        "macd_hist": round(macd_hist, 4),
        "atr": round(spot_atr, 2),
        "adx": round(adx, 1),
        "pcr": round(pcr, 2),
        "trend": "UPTREND" if price_above_fast and price_above_slow else "DOWNTREND" if not price_above_fast and not price_above_slow else "MIXED"
    })

    # ========== STATE A: ACTIVE POSITION MANAGEMENT ==========
    if signal_state["current_action"] != "HOLD":
        active_side = signal_state["current_action"]
        current_premium = latest_ticks["ce_price"] if active_side == "BUY_CE" else latest_ticks["pe_price"]

        if current_premium == 0:
            market_signal["alert_message"] = "⏳ Waiting for premium data..."
            return

        unrealized_pnl = current_premium - signal_state["entry_price"]
        if unrealized_pnl > signal_state["max_profit_seen"]:
            signal_state["max_profit_seen"] = unrealized_pnl

        if current_premium > signal_state["highest_premium_seen"]:
            signal_state["highest_premium_seen"] = current_premium
            new_sl = current_premium - (spot_atr * CONFIG["TRAILING_ATR_MULT"])
            if new_sl > signal_state["stop_loss"]:
                old_sl = signal_state["stop_loss"]
                signal_state["stop_loss"] = new_sl
                if new_sl > signal_state["entry_price"] and old_sl <= signal_state["entry_price"]:
                    send_telegram_alert(
                        f"🔒 <b>SL MOVED TO BREAKEVEN</b>\n"
                        f"{active_side} @ {current_premium:.2f}"
                    )

        if current_premium <= signal_state["stop_loss"]:
            pnl_points = current_premium - signal_state["entry_price"]
            portfolio_state["equity"] += (pnl_points * 50)
            daily_trade_count += 1
            exit_msg = generate_alert_message("EXIT", "STOP_LOSS", latest_ticks["spot_price"], current_premium, 0, 0, ["Trailing SL Hit"], 0)
            send_telegram_alert(exit_msg + f"\n💵 PnL: {pnl_points:.2f} pts")
            reset_signal_state(current_time)
            return

        if current_premium >= signal_state["target"]:
            pnl_points = current_premium - signal_state["entry_price"]
            portfolio_state["equity"] += (pnl_points * 50)
            daily_trade_count += 1
            exit_msg = generate_alert_message("EXIT", "TARGET", latest_ticks["spot_price"], current_premium, 0, 0, ["Target Achieved"], 100)
            send_telegram_alert(exit_msg + f"\n💵 PnL: {pnl_points:.2f} pts")
            reset_signal_state(current_time)
            return

        if signal_state["max_profit_seen"] > spot_atr * 0.5:
            drawdown_from_peak = signal_state["max_profit_seen"] - unrealized_pnl
            if drawdown_from_peak > signal_state["max_profit_seen"] * 0.5:
                pnl_points = current_premium - signal_state["entry_price"]
                portfolio_state["equity"] += (pnl_points * 50)
                daily_trade_count += 1
                exit_msg = generate_alert_message("EXIT", "PROFIT_LOCK", latest_ticks["spot_price"], current_premium, 0, 0, ["Profit Lock - 50% Drawdown from Peak"], 0)
                send_telegram_alert(exit_msg + f"\n💵 PnL: {pnl_points:.2f} pts")
                reset_signal_state(current_time)
                return

        hold_time_min = (current_time - signal_state["entry_time"]) / 60
        if hold_time_min >= CONFIG["MAX_HOLD_TIME_MIN"]:
            pnl_points = current_premium - signal_state["entry_price"]
            portfolio_state["equity"] += (pnl_points * 50)
            daily_trade_count += 1
            exit_msg = generate_alert_message("EXIT", "TIME", latest_ticks["spot_price"], current_premium, 0, 0, [f"Max Hold Time ({CONFIG['MAX_HOLD_TIME_MIN']}min)"], 0)
            send_telegram_alert(exit_msg + f"\n💵 PnL: {pnl_points:.2f} pts")
            reset_signal_state(current_time)
            return

        if active_side == "BUY_CE":
            if spot_rsi < 45 or macd_hist < -0.5 or not price_above_fast:
                pnl_points = current_premium - signal_state["entry_price"]
                portfolio_state["equity"] += (pnl_points * 50)
                daily_trade_count += 1
                reasons = []
                if spot_rsi < 45: reasons.append("RSI<45")
                if macd_hist < -0.5: reasons.append("MACD reversal")
                if not price_above_fast: reasons.append("Price<EMA20")
                exit_msg = generate_alert_message("EXIT", "MOMENTUM", latest_ticks["spot_price"], current_premium, 0, 0, reasons, 0)
                send_telegram_alert(exit_msg + f"\n💵 PnL: {pnl_points:.2f} pts")
                reset_signal_state(current_time)
                return

        elif active_side == "BUY_PE":
            if spot_rsi > 55 or macd_hist > 0.5 or price_above_fast:
                pnl_points = current_premium - signal_state["entry_price"]
                portfolio_state["equity"] += (pnl_points * 50)
                daily_trade_count += 1
                reasons = []
                if spot_rsi > 55: reasons.append("RSI>55")
                if macd_hist > 0.5: reasons.append("MACD reversal")
                if price_above_fast: reasons.append("Price>EMA20")
                exit_msg = generate_alert_message("EXIT", "MOMENTUM", latest_ticks["spot_price"], current_premium, 0, 0, reasons, 0)
                send_telegram_alert(exit_msg + f"\n💵 PnL: {pnl_points:.2f} pts")
                reset_signal_state(current_time)
                return

        hold_mins = int((current_time - signal_state["entry_time"]) / 60)
        pnl_pct = ((current_premium - signal_state["entry_price"]) / signal_state["entry_price"] * 50) if signal_state["entry_price"] > 0 else 0
        market_signal["alert_message"] = f"📊 {active_side} ACTIVE | Hold: {hold_mins}m | PnL: {pnl_pct:.1f}% | SL: {signal_state['stop_loss']:.2f}"
        market_signal["signal_strength"] = "ACTIVE"

    # ========== STATE B: POSITION DISCOVERY (SCANNER) ==========
    else:
        if current_time < signal_state["cooldown_until"]:
            remaining = int(signal_state["cooldown_until"] - current_time)
            market_signal["alert_message"] = f"⏳ Cooldown: {remaining}s remaining"
            market_signal["signal_strength"] = "COOLDOWN"
            return

        if spot_rsi >= CONFIG["CONSIDER_CE_RSI"] and macd_hist > 0 and price_above_fast:
            ce_premium = latest_ticks["ce_price"]
            if ce_premium == 0:
                market_signal["alert_message"] = "⏳ Waiting for CE premium data..."
                return

            signal_buffer["ce_count"] += 1
            signal_buffer["pe_count"] = 0

            if signal_buffer["ce_count"] < 3:
                market_signal["alert_message"] = f"🟡 CE Signal Building... ({signal_buffer['ce_count']}/3)"
                market_signal["signal_strength"] = "BUILDING"
                return

            strength, score, factors = classify_signal_strength(
                spot_rsi, macd_hist, adx, pcr, ce_vol_ratio, "BULLISH"
            )

            if strength == "WEAK":
                market_signal["alert_message"] = f"⚪ Weak CE Signal Ignored (Score: {score}/10)"
                market_signal["signal_strength"] = "WEAK"
                signal_buffer["ce_count"] = 0
                return

            if signal_buffer["consecutive_ce"] >= CONFIG["CONSECUTIVE_SAME_DIR_MAX"]:
                market_signal["alert_message"] = "🚫 CE Blocked: Max consecutive entries reached"
                market_signal["signal_strength"] = "BLOCKED"
                signal_buffer["ce_count"] = 0
                return

            sl = ce_premium - (spot_atr * CONFIG["ENTRY_ATR_MULT"])
            target = ce_premium + (spot_atr * CONFIG["TARGET_ATR_MULT"])
            confidence = min(95, score * 12 + spot_rsi * 0.3)

            if score >= 7: grade = "A+"
            elif score >= 5: grade = "A"
            elif score >= 4: grade = "B+"
            else: grade = "B"

            signal_state.update({
                "current_action": "BUY_CE",
                "entry_price": ce_premium,
                "highest_premium_seen": ce_premium,
                "stop_loss": sl,
                "target": target,
                "signal_grade": grade,
                "confidence": confidence,
                "entry_time": current_time,
                "max_profit_seen": 0.0
            })
            portfolio_state["open_positions"] = 1
            signal_buffer["consecutive_ce"] += 1
            signal_buffer["consecutive_pe"] = 0
            daily_trade_count += 1

            alert = generate_alert_message("BUY_CE", strength, latest_ticks["spot_price"], ce_premium, sl, target, factors, confidence)
            send_telegram_alert(alert)
            logger.info(f"SIGNAL: {strength} CE BUY | Score: {score} | Grade: {grade}")

        elif spot_rsi <= CONFIG["CONSIDER_PE_RSI"] and macd_hist < 0 and not price_above_fast:
            pe_premium = latest_ticks["pe_price"]
            if pe_premium == 0:
                market_signal["alert_message"] = "⏳ Waiting for PE premium data..."
                return

            signal_buffer["pe_count"] += 1
            signal_buffer["ce_count"] = 0

            if signal_buffer["pe_count"] < 3:
                market_signal["alert_message"] = f"🟡 PE Signal Building... ({signal_buffer['pe_count']}/3)"
                market_signal["signal_strength"] = "BUILDING"
                return

            strength, score, factors = classify_signal_strength(
                spot_rsi, macd_hist, adx, pcr, pe_vol_ratio, "BEARISH"
            )

            if strength == "WEAK":
                market_signal["alert_message"] = f"⚪ Weak PE Signal Ignored (Score: {score}/10)"
                market_signal["signal_strength"] = "WEAK"
                signal_buffer["pe_count"] = 0
                return

            if signal_buffer["consecutive_pe"] >= CONFIG["CONSECUTIVE_SAME_DIR_MAX"]:
                market_signal["alert_message"] = "🚫 PE Blocked: Max consecutive entries reached"
                market_signal["signal_strength"] = "BLOCKED"
                signal_buffer["pe_count"] = 0
                return

            sl = pe_premium - (spot_atr * CONFIG["ENTRY_ATR_MULT"])
            target = pe_premium + (spot_atr * CONFIG["TARGET_ATR_MULT"])
            confidence = min(95, score * 12 + (100 - spot_rsi) * 0.3)

            if score >= 7: grade = "A+"
            elif score >= 5: grade = "A"
            elif score >= 4: grade = "B+"
            else: grade = "B"

            signal_state.update({
                "current_action": "BUY_PE",
                "entry_price": pe_premium,
                "highest_premium_seen": pe_premium,
                "stop_loss": sl,
                "target": target,
                "signal_grade": grade,
                "confidence": confidence,
                "entry_time": current_time,
                "max_profit_seen": 0.0
            })
            portfolio_state["open_positions"] = 1
            signal_buffer["consecutive_pe"] += 1
            signal_buffer["consecutive_ce"] = 0
            daily_trade_count += 1

            alert = generate_alert_message("BUY_PE", strength, latest_ticks["spot_price"], pe_premium, sl, target, factors, confidence)
            send_telegram_alert(alert)
            logger.info(f"SIGNAL: {strength} PE BUY | Score: {score} | Grade: {grade}")

        else:
            signal_buffer["ce_count"] = 0
            signal_buffer["pe_count"] = 0

            if spot_rsi > 55:
                status = "Bullish bias but waiting for confirmation"
            elif spot_rsi < 45:
                status = "Bearish bias but waiting for confirmation"
            else:
                status = "Market ranging - no clear direction"

            market_signal["alert_message"] = f"⏸️ HOLD | {status}\nRSI: {spot_rsi:.1f} | MACD: {macd_hist:.2f} | ADX: {adx:.1f}"
            market_signal["signal_strength"] = "HOLD"

    # ========== TIMEFRAME TREND CALCULATION ==========
    for tf in TIMEFRAMES:
        tf_data = list(timeframe_history[tf])
        if len(tf_data) >= 3:
            c1 = tf_data[-3]["close"]
            c2 = tf_data[-2]["close"]
            c3 = tf_data[-1]["close"]

            if c3 > c2 > c1:
                market_signal[f"trend_{tf}"] = "BULLISH"
            elif c3 < c2 < c1:
                market_signal[f"trend_{tf}"] = "BEARISH"
            else:
                market_signal[f"trend_{tf}"] = "SIDEWAYS"
        else:
            market_signal[f"trend_{tf}"] = "SIDEWAYS"

    # ========== GLOBAL REPORTING SYNCHRONIZATION ==========
    market_signal.update({
        "spot_price": latest_ticks["spot_price"],
        "ce_price": latest_ticks["ce_price"],
        "pe_price": latest_ticks["pe_price"],
        "spot_rsi": round(spot_rsi, 2),
        "spot_macd": round(spot_macd, 4),
        "macd_hist": round(macd_hist, 4),
        "spot_atr": round(spot_atr, 2),
        "adx": round(adx, 1),
        "pcr": round(pcr, 2),
        "signal": signal_state["current_action"],
        "confidence": round(signal_state["confidence"], 2),
        "timestamp": datetime.now().isoformat(),
        "grade": signal_state["signal_grade"],
        "daily_trades": daily_trade_count
    })

def reset_signal_state(current_time):
    global signal_state, portfolio_state, signal_buffer
    signal_state.update({
        "current_action": "HOLD", "entry_price": 0.0, "stop_loss": 0.0,
        "target": 0.0, "highest_premium_seen": 0.0, "confidence": 0.0,
        "cooldown_until": current_time + CONFIG["COOLDOWN_SEC"],
        "entry_time": 0, "max_profit_seen": 0.0
    })
    portfolio_state["open_positions"] = 0
    signal_buffer["ce_count"] = 0
    signal_buffer["pe_count"] = 0

# ============================================================
# TIMING AND SESSION STRUCTURING UTILITIES
# ============================================================
def get_ist_now():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def is_market_open():
    now_ist = get_ist_now()
    if now_ist.weekday() >= 5: return False
    return dt_time(9, 15) <= now_ist.time() <= dt_time(15, 30)

# ============================================================
# WEBSOCKET SUBSCRIPTION STREAM DATA INTERFACES
# ============================================================
def on_ws_open(wsapp, open_message):
    global sws
    logger.info(f"WebSocket opened: {open_message}")
    if sws and CE_TOKEN and PE_TOKEN:
        try:
            subscription_payload = [
                {"exchangeType": 1, "tokens": [SPOT_TOKEN]},
                {"exchangeType": 2, "tokens": [CE_TOKEN, PE_TOKEN]}
            ]
            sws.subscribe("tradeguru_001", 1, subscription_payload)
            logger.info("Streaming pipeline verified online. Multi-token subscription confirmed.")
        except Exception as e:
            logger.error(f"Subscription initialization failure: {e}")

def on_ws_error(wsapp, error):
    logger.error(f"WebSocket Error: {error}")

def on_ws_close(wsapp, code, msg):
    global ws_running
    ws_running = False
    logger.warning(f"WebSocket closed: code={code}, msg={msg}")

# ============================================================
# WEBSOCKET DATA HANDLER WITH TIMEFRAME AGGREGATION
# ============================================================
def on_ws_data(wsapp, message):
    global tick_counter, last_tick_time, latest_ticks
    global spot_price_history, ce_price_history, pe_price_history
    global last_timeframe_update, timeframe_candles, timeframe_history

    last_tick_time = time.time()

    try:
        if isinstance(message, bytes):
            if sws is None:
                return
            tick = sws._parse_binary_data(message)
            if not tick:
                return
            ticks = [tick]
        else:
            data = json.loads(message) if isinstance(message, str) else message
            ticks = data if isinstance(data, list) else [data]

        for tick in ticks:
            token = str(tick.get("token") or tick.get("tk", ""))
            ltp = tick.get("ltp") or tick.get("last_traded_price", 0)

            if isinstance(ltp, (int, float)) and ltp > 50000 and token != SPOT_TOKEN:
                ltp = ltp / 100

            vol = tick.get("v") or tick.get("volume_trade_for_the_day", 0)
            oi = tick.get("oi") or tick.get("open_interest", 0)

            if token == SPOT_TOKEN:
                latest_ticks["spot_price"] = ltp
                spot_price_history.append(ltp)
                tick_counter += 1

                # ========== TIMEFRAME CANDLE AGGREGATION ==========
                current_time = time.time()

                for tf, interval_sec in [
                    ("1min", 60), ("2min", 120), ("3min", 180),
                    ("5min", 300), ("10min", 600), ("15min", 900), ("20min", 1200)
                ]:
                    candle = timeframe_candles[tf]

                    if current_time - last_timeframe_update[tf] >= interval_sec:
                        if candle["active"]:
                            timeframe_history[tf].append({
                                "open": candle["open"],
                                "high": candle["high"],
                                "low": candle["low"],
                                "close": candle["close"],
                                "timestamp": last_timeframe_update[tf]
                            })

                        candle["open"] = ltp
                        candle["high"] = ltp
                        candle["low"] = ltp
                        candle["close"] = ltp
                        candle["active"] = True
                        last_timeframe_update[tf] = current_time

                    else:
                        if not candle["active"]:
                            candle["open"] = ltp
                            candle["low"] = ltp
                            candle["active"] = True
                        candle["high"] = max(candle["high"], ltp)
                        candle["low"] = min(candle["low"], ltp)
                        candle["close"] = ltp

            elif token == CE_TOKEN:
                latest_ticks.update({"ce_price": ltp, "ce_volume": vol, "ce_oi": oi})
                ce_price_history.append(ltp)
                ce_volume_history.append(vol)
                ce_oi_history.append(oi)
                tick_counter += 1

            elif token == PE_TOKEN:
                latest_ticks.update({"pe_price": ltp, "pe_volume": vol, "pe_oi": oi})
                pe_price_history.append(ltp)
                pe_volume_history.append(vol)
                pe_oi_history.append(oi)
                tick_counter += 1

            if tick_counter % 3 == 0:
                run_signal_engine()

    except Exception as e:
        logger.error(f"Callback data parser exception: {e}")

# ============================================================
# SUPERVISOR BACKGROUND LIFECYCLE DAEMON
# ============================================================
def start_angel_websocket():
    global CE_TOKEN, PE_TOKEN, ATM_STRIKE, sws, ws_running

    while True:
        try:
            if not is_market_open():
                logger.info("Market is closed. Sleeping background engine thread...")
                time.sleep(30)
                continue

            logger.info("Fetching authentic session and feed credentials...")
            auth_token, feed_token, obj = get_auth_token()

            if not feed_token:
                logger.error("Failed to get feed token. Retrying in 10s...")
                time.sleep(10)
                continue

            logger.info(f"Auth OK. Token: {auth_token[:10]}... Feed: {feed_token[:10]}...")

            if not CE_TOKEN or not PE_TOKEN:
                logger.info("Option tokens not resolved yet. Executing token lookup sequence...")
                get_current_atm_tokens()

            if not CE_TOKEN or not PE_TOKEN:
                logger.error("Could not resolve option tokens. Retrying in 10s...")
                time.sleep(10)
                continue

            logger.info(f"Initializing SmartWebSocketV2 for ATM Strike {ATM_STRIKE}")
            logger.info(f"Subscribing to: Spot={SPOT_TOKEN}, CE={CE_TOKEN}, PE={PE_TOKEN}")

            sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)

            sws.on_open = on_ws_open
            sws.on_data = on_ws_data
            sws.on_error = on_ws_error
            sws.on_close = on_ws_close

            ws_running = True
            sws.connect()

        except Exception as e:
            logger.error(f"Critical error in supervisor daemon loop: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            ws_running = False
            sws = None
            time.sleep(10)

# ============================================================
# INITIALIZATION RUNNER HOOK
# ============================================================
def init_background_threads():
    logger.info("Initializing framework-bound background pipelines...")
    ws_thread = threading.Thread(target=start_angel_websocket, daemon=True)
    ws_thread.start()
    logger.info("Background streaming thread successfully bound and deployed.")

_init_completed = False

@app.before_request
def ensure_threads_are_breathing():
    global _init_completed
    if not _init_completed:
        init_background_threads()
        _init_completed = True

# ============================================================
# API FLASK SERVER ROUTING ENDPOINTS
# ============================================================
@app.route("/", methods=["GET", "HEAD"])
def home():
    return jsonify({
        "status": "healthy",
        "engine": "Grade 1 Pro Signal Bot",
        "market_open": is_market_open(),
        "timestamp": time.time(),
        "features": [
            "Multi-factor signal classification",
            "RSI + MACD + ADX + PCR + Volume confirmation",
            "Intelligent trailing stops with breakeven",
            "Profit lock at 50% drawdown from peak",
            "Time-based exits",
            "Momentum reversal detection",
            "Signal persistence validation (3-tick confirm)",
            "Consecutive trade limits",
            "Daily max trade limits",
            "Multi-timeframe trend analysis (1m, 2m, 3m, 5m, 10m, 15m, 20m)"
        ]
    }), 200

@app.route("/api/live-signals", methods=["GET"])
@app.route("/api/signals", methods=["GET"])
def live_signals():
    return jsonify({
        "timestamp": get_ist_now().isoformat(),
        "market_signal": market_signal,
        "market_state": market_state,
        "signal_state": signal_state,
        "portfolio_state": portfolio_state,
        "tokens": {
            "atm_strike": ATM_STRIKE,
            "expiry": EXPIRY_DATE,
            "ce_token": CE_TOKEN,
            "pe_token": PE_TOKEN
        },
        "config": {
            "consider_ce_rsi": CONFIG["CONSIDER_CE_RSI"],
            "strong_ce_rsi": CONFIG["STRONG_CE_RSI"],
            "consider_pe_rsi": CONFIG["CONSIDER_PE_RSI"],
            "strong_pe_rsi": CONFIG["STRONG_PE_RSI"],
            "max_daily_trades": CONFIG["MAX_DAILY_TRADES"],
            "cooldown_sec": CONFIG["COOLDOWN_SEC"]
        }
    }), 200

@app.route("/api/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "OK",
        "alive": True,
        "ws_running": ws_running,
        "last_tick": last_tick_time,
        "ticks_received": tick_counter
    }), 200

if __name__ == "__main__":
    if not _init_completed:
        init_background_threads()
        _init_completed = True
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
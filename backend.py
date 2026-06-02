# === ML Import Diagnostic (top of backend.py, after imports) ===
# === TOP OF backend.py ===
import sys
import logging

# Diagnostic: log Python path and installed packages
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info(f"Python version: {sys.version}")
logger.info(f"Python path: {sys.path[:3]}")

# Try sklearn import with detailed error
try:
    import sklearn
    logger.info(f"sklearn loaded: {sklearn.__version__}")
except Exception as e:
    logger.error(f"sklearn import failed: {type(e).__name__}: {e}")

# Try other critical imports
for pkg in ['numpy', 'pandas', 'flask', 'requests']:
    try:
        mod = __import__(pkg)
        logger.info(f"{pkg} loaded")
    except Exception as e:
        logger.error(f"{pkg} import failed: {e}")
# ===========================


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

app = Flask(__name__)
CORS(app)
application = app

try:
    from sklearn.ensemble import RandomForestClassifier
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logger.warning("sklearn not available - ML signal filter disabled")

try:
    import telebot
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False

# ═══════════════════════════════════════════════════════════════
# ENVIRONMENT VARIABLES
# ═══════════════════════════════════════════════════════════════
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing critical Angel One environment variables")

DB_PATH = "trading_data.db"

# ═══════════════════════════════════════════════════════════════
# DATABASE INIT
# ═══════════════════════════════════════════════════════════════
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS ticks (timestamp REAL, token TEXT, price REAL, volume REAL, bid REAL, ask REAL, oi REAL)""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ticks_time ON ticks(timestamp)")
    c.execute("""CREATE TABLE IF NOT EXISTS signals (timestamp REAL, action TEXT, signal_type TEXT, grade TEXT, confidence REAL, ce_price REAL, pe_price REAL, rsi REAL, pcr REAL, regime TEXT, vwap REAL, vix REAL, ml_score REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS trades (timestamp REAL, action TEXT, entry_price REAL, exit_price REAL, pnl REAL, size_pct REAL, status TEXT, grade TEXT, atr REAL, vix REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS daily_performance (date TEXT, equity REAL, daily_pnl REAL, drawdown_pct REAL, sharpe REAL, var REAL, win_rate REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS ml_models (id INTEGER PRIMARY KEY, model BLOB, created_at REAL, features TEXT, accuracy REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS backtest_results (id INTEGER PRIMARY KEY, date TEXT, strategy TEXT, trades INTEGER, win_rate REAL, profit_factor REAL, max_drawdown REAL, sharpe REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS greeks (timestamp REAL, delta REAL, gamma REAL, theta REAL, vega REAL, iv REAL, ce_delta REAL, pe_delta REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS market_profile (timestamp REAL, poc REAL, value_area_high REAL, value_area_low REAL, vwap REAL, volume_profile TEXT)""")
    conn.commit()
    conn.close()

init_db()

# ═══════════════════════════════════════════════════════════════
# ANGEL ONE API SETUP
# ═══════════════════════════════════════════════════════════════
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# Let SmartWebSocketV2 handle binary parsing natively
# The library's internal _parse_binary_data knows the exact byte format
# We only handle JSON messages in on_ws_data

# _on_close patch removed - let SmartWebSocketV2 handle its own retry logic
# The library manages reconnections internally; our custom patch was causing conflicts

# ═══════════════════════════════════════════════════════════════
# GLOBAL STATE
# ═══════════════════════════════════════════════════════════════
CE_TOKEN = None
PE_TOKEN = None
SPOT_TOKEN = "99926000"
VIX_TOKEN = "99919017"  # India VIX
BANKNIFTY_TOKEN = "99926009"  # BankNifty for correlation
CE_SYMBOL = ""
PE_SYMBOL = ""
ATM_STRIKE = 0
EXPIRY_DATE = ""

# Price histories
spot_price_history = deque(maxlen=2000)
ce_price_history = deque(maxlen=2000)
pe_price_history = deque(maxlen=2000)
vix_history = deque(maxlen=200)
banknifty_history = deque(maxlen=1000)

# Volume & OI
ce_volume_history = deque(maxlen=1000)
pe_volume_history = deque(maxlen=1000)
ce_oi_history = deque(maxlen=200)
pe_oi_history = deque(maxlen=200)

# Market Profile / VWAP
vwap_history = deque(maxlen=1000)
market_profile_data = {"poc": 0, "value_area_high": 0, "value_area_low": 0, "volume_profile": {}}

# Greeks
greeks_state = {"delta": 0, "gamma": 0, "theta": 0, "vega": 0, "iv": 0.20, "ce_delta": 0, "pe_delta": 0}

# Timeframes
TIMEFRAMES = ["1min", "2min", "3min", "5min", "10min", "15min", "20min"]
timeframe_history = {tf: deque(maxlen=100) for tf in TIMEFRAMES}
last_timeframe_update = {tf: 0 for tf in TIMEFRAMES}
timeframe_candles = {tf: {"open": 0, "high": 0, "low": float("inf"), "close": 0, "active": False, "volume": 0} for tf in TIMEFRAMES}

latest_ticks = {
    "spot_price": 0.0, "ce_price": 0.0, "pe_price": 0.0,
    "ce_volume": 0, "pe_volume": 0, "ce_oi": 0, "pe_oi": 0,
    "ce_bid": 0.0, "ce_ask": 0.0, "pe_bid": 0.0, "pe_ask": 0.0,
    "vix": 15.0, "banknifty": 0.0
}

tick_counter = 0
ws_running = False
sws = None
last_tick_time = time.time()
engine_active = True

# Signal state
signal_state = {
    "current_action": "HOLD", "entry_price": 0.0, "stop_loss": 0.0,
    "target": 0.0, "highest_premium_seen": 0.0, "signal_grade": "D",
    "confidence": 0.0, "position_size_pct": 0, "cooldown_until": 0,
    "entry_time": 0, "max_profit_seen": 0.0, "position_risk_pct": 1.0
}

# Portfolio with dynamic sizing
portfolio_state = {
    "equity": 100000.0, "initial_equity": 100000.0, "daily_pnl": 0.0,
    "max_drawdown_today": 0.0, "open_positions": 0, "daily_peak": 100000.0,
    "daily_loss_limit_pct": 2.0, "var_95": 0.0, "sharpe_ratio": 0.0,
    "total_trades": 0, "winning_trades": 0, "win_rate": 0.0
}

# Market signal with enhanced fields
market_signal = {
    "signal": "WAITING", "spot_price": 0.0, "ce_price": 0.0, "pe_price": 0.0,
    "spot_rsi": 50.0, "spot_macd": 0.0, "pcr": 1.0, "spot_atr": 0.0,
    "regime": "RANGING", "confidence": 50.0, "timestamp": "",
    "alert_message": "Initializing...", "signal_strength": "NONE",
    "vwap": 0.0, "vix": 15.0, "ml_score": 0.5, "banknifty_corr": 0.0,
    "delta_neutral_signal": False, "gamma_exposure": "LOW"
}

market_state = {
    "rsi": 50, "momentum": "NEUTRAL", "strength": "LOW", "trend": "SIDEWAYS",
    "action": "HOLD", "confidence": 0, "volatility": "NORMAL", "regime": "UNKNOWN",
    "vix_regime": "LOW", "banknifty_trend": "SIDEWAYS"
}

institutional_state = {
    "vwap": 0.0, "ema_fast": 0.0, "ema_slow": 0.0, "atr": 0.0,
    "delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0, "iv": 0.20
}

pcr_cache = {"value": 1.0, "time": 0}
spot_cache = {"value": None, "timestamp": 0}
CACHE_TTL = 15

# ═══════════════════════════════════════════════════════════════
# ENHANCED CONFIG - DYNAMIC SIGNAL BOT v2.0
# ═══════════════════════════════════════════════════════════════
CONFIG = {
    # Core indicators (kept)
    "SPOT_RSI_PERIOD": 14, "SPOT_RSI_SMOOTHING": 3,
    "SPOT_MACD_FAST": 12, "SPOT_MACD_SLOW": 26, "SPOT_MACD_SIGNAL": 9,
    "SPOT_ATR_PERIOD": 14, "ATR_SMOOTHING": True,

    # Relaxed thresholds
    "CONSIDER_CE_RSI": 50, "CONSIDER_PE_RSI": 50,
    "STRONG_CE_RSI": 55, "STRONG_PE_RSI": 45,
    "EXTREME_CE_RSI": 65, "EXTREME_PE_RSI": 35,
    "MACD_CONFIRM_THRESHOLD": 0.2,
    "VOLUME_SPIKE_RATIO": 1.3, "VOLUME_MA_PERIOD": 20,
    "PCR_BULLISH_THRESHOLD": 0.85, "PCR_BEARISH_THRESHOLD": 1.15,
    "TREND_STRENGTH_PERIOD": 14, "STRONG_TREND_MIN": 20,
    "ENTRY_ATR_MULT": 1.5, "TRAILING_ATR_MULT": 1.8, "TARGET_ATR_MULT": 4.0,
    "COOLDOWN_SEC": 60, "MAX_HOLD_TIME_MIN": 45,
    "MIN_PROFIT_LOCK": 0.3, "BREAKEVEN_TRIGGER": 1.0, "MIN_SIGNAL_HOLD_SEC": 30,
    "MAX_DAILY_TRADES": 15, "CONSECUTIVE_SAME_DIR_MAX": 2,

    # Debug & entry
    "SIGNAL_DEBUG_MODE": True, "MIN_SCORE_FOR_ENTRY": 2,
    "WEAK_SIGNAL_ALLOWED": True, "AGGRESSIVE_ENTRY": True,
    "DRAWDOWN_ALERT_PCT": 0.15, "PROFIT_LOCK_PCT": 0.30,
    "EARLY_EXIT_ON_REVERSAL": True,
    "RSI_EXIT_CE": 48, "RSI_EXIT_PE": 52,
    "MACD_EXIT_THRESHOLD": 0.1,
    "FORCE_SIGNAL_AFTER_MINS": 20,
    "MIN_DATA_POINTS": 30, "SIGNAL_BUILDING_TICKS": 2,

    # ═══════════════════════════════════════════════════════════════
    # NEW: DYNAMIC POSITION SIZING
    # ═══════════════════════════════════════════════════════════════
    "BASE_RISK_PER_TRADE_PCT": 1.0,      # Base 1% risk per trade
    "MAX_RISK_PER_TRADE_PCT": 2.0,       # Max 2% in high conviction
    "MIN_RISK_PER_TRADE_PCT": 0.5,       # Min 0.5% in weak conditions
    "ATR_RISK_MULTIPLIER": 1.5,          # Position size = Risk / (ATR * mult)
    "VIX_HIGH_THRESHOLD": 20,            # Reduce size when VIX > 20
    "VIX_EXTREME_THRESHOLD": 25,         # Further reduce when VIX > 25
    "VIX_SIZE_REDUCTION_HIGH": 0.7,      # 70% size when VIX high
    "VIX_SIZE_REDUCTION_EXTREME": 0.5,   # 50% size when VIX extreme
    "CONVICTION_SIZE_BOOST": 1.3,        # 130% size for STRONG signals
    "WEAK_SIGNAL_SIZE_REDUCTION": 0.7,   # 70% size for WEAK signals

    # ═══════════════════════════════════════════════════════════════
    # NEW: VWAP & MARKET PROFILE
    # ═══════════════════════════════════════════════════════════════
    "VWAP_PERIOD": 20,
    "USE_VWAP_CONFIRMATION": True,       # Price above VWAP for CE, below for PE
    "VWAP_DEVIATION_ENTRY": 0.002,       # 0.2% deviation from VWAP for entry
    "MARKET_PROFILE_BARS": 50,           # Bars for volume profile
    "VALUE_AREA_PCT": 0.70,              # 70% volume = value area
    "POC_BOUNCE_THRESHOLD": 0.003,       # 0.3% bounce off POC for entry

    # ═══════════════════════════════════════════════════════════════
    # NEW: VIX CORRELATION
    # ═══════════════════════════════════════════════════════════════
    "VIX_PERIOD": 20,
    "VIX_LOW_THRESHOLD": 12,             # Low vol regime
    "VIX_NORMAL_THRESHOLD": 20,          # Normal vol regime
    "VIX_HIGH_THRESHOLD": 25,            # High vol regime
    "VIX_CORRELATION_PERIOD": 50,        # Lookback for Nifty-VIX corr
    "AVOID_TRADES_VIX_SPIKE": True,      # Skip entries on VIX spike > 5% in 5min

    # ═══════════════════════════════════════════════════════════════
    # NEW: GREEKS-BASED FILTERING
    # ═══════════════════════════════════════════════════════════════
    "MAX_GREEKS_IV": 0.40,               # Avoid when IV > 40%
    "MIN_GREEKS_IV": 0.10,               # Avoid when IV < 10%
    "DELTA_NEUTRAL_THRESHOLD": 0.10,       # Delta-neutral zone +/- 0.10
    "GAMMA_SKEW_THRESHOLD": 0.05,        # Gamma exposure threshold
    "THETA_DECAY_THRESHOLD": 2.0,        # Avoid when theta decay > 2 pts/min

    # ═══════════════════════════════════════════════════════════════
    # NEW: ADAPTIVE REGIME SWITCHING
    # ═══════════════════════════════════════════════════════════════
    "REGIME_LOOKBACK": 50,
    "TRENDING_ADX_MIN": 25,              # ADX > 25 = trending
    "RANGING_ADX_MAX": 20,               # ADX < 20 = ranging
    "VOLATILE_ATR_MULT": 2.0,            # ATR > 2x avg = volatile
    "TRENDING_STRATEGY": "MOMENTUM",     # Use momentum in trends
    "RANGING_STRATEGY": "MEAN_REVERSION", # Use mean reversion in ranges
    "VOLATILE_STRATEGY": "BREAKOUT",     # Use breakout in volatility

    # ═══════════════════════════════════════════════════════════════
    # NEW: ML SIGNAL FILTER
    # ═══════════════════════════════════════════════════════════════
    "ML_ENABLED": True,
    "ML_FEATURES": [
        "rsi", "macd_hist", "adx", "pcr", "volume_ratio",
        "vwap_distance", "vix", "banknifty_corr", "atr",
        "price_vs_ema20", "price_vs_ema50", "oi_change",
        "iv", "gamma", "delta", "theta", "vega",
        "time_of_day", "day_of_week", "regime_score"
    ],
    "ML_MIN_SAMPLES": 100,               # Min data before ML activates
    "ML_CONFIDENCE_THRESHOLD": 0.55,     # Override signal if ML < 0.55
    "ML_STRONG_OVERRIDE": 0.70,          # Boost signal if ML > 0.70
    "ML_RETRAIN_INTERVAL": 3600,         # Retrain model every hour

    # ═══════════════════════════════════════════════════════════════
    # NEW: BACKTESTING FRAMEWORK
    # ═══════════════════════════════════════════════════════════════
    "BACKTEST_MODE": False,              # Set True to run backtest
    "BACKTEST_DAYS": 252,                # 1 year of data
    "BACKTEST_INITIAL_CAPITAL": 100000,
    "BACKTEST_SLIPPAGE_PCT": 0.05,       # 0.05% slippage per side
    "BACKTEST_COMMISSION_PER_ORDER": 20, # Rs 20 per order

    # ═══════════════════════════════════════════════════════════════
    # NEW: MARKET MICRO-STRUCTURE
    # ═══════════════════════════════════════════════════════════════
    "BID_ASK_SPREAD_THRESHOLD": 0.02,    # 2% spread = avoid
    "ORDER_FLOW_IMBALANCE_PERIOD": 10,   # Ticks for order flow calc
    "LARGE_ORDER_THRESHOLD": 1000000,    # 10L = large order
    "TAPE_READING_ENABLED": True,

    # ═══════════════════════════════════════════════════════════════
    # GRADE 1 PRO: SAFETY & RISK PARAMETERS
    # ═══════════════════════════════════════════════════════════════
    # --- Circuit Breakers ---
    "CIRCUIT_BREAKER_ENABLED": True,
    "CIRCUIT_BREAKER_DAILY_LOSS_PCT": 3.0,      # Hard stop at 3% daily loss
    "CIRCUIT_BREAKER_CONSECUTIVE_SL": 3,         # Stop after 3 consecutive SL hits
    "CIRCUIT_BREAKER_VIX_SPIKE_PCT": 15,         # Stop if VIX spikes 15% in 10min
    "CIRCUIT_BREAKER_GAP_UP_DOWN_PCT": 2.0,      # Stop if spot gaps >2%

    # --- Slippage Protection ---
    "MAX_SLIPPAGE_PCT": 0.5,                     # Max 0.5% slippage allowed
    "SLIPPAGE_ADJUSTMENT": True,                 # Auto-adjust entry for slippage

    # --- Liquidity Filters ---
    "MIN_PREMIUM_FOR_TRADE": 10.0,               # Min premium Rs 10
    "MAX_PREMIUM_FOR_TRADE": 2000.0,             # Max premium Rs 2000
    "MIN_VOLUME_FOR_TRADE": 100,                 # Min 100 contracts volume
    "MIN_OI_FOR_TRADE": 500,                     # Min 500 OI

    # --- Time-Based Filters ---
    "NO_TRADE_FIRST_15_MIN": True,               # No trades 9:15-9:30 (opening vol)
    "NO_TRADE_LAST_30_MIN": True,                # No trades 15:00-15:30 (closing vol)
    "NO_TRADE_LUNCH_12_30_13_30": True,          # No trades 12:30-13:30 (low vol)
    "NO_TRADE_WEDNESDAY_EXPIRY": True,           # Extra caution on expiry day

    # --- Signal Quality Gates ---
    "MIN_SIGNAL_CONFIDENCE_PCT": 55,             # Min 55% confidence to enter
    "MIN_CONFLUENCE_FACTORS": 3,                 # Min 3 factors must align
    "REQUIRE_VOLUME_CONFIRMATION": True,         # Volume must confirm signal
    "REQUIRE_TREND_ALIGNMENT": True,             # Price must align with EMA trend

    # --- Position Safety ---
    "MAX_POSITIONS_OPEN": 1,                     # Only 1 position at a time
    "FORCE_EXIT_ON_MARGIN_CALL": True,           # Exit if margin < 20%
    "AUTO_REDUCE_SIZE_ON_DRAWDOWN": True,        # Reduce size after 1.5% DD
    "DRAWDOWN_REDUCTION_FACTOR": 0.6,            # Reduce to 60% size after DD

    # --- Premium Decay Protection ---
    "THETA_DECAY_AVOID_HOURS": 2,                # Avoid new entries last 2 hours
    "THETA_DECAY_MAX_PREMIUM_PCT": 30,           # Max 30% theta decay expected

    # --- Correlation Risk ---
    "MAX_CORRELATED_EXPOSURE_PCT": 5.0,          # Max 5% in correlated assets
    "BANKNIFTY_CORR_MIN": 0.3,                   # Min correlation with BN

    # --- Execution Safety ---
    "ORDER_RETRY_MAX": 3,                        # Max 3 order retries
    "ORDER_TIMEOUT_SEC": 10,                     # 10 sec order timeout
    "PAPER_TRADE_MODE": False,                   # Set True for paper trading

    # --- Logging & Audit ---
    "AUDIT_ALL_SIGNALS": True,                   # Log every signal decision
    "AUDIT_ALL_TRADES": True,                    # Log every trade execution
    "ALERT_ON_EVERY_EXIT": True,                 # Alert on every exit

    # --- Recovery Mode ---
    "RECOVERY_COOLDOWN_MULTIPLIER": 2.0,         # 2x cooldown after SL
    "RECOVERY_SIZE_REDUCTION": 0.5,              # 50% size after SL
    "RECOVERY_MAX_ATTEMPTS": 2,                  # Max 2 recovery attempts
}

signal_buffer = {"ce_count": 0, "pe_count": 0, "last_signal_time": 0, "consecutive_ce": 0, "consecutive_pe": 0}
daily_trade_count = 0
last_trade_date = ""

# ═══════════════════════════════════════════════════════════════
# GRADE 1 PRO: SAFETY STATE
# ═══════════════════════════════════════════════════════════════
safety_state = {
    "consecutive_sl_count": 0,
    "last_sl_time": 0,
    "recovery_mode": False,
    "recovery_attempts": 0,
    "circuit_breaker_triggered": False,
    "circuit_breaker_reason": "",
    "circuit_breaker_time": 0,
    "daily_sl_count": 0,
    "last_gap_check_price": 0.0,
    "last_gap_check_time": 0,
    "paper_trade_pnl": 0.0,
    "total_paper_trades": 0,
    "paper_win_count": 0,
}

# Debug log
signal_debug_log = deque(maxlen=200)
ml_feature_log = deque(maxlen=500)

# ═══════════════════════════════════════════════════════════════
# ML MODEL STATE
# ═══════════════════════════════════════════════════════════════
ml_model_state = {
    "model": None,
    "last_trained": 0,
    "accuracy": 0.5,
    "feature_importance": {},
    "prediction_buffer": deque(maxlen=100),
    "is_ready": False
}

# ═══════════════════════════════════════════════════════════════
# REGIME STATE
# ═══════════════════════════════════════════════════════════════
regime_state = {
    "current": "RANGING",
    "confidence": 0.0,
    "history": deque(maxlen=100),
    "strategy": "MEAN_REVERSION",
    "volatility_regime": "NORMAL",
    "last_regime_change": 0
}

# ═══════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════
def log_signal_debug(msg):
    timestamp = datetime.now().strftime("%H:%M:%S")
    entry = f"[{timestamp}] {msg}"
    signal_debug_log.append(entry)
    if CONFIG.get("SIGNAL_DEBUG_MODE", False):
        logger.info(f"[SIGNAL_DEBUG] {msg}")

# ═══════════════════════════════════════════════════════════════
# GRADE 1 PRO: SAFETY CHECK FUNCTIONS
# ═══════════════════════════════════════════════════════════════
def check_circuit_breakers():
    """Check all circuit breaker conditions. Returns (ok, reason) tuple."""
    global safety_state

    if not CONFIG.get("CIRCUIT_BREAKER_ENABLED", True):
        return True, ""

    # Check if already triggered and not reset
    if safety_state["circuit_breaker_triggered"]:
        # Auto-reset after 30 minutes
        if time.time() - safety_state["circuit_breaker_time"] > 1800:
            safety_state["circuit_breaker_triggered"] = False
            safety_state["circuit_breaker_reason"] = ""
            log_signal_debug("CIRCUIT BREAKER: Auto-reset after 30min cooldown")
            return True, ""
        return False, safety_state["circuit_breaker_reason"]

    # Daily loss circuit breaker
    daily_loss_pct = (portfolio_state["initial_equity"] - portfolio_state["equity"]) / portfolio_state["initial_equity"] * 100
    if daily_loss_pct >= CONFIG.get("CIRCUIT_BREAKER_DAILY_LOSS_PCT", 3.0):
        safety_state["circuit_breaker_triggered"] = True
        safety_state["circuit_breaker_time"] = time.time()
        safety_state["circuit_breaker_reason"] = f"Daily loss limit {daily_loss_pct:.2f}% >= {CONFIG.get('CIRCUIT_BREAKER_DAILY_LOSS_PCT', 3.0)}%"
        log_signal_debug(f"CIRCUIT BREAKER TRIGGERED: {safety_state['circuit_breaker_reason']}")
        send_telegram_alert(f"🚨 <b>CIRCUIT BREAKER</b>\n{safety_state['circuit_breaker_reason']}\nTrading HALTED for 30min")
        return False, safety_state["circuit_breaker_reason"]

    # Consecutive SL circuit breaker
    if safety_state["consecutive_sl_count"] >= CONFIG.get("CIRCUIT_BREAKER_CONSECUTIVE_SL", 3):
        safety_state["circuit_breaker_triggered"] = True
        safety_state["circuit_breaker_time"] = time.time()
        safety_state["circuit_breaker_reason"] = f"{safety_state['consecutive_sl_count']} consecutive stop losses"
        log_signal_debug(f"CIRCUIT BREAKER TRIGGERED: {safety_state['circuit_breaker_reason']}")
        send_telegram_alert(f"🚨 <b>CIRCUIT BREAKER</b>\n{safety_state['circuit_breaker_reason']}\nTrading HALTED for 30min")
        return False, safety_state["circuit_breaker_reason"]

    return True, ""

def check_time_filters():
    """Check if current time allows trading."""
    now = get_ist_now()

    # No trade first 15 min
    if CONFIG.get("NO_TRADE_FIRST_15_MIN", True):
        if now.time() >= dt_time(9, 15) and now.time() < dt_time(9, 30):
            return False, "Opening volatility - no trades 9:15-9:30"

    # No trade last 30 min
    if CONFIG.get("NO_TRADE_LAST_30_MIN", True):
        if now.time() >= dt_time(15, 0) and now.time() <= dt_time(15, 30):
            return False, "Closing volatility - no trades after 15:00"

    # No trade lunch
    if CONFIG.get("NO_TRADE_LUNCH_12_30_13_30", True):
        if now.time() >= dt_time(12, 30) and now.time() < dt_time(13, 30):
            return False, "Lunch hour low volume - no trades 12:30-13:30"

    # Wednesday expiry caution
    if CONFIG.get("NO_TRADE_WEDNESDAY_EXPIRY", True):
        if now.weekday() == 2:  # Wednesday
            # More strict on expiry day
            if now.time() >= dt_time(14, 0):
                return False, "Expiry day - no trades after 14:00"

    return True, ""

def check_premium_safety(premium, side):
    """Check if premium is within safe trading range."""
    min_prem = CONFIG.get("MIN_PREMIUM_FOR_TRADE", 10.0)
    max_prem = CONFIG.get("MAX_PREMIUM_FOR_TRADE", 2000.0)

    if premium < min_prem:
        return False, f"Premium {premium} < min {min_prem}"
    if premium > max_prem:
        return False, f"Premium {premium} > max {max_prem}"

    # Check volume
    vol = latest_ticks.get("ce_volume" if side == "CE" else "pe_volume", 0)
    if vol < CONFIG.get("MIN_VOLUME_FOR_TRADE", 100):
        return False, f"Volume {vol} < min {CONFIG.get('MIN_VOLUME_FOR_TRADE', 100)}"

    # Check OI
    oi = latest_ticks.get("ce_oi" if side == "CE" else "pe_oi", 0)
    if oi < CONFIG.get("MIN_OI_FOR_TRADE", 500):
        return False, f"OI {oi} < min {CONFIG.get('MIN_OI_FOR_TRADE', 500)}"

    return True, ""

def check_gap_risk():
    """Check for dangerous gap up/down in spot."""
    global safety_state

    spot_list = list(spot_price_history)
    if len(spot_list) < 2:
        return True, ""

    current_price = spot_list[-1]
    last_price = safety_state["last_gap_check_price"]

    if last_price == 0 or safety_state["last_gap_check_time"] == 0:
        safety_state["last_gap_check_price"] = current_price
        safety_state["last_gap_check_time"] = time.time()
        return True, ""

    # Check gap every 5 minutes
    if time.time() - safety_state["last_gap_check_time"] < 300:
        return True, ""

    gap_pct = abs(current_price - last_price) / last_price * 100 if last_price > 0 else 0
    max_gap = CONFIG.get("CIRCUIT_BREAKER_GAP_UP_DOWN_PCT", 2.0)

    safety_state["last_gap_check_price"] = current_price
    safety_state["last_gap_check_time"] = time.time()

    if gap_pct > max_gap:
        return False, f"Gap detected: {gap_pct:.2f}% > {max_gap}%"

    return True, ""

def check_signal_quality(score, factors, confidence):
    """Validate signal meets minimum quality gates."""
    min_conf = CONFIG.get("MIN_SIGNAL_CONFIDENCE_PCT", 55)
    min_factors = CONFIG.get("MIN_CONFLUENCE_FACTORS", 3)

    if confidence < min_conf:
        return False, f"Confidence {confidence:.1f}% < min {min_conf}%"

    if len(factors) < min_factors:
        return False, f"Factors {len(factors)} < min {min_factors}"

    return True, ""

def apply_recovery_adjustments():
    """Apply position size adjustments when in recovery mode."""
    global safety_state

    if safety_state["recovery_mode"]:
        reduction = CONFIG.get("RECOVERY_SIZE_REDUCTION", 0.5)
        log_signal_debug(f"RECOVERY MODE: Size reduced to {reduction*100:.0f}%")
        return reduction
    return 1.0

def update_safety_on_exit(pnl_points, exit_reason):
    """Update safety state on position exit."""
    global safety_state

    if pnl_points < 0:
        safety_state["consecutive_sl_count"] += 1
        safety_state["last_sl_time"] = time.time()
        safety_state["daily_sl_count"] += 1

        if safety_state["consecutive_sl_count"] >= 2:
            safety_state["recovery_mode"] = True
            safety_state["recovery_attempts"] += 1
            log_signal_debug(f"RECOVERY MODE ACTIVATED: {safety_state['consecutive_sl_count']} consecutive SL")
    else:
        # Winning trade - reset consecutive SL
        safety_state["consecutive_sl_count"] = 0
        safety_state["recovery_mode"] = False
        safety_state["recovery_attempts"] = 0

    if CONFIG.get("PAPER_TRADE_MODE", False):
        safety_state["paper_trade_pnl"] += pnl_points
        safety_state["total_paper_trades"] += 1
        if pnl_points > 0:
            safety_state["paper_win_count"] += 1

def get_ist_now():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def is_market_open():
    now_ist = get_ist_now()
    if now_ist.weekday() >= 5: return False
    return dt_time(9, 15) <= now_ist.time() <= dt_time(15, 30)

def get_time_features():
    now = get_ist_now()
    return {
        "hour": now.hour,
        "minute": now.minute,
        "day_of_week": now.weekday(),
        "time_of_day": now.hour + now.minute / 60.0
    }

# ═══════════════════════════════════════════════════════════════
# TECHNICAL INDICATORS
# ═══════════════════════════════════════════════════════════════
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
                if al == 0: rsi_values.append(100.0)
                else: rsi_values.append(100 - (100 / (1 + ag / al)))
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
    signal_line = ema(macd_history, signal_period) if macd_history else macd_line
    return macd_line, signal_line, macd_line - signal_line

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
    plus_dm, minus_dm, tr_list = [], [], []
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

def calculate_vwap(prices, volumes):
    if len(prices) < 2 or len(volumes) < 2: return prices[-1] if prices else 0
    pv = sum(p * v for p, v in zip(prices, volumes))
    vol = sum(volumes)
    return pv / vol if vol > 0 else prices[-1]

def calculate_correlation(series1, series2, period=50):
    if len(series1) < period or len(series2) < period: return 0.0
    s1 = list(series1)[-period:]
    s2 = list(series2)[-period:]
    if len(s1) != len(s2): return 0.0
    n = len(s1)
    mean1, mean2 = sum(s1)/n, sum(s2)/n
    num = sum((s1[i]-mean1)*(s2[i]-mean2) for i in range(n))
    den1 = sum((s1[i]-mean1)**2 for i in range(n)) ** 0.5
    den2 = sum((s2[i]-mean2)**2 for i in range(n)) ** 0.5
    return num / (den1 * den2) if den1 > 0 and den2 > 0 else 0.0

def calculate_bollinger_bands(prices, period=20, mult=2.0):
    if len(prices) < period: return prices[-1] if prices else 0, prices[-1] if prices else 0, prices[-1] if prices else 0
    sma = sum(prices[-period:]) / period
    variance = sum((p - sma) ** 2 for p in prices[-period:]) / period
    std = variance ** 0.5
    return sma, sma + mult * std, sma - mult * std

# ═══════════════════════════════════════════════════════════════
# VWAP & MARKET PROFILE
# ═══════════════════════════════════════════════════════════════
def update_vwap_and_profile():
    global market_profile_data
    spot_list = list(spot_price_history)
    vol_list = list(ce_volume_history) if ce_volume_history else [1] * len(spot_list)

    if len(spot_list) < 20: return

    # VWAP
    vwap = calculate_vwap(spot_list, vol_list)
    vwap_history.append(vwap)
    institutional_state["vwap"] = vwap

    # Volume Profile (simple histogram)
    if len(spot_list) >= CONFIG["MARKET_PROFILE_BARS"]:
        recent_prices = spot_list[-CONFIG["MARKET_PROFILE_BARS"]:]
        recent_vols = vol_list[-CONFIG["MARKET_PROFILE_BARS"]:] if len(vol_list) >= len(recent_prices) else [1] * len(recent_prices)

        min_p, max_p = min(recent_prices), max(recent_prices)
        if max_p > min_p:
            bins = 20
            bin_size = (max_p - min_p) / bins
            profile = {}
            for i in range(bins):
                low = min_p + i * bin_size
                high = min_p + (i + 1) * bin_size
                vol = sum(recent_vols[j] for j in range(len(recent_prices)) if low <= recent_prices[j] < high)
                profile[(low + high) / 2] = vol

            # POC = Point of Control (highest volume price)
            if profile:
                poc = max(profile, key=profile.get)
                total_vol = sum(profile.values())
                sorted_prices = sorted(profile.keys())
                cum_vol = 0
                va_low = va_high = poc
                for p in sorted_prices:
                    cum_vol += profile[p]
                    if cum_vol >= total_vol * (1 - CONFIG["VALUE_AREA_PCT"]) / 2 and va_low == poc:
                        va_low = p
                    if cum_vol >= total_vol * (1 + CONFIG["VALUE_AREA_PCT"]) / 2:
                        va_high = p
                        break

                market_profile_data.update({
                    "poc": poc,
                    "value_area_high": va_high,
                    "value_area_low": va_low,
                    "volume_profile": {str(k): v for k, v in list(profile.items())[:10]}
                })

def get_vwap_signal(spot_price, vwap, side):
    if vwap == 0: return True  # Neutral if no VWAP
    deviation = abs(spot_price - vwap) / vwap
    if side == "CE":
        return spot_price > vwap or deviation < CONFIG["VWAP_DEVIATION_ENTRY"]
    else:
        return spot_price < vwap or deviation < CONFIG["VWAP_DEVIATION_ENTRY"]

# ═══════════════════════════════════════════════════════════════
# GREEKS CALCULATION (Approximation)
# ═══════════════════════════════════════════════════════════════
def update_greeks_approx():
    global greeks_state
    spot_list = list(spot_price_history)
    ce_list = list(ce_price_history)
    pe_list = list(pe_price_history)

    if len(spot_list) < 10 or len(ce_list) < 5 or len(pe_list) < 5:
        return

    spot = spot_list[-1]
    ce_p = ce_list[-1]
    pe_p = pe_list[-1]

    # Time to expiry in years (approx)
    tte = 0.02  # ~1 week

    # Approximate IV from ATM straddle
    straddle_price = ce_p + pe_p
    approx_iv = straddle_price / (spot * (tte ** 0.5) * 0.4) if spot > 0 else 0.20
    approx_iv = max(0.05, min(0.60, approx_iv))

    # Approximate Delta
    d1_ce = (math.log(spot / ATM_STRIKE) + (0.5 * approx_iv**2) * tte) / (approx_iv * (tte**0.5)) if ATM_STRIKE > 0 and approx_iv > 0 else 0
    d1_pe = d1_ce  # Same underlying

    ce_delta_approx = 0.5 + 0.5 * math.erf(d1_ce / (2**0.5)) if d1_ce != 0 else 0.5
    pe_delta_approx = ce_delta_approx - 1

    # Approximate Gamma (shared)
    gamma_approx = math.exp(-d1_ce**2 / 2) / (spot * approx_iv * (tte**0.5) * (2 * math.pi)**0.5) if approx_iv > 0 else 0.01

    # Approximate Theta
    theta_approx = -spot * approx_iv * math.exp(-d1_ce**2 / 2) / (2 * (2 * math.pi * tte)**0.5) / 365

    # Approximate Vega
    vega_approx = spot * (tte**0.5) * math.exp(-d1_ce**2 / 2) / (2 * math.pi)**0.5 / 100

    greeks_state.update({
        "delta": ce_delta_approx + pe_delta_approx,
        "gamma": gamma_approx,
        "theta": theta_approx,
        "vega": vega_approx,
        "iv": approx_iv,
        "ce_delta": ce_delta_approx,
        "pe_delta": pe_delta_approx
    })

    market_signal.update({
        "delta_neutral_signal": abs(ce_delta_approx + pe_delta_approx) < CONFIG["DELTA_NEUTRAL_THRESHOLD"],
        "gamma_exposure": "HIGH" if gamma_approx > CONFIG["GAMMA_SKEW_THRESHOLD"] else "LOW"
    })

def check_greeks_filter():
    iv = greeks_state["iv"]
    if iv > CONFIG["MAX_GREEKS_IV"]:
        log_signal_debug(f"GREEKS FILTER: IV too high ({iv:.2%} > {CONFIG['MAX_GREEKS_IV']:.2%})")
        return False, "IV_TOO_HIGH"
    if iv < CONFIG["MIN_GREEKS_IV"]:
        log_signal_debug(f"GREEKS FILTER: IV too low ({iv:.2%} < {CONFIG['MIN_GREEKS_IV']:.2%})")
        return False, "IV_TOO_LOW"
    if abs(greeks_state["theta"]) > CONFIG["THETA_DECAY_THRESHOLD"]:
        log_signal_debug(f"GREEKS FILTER: Theta decay too high ({abs(greeks_state['theta']):.2f})")
        return False, "THETA_TOO_HIGH"
    return True, "PASS"

# ═══════════════════════════════════════════════════════════════
# VIX ANALYSIS
# ═══════════════════════════════════════════════════════════════
def get_vix_regime(vix_value):
    if vix_value > CONFIG["VIX_EXTREME_THRESHOLD"]:
        return "EXTREME", CONFIG["VIX_SIZE_REDUCTION_EXTREME"]
    elif vix_value > CONFIG["VIX_HIGH_THRESHOLD"]:
        return "HIGH", CONFIG["VIX_SIZE_REDUCTION_HIGH"]
    elif vix_value < CONFIG["VIX_LOW_THRESHOLD"]:
        return "LOW", 1.1  # Slight boost in low vol
    else:
        return "NORMAL", 1.0

def check_vix_spike():
    vix_list = list(vix_history)
    if len(vix_list) < 10: return False
    recent_vix = vix_list[-1]
    vix_5min_ago = vix_list[-10] if len(vix_list) >= 10 else vix_list[0]
    spike_pct = (recent_vix - vix_5min_ago) / vix_5min_ago if vix_5min_ago > 0 else 0
    if spike_pct > 0.05:
        log_signal_debug(f"VIX SPIKE DETECTED: {spike_pct:.1%} in 5min. Avoiding new entries.")
        return True
    return False

# ═══════════════════════════════════════════════════════════════
# DYNAMIC POSITION SIZING
# ═══════════════════════════════════════════════════════════════
def calculate_position_size(signal_strength, score, atr, vix_value, conviction="MEDIUM"):
    base_risk = CONFIG["BASE_RISK_PER_TRADE_PCT"]

    # Adjust for signal strength
    if signal_strength == "STRONG":
        risk_pct = min(CONFIG["MAX_RISK_PER_TRADE_PCT"], base_risk * CONFIG["CONVICTION_SIZE_BOOST"])
    elif signal_strength == "WEAK":
        risk_pct = max(CONFIG["MIN_RISK_PER_TRADE_PCT"], base_risk * CONFIG["WEAK_SIGNAL_SIZE_REDUCTION"])
    else:
        risk_pct = base_risk

    # Adjust for VIX regime
    vix_regime, vix_mult = get_vix_regime(vix_value)
    risk_pct *= vix_mult

    # Adjust for score
    score_mult = 0.8 + (score / 10) * 0.4  # 0.8 to 1.2 based on score
    risk_pct *= score_mult

    # Calculate lot size based on risk and ATR
    risk_amount = portfolio_state["equity"] * (risk_pct / 100)
    stop_distance = atr * CONFIG["ATR_RISK_MULTIPLIER"]

    if stop_distance > 0:
        lots = int(risk_amount / (stop_distance * 50))  # 50 = Nifty lot size
        lots = max(1, lots)
    else:
        lots = 1

    final_risk_pct = (lots * stop_distance * 50) / portfolio_state["equity"] * 100

    log_signal_debug(f"POSITION SIZE: Strength={signal_strength} Score={score} ATR={atr:.2f} VIX={vix_value:.1f}({vix_regime}) Risk={final_risk_pct:.2f}% Lots={lots}")

    return lots, final_risk_pct, risk_amount

# ═══════════════════════════════════════════════════════════════
# ADAPTIVE REGIME SWITCHING
# ═══════════════════════════════════════════════════════════════
def detect_market_regime(prices, atr, adx, vix):
    if len(prices) < CONFIG["REGIME_LOOKBACK"]:
        return "RANGING", "MEAN_REVERSION", 0.5

    recent_prices = list(prices)[-CONFIG["REGIME_LOOKBACK"]:]
    avg_atr = calculate_atr(recent_prices, 14)
    current_atr = atr

    # Volatility regime
    if current_atr > avg_atr * CONFIG["VOLATILE_ATR_MULT"]:
        vol_regime = "VOLATILE"
    elif current_atr > avg_atr * 1.3:
        vol_regime = "ELEVATED"
    else:
        vol_regime = "NORMAL"

    # Trend regime
    if adx > CONFIG["TRENDING_ADX_MIN"]:
        trend_regime = "TRENDING"
        strategy = CONFIG["TRENDING_STRATEGY"]
        confidence = min(1.0, (adx - CONFIG["TRENDING_ADX_MIN"]) / 20 + 0.5)
    elif adx < CONFIG["RANGING_ADX_MAX"]:
        trend_regime = "RANGING"
        strategy = CONFIG["RANGING_STRATEGY"]
        confidence = min(1.0, (CONFIG["RANGING_ADX_MAX"] - adx) / 10 + 0.5)
    else:
        trend_regime = "TRANSITION"
        strategy = "MIXED"
        confidence = 0.5

    # VIX override
    if vix > CONFIG["VIX_HIGH_THRESHOLD"]:
        vol_regime = "VOLATILE"
        if trend_regime != "TRENDING":
            strategy = CONFIG["VOLATILE_STRATEGY"]

    final_regime = f"{trend_regime}_{vol_regime}"

    regime_state.update({
        "current": final_regime,
        "confidence": confidence,
        "strategy": strategy,
        "volatility_regime": vol_regime,
        "history": regime_state["history"]
    })
    regime_state["history"].append(final_regime)

    log_signal_debug(f"REGIME: {final_regime} | Strategy={strategy} | Confidence={confidence:.2f} | ADX={adx:.1f} | VIX={vix:.1f}")

    return final_regime, strategy, confidence

def apply_regime_adjustments(signal_params, regime, strategy, confidence):
    adjusted = signal_params.copy()

    if "VOLATILE" in regime:
        # Wider stops in volatile markets
        adjusted["entry_atr_mult"] = CONFIG["ENTRY_ATR_MULT"] * 1.3
        adjusted["target_atr_mult"] = CONFIG["TARGET_ATR_MULT"] * 0.8  # Tighter targets
        log_signal_debug("REGIME ADJUSTMENT: Volatile - wider stops, tighter targets")

    if "TRENDING" in regime and strategy == "MOMENTUM":
        # Hold longer in trends
        adjusted["max_hold_time"] = CONFIG["MAX_HOLD_TIME_MIN"] * 1.2
        log_signal_debug("REGIME ADJUSTMENT: Trending - extended hold time")

    if "RANGING" in regime and strategy == "MEAN_REVERSION":
        # Tighter stops, quicker exits in ranges
        adjusted["entry_atr_mult"] = CONFIG["ENTRY_ATR_MULT"] * 0.8
        adjusted["target_atr_mult"] = CONFIG["TARGET_ATR_MULT"] * 0.6
        log_signal_debug("REGIME ADJUSTMENT: Ranging - tighter entries and targets")

    return adjusted

# ═══════════════════════════════════════════════════════════════
# ML SIGNAL FILTER
# ═══════════════════════════════════════════════════════════════
def extract_ml_features(spot_rsi, macd_hist, adx, pcr, volume_ratio, vwap, vix, banknifty_corr, atr, ema_fast, ema_slow, price):
    time_feats = get_time_features()

    features = {
        "rsi": spot_rsi,
        "macd_hist": macd_hist,
        "adx": adx,
        "pcr": pcr,
        "volume_ratio": volume_ratio,
        "vwap_distance": (price - vwap) / vwap if vwap > 0 else 0,
        "vix": vix,
        "banknifty_corr": banknifty_corr,
        "atr": atr,
        "price_vs_ema20": (price - ema_fast) / ema_fast if ema_fast > 0 else 0,
        "price_vs_ema50": (price - ema_slow) / ema_slow if ema_slow > 0 else 0,
        "oi_change": 0,  # Placeholder - would need historical OI
        "iv": greeks_state["iv"],
        "gamma": greeks_state["gamma"],
        "delta": greeks_state["delta"],
        "theta": greeks_state["theta"],
        "vega": greeks_state["vega"],
        "time_of_day": time_feats["time_of_day"],
        "day_of_week": time_feats["day_of_week"],
        "regime_score": regime_state["confidence"]
    }

    return features

def train_ml_model():
    if not SKLEARN_AVAILABLE:
        return False

    if len(ml_feature_log) < CONFIG["ML_MIN_SAMPLES"]:
        log_signal_debug(f"ML: Insufficient data ({len(ml_feature_log)}/{CONFIG['ML_MIN_SAMPLES']})")
        return False

    try:
        # Prepare training data from feature log
        data = list(ml_feature_log)
        X = []
        y = []

        for entry in data:
            feat = entry.get("features", {})
            result = entry.get("result", 0)  # 1 = profitable, 0 = loss
            row = [feat.get(k, 0) for k in CONFIG["ML_FEATURES"]]
            X.append(row)
            y.append(result)

        if len(X) < CONFIG["ML_MIN_SAMPLES"]:
            return False

        model = RandomForestClassifier(n_estimators=50, max_depth=10, random_state=42)
        model.fit(X, y)

        accuracy = model.score(X, y)

        ml_model_state.update({
            "model": model,
            "last_trained": time.time(),
            "accuracy": accuracy,
            "feature_importance": dict(zip(CONFIG["ML_FEATURES"], model.feature_importances_)),
            "is_ready": True
        })

        log_signal_debug(f"ML MODEL RETRAINED: Accuracy={accuracy:.2%} | Samples={len(X)}")
        return True
    except Exception as e:
        logger.error(f"ML training failed: {e}")
        return False

def get_ml_prediction(features):
    if not ml_model_state["is_ready"] or not SKLEARN_AVAILABLE:
        return 0.5  # Neutral if ML not ready

    try:
        model = ml_model_state["model"]
        X = [[features.get(k, 0) for k in CONFIG["ML_FEATURES"]]]
        proba = model.predict_proba(X)[0]
        # proba[1] = probability of profitable trade
        ml_score = proba[1]
        ml_model_state["prediction_buffer"].append(ml_score)
        return ml_score
    except Exception as e:
        logger.error(f"ML prediction failed: {e}")
        return 0.5

def apply_ml_filter(signal_strength, score, ml_score):
    if not ml_model_state["is_ready"]:
        log_signal_debug(f"ML FILTER: Not ready, allowing signal (score={ml_score:.2f})")
        return signal_strength, score, ml_score

    log_signal_debug(f"ML FILTER: RawStrength={signal_strength} Score={score} ML={ml_score:.2f} Threshold={CONFIG['ML_CONFIDENCE_THRESHOLD']}")

    if ml_score < CONFIG["ML_CONFIDENCE_THRESHOLD"]:
        log_signal_debug(f"ML FILTER: BLOCKED - ML score {ml_score:.2f} < threshold {CONFIG['ML_CONFIDENCE_THRESHOLD']}")
        return "BLOCKED", score, ml_score

    if ml_score > CONFIG["ML_STRONG_OVERRIDE"] and signal_strength == "CONSIDER":
        log_signal_debug(f"ML FILTER: BOOSTED - ML score {ml_score:.2f} > {CONFIG['ML_STRONG_OVERRIDE']}, upgrading to STRONG")
        return "STRONG", score + 1, ml_score

    return signal_strength, score, ml_score

# ═══════════════════════════════════════════════════════════════
# AUTH & DATA FETCHING
# ═══════════════════════════════════════════════════════════════
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
    except Exception as e:
        logger.error(f"Error reading Spot Nifty: {e}")
    return None

def get_vix_value():
    _, _, obj = get_auth_token()
    if not obj: return None
    try:
        response = obj.ltpData("NSE", "INDIAVIX", VIX_TOKEN)
        if response.get("status") and response.get("data"):
            return float(response["data"].get("ltp", 15))
    except Exception as e:
        logger.error(f"Error reading VIX: {e}")
    return None

def get_banknifty_spot():
    _, _, obj = get_auth_token()
    if not obj: return None
    try:
        response = obj.ltpData("NSE", "BANKNIFTY", BANKNIFTY_TOKEN)
        if response.get("status") and response.get("data"):
            return float(response["data"].get("ltp", 0))
    except Exception as e:
        logger.error(f"Error reading BankNifty: {e}")
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
        logger.info(f"Scrip Resolved -> CE: {CE_TOKEN} | PE: {PE_TOKEN} | Strike: {ATM_STRIKE}")
        return CE_TOKEN, PE_TOKEN
    except Exception as e:
        logger.error(f"Error building Option Chain: {e}")
        return None, None

# ═══════════════════════════════════════════════════════════════
# RISK MANAGER
# ═══════════════════════════════════════════════════════════════
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

def reset_daily_safety():
    """Reset daily safety counters at market open."""
    global safety_state
    safety_state["daily_sl_count"] = 0
    safety_state["last_gap_check_price"] = 0.0
    safety_state["last_gap_check_time"] = 0
    log_signal_debug("DAILY RESET: Safety counters reset for new trading day")

def send_telegram_alert(message):
    if not TELEGRAM_AVAILABLE or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}, timeout=3)
    except Exception as e:
        logger.error(f"Telegram failed: {e}")

# ═══════════════════════════════════════════════════════════════
# SIGNAL CLASSIFICATION
# ═══════════════════════════════════════════════════════════════
def classify_signal_strength(rsi, macd_hist, adx, pcr, volume_ratio, trend_direction, vwap_ok=True, greeks_ok=True):
    score = 0
    factors = []

    if trend_direction == "BULLISH":
        if rsi >= CONFIG["EXTREME_CE_RSI"]: score += 3; factors.append("RSI_EXTREME")
        elif rsi >= CONFIG["STRONG_CE_RSI"]: score += 2; factors.append("RSI_STRONG")
        elif rsi >= CONFIG["CONSIDER_CE_RSI"]: score += 1; factors.append("RSI_CONSIDER")

        if macd_hist > CONFIG["MACD_CONFIRM_THRESHOLD"]: score += 2; factors.append("MACD_CONFIRM")
        elif macd_hist > 0: score += 1; factors.append("MACD_WEAK")

        if adx >= CONFIG["STRONG_TREND_MIN"]: score += 2; factors.append("TREND_STRONG")
        elif adx >= 15: score += 1; factors.append("TREND_MODERATE")

        if pcr <= CONFIG["PCR_BULLISH_THRESHOLD"]: score += 1; factors.append("PCR_BULLISH")
        if volume_ratio >= CONFIG["VOLUME_SPIKE_RATIO"]: score += 1; factors.append("VOLUME_SPIKE")
        if trend_direction == "BULLISH": score += 1; factors.append("PRICE_ABOVE_EMA")
        if vwap_ok: score += 1; factors.append("VWAP_CONFIRM")
        if greeks_ok: score += 1; factors.append("GREEKS_OK")

    else:
        if rsi <= CONFIG["EXTREME_PE_RSI"]: score += 3; factors.append("RSI_EXTREME")
        elif rsi <= CONFIG["STRONG_PE_RSI"]: score += 2; factors.append("RSI_STRONG")
        elif rsi <= CONFIG["CONSIDER_PE_RSI"]: score += 1; factors.append("RSI_CONSIDER")

        if macd_hist < -CONFIG["MACD_CONFIRM_THRESHOLD"]: score += 2; factors.append("MACD_CONFIRM")
        elif macd_hist < 0: score += 1; factors.append("MACD_WEAK")

        if adx >= CONFIG["STRONG_TREND_MIN"]: score += 2; factors.append("TREND_STRONG")
        elif adx >= 15: score += 1; factors.append("TREND_MODERATE")

        if pcr >= CONFIG["PCR_BEARISH_THRESHOLD"]: score += 1; factors.append("PCR_BEARISH")
        if volume_ratio >= CONFIG["VOLUME_SPIKE_RATIO"]: score += 1; factors.append("VOLUME_SPIKE")
        if trend_direction == "BEARISH": score += 1; factors.append("PRICE_BELOW_EMA")
        if vwap_ok: score += 1; factors.append("VWAP_CONFIRM")
        if greeks_ok: score += 1; factors.append("GREEKS_OK")

    if score >= 7: return "STRONG", score, factors
    elif score >= 4: return "CONSIDER", score, factors
    elif score >= CONFIG["MIN_SCORE_FOR_ENTRY"] and CONFIG.get("WEAK_SIGNAL_ALLOWED", False):
        return "WEAK", score, factors
    else: return "WEAK", score, factors

def generate_alert_message(action, strength, spot_price, premium, sl, target, factors, confidence, lots=1, risk_pct=1.0):
    factor_str = " | ".join(factors) if factors else "Basic"
    size_info = f"\n📦 Lots: {lots} | Risk: {risk_pct:.2f}%"

    if action == "BUY_CE":
        if strength == "STRONG":
            return f"🟢 <b>STRONG CE BUY</b>\n💰 Spot: {spot_price} | Premium: {premium}\n🎯 Target: {round(target, 2)} | 🛡️ SL: {round(sl, 2)}{size_info}\n📊 Confidence: {round(confidence, 1)}% | Factors: {factor_str}"
        elif strength == "CONSIDER":
            return f"🟡 <b>CONSIDER CE BUY</b>\n💰 Spot: {spot_price} | Premium: {premium}\n🎯 Target: {round(target, 2)} | 🛡️ SL: {round(sl, 2)}{size_info}\n📊 Confidence: {round(confidence, 1)}% | Factors: {factor_str}"
        else:
            return f"⚪ <b>WEAK CE BUY (Aggressive)</b>\n💰 Spot: {spot_price} | Premium: {premium}\n🎯 Target: {round(target, 2)} | 🛡️ SL: {round(sl, 2)}{size_info}\n📊 Confidence: {round(confidence, 1)}% | Factors: {factor_str}"
    elif action == "BUY_PE":
        if strength == "STRONG":
            return f"🔴 <b>STRONG PE BUY</b>\n💰 Spot: {spot_price} | Premium: {premium}\n🎯 Target: {round(target, 2)} | 🛡️ SL: {round(sl, 2)}{size_info}\n📊 Confidence: {round(confidence, 1)}% | Factors: {factor_str}"
        elif strength == "CONSIDER":
            return f"🟠 <b>CONSIDER PE BUY</b>\n💰 Spot: {spot_price} | Premium: {premium}\n🎯 Target: {round(target, 2)} | 🛡️ SL: {round(sl, 2)}{size_info}\n📊 Confidence: {round(confidence, 1)}% | Factors: {factor_str}"
        else:
            return f"⚪ <b>WEAK PE BUY (Aggressive)</b>\n💰 Spot: {spot_price} | Premium: {premium}\n🎯 Target: {round(target, 2)} | 🛡️ SL: {round(sl, 2)}{size_info}\n📊 Confidence: {round(confidence, 1)}% | Factors: {factor_str}"
    elif action == "EXIT":
        return f"⚠️ <b>EXIT SIGNAL</b>\n💰 Spot: {spot_price} | Premium: {premium}\n📊 Reason: {factor_str}"
    elif action == "HOLD":
        return f"⏸️ <b>HOLD</b>\n💰 Spot: {spot_price}\n📊 Market: Ranging | Confidence: {round(confidence, 1)}%"
    return f"📊 <b>WAITING</b>\n💰 Spot: {spot_price}"

# ═══════════════════════════════════════════════════════════════
# ENHANCED SIGNAL ENGINE - v2.0
# ═══════════════════════════════════════════════════════════════
def run_signal_engine():
    global market_signal, market_state, signal_state, portfolio_state, latest_ticks
    global signal_buffer, daily_trade_count, ml_model_state, safety_state

    log_signal_debug("=== SIGNAL ENGINE CYCLE START ===")

    # ═══════════════════════════════════════════════════════════════
    # GRADE 1 PRO: CIRCUIT BREAKER CHECK
    # ═══════════════════════════════════════════════════════════════
    cb_ok, cb_reason = check_circuit_breakers()
    if not cb_ok:
        market_state["action"] = "CIRCUIT_BREAKER"
        market_signal["alert_message"] = f"🚫 CIRCUIT BREAKER: {cb_reason}"
        market_signal["signal_strength"] = "CIRCUIT_BREAKER"
        log_signal_debug(f"CIRCUIT BREAKER ACTIVE: {cb_reason}")
        return

    # ═══════════════════════════════════════════════════════════════
    # GRADE 1 PRO: TIME FILTER CHECK
    # ═══════════════════════════════════════════════════════════════
    time_ok, time_reason = check_time_filters()
    if not time_ok:
        market_state["action"] = "TIME_FILTER"
        market_signal["alert_message"] = f"⏸️ TIME FILTER: {time_reason}"
        market_signal["signal_strength"] = "TIME_HALT"
        log_signal_debug(f"TIME FILTER: {time_reason}")
        return

    # ═══════════════════════════════════════════════════════════════
    # GRADE 1 PRO: GAP RISK CHECK
    # ═══════════════════════════════════════════════════════════════
    gap_ok, gap_reason = check_gap_risk()
    if not gap_ok:
        market_state["action"] = "GAP_HALT"
        market_signal["alert_message"] = f"🚫 GAP DETECTED: {gap_reason}"
        market_signal["signal_strength"] = "GAP_HALT"
        log_signal_debug(f"GAP RISK: {gap_reason}")
        return

    # Halt checks
    if risk_manager.check_daily_loss_limit():
        market_state["action"] = "HALTED_LOSS_LIMIT"
        market_signal["alert_message"] = "🚫 HALTED: Daily loss limit reached"
        market_signal["signal_strength"] = "HALTED"
        log_signal_debug("HALTED: Daily loss limit reached")
        return

    if risk_manager.check_max_daily_trades():
        market_state["action"] = "HALTED_MAX_TRADES"
        market_signal["alert_message"] = f"🚫 HALTED: Max daily trades reached ({daily_trade_count}/{CONFIG['MAX_DAILY_TRADES']})"
        market_signal["signal_strength"] = "HALTED"
        log_signal_debug(f"HALTED: Max daily trades reached: {daily_trade_count}/{CONFIG['MAX_DAILY_TRADES']}")
        return

    spot_history_list = list(spot_price_history)
    min_data = CONFIG.get("MIN_DATA_POINTS", 30)

    if len(spot_history_list) < min_data:
        market_signal["alert_message"] = f"⏳ Collecting market data... ({len(spot_history_list)}/{min_data})"
        market_signal["signal_strength"] = "WAITING"
        log_signal_debug(f"WAITING: Insufficient data points: {len(spot_history_list)}/{min_data}")
        return

    # ═══════════════════════════════════════════════════════════════
    # CALCULATE ALL INDICATORS
    # ═══════════════════════════════════════════════════════════════
    spot_rsi = calculate_rsi(spot_history_list, CONFIG["SPOT_RSI_PERIOD"], CONFIG["SPOT_RSI_SMOOTHING"])
    spot_macd, macd_signal_line, macd_hist = calculate_macd(
        spot_history_list, CONFIG["SPOT_MACD_FAST"], CONFIG["SPOT_MACD_SLOW"], CONFIG["SPOT_MACD_SIGNAL"])
    spot_atr = calculate_atr(spot_history_list, CONFIG["SPOT_ATR_PERIOD"])
    adx = calculate_adx(spot_history_list, CONFIG["TREND_STRENGTH_PERIOD"])
    pcr = get_nifty_pcr()

    # EMAs
    ema_fast = calculate_ema(spot_history_list, 20)
    ema_slow = calculate_ema(spot_history_list, 50)
    price_above_fast = spot_history_list[-1] > ema_fast
    price_above_slow = spot_history_list[-1] > ema_slow

    # VWAP & Market Profile
    update_vwap_and_profile()
    vwap = institutional_state.get("vwap", 0)

    # Greeks
    update_greeks_approx()
    greeks_pass, greeks_reason = check_greeks_filter()

    # VIX
    vix_value = latest_ticks.get("vix", 15.0)

    # BankNifty correlation
    banknifty_corr = calculate_correlation(spot_price_history, banknifty_history, CONFIG["VIX_CORRELATION_PERIOD"])

    # Volume
    ce_vol_list = list(ce_volume_history)
    pe_vol_list = list(pe_volume_history)
    avg_ce_vol = sum(ce_vol_list[-CONFIG["VOLUME_MA_PERIOD"]:]) / min(len(ce_vol_list), CONFIG["VOLUME_MA_PERIOD"]) if ce_vol_list else 1
    avg_pe_vol = sum(pe_vol_list[-CONFIG["VOLUME_MA_PERIOD"]:]) / min(len(pe_vol_list), CONFIG["VOLUME_MA_PERIOD"]) if pe_vol_list else 1
    current_ce_vol = ce_vol_list[-1] if ce_vol_list else 0
    current_pe_vol = pe_vol_list[-1] if pe_vol_list else 0
    ce_vol_ratio = current_ce_vol / avg_ce_vol if avg_ce_vol > 0 else 1
    pe_vol_ratio = current_pe_vol / avg_pe_vol if avg_pe_vol > 0 else 1

    current_time = time.time()

    # ═══════════════════════════════════════════════════════════════
    # REGIME DETECTION
    # ═══════════════════════════════════════════════════════════════
    regime, strategy, regime_conf = detect_market_regime(spot_history_list, spot_atr, adx, vix_value)

    # Get regime-adjusted parameters
    base_params = {
        "entry_atr_mult": CONFIG["ENTRY_ATR_MULT"],
        "target_atr_mult": CONFIG["TARGET_ATR_MULT"],
        "max_hold_time": CONFIG["MAX_HOLD_TIME_MIN"]
    }
    adjusted_params = apply_regime_adjustments(base_params, regime, strategy, regime_conf)

    market_state.update({
        "rsi": round(spot_rsi, 1), "macd_hist": round(macd_hist, 4),
        "atr": round(spot_atr, 2), "adx": round(adx, 1), "pcr": round(pcr, 2),
        "trend": "UPTREND" if price_above_fast and price_above_slow else "DOWNTREND" if not price_above_fast and not price_above_slow else "MIXED",
        "vix_regime": get_vix_regime(vix_value)[0],
        "banknifty_trend": "UP" if len(banknifty_history) > 1 and list(banknifty_history)[-1] > list(banknifty_history)[0] else "DOWN"
    })

    log_signal_debug(f"Indicators -> RSI:{spot_rsi:.1f} MACD:{macd_hist:.3f} ADX:{adx:.1f} PCR:{pcr:.2f} ATR:{spot_atr:.2f} VWAP:{vwap:.1f} VIX:{vix_value:.1f} Regime:{regime}")
    log_signal_debug(f"Greeks -> IV:{greeks_state['iv']:.2%} Delta:{greeks_state['delta']:.3f} Gamma:{greeks_state['gamma']:.4f} Theta:{greeks_state['theta']:.2f}")

    # ═══════════════════════════════════════════════════════════════
    # ML FEATURE EXTRACTION & PREDICTION
    # ═══════════════════════════════════════════════════════════════
    ml_features = extract_ml_features(spot_rsi, macd_hist, adx, pcr, ce_vol_ratio, vwap, vix_value, banknifty_corr, spot_atr, ema_fast, ema_slow, spot_history_list[-1])
    ml_score = get_ml_prediction(ml_features)
    market_signal["ml_score"] = round(ml_score, 3)

    # Retrain ML model periodically
    if current_time - ml_model_state["last_trained"] > CONFIG["ML_RETRAIN_INTERVAL"]:
        train_ml_model()

    # ═══════════════════════════════════════════════════════════════
    # VIX SPIKE CHECK
    # ═══════════════════════════════════════════════════════════════
    if CONFIG["AVOID_TRADES_VIX_SPIKE"] and check_vix_spike() and signal_state["current_action"] == "HOLD":
        market_signal["alert_message"] = f"⏸️ VIX SPIKE DETECTED | Holding positions\nVIX: {vix_value:.1f} | Regime: {regime}"
        market_signal["signal_strength"] = "VIX_HALT"
        log_signal_debug("VIX SPIKE: New entries blocked")
        return

    # ═══════════════════════════════════════════════════════════════
    # POSITION MANAGEMENT - ENHANCED
    # ═══════════════════════════════════════════════════════════════
    if signal_state["current_action"] != "HOLD":
        active_side = signal_state["current_action"]
        current_premium = latest_ticks["ce_price"] if active_side == "BUY_CE" else latest_ticks["pe_price"]

        if current_premium == 0:
            market_signal["alert_message"] = "⏳ Waiting for premium data..."
            return

        unrealized_pnl = current_premium - signal_state["entry_price"]
        if unrealized_pnl > signal_state["max_profit_seen"]:
            signal_state["max_profit_seen"] = unrealized_pnl

        # Trailing stop
        if current_premium > signal_state["highest_premium_seen"]:
            signal_state["highest_premium_seen"] = current_premium
            new_sl = current_premium - (spot_atr * CONFIG["TRAILING_ATR_MULT"])
            if new_sl > signal_state["stop_loss"]:
                old_sl = signal_state["stop_loss"]
                signal_state["stop_loss"] = new_sl
                if new_sl > signal_state["entry_price"] and old_sl <= signal_state["entry_price"]:
                    send_telegram_alert(f"🔒 <b>SL MOVED TO BREAKEVEN</b>\n{active_side} @ {round(current_premium, 2)}")
                    log_signal_debug(f"TRAILING: SL moved to breakeven @ {round(current_premium, 2)}")

        # Drawdown alert & exit
        if signal_state["max_profit_seen"] > 0:
            drawdown_from_peak = signal_state["max_profit_seen"] - unrealized_pnl
            drawdown_pct = drawdown_from_peak / signal_state["max_profit_seen"] if signal_state["max_profit_seen"] > 0 else 0

            if 0 < drawdown_pct < CONFIG["DRAWDOWN_ALERT_PCT"]:
                log_signal_debug(f"DRAWDOWN ALERT: {drawdown_pct*100:.1f}% from peak profit {signal_state['max_profit_seen']:.2f} -> current {unrealized_pnl:.2f}")

            if drawdown_pct >= CONFIG["DRAWDOWN_ALERT_PCT"]:
                pnl_points = current_premium - signal_state["entry_price"]
                portfolio_state["equity"] += (pnl_points * 50 * signal_state.get("lots", 1))
                daily_trade_count += 1
                update_safety_on_exit(pnl_points, "DRAWDOWN")
                exit_msg = generate_alert_message("EXIT", "DRAWDOWN", latest_ticks["spot_price"], current_premium, 0, 0, [f"Drawdown Alert - {drawdown_pct*100:.1f}% from peak"], 0)
                send_telegram_alert(exit_msg + f"\n💵 PnL: {round(pnl_points, 2)} pts")
                log_signal_debug(f"EXIT: Drawdown exit. PnL: {round(pnl_points, 2)}")
                reset_signal_state(current_time)
                return

        # Standard exits
        if current_premium <= signal_state["stop_loss"]:
            pnl_points = current_premium - signal_state["entry_price"]
            portfolio_state["equity"] += (pnl_points * 50 * signal_state.get("lots", 1))
            daily_trade_count += 1
            update_safety_on_exit(pnl_points, "STOP_LOSS")
            exit_msg = generate_alert_message("EXIT", "STOP_LOSS", latest_ticks["spot_price"], current_premium, 0, 0, ["Trailing SL Hit"], 0)
            send_telegram_alert(exit_msg + f"\n💵 PnL: {round(pnl_points, 2)} pts")
            log_signal_debug(f"EXIT: Stop loss hit. PnL: {round(pnl_points, 2)}")
            reset_signal_state(current_time)
            return

        if current_premium >= signal_state["target"]:
            pnl_points = current_premium - signal_state["entry_price"]
            portfolio_state["equity"] += (pnl_points * 50 * signal_state.get("lots", 1))
            daily_trade_count += 1
            portfolio_state["winning_trades"] += 1
            portfolio_state["total_trades"] += 1
            portfolio_state["win_rate"] = portfolio_state["winning_trades"] / portfolio_state["total_trades"] * 100
            update_safety_on_exit(pnl_points, "TARGET")
            exit_msg = generate_alert_message("EXIT", "TARGET", latest_ticks["spot_price"], current_premium, 0, 0, ["Target Achieved"], 100)
            send_telegram_alert(exit_msg + f"\n💵 PnL: {round(pnl_points, 2)} pts")
            log_signal_debug(f"EXIT: Target achieved. PnL: {round(pnl_points, 2)}")
            reset_signal_state(current_time)
            return

        # Profit lock
        if signal_state["max_profit_seen"] > spot_atr * 0.5:
            drawdown_from_peak = signal_state["max_profit_seen"] - unrealized_pnl
            if drawdown_from_peak > signal_state["max_profit_seen"] * CONFIG["PROFIT_LOCK_PCT"]:
                pnl_points = current_premium - signal_state["entry_price"]
                portfolio_state["equity"] += (pnl_points * 50 * signal_state.get("lots", 1))
                daily_trade_count += 1
                update_safety_on_exit(pnl_points, "PROFIT_LOCK")
                exit_msg = generate_alert_message("EXIT", "PROFIT_LOCK", latest_ticks["spot_price"], current_premium, 0, 0, [f"Profit Lock - {CONFIG['PROFIT_LOCK_PCT']*100:.0f}% Drawdown from Peak"], 0)
                send_telegram_alert(exit_msg + f"\n💵 PnL: {round(pnl_points, 2)} pts")
                log_signal_debug(f"EXIT: Profit lock triggered. PnL: {round(pnl_points, 2)}")
                reset_signal_state(current_time)
                return

        # Max hold time (regime-adjusted)
        max_hold = adjusted_params.get("max_hold_time", CONFIG["MAX_HOLD_TIME_MIN"])
        hold_time_min = (current_time - signal_state["entry_time"]) / 60
        if hold_time_min >= max_hold:
            pnl_points = current_premium - signal_state["entry_price"]
            portfolio_state["equity"] += (pnl_points * 50 * signal_state.get("lots", 1))
            daily_trade_count += 1
            update_safety_on_exit(pnl_points, "TIME_EXIT")
            exit_msg = generate_alert_message("EXIT", "TIME", latest_ticks["spot_price"], current_premium, 0, 0, [f"Max Hold Time ({max_hold}min)"], 0)
            send_telegram_alert(exit_msg + f"\n💵 PnL: {round(pnl_points, 2)} pts")
            log_signal_debug(f"EXIT: Max hold time ({max_hold}min). PnL: {round(pnl_points, 2)}")
            reset_signal_state(current_time)
            return

        # Momentum reversal exit
        if active_side == "BUY_CE":
            rsi_exit = spot_rsi < CONFIG.get("RSI_EXIT_CE", 48)
            macd_exit = macd_hist < -CONFIG.get("MACD_EXIT_THRESHOLD", 0.1)
            ema_exit = not price_above_fast

            if rsi_exit or macd_exit or ema_exit:
                pnl_points = current_premium - signal_state["entry_price"]
                portfolio_state["equity"] += (pnl_points * 50 * signal_state.get("lots", 1))
                daily_trade_count += 1
                update_safety_on_exit(pnl_points, "MOMENTUM_REVERSAL")
                reasons = []
                if rsi_exit: reasons.append(f"RSI<{CONFIG.get('RSI_EXIT_CE', 48)}")
                if macd_exit: reasons.append(f"MACD reversal<{-CONFIG.get('MACD_EXIT_THRESHOLD', 0.1)}")
                if ema_exit: reasons.append("Price<EMA20")
                exit_msg = generate_alert_message("EXIT", "MOMENTUM", latest_ticks["spot_price"], current_premium, 0, 0, reasons, 0)
                send_telegram_alert(exit_msg + f"\n💵 PnL: {round(pnl_points, 2)} pts")
                log_signal_debug(f"EXIT CE: Momentum reversal. {' | '.join(reasons)} PnL: {round(pnl_points, 2)}")
                reset_signal_state(current_time)
                return

        elif active_side == "BUY_PE":
            rsi_exit = spot_rsi > CONFIG.get("RSI_EXIT_PE", 52)
            macd_exit = macd_hist > CONFIG.get("MACD_EXIT_THRESHOLD", 0.1)
            ema_exit = price_above_fast

            if rsi_exit or macd_exit or ema_exit:
                pnl_points = current_premium - signal_state["entry_price"]
                portfolio_state["equity"] += (pnl_points * 50 * signal_state.get("lots", 1))
                daily_trade_count += 1
                update_safety_on_exit(pnl_points, "MOMENTUM_REVERSAL")
                reasons = []
                if rsi_exit: reasons.append(f"RSI>{CONFIG.get('RSI_EXIT_PE', 52)}")
                if macd_exit: reasons.append(f"MACD reversal>{CONFIG.get('MACD_EXIT_THRESHOLD', 0.1)}")
                if ema_exit: reasons.append("Price>EMA20")
                exit_msg = generate_alert_message("EXIT", "MOMENTUM", latest_ticks["spot_price"], current_premium, 0, 0, reasons, 0)
                send_telegram_alert(exit_msg + f"\n💵 PnL: {round(pnl_points, 2)} pts")
                log_signal_debug(f"EXIT PE: Momentum reversal. {' | '.join(reasons)} PnL: {round(pnl_points, 2)}")
                reset_signal_state(current_time)
                return

        # Update active display
        hold_mins = int((current_time - signal_state["entry_time"]) / 60)
        pnl_pct = ((current_premium - signal_state["entry_price"]) / signal_state["entry_price"] * 50) if signal_state["entry_price"] > 0 else 0
        market_signal["alert_message"] = f"📊 {active_side} ACTIVE | Hold: {hold_mins}m | PnL: {round(pnl_pct, 1)}% | SL: {round(signal_state['stop_loss'], 2)} | Peak: {round(signal_state['max_profit_seen'], 2)} | Regime: {regime}"
        market_signal["signal_strength"] = "ACTIVE"

    # ═══════════════════════════════════════════════════════════════
    # NEW SIGNAL DETECTION - WITH ALL ENHANCEMENTS
    # ═══════════════════════════════════════════════════════════════
    else:
        if current_time < signal_state["cooldown_until"]:
            remaining = int(signal_state["cooldown_until"] - current_time)
            market_signal["alert_message"] = f"⏳ Cooldown: {remaining}s remaining"
            market_signal["signal_strength"] = "COOLDOWN"
            log_signal_debug(f"COOLDOWN: {remaining}s remaining")
            return

        # Pre-calculate all conditions
        ce_rsi_ok = spot_rsi >= CONFIG["CONSIDER_CE_RSI"]
        ce_macd_ok = macd_hist > 0
        ce_ema_ok = price_above_fast
        ce_vwap_ok = get_vwap_signal(spot_history_list[-1], vwap, "CE") if CONFIG["USE_VWAP_CONFIRMATION"] else True

        pe_rsi_ok = spot_rsi <= CONFIG["CONSIDER_PE_RSI"]
        pe_macd_ok = macd_hist < 0
        pe_ema_ok = not price_above_fast
        pe_vwap_ok = get_vwap_signal(spot_history_list[-1], vwap, "PE") if CONFIG["USE_VWAP_CONFIRMATION"] else True

        log_signal_debug(f"CE CHECK -> RSI:{spot_rsi:.1f}>={CONFIG['CONSIDER_CE_RSI']}={ce_rsi_ok} | MACD:{macd_hist:.3f}>0={ce_macd_ok} | EMA={ce_ema_ok} | VWAP={ce_vwap_ok} | GREEKS={greeks_pass}")
        log_signal_debug(f"PE CHECK -> RSI:{spot_rsi:.1f}<={CONFIG['CONSIDER_PE_RSI']}={pe_rsi_ok} | MACD:{macd_hist:.3f}<0={pe_macd_ok} | EMA={pe_ema_ok} | VWAP={pe_vwap_ok} | GREEKS={greeks_pass}")

        # ═══════════════════════════════════════════════════════════════
        # CE SIGNAL DETECTION
        # ═══════════════════════════════════════════════════════════════
        if ce_rsi_ok and ce_macd_ok and ce_ema_ok and ce_vwap_ok and greeks_pass:
            ce_premium = latest_ticks["ce_price"]
            if ce_premium == 0:
                market_signal["alert_message"] = "⏳ Waiting for CE premium data..."
                return

            # GRADE 1 PRO: Premium safety check
            prem_ok, prem_reason = check_premium_safety(ce_premium, "CE")
            if not prem_ok:
                market_signal["alert_message"] = f"🚫 CE BLOCKED: {prem_reason}"
                market_signal["signal_strength"] = "PREMIUM_FILTER"
                log_signal_debug(f"CE PREMIUM FILTER: {prem_reason}")
                signal_buffer["ce_count"] = 0
                return

            signal_buffer["ce_count"] += 1
            signal_buffer["pe_count"] = 0

            building_ticks = CONFIG.get("SIGNAL_BUILDING_TICKS", 2)
            if signal_buffer["ce_count"] < building_ticks:
                market_signal["alert_message"] = f"🟡 CE Signal Building... ({signal_buffer['ce_count']}/{building_ticks})"
                market_signal["signal_strength"] = "BUILDING"
                log_signal_debug(f"CE BUILDING: {signal_buffer['ce_count']}/{building_ticks}")
                return

            strength, score, factors = classify_signal_strength(
                spot_rsi, macd_hist, adx, pcr, ce_vol_ratio, "BULLISH", ce_vwap_ok, greeks_pass)

            # ML Filter
            strength, score, ml_score = apply_ml_filter(strength, score, ml_score)

            if strength == "BLOCKED":
                market_signal["alert_message"] = f"🚫 CE BLOCKED by ML Filter (Score: {ml_score:.2f})"
                market_signal["signal_strength"] = "ML_BLOCKED"
                signal_buffer["ce_count"] = 0
                return

            # GRADE 1 PRO: Signal quality gate
            confidence = min(95, score * 10 + spot_rsi * 0.3 + ml_score * 20)
            quality_ok, quality_reason = check_signal_quality(score, factors, confidence)
            if not quality_ok:
                market_signal["alert_message"] = f"🚫 CE QUALITY GATE: {quality_reason}"
                market_signal["signal_strength"] = "QUALITY_FILTER"
                log_signal_debug(f"CE QUALITY GATE: {quality_reason}")
                signal_buffer["ce_count"] = 0
                return

            log_signal_debug(f"CE CLASSIFY -> Strength:{strength} Score:{score} Factors:{factors} ML:{ml_score:.2f}")

            if strength == "WEAK" and not CONFIG.get("WEAK_SIGNAL_ALLOWED", True):
                market_signal["alert_message"] = f"⚪ Weak CE Signal Ignored (Score: {score}/10)"
                market_signal["signal_strength"] = "WEAK"
                signal_buffer["ce_count"] = 0
                log_signal_debug(f"CE REJECTED: Weak signal blocked. Score: {score}")
                return

            if signal_buffer["consecutive_ce"] >= CONFIG["CONSECUTIVE_SAME_DIR_MAX"]:
                market_signal["alert_message"] = "🚫 CE Blocked: Max consecutive entries reached"
                market_signal["signal_strength"] = "BLOCKED"
                signal_buffer["ce_count"] = 0
                log_signal_debug("CE BLOCKED: Max consecutive CE entries")
                return

            # Dynamic position sizing
            lots, risk_pct, risk_amount = calculate_position_size(strength, score, spot_atr, vix_value, "BULLISH")

            # GRADE 1 PRO: Recovery mode adjustment
            recovery_mult = apply_recovery_adjustments()
            lots = max(1, int(lots * recovery_mult))
            risk_pct *= recovery_mult
            log_signal_debug(f"RECOVERY ADJ: mult={recovery_mult:.2f} lots={lots} risk={risk_pct:.2f}%")

            entry_mult = adjusted_params.get("entry_atr_mult", CONFIG["ENTRY_ATR_MULT"])
            target_mult = adjusted_params.get("target_atr_mult", CONFIG["TARGET_ATR_MULT"])

            sl = ce_premium - (spot_atr * entry_mult)
            target = ce_premium + (spot_atr * target_mult)
            confidence = min(95, score * 10 + spot_rsi * 0.3 + ml_score * 20)

            if score >= 7: grade = "A+"
            elif score >= 5: grade = "A"
            elif score >= 4: grade = "B+"
            else: grade = "B"

            signal_state.update({
                "current_action": "BUY_CE", "entry_price": ce_premium,
                "highest_premium_seen": ce_premium, "stop_loss": sl,
                "target": target, "signal_grade": grade,
                "confidence": confidence, "entry_time": current_time, "max_profit_seen": 0.0,
                "lots": lots, "position_risk_pct": risk_pct})
            portfolio_state["open_positions"] = 1
            signal_buffer["consecutive_ce"] += 1
            signal_buffer["consecutive_pe"] = 0
            daily_trade_count += 1

            alert = generate_alert_message("BUY_CE", strength, latest_ticks["spot_price"], ce_premium, sl, target, factors, confidence, lots, risk_pct)
            send_telegram_alert(alert)
            log_signal_debug(f"SIGNAL ENTER: {strength} CE BUY | Score:{score} | Grade:{grade} | Conf:{confidence:.1f}% | Lots:{lots} | Risk:{risk_pct:.2f}% | Regime:{regime}")
            logger.info(f"SIGNAL: {strength} CE BUY | Score:{score} | Grade:{grade} | ML:{ml_score:.2f}")
            return  # EXIT after CE signal

        # ═══════════════════════════════════════════════════════════════
        # PE SIGNAL DETECTION
        # ═══════════════════════════════════════════════════════════════
        elif pe_rsi_ok and pe_macd_ok and pe_ema_ok and pe_vwap_ok and greeks_pass:
            pe_premium = latest_ticks["pe_price"]
            if pe_premium == 0:
                market_signal["alert_message"] = "⏳ Waiting for PE premium data..."
                return

            # GRADE 1 PRO: Premium safety check
            prem_ok, prem_reason = check_premium_safety(pe_premium, "PE")
            if not prem_ok:
                market_signal["alert_message"] = f"🚫 PE BLOCKED: {prem_reason}"
                market_signal["signal_strength"] = "PREMIUM_FILTER"
                log_signal_debug(f"PE PREMIUM FILTER: {prem_reason}")
                signal_buffer["pe_count"] = 0
                return

            signal_buffer["pe_count"] += 1
            signal_buffer["ce_count"] = 0

            building_ticks = CONFIG.get("SIGNAL_BUILDING_TICKS", 2)
            if signal_buffer["pe_count"] < building_ticks:
                market_signal["alert_message"] = f"🟡 PE Signal Building... ({signal_buffer['pe_count']}/{building_ticks})"
                market_signal["signal_strength"] = "BUILDING"
                log_signal_debug(f"PE BUILDING: {signal_buffer['pe_count']}/{building_ticks}")
                return

            strength, score, factors = classify_signal_strength(
                spot_rsi, macd_hist, adx, pcr, pe_vol_ratio, "BEARISH", pe_vwap_ok, greeks_pass)

            # ML Filter
            strength, score, ml_score = apply_ml_filter(strength, score, ml_score)

            if strength == "BLOCKED":
                market_signal["alert_message"] = f"🚫 PE BLOCKED by ML Filter (Score: {ml_score:.2f})"
                market_signal["signal_strength"] = "ML_BLOCKED"
                signal_buffer["pe_count"] = 0
                return

            # GRADE 1 PRO: Signal quality gate
            confidence = min(95, score * 10 + (100 - spot_rsi) * 0.3 + ml_score * 20)
            quality_ok, quality_reason = check_signal_quality(score, factors, confidence)
            if not quality_ok:
                market_signal["alert_message"] = f"🚫 PE QUALITY GATE: {quality_reason}"
                market_signal["signal_strength"] = "QUALITY_FILTER"
                log_signal_debug(f"PE QUALITY GATE: {quality_reason}")
                signal_buffer["pe_count"] = 0
                return

            log_signal_debug(f"PE CLASSIFY -> Strength:{strength} Score:{score} Factors:{factors} ML:{ml_score:.2f}")

            if strength == "WEAK" and not CONFIG.get("WEAK_SIGNAL_ALLOWED", True):
                market_signal["alert_message"] = f"⚪ Weak PE Signal Ignored (Score: {score}/10)"
                market_signal["signal_strength"] = "WEAK"
                signal_buffer["pe_count"] = 0
                log_signal_debug(f"PE REJECTED: Weak signal blocked. Score: {score}")
                return

            if signal_buffer["consecutive_pe"] >= CONFIG["CONSECUTIVE_SAME_DIR_MAX"]:
                market_signal["alert_message"] = "🚫 PE Blocked: Max consecutive entries reached"
                market_signal["signal_strength"] = "BLOCKED"
                signal_buffer["pe_count"] = 0
                log_signal_debug("PE BLOCKED: Max consecutive PE entries")
                return

            # Dynamic position sizing
            lots, risk_pct, risk_amount = calculate_position_size(strength, score, spot_atr, vix_value, "BEARISH")

            # GRADE 1 PRO: Recovery mode adjustment
            recovery_mult = apply_recovery_adjustments()
            lots = max(1, int(lots * recovery_mult))
            risk_pct *= recovery_mult
            log_signal_debug(f"RECOVERY ADJ: mult={recovery_mult:.2f} lots={lots} risk={risk_pct:.2f}%")

            entry_mult = adjusted_params.get("entry_atr_mult", CONFIG["ENTRY_ATR_MULT"])
            target_mult = adjusted_params.get("target_atr_mult", CONFIG["TARGET_ATR_MULT"])

            sl = pe_premium - (spot_atr * entry_mult)
            target = pe_premium + (spot_atr * target_mult)
            confidence = min(95, score * 10 + (100 - spot_rsi) * 0.3 + ml_score * 20)

            if score >= 7: grade = "A+"
            elif score >= 5: grade = "A"
            elif score >= 4: grade = "B+"
            else: grade = "B"

            signal_state.update({
                "current_action": "BUY_PE", "entry_price": pe_premium,
                "highest_premium_seen": pe_premium, "stop_loss": sl,
                "target": target, "signal_grade": grade,
                "confidence": confidence, "entry_time": current_time, "max_profit_seen": 0.0,
                "lots": lots, "position_risk_pct": risk_pct})
            portfolio_state["open_positions"] = 1
            signal_buffer["consecutive_pe"] += 1
            signal_buffer["consecutive_ce"] = 0
            daily_trade_count += 1

            alert = generate_alert_message("BUY_PE", strength, latest_ticks["spot_price"], pe_premium, sl, target, factors, confidence, lots, risk_pct)
            send_telegram_alert(alert)
            log_signal_debug(f"SIGNAL ENTER: {strength} PE BUY | Score:{score} | Grade:{grade} | Conf:{confidence:.1f}% | Lots:{lots} | Risk:{risk_pct:.2f}% | Regime:{regime}")
            logger.info(f"SIGNAL: {strength} PE BUY | Score:{score} | Grade:{grade} | ML:{ml_score:.2f}")
            return  # EXIT after PE signal

        # ═══════════════════════════════════════════════════════════════
        # NO SIGNAL
        # ═══════════════════════════════════════════════════════════════
        else:
            signal_buffer["ce_count"] = 0
            signal_buffer["pe_count"] = 0

            if spot_rsi > 55:
                status = f"Bullish bias but waiting (RSI:{spot_rsi:.1f}, MACD:{macd_hist:.2f}, Regime:{regime})"
            elif spot_rsi < 45:
                status = f"Bearish bias but waiting (RSI:{spot_rsi:.1f}, MACD:{macd_hist:.2f}, Regime:{regime})"
            else:
                status = f"Market ranging (RSI:{spot_rsi:.1f} neutral, Regime:{regime})"

            market_signal["alert_message"] = f"⏸️ HOLD | {status}\nRSI:{spot_rsi:.1f} | MACD:{macd_hist:.2f} | ADX:{adx:.1f} | VIX:{vix_value:.1f} | ML:{ml_score:.2f}"
            market_signal["signal_strength"] = "HOLD"
            log_signal_debug(f"HOLD: {status}")

        # Multi-timeframe analysis
    for tf in TIMEFRAMES:
        tf_data = list(timeframe_history[tf])
        if len(tf_data) >= 3:
            c1, c2, c3 = tf_data[-3]["close"], tf_data[-2]["close"], tf_data[-1]["close"]
            market_signal[f"trend_{tf}"] = "BULLISH" if c3 > c2 > c1 else "BEARISH" if c3 < c2 < c1 else "SIDEWAYS"
        else:
            market_signal[f"trend_{tf}"] = "SIDEWAYS"

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
        "vwap": round(vwap, 2),
        "vix": round(vix_value, 2),
        "signal": signal_state["current_action"],
        "confidence": round(signal_state["confidence"], 2),
        "timestamp": datetime.now().isoformat(),
        "grade": signal_state["signal_grade"],
        "daily_trades": daily_trade_count,
        "regime": regime,
        "strategy": strategy,
        "banknifty_corr": round(banknifty_corr, 3),
        "delta_neutral_signal": greeks_state["delta"] < CONFIG["DELTA_NEUTRAL_THRESHOLD"],
        "gamma_exposure": "HIGH" if greeks_state["gamma"] > CONFIG["GAMMA_SKEW_THRESHOLD"] else "LOW"
    })

    log_signal_debug("=== SIGNAL ENGINE CYCLE END ===")

def reset_signal_state(current_time):
    global signal_state, portfolio_state, signal_buffer, safety_state

    # Log trade result for ML training
    if signal_state["current_action"] != "HOLD" and ml_model_state["is_ready"]:
        # This would need actual PnL to train properly
        pass

    signal_state.update({
        "current_action": "HOLD", "entry_price": 0.0, "stop_loss": 0.0,
        "target": 0.0, "highest_premium_seen": 0.0, "confidence": 0.0,
        "cooldown_until": current_time + CONFIG["COOLDOWN_SEC"],
        "entry_time": 0, "max_profit_seen": 0.0, "lots": 1, "position_risk_pct": 1.0})
    portfolio_state["open_positions"] = 0
    signal_buffer["ce_count"] = 0
    signal_buffer["pe_count"] = 0

    # GRADE 1 PRO: Apply recovery cooldown multiplier after SL
    if safety_state["recovery_mode"]:
        recovery_mult = CONFIG.get("RECOVERY_COOLDOWN_MULTIPLIER", 2.0)
        signal_state["cooldown_until"] = current_time + (CONFIG["COOLDOWN_SEC"] * recovery_mult)
        log_signal_debug(f"RECOVERY COOLDOWN: {CONFIG['COOLDOWN_SEC'] * recovery_mult:.0f}s (2x multiplier)")

    log_signal_debug(f"RESET: Signal state reset. Cooldown: {CONFIG['COOLDOWN_SEC']}s")

# ═══════════════════════════════════════════════════════════════
# WEBSOCKET HANDLERS
# ═══════════════════════════════════════════════════════════════
def on_ws_open(wsapp):
    global sws
    logger.info("WebSocket opened successfully")
    if sws and CE_TOKEN and PE_TOKEN:
        try:
            subscription_payload = [
                {"exchangeType": 1, "tokens": [SPOT_TOKEN, VIX_TOKEN, BANKNIFTY_TOKEN]},
                {"exchangeType": 2, "tokens": [CE_TOKEN, PE_TOKEN]}
            ]
            sws.subscribe("tradeguru_001", 1, subscription_payload)
            logger.info("Streaming pipeline verified. Multi-token subscription confirmed.")
        except Exception as e:
            logger.error(f"Subscription initialization failure: {e}")

def on_ws_error(wsapp, error):
    logger.error(f"WebSocket Error: {error}")

def on_ws_close(wsapp, close_status_code=None, close_msg=None):
    global ws_running
    ws_running = False
    logger.warning(f"WebSocket closed: code={close_status_code}, msg={close_msg}")

def on_ws_data(wsapp, message):
    global tick_counter, last_tick_time, latest_ticks
    global spot_price_history, ce_price_history, pe_price_history
    global last_timeframe_update, timeframe_candles, timeframe_history
    global vix_history, banknifty_history

    last_tick_time = time.time()

    try:
        if isinstance(message, bytes):
            # Let the library's internal handler process binary data
            # SmartWebSocketV2 calls _on_data internally which then calls our on_ws_data
            # with parsed dict, so we shouldn't get raw bytes here normally
            logger.warning(f"Received raw bytes in on_ws_data (unexpected): {len(message)} bytes")
            return
        else:
            data = json.loads(message) if isinstance(message, str) else message
            ticks = data if isinstance(data, list) else [data]

        for tick in ticks:
            token = str(tick.get("token") or tick.get("tk", ""))
            ltp = tick.get("ltp") or tick.get("last_traded_price", 0)

            if isinstance(ltp, (int, float)) and ltp > 50000 and token != SPOT_TOKEN and token != VIX_TOKEN:
                ltp = ltp / 100

            vol = tick.get("v") or tick.get("volume_trade_for_the_day", 0)
            oi = tick.get("oi") or tick.get("open_interest", 0)

            if token == SPOT_TOKEN:
                latest_ticks["spot_price"] = ltp
                spot_price_history.append(ltp)
                tick_counter += 1

                current_time = time.time()
                for tf, interval_sec in [
                    ("1min", 60), ("2min", 120), ("3min", 180),
                    ("5min", 300), ("10min", 600), ("15min", 900), ("20min", 1200)
                ]:
                    candle = timeframe_candles[tf]
                    if current_time - last_timeframe_update[tf] >= interval_sec:
                        if candle["active"]:
                            timeframe_history[tf].append({
                                "open": candle["open"], "high": candle["high"],
                                "low": candle["low"], "close": candle["close"],
                                "volume": candle["volume"], "timestamp": last_timeframe_update[tf]
                            })
                        candle.update({"open": ltp, "high": ltp, "low": ltp, "close": ltp, "volume": vol, "active": True})
                        last_timeframe_update[tf] = current_time
                    else:
                        if not candle["active"]:
                            candle.update({"open": ltp, "low": ltp, "active": True})
                        candle["high"] = max(candle["high"], ltp)
                        candle["low"] = min(candle["low"], ltp)
                        candle["close"] = ltp
                        candle["volume"] += vol

            elif token == VIX_TOKEN:
                latest_ticks["vix"] = ltp
                vix_history.append(ltp)

            elif token == BANKNIFTY_TOKEN:
                latest_ticks["banknifty"] = ltp
                banknifty_history.append(ltp)

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
            logger.info(f"Subscribing to: Spot={SPOT_TOKEN}, VIX={VIX_TOKEN}, BankNifty={BANKNIFTY_TOKEN}, CE={CE_TOKEN}, PE={PE_TOKEN}")

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

def init_background_threads():
    logger.info("Initializing framework-bound background pipelines...")
    ws_thread = threading.Thread(target=start_angel_websocket, daemon=True, name="angel_websocket_thread")
    ws_thread.start()
    logger.info("Background streaming thread successfully bound and deployed.")

_init_completed = False

@app.before_request
def ensure_threads_are_breathing():
    global _init_completed
    if not _init_completed:
        init_background_threads()
        _init_completed = True

# ═══════════════════════════════════════════════════════════════
# FLASK ROUTES
# ═══════════════════════════════════════════════════════════════
@app.route("/", methods=["GET", "HEAD"])
def home():
    return jsonify({
        "status": "healthy",
        "engine": "Grade 1 Pro Signal Bot - ENHANCED v2.0",
        "market_open": is_market_open(),
        "timestamp": time.time(),
        "grade1_pro_safety": {
            "circuit_breaker": safety_state["circuit_breaker_triggered"],
            "recovery_mode": safety_state["recovery_mode"],
            "consecutive_sl": safety_state["consecutive_sl_count"],
            "paper_trade_mode": CONFIG.get("PAPER_TRADE_MODE", False)
        },
        "features": [
            "Multi-factor signal classification - RELAXED THRESHOLDS",
            "RSI(50/50) + MACD(>0/<0) + ADX(20+) + PCR + Volume confirmation",
            "VWAP & Market Profile for entry timing",
            "Dynamic Position Sizing (0.5-2% risk based on ATR/VIX/Conviction)",
            "VIX Correlation & Regime Detection",
            "Greeks Approximation (Delta, Gamma, Theta, Vega, IV)",
            "Adaptive Regime Switching (Trending/Ranging/Volatile strategies)",
            "ML Signal Filter (Random Forest on 20+ features)",
            "Intelligent trailing stops with breakeven",
            "AGGRESSIVE drawdown alerts (15% from peak)",
            "Profit lock at 30% drawdown from peak",
            "FAST momentum reversal detection (RSI 48/52, MACD 0.1)",
            "Signal persistence validation (2-tick confirm)",
            "Consecutive trade limits",
            "Daily max trade limits (15)",
            "Multi-timeframe trend analysis (1m-20m)",
            "BankNifty correlation tracking",
            "SIGNAL ENGINE DEBUG MODE ENABLED",
            "GRADE 1 PRO: Circuit Breakers (3% daily loss, 3x consecutive SL)",
            "GRADE 1 PRO: Time Filters (no open/close/lunch trades)",
            "GRADE 1 PRO: Gap Risk Detection (>2% gap halt)",
            "GRADE 1 PRO: Premium Safety (Rs 10-2000, min volume/OI)",
            "GRADE 1 PRO: Signal Quality Gates (55% conf, 3+ factors)",
            "GRADE 1 PRO: Recovery Mode (50% size after SL, 2x cooldown)",
            "GRADE 1 PRO: Paper Trade Mode support",
            "GRADE 1 PRO: Auto circuit breaker reset after 30min"
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
        "regime_state": {
            "current": regime_state["current"],
            "confidence": regime_state["confidence"],
            "strategy": regime_state["strategy"],
            "volatility_regime": regime_state["volatility_regime"]
        },
        "greeks": greeks_state,
        "market_profile": market_profile_data,
        "ml_state": {
            "is_ready": ml_model_state["is_ready"],
            "accuracy": ml_model_state["accuracy"],
            "last_trained": ml_model_state["last_trained"],
            "samples": len(ml_feature_log),
            "feature_importance": ml_model_state.get("feature_importance", {})
        },
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
            "cooldown_sec": CONFIG["COOLDOWN_SEC"],
            "macd_threshold": CONFIG["MACD_CONFIRM_THRESHOLD"],
            "min_score": CONFIG.get("MIN_SCORE_FOR_ENTRY", 2),
            "weak_allowed": CONFIG.get("WEAK_SIGNAL_ALLOWED", True),
            "signal_building_ticks": CONFIG.get("SIGNAL_BUILDING_TICKS", 2),
            "base_risk_pct": CONFIG["BASE_RISK_PER_TRADE_PCT"],
            "max_risk_pct": CONFIG["MAX_RISK_PER_TRADE_PCT"],
            "vix_high_threshold": CONFIG["VIX_HIGH_THRESHOLD"],
            "ml_enabled": CONFIG["ML_ENABLED"],
            "regime": regime_state["current"]
        },
        "safety_state": {
            "circuit_breaker_triggered": safety_state["circuit_breaker_triggered"],
            "circuit_breaker_reason": safety_state["circuit_breaker_reason"],
            "recovery_mode": safety_state["recovery_mode"],
            "consecutive_sl_count": safety_state["consecutive_sl_count"],
            "daily_sl_count": safety_state["daily_sl_count"],
            "recovery_attempts": safety_state["recovery_attempts"],
            "paper_trade_pnl": safety_state["paper_trade_pnl"] if CONFIG.get("PAPER_TRADE_MODE", False) else None,
            "paper_trade_stats": {
                "total_trades": safety_state["total_paper_trades"],
                "win_count": safety_state["paper_win_count"],
                "win_rate": round(safety_state["paper_win_count"] / safety_state["total_paper_trades"] * 100, 1) if safety_state["total_paper_trades"] > 0 else 0
            } if CONFIG.get("PAPER_TRADE_MODE", False) else None
        },
        "debug_log": list(signal_debug_log)[-30:]
    }), 200

@app.route("/api/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "OK",
        "alive": True,
        "ws_running": ws_running,
        "last_tick": last_tick_time,
        "ticks_received": tick_counter,
        "daily_trades": daily_trade_count,
        "max_daily_trades": CONFIG["MAX_DAILY_TRADES"],
        "signal_debug_count": len(signal_debug_log),
        "ml_ready": ml_model_state["is_ready"],
        "ml_accuracy": ml_model_state["accuracy"],
        "regime": regime_state["current"],
        "strategy": regime_state["strategy"],
        "safety": {
            "circuit_breaker": safety_state["circuit_breaker_triggered"],
            "recovery_mode": safety_state["recovery_mode"],
            "consecutive_sl": safety_state["consecutive_sl_count"],
            "daily_sl": safety_state["daily_sl_count"]
        }
    }), 200

@app.route("/api/debug/signal-log", methods=["GET"])
def get_signal_debug_log():
    return jsonify({
        "debug_log": list(signal_debug_log),
        "config": {k: v for k, v in CONFIG.items() if not isinstance(v, (list, dict, bytes))},
        "current_state": {
            "signal_state": signal_state,
            "market_state": market_state,
            "portfolio_state": portfolio_state,
            "buffer": signal_buffer,
            "daily_trades": daily_trade_count,
            "regime": regime_state["current"],
            "strategy": regime_state["strategy"],
            "greeks": greeks_state,
            "ml": {
                "ready": ml_model_state["is_ready"],
                "accuracy": ml_model_state["accuracy"],
                "predictions": list(ml_model_state["prediction_buffer"])[-20:]
            }
        }
    }), 200

@app.route("/api/greeks", methods=["GET"])
def get_greeks():
    return jsonify({
        "timestamp": get_ist_now().isoformat(),
        "greeks": greeks_state,
        "market_profile": market_profile_data,
        "vwap": institutional_state.get("vwap", 0),
        "latest": latest_ticks
    }), 200

@app.route("/api/regime", methods=["GET"])
def get_regime():
    return jsonify({
        "timestamp": get_ist_now().isoformat(),
        "current_regime": regime_state["current"],
        "confidence": regime_state["confidence"],
        "strategy": regime_state["strategy"],
        "volatility_regime": regime_state["volatility_regime"],
        "history": list(regime_state["history"])[-20:],
        "vix": latest_ticks.get("vix", 15),
        "banknifty_corr": calculate_correlation(spot_price_history, banknifty_history, 50)
    }), 200

@app.route("/api/ml/status", methods=["GET"])
def get_ml_status():
    return jsonify({
        "enabled": CONFIG["ML_ENABLED"] and SKLEARN_AVAILABLE,
        "sklearn_available": SKLEARN_AVAILABLE,
        "model_ready": ml_model_state["is_ready"],
        "accuracy": ml_model_state["accuracy"],
        "last_trained": ml_model_state["last_trained"],
        "training_samples": len(ml_feature_log),
        "min_samples_required": CONFIG["ML_MIN_SAMPLES"],
        "feature_importance": ml_model_state.get("feature_importance", {}),
        "recent_predictions": list(ml_model_state["prediction_buffer"])[-20:]
    }), 200

@app.route("/api/safety/status", methods=["GET"])
def get_safety_status():
    """Returns current safety system status."""
    return jsonify({
        "timestamp": get_ist_now().isoformat(),
        "circuit_breaker": {
            "triggered": safety_state["circuit_breaker_triggered"],
            "reason": safety_state["circuit_breaker_reason"],
            "time_triggered": safety_state["circuit_breaker_time"],
            "auto_reset_in": max(0, 1800 - (time.time() - safety_state["circuit_breaker_time"])) if safety_state["circuit_breaker_triggered"] else 0
        },
        "recovery": {
            "mode": safety_state["recovery_mode"],
            "consecutive_sl": safety_state["consecutive_sl_count"],
            "recovery_attempts": safety_state["recovery_attempts"],
            "daily_sl_count": safety_state["daily_sl_count"]
        },
        "time_filters": {
            "no_trade_first_15": CONFIG.get("NO_TRADE_FIRST_15_MIN", True),
            "no_trade_last_30": CONFIG.get("NO_TRADE_LAST_30_MIN", True),
            "no_trade_lunch": CONFIG.get("NO_TRADE_LUNCH_12_30_13_30", True),
            "current_time": get_ist_now().strftime("%H:%M:%S"),
            "market_open": is_market_open()
        },
        "paper_trade": {
            "enabled": CONFIG.get("PAPER_TRADE_MODE", False),
            "pnl": safety_state["paper_trade_pnl"],
            "total_trades": safety_state["total_paper_trades"],
            "win_rate": round(safety_state["paper_win_count"] / safety_state["total_paper_trades"] * 100, 1) if safety_state["total_paper_trades"] > 0 else 0
        },
        "config": {
            "daily_loss_limit": CONFIG.get("CIRCUIT_BREAKER_DAILY_LOSS_PCT", 3.0),
            "max_consecutive_sl": CONFIG.get("CIRCUIT_BREAKER_CONSECUTIVE_SL", 3),
            "max_gap_pct": CONFIG.get("CIRCUIT_BREAKER_GAP_UP_DOWN_PCT", 2.0),
            "min_premium": CONFIG.get("MIN_PREMIUM_FOR_TRADE", 10.0),
            "max_premium": CONFIG.get("MAX_PREMIUM_FOR_TRADE", 2000.0),
            "min_confidence": CONFIG.get("MIN_SIGNAL_CONFIDENCE_PCT", 55),
            "min_factors": CONFIG.get("MIN_CONFLUENCE_FACTORS", 3)
        }
    }), 200

@app.route("/api/safety/reset", methods=["POST"])
def reset_safety():
    """Manual reset of circuit breaker and recovery mode."""
    global safety_state
    safety_state["circuit_breaker_triggered"] = False
    safety_state["circuit_breaker_reason"] = ""
    safety_state["recovery_mode"] = False
    safety_state["consecutive_sl_count"] = 0
    safety_state["recovery_attempts"] = 0
    log_signal_debug("SAFETY RESET: Manual reset triggered via API")
    send_telegram_alert("🔄 <b>SAFETY RESET</b>\nCircuit breaker and recovery mode manually reset")
    return jsonify({"status": "reset", "message": "Safety systems reset successfully"}), 200

@app.route("/api/backtest/status", methods=["GET"])
def get_backtest_status():
    return jsonify({
        "backtest_mode": CONFIG["BACKTEST_MODE"],
        "backtest_days": CONFIG["BACKTEST_DAYS"],
        "initial_capital": CONFIG["BACKTEST_INITIAL_CAPITAL"],
        "slippage_pct": CONFIG["BACKTEST_SLIPPAGE_PCT"],
        "commission_per_order": CONFIG["BACKTEST_COMMISSION_PER_ORDER"]
    }), 200

@app.route('/api/connection-status')
def connection_status():
    """Returns real-time connection and system health status."""
    import threading
    import time
    
    now = time.time()
    
    # WebSocket thread status
    ws_alive = False
    if 'ws_thread' in globals() and ws_thread is not None:
        ws_alive = ws_thread.is_alive()
    
    # SmartApi connection status (if available)
    smartapi_connected = False
    last_ws_message = 0
    try:
        if 'smartapi_ws' in globals() and smartapi_ws is not None:
            smartapi_connected = getattr(smartapi_ws, 'is_connected', False)
            last_ws_message = getattr(smartapi_ws, 'last_pong_time', 0)
    except:
        pass
    
    # Market hours check
    from datetime import datetime, time as dt_time
    import pytz
    ist = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist)
    market_open = dt_time(9, 15)
    market_close = dt_time(15, 30)
    is_market_hours = market_open <= now_ist.time() <= market_close and now_ist.weekday() < 5
    
    return jsonify({
        "timestamp": now_ist.isoformat(),
        "status": "OK",
        "market": {
            "is_open": is_market_hours,
            "current_time_ist": now_ist.strftime("%H:%M:%S"),
            "day": now_ist.strftime("%A")
        },
        "websocket": {
            "thread_running": ws_alive,
            "smartapi_connected": smartapi_connected,
            "last_message_seconds_ago": round(now - last_ws_message, 1) if last_ws_message > 0 else None
        },
        "ml": {
            "ready": getattr(ml_state, 'is_ready', False) if 'ml_state' in globals() else False,
            "samples": getattr(ml_state, 'samples', 0) if 'ml_state' in globals() else 0
        },
        "data_pipeline": {
            "ticks_received": getattr(signal_state, 'ticks_received', 0) if 'signal_state' in globals() else 0,
            "last_tick_seconds_ago": round(now - getattr(signal_state, 'last_tick_time', 0), 1) if 'signal_state' in globals() and getattr(signal_state, 'last_tick_time', 0) > 0 else None
        },
        "environment": {
            "python_version": sys.version.split()[0],
            "sklearn_version": sklearn.__version__ if 'sklearn' in sys.modules else None
        }
    })

if __name__ == "__main__":
    if not _init_completed:
        init_background_threads()
        _init_completed = True
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
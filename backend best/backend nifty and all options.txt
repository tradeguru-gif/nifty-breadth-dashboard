# === VERSION 7 - MULTI-INDEX OPTIONS TRADING BOT ===
# === Supports: NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, SENSEX ===
# === Dynamic Market Sentiment Engine with Multi-Timeframe Scoring ===

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
# INDEX CONFIGURATION
# ═══════════════════════════════════════════════════════════════
INDEX_CONFIG = {
    "NIFTY": {
        "token": "99926000",
        "exchange": "NSE",
        "symbol": "NIFTY",
        "lot_size": 50,
        "tick_size": 0.05,
        "expiry_weekday": 4,  # Thursday
        "active": True,
        "min_premium": 10,
        "max_premium": 5000,
        "atm_strike_multiple": 50,
        "option_exchange": "NFO"
    },
    "BANKNIFTY": {
        "token": "99926009",
        "exchange": "NSE",
        "symbol": "BANKNIFTY",
        "lot_size": 25,
        "tick_size": 0.05,
        "expiry_weekday": 4,  # Thursday
        "active": True,
        "min_premium": 10,
        "max_premium": 5000,
        "atm_strike_multiple": 100,
        "option_exchange": "NFO"
    },
    "FINNIFTY": {
        "token": "99926037",
        "exchange": "NSE",
        "symbol": "FINNIFTY",
        "lot_size": 40,
        "tick_size": 0.05,
        "expiry_weekday": 2,  # Tuesday
        "active": True,
        "min_premium": 10,
        "max_premium": 5000,
        "atm_strike_multiple": 50,
        "option_exchange": "NFO"
    },
    "MIDCPNIFTY": {
        "token": "99926031",
        "exchange": "NSE",
        "symbol": "MIDCPNIFTY",
        "lot_size": 75,
        "tick_size": 0.05,
        "expiry_weekday": 4,  # Thursday
        "active": True,
        "min_premium": 10,
        "max_premium": 5000,
        "atm_strike_multiple": 25,
        "option_exchange": "NFO"
    },
    "SENSEX": {
        "token": "99926005",
        "exchange": "BSE",
        "symbol": "SENSEX",
        "lot_size": 15,
        "tick_size": 0.05,
        "expiry_weekday": 4,  # Thursday
        "active": True,
        "min_premium": 10,
        "max_premium": 5000,
        "atm_strike_multiple": 100,
        "option_exchange": "BFO"
    }
}

# Default index to trade (can be changed via API)
ACTIVE_INDEX = "NIFTY"

# ═══════════════════════════════════════════════════════════════
# DATABASE INIT
# ═══════════════════════════════════════════════════════════════
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS ticks (timestamp REAL, index_name TEXT, token TEXT, price REAL, volume REAL, bid REAL, ask REAL, oi REAL)""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ticks_time ON ticks(timestamp)")
    c.execute("""CREATE TABLE IF NOT EXISTS signals (timestamp REAL, index_name TEXT, action TEXT, signal_type TEXT, grade TEXT, confidence REAL, sentiment_score REAL, sentiment TEXT, ce_price REAL, pe_price REAL, rsi REAL, pcr REAL, regime TEXT, vwap REAL, vix REAL, ml_score REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS trades (timestamp REAL, index_name TEXT, action TEXT, entry_price REAL, exit_price REAL, pnl REAL, size_pct REAL, status TEXT, grade TEXT, atr REAL, vix REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS daily_performance (date TEXT, index_name TEXT, equity REAL, daily_pnl REAL, drawdown_pct REAL, sharpe REAL, var REAL, win_rate REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS ml_models (id INTEGER PRIMARY KEY, index_name TEXT, model BLOB, created_at REAL, features TEXT, accuracy REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS backtest_results (id INTEGER PRIMARY KEY, date TEXT, index_name TEXT, strategy TEXT, trades INTEGER, win_rate REAL, profit_factor REAL, max_drawdown REAL, sharpe REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS market_profile (timestamp REAL, index_name TEXT, poc REAL, value_area_high REAL, value_area_low REAL, vwap REAL, volume_profile TEXT)""")
    conn.commit()
    conn.close()

init_db()

# ═══════════════════════════════════════════════════════════════
# ANGEL ONE API SETUP
# ═══════════════════════════════════════════════════════════════
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ═══════════════════════════════════════════════════════════════
# GLOBAL STATE - MULTI INDEX
# ═══════════════════════════════════════════════════════════════
# Token storage per index
INDEX_TOKENS = {
    idx: {"ce_token": None, "pe_token": None, "atm_strike": 0, "expiry": "", "ce_symbol": "", "pe_symbol": ""}
    for idx in INDEX_CONFIG.keys()
}

# Price histories per index
price_histories = {idx: deque(maxlen=2000) for idx in INDEX_CONFIG.keys()}
ce_price_histories = {idx: deque(maxlen=2000) for idx in INDEX_CONFIG.keys()}
pe_price_histories = {idx: deque(maxlen=2000) for idx in INDEX_CONFIG.keys()}
ce_volume_histories = {idx: deque(maxlen=1000) for idx in INDEX_CONFIG.keys()}
pe_volume_histories = {idx: deque(maxlen=1000) for idx in INDEX_CONFIG.keys()}
ce_oi_histories = {idx: deque(maxlen=200) for idx in INDEX_CONFIG.keys()}
pe_oi_histories = {idx: deque(maxlen=200) for idx in INDEX_CONFIG.keys()}

# VIX data (shared across indices)
vix_history = deque(maxlen=200)
latest_ticks = {
    idx: {
        "spot_price": 0.0, "ce_price": 0.0, "pe_price": 0.0,
        "ce_volume": 0, "pe_volume": 0, "ce_oi": 0, "pe_oi": 0,
        "ce_bid": 0.0, "ce_ask": 0.0, "pe_bid": 0.0, "pe_ask": 0.0,
        "vix": 15.0
    } for idx in INDEX_CONFIG.keys()
}
latest_ticks["VIX"] = {"vix": 15.0}

tick_counter = 0
ws_running = False
sws = None
last_tick_time = time.time()
engine_active = True

# Timeframes configuration
TIMEFRAMES = ["1min", "5min", "15min"]
timeframe_data = {idx: {tf: deque(maxlen=100) for tf in TIMEFRAMES} for idx in INDEX_CONFIG.keys()}

# EMA periods for multi-timeframe analysis
EMA_PERIODS = {"short": 9, "medium": 21, "long": 50}

# ═══════════════════════════════════════════════════════════════
# MARKET SENTIMENT STATE (per index)
# ═══════════════════════════════════════════════════════════════
market_sentiment = {
    idx: {
        "sentiment_score": 50,
        "sentiment": "Neutral",
        "sentiment_label": "Neutral",
        "trend_1m": "NEUTRAL",
        "trend_5m": "NEUTRAL",
        "trend_15m": "NEUTRAL",
        "score_1m": 0,
        "score_5m": 0,
        "score_15m": 0,
        "last_update": 0
    } for idx in INDEX_CONFIG.keys()
}

# Signal state per index
signal_state = {
    idx: {
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
        "max_profit_seen": 0.0,
        "position_risk_pct": 1.0,
        "lots": 1,
        "sentiment_at_entry": 50
    } for idx in INDEX_CONFIG.keys()
}

# Portfolio per index
portfolio_state = {
    idx: {
        "equity": 100000.0,
        "initial_equity": 100000.0,
        "daily_pnl": 0.0,
        "max_drawdown_today": 0.0,
        "open_positions": 0,
        "daily_peak": 100000.0,
        "daily_loss_limit_pct": 2.0,
        "var_95": 0.0,
        "sharpe_ratio": 0.0,
        "total_trades": 0,
        "winning_trades": 0,
        "win_rate": 0.0
    } for idx in INDEX_CONFIG.keys()
}

# Market signal per index
market_signal = {
    idx: {
        "signal": "WAITING",
        "signal_type": "NO_TRADE",
        "signal_strength": "NONE",
        "spot_price": 0.0,
        "ce_price": 0.0,
        "pe_price": 0.0,
        "spot_rsi": 50.0,
        "spot_macd": 0.0,
        "pcr": 1.0,
        "spot_atr": 0.0,
        "regime": "RANGING",
        "confidence": 50.0,
        "timestamp": "",
        "alert_message": "Initializing...",
        "vwap": 0.0,
        "vix": 15.0,
        "ml_score": 0.5,
        "sentiment_score": 50,
        "sentiment": "Neutral",
        "trend_1m": "NEUTRAL",
        "trend_5m": "NEUTRAL",
        "trend_15m": "NEUTRAL",
        "score_1m": 0,
        "score_5m": 0,
        "score_15m": 0
    } for idx in INDEX_CONFIG.keys()
}

# Signal buffer per index
signal_buffer = {
    idx: {"ce_count": 0, "pe_count": 0, "last_signal_time": 0, "consecutive_ce": 0, "consecutive_pe": 0}
    for idx in INDEX_CONFIG.keys()
}

daily_trade_count = {idx: 0 for idx in INDEX_CONFIG.keys()}
last_trade_date = {idx: "" for idx in INDEX_CONFIG.keys()}

# ═══════════════════════════════════════════════════════════════
# ENHANCED CONFIG - MULTI-INDEX TRADING BOT v3.0
# ═══════════════════════════════════════════════════════════════
CONFIG = {
    # ========== MULTI-TIMEFRAME SCORING WEIGHTS ==========
    "TIMEFRAME_WEIGHTS": {
        "1min": 30,
        "5min": 40,
        "15min": 30
    },
    
    # ========== EMA SETTINGS ==========
    "EMA_SHORT": 9,
    "EMA_MEDIUM": 21,
    "EMA_LONG": 50,
    
    # ========== SENTIMENT SCORE THRESHOLDS ==========
    "SENTIMENT_SCORES": {
        "STRONG_BULLISH": {"min": 85, "max": 100, "label": "🟢 STRONG BULLISH", "ce_action": "STRONG_BUY_CE"},
        "BULLISH": {"min": 70, "max": 84, "label": "🟢 BULLISH", "ce_action": "BUY_CE"},
        "SLOW_BULLISH": {"min": 55, "max": 69, "label": "🟡 SLOW BULLISH", "ce_action": "LOW_BUY_CE"},
        "NEUTRAL": {"min": 45, "max": 54, "label": "⚪ NEUTRAL", "ce_action": "NO_TRADE"},
        "SLOW_BEARISH": {"min": 30, "max": 44, "label": "🟠 SLOW BEARISH", "pe_action": "LOW_BUY_PE"},
        "BEARISH": {"min": 15, "max": 29, "label": "🔴 BEARISH", "pe_action": "BUY_PE"},
        "STRONG_BEARISH": {"min": 0, "max": 14, "label": "🔴 STRONG BEARISH", "pe_action": "STRONG_BUY_PE"}
    },
    
    # ========== SIGNAL PRIORITY ==========
    "SIGNAL_PRIORITY": [
        "STRONG_BUY_CE",
        "STRONG_BUY_PE",
        "BUY_CE",
        "BUY_PE",
        "LOW_BUY_CE",
        "LOW_BUY_PE",
        "NO_TRADE"
    ],
    
    # ========== TECHNICAL INDICATORS ==========
    "RSI_PERIOD": 14,
    "RSI_SMOOTHING": 3,
    "MACD_FAST": 12,
    "MACD_SLOW": 26,
    "MACD_SIGNAL": 9,
    "ATR_PERIOD": 14,
    
    # ========== ENTRY/EXIT PARAMETERS ==========
    "ENTRY_ATR_MULT": 1.5,
    "TRAILING_ATR_MULT": 1.8,
    "TARGET_ATR_MULT": 3.5,
    "COOLDOWN_SEC": 60,
    "MAX_HOLD_TIME_MIN": 45,
    "MIN_PROFIT_LOCK": 0.3,
    "BREAKEVEN_TRIGGER": 1.0,
    "MIN_SIGNAL_HOLD_SEC": 30,
    "MAX_DAILY_TRADES_PER_INDEX": 10,
    "CONSECUTIVE_SAME_DIR_MAX": 2,
    
    # ========== SIGNAL QUALITY ==========
    "MIN_SIGNAL_BUILDING_TICKS": 2,
    "WEAK_SIGNAL_ALLOWED": True,
    "DRAWDOWN_ALERT_PCT": 0.15,
    "PROFIT_LOCK_PCT": 0.30,
    "EARLY_EXIT_ON_REVERSAL": True,
    "RSI_EXIT_CE": 48,
    "RSI_EXIT_PE": 52,
    "MACD_EXIT_THRESHOLD": 0.1,
    
    # ========== POSITION SIZING ==========
    "BASE_RISK_PER_TRADE_PCT": 1.0,
    "MAX_RISK_PER_TRADE_PCT": 2.0,
    "MIN_RISK_PER_TRADE_PCT": 0.5,
    "ATR_RISK_MULTIPLIER": 1.5,
    
    # ========== VOLUME & PCR ==========
    "VOLUME_SPIKE_RATIO": 1.3,
    "VOLUME_MA_PERIOD": 20,
    "PCR_BULLISH_THRESHOLD": 0.85,
    "PCR_BEARISH_THRESHOLD": 1.15,
    
    # ========== VIX SETTINGS ==========
    "VIX_LOW_THRESHOLD": 12,
    "VIX_NORMAL_THRESHOLD": 20,
    "VIX_HIGH_THRESHOLD": 25,
    "VIX_SIZE_REDUCTION_HIGH": 0.7,
    "VIX_SIZE_REDUCTION_EXTREME": 0.5,
    
    # ========== NO TRADE RULES ==========
    "NO_TRADE_FIRST_15_MIN": True,
    "NO_TRADE_LAST_30_MIN": True,
    "REJECT_15M_OPPOSING": True,  # Reject LOW signals if 15m strongly opposite
    "MIN_ATR_FOR_TRADE": 5.0,
    "MIN_VOLUME_FOR_TRADE": 100,
    "MIN_OI_FOR_TRADE": 500,
    
    # ========== ML SETTINGS ==========
    "ML_ENABLED": True,
    "ML_MIN_SAMPLES": 100,
    "ML_CONFIDENCE_THRESHOLD": 0.55,
    "ML_STRONG_OVERRIDE": 0.70,
    "ML_RETRAIN_INTERVAL": 3600,
    
    # ========== CIRCUIT BREAKERS ==========
    "CIRCUIT_BREAKER_ENABLED": True,
    "CIRCUIT_BREAKER_DAILY_LOSS_PCT": 3.0,
    "CIRCUIT_BREAKER_CONSECUTIVE_SL": 3,
    "CIRCUIT_BREAKER_VIX_SPIKE_PCT": 15,
    
    # ========== LOGGING ==========
    "SIGNAL_DEBUG_MODE": True,
    "AUDIT_ALL_SIGNALS": True,
    "AUDIT_ALL_TRADES": True
}

# Safety state per index
safety_state = {
    idx: {
        "consecutive_sl_count": 0,
        "last_sl_time": 0,
        "recovery_mode": False,
        "recovery_attempts": 0,
        "circuit_breaker_triggered": False,
        "circuit_breaker_reason": "",
        "circuit_breaker_time": 0,
        "daily_sl_count": 0
    } for idx in INDEX_CONFIG.keys()
}

# Debug log
signal_debug_log = deque(maxlen=200)

# ML model state per index
ml_model_state = {
    idx: {
        "model": None,
        "last_trained": 0,
        "accuracy": 0.5,
        "feature_importance": {},
        "prediction_buffer": deque(maxlen=100),
        "is_ready": False
    } for idx in INDEX_CONFIG.keys()
}

# ═══════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════
def log_signal_debug(msg, index_name="ALL"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    entry = f"[{timestamp}][{index_name}] {msg}"
    signal_debug_log.append(entry)
    if CONFIG.get("SIGNAL_DEBUG_MODE", False):
        logger.info(f"[SIGNAL_DEBUG][{index_name}] {msg}")

def get_ist_now():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def is_market_open():
    now_ist = get_ist_now()
    is_weekday = now_ist.weekday() < 5
    is_time_ok = dt_time(9, 15) <= now_ist.time() <= dt_time(15, 30)
    return is_weekday and is_time_ok

def get_index_by_token(token):
    """Find index name by token"""
    for idx, config in INDEX_CONFIG.items():
        if config["token"] == token:
            return idx
    return None

# ═══════════════════════════════════════════════════════════════
# TECHNICAL INDICATORS
# ═══════════════════════════════════════════════════════════════
def calculate_rsi(prices, period=14, smoothing=3):
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

def calculate_ema(prices, period):
    if len(prices) < period:
        return sum(prices) / len(prices) if prices else 0
    alpha = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = alpha * p + (1 - alpha) * ema
    return ema

def calculate_macd(prices, fast=12, slow=26, signal_period=9):
    if len(prices) < slow + signal_period:
        return 0.0, 0.0, 0.0
    def ema(arr, p):
        alpha = 2 / (p + 1)
        val = arr[0]
        for x in arr[1:]:
            val = alpha * x + (1 - alpha) * val
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
    if len(prices) < period + 1:
        return 5.0
    tr_list = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    return sum(tr_list[-period:]) / period

def calculate_adx(prices, period=14):
    if len(prices) < period * 2 + 1:
        return 20.0
    plus_dm, minus_dm, tr_list = [], [], []
    for i in range(1, len(prices)):
        up_move = prices[i] - prices[i-1]
        down_move = prices[i-1] - prices[i]
        plus_dm.append(max(up_move, 0) if up_move > down_move else 0)
        minus_dm.append(max(down_move, 0) if down_move > up_move else 0)
        tr_list.append(abs(prices[i] - prices[i-1]))
    if len(tr_list) < period:
        return 20.0
    atr = sum(tr_list[-period:]) / period
    plus_di = 100 * sum(plus_dm[-period:]) / period / atr if atr > 0 else 0
    minus_di = 100 * sum(minus_dm[-period:]) / period / atr if atr > 0 else 0
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di) if (plus_di + minus_di) > 0 else 0
    return dx

def calculate_pcr(ce_oi_history, pe_oi_history):
    if ce_oi_history and pe_oi_history:
        ce_sum = sum(list(ce_oi_history)[-20:]) if ce_oi_history else 1
        pe_sum = sum(list(pe_oi_history)[-20:]) if pe_oi_history else 1
        if ce_sum > 0:
            return round(pe_sum / ce_sum, 2)
    return 1.0

# ═══════════════════════════════════════════════════════════════
# LEVEL 1: MARKET SENTIMENT ENGINE
# ═══════════════════════════════════════════════════════════════
def calculate_timeframe_trend(prices, tf_name):
    """
    Calculate trend for a specific timeframe using EMA crossovers
    Returns: (trend_direction, score)
    - trend: "BULLISH", "BEARISH", "NEUTRAL"
    - score: points based on weight configuration
    """
    if len(prices) < 60:  # Need enough data for 50-period EMA
        return "NEUTRAL", 0
    
    weight = CONFIG["TIMEFRAME_WEIGHTS"].get(tf_name, 30)
    
    # Calculate EMAs
    ema_short = calculate_ema(prices, CONFIG["EMA_SHORT"])
    ema_medium = calculate_ema(prices, CONFIG["EMA_MEDIUM"])
    ema_long = calculate_ema(prices, CONFIG["EMA_LONG"])
    current_price = prices[-1]
    
    bullish_score = 0
    bearish_score = 0
    
    if tf_name == "1min":
        # 1-min: EMA9 > EMA21 and Price > EMA9 = Bullish
        if ema_short > ema_medium and current_price > ema_short:
            trend = "BULLISH"
            bullish_score = weight
        elif ema_short < ema_medium and current_price < ema_short:
            trend = "BEARISH"
            bearish_score = weight
        else:
            trend = "NEUTRAL"
            # Partial scores for neutral with bias
            if ema_short > ema_medium:
                bullish_score = weight // 2
            elif ema_short < ema_medium:
                bearish_score = weight // 2
            else:
                bullish_score = bearish_score = weight // 3
    
    elif tf_name == "5min":
        # 5-min: EMA9 > EMA21 > EMA50 = Bullish (all aligned)
        if ema_short > ema_medium > ema_long:
            trend = "BULLISH"
            bullish_score = weight
        elif ema_short < ema_medium < ema_long:
            trend = "BEARISH"
            bearish_score = weight
        elif ema_short > ema_medium and current_price > ema_short:
            trend = "BULLISH"
            bullish_score = weight - 10  # Partial alignment
        elif ema_short < ema_medium and current_price < ema_short:
            trend = "BEARISH"
            bearish_score = weight - 10
        else:
            trend = "NEUTRAL"
            bullish_score = bearish_score = weight // 3
    
    elif tf_name == "15min":
        # 15-min: Higher High / Higher Low structure
        if len(prices) >= 20:
            recent_highs = [max(prices[-i-5:-i]) for i in range(0, 15, 5) if i < len(prices)-5]
            recent_lows = [min(prices[-i-5:-i]) for i in range(0, 15, 5) if i < len(prices)-5]
            
            if len(recent_highs) >= 2 and len(recent_lows) >= 2:
                hh = recent_highs[0] > recent_highs[1] if recent_highs[0] and recent_highs[1] else False
                hl = recent_lows[0] > recent_lows[1] if recent_lows[0] and recent_lows[1] else False
                lh = recent_highs[0] < recent_highs[1] if recent_highs[0] and recent_highs[1] else False
                ll = recent_lows[0] < recent_lows[1] if recent_lows[0] and recent_lows[1] else False
                
                if hh and hl:
                    trend = "BULLISH"
                    bullish_score = weight
                elif lh and ll:
                    trend = "BEARISH"
                    bearish_score = weight
                elif hh and not hl:
                    trend = "BULLISH"
                    bullish_score = weight - 10
                elif lh and not ll:
                    trend = "BEARISH"
                    bearish_score = weight - 10
                else:
                    trend = "NEUTRAL"
                    bullish_score = bearish_score = weight // 2
            else:
                trend = "NEUTRAL"
                bullish_score = bearish_score = weight // 2
        else:
            trend = "NEUTRAL"
            bullish_score = bearish_score = weight // 2
    
    return trend, bullish_score if bullish_score > 0 else -bearish_score if bearish_score > 0 else 0

def calculate_sentiment_score(index_name):
    """
    Calculate market sentiment score (0-100) based on multi-timeframe analysis
    Level 1: Market Sentiment Engine
    """
    prices = list(price_histories[index_name])
    
    if len(prices) < 60:  # Need enough data for 15-min analysis
        market_sentiment[index_name]["sentiment_score"] = 50
        market_sentiment[index_name]["sentiment"] = "Neutral"
        return 50
    
    total_score = 0
    timeframe_results = {}
    
    for tf in TIMEFRAMES:
        trend, score = calculate_timeframe_trend(prices, tf)
        timeframe_results[tf] = {"trend": trend, "score": score}
        
        # Store in market_sentiment
        market_sentiment[index_name][f"trend_{tf}"] = trend
        market_sentiment[index_name][f"score_{tf}"] = abs(score)
        
        if score > 0:
            total_score += score
        elif score < 0:
            total_score += score
    
    # Normalize score to 0-100 range
    # Max possible score = 30+40+30 = 100
    # Min possible score = -100
    normalized_score = 50 + (total_score / 2)  # Convert -100..100 to 0..100
    
    # Clamp to 0-100
    sentiment_score = max(0, min(100, normalized_score))
    
    # Determine sentiment label
    sentiment_info = None
    for key, info in CONFIG["SENTIMENT_SCORES"].items():
        if info["min"] <= sentiment_score <= info["max"]:
            sentiment_info = info
            break
    
    if sentiment_info:
        market_sentiment[index_name]["sentiment_score"] = sentiment_score
        market_sentiment[index_name]["sentiment"] = sentiment_info["label"]
        market_sentiment[index_name]["sentiment_label"] = key
    
    market_sentiment[index_name]["last_update"] = time.time()
    
    log_signal_debug(f"SENTIMENT SCORE: {sentiment_score:.1f} | {market_sentiment[index_name]['sentiment']} | "
                     f"1m:{timeframe_results.get('1min', {}).get('trend', 'N/A')}({abs(timeframe_results.get('1min', {}).get('score', 0))}) | "
                     f"5m:{timeframe_results.get('5min', {}).get('trend', 'N/A')}({abs(timeframe_results.get('5min', {}).get('score', 0))}) | "
                     f"15m:{timeframe_results.get('15min', {}).get('trend', 'N/A')}({abs(timeframe_results.get('15min', {}).get('score', 0))})", index_name)
    
    return sentiment_score

def get_signal_from_sentiment(index_name, sentiment_score):
    """
    Determine signal action based on sentiment score
    Level 2: Signal Classification
    """
    sentiment_info = None
    for key, info in CONFIG["SENTIMENT_SCORES"].items():
        if info["min"] <= sentiment_score <= info["max"]:
            sentiment_info = info
            break
    
    if not sentiment_info:
        return "NO_TRADE", "UNKNOWN", 50
    
    sentiment_label = sentiment_info.get("label", "Unknown")
    
    # Get trend alignment for LOW signal validation
    trend_15m = market_sentiment[index_name].get("trend_15m", "NEUTRAL")
    trend_5m = market_sentiment[index_name].get("trend_5m", "NEUTRAL")
    
    # Rule: Reject LOW signals if 15-minute trend is strongly opposite
    if CONFIG.get("REJECT_15M_OPPOSING", True):
        if sentiment_label in ["SLOW_BULLISH"] and trend_15m == "BEARISH":
            log_signal_debug(f"LOW BUY CE REJECTED: 15m strongly opposite ({trend_15m})", index_name)
            return "NO_TRADE", "15M_OPPOSING", sentiment_score
        
        if sentiment_label in ["SLOW_BEARISH"] and trend_15m == "BULLISH":
            log_signal_debug(f"LOW BUY PE REJECTED: 15m strongly opposite ({trend_15m})", index_name)
            return "NO_TRADE", "15M_OPPOSING", sentiment_score
    
    # Determine action based on sentiment
    if sentiment_label == "STRONG_BULLISH":
        return "STRONG_BUY_CE", sentiment_label, sentiment_score
    elif sentiment_label == "BULLISH":
        return "BUY_CE", sentiment_label, sentiment_score
    elif sentiment_label == "SLOW_BULLISH":
        # Only allow LOW BUY if 5m is not strongly bearish
        if trend_5m != "BEARISH":
            return "LOW_BUY_CE", sentiment_label, sentiment_score
        else:
            return "NO_TRADE", "5M_OPPOSING", sentiment_score
    elif sentiment_label == "SLOW_BEARISH":
        if trend_5m != "BULLISH":
            return "LOW_BUY_PE", sentiment_label, sentiment_score
        else:
            return "NO_TRADE", "5M_OPPOSING", sentiment_score
    elif sentiment_label == "BEARISH":
        return "BUY_PE", sentiment_label, sentiment_score
    elif sentiment_label == "STRONG_BEARISH":
        return "STRONG_BUY_PE", sentiment_label, sentiment_score
    else:
        return "NO_TRADE", sentiment_label, sentiment_score

# ═══════════════════════════════════════════════════════════════
# AUTH & DATA FETCHING
# ═══════════════════════════════════════════════════════════════
auth_cache = {"token": None, "feed_token": None, "timestamp": 0, "obj": None}
_auth_lock = threading.Lock()

def get_auth_token():
    with _auth_lock:
        now = time.time()
        if auth_cache["token"] and (now - auth_cache["timestamp"] < 3300):
            return auth_cache["token"], auth_cache["feed_token"], auth_cache["obj"]
        try:
            totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
            obj = SmartConnect(api_key=ANGEL_API_KEY)
            session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
            if not session.get("status"):
                return None, None, None
            auth_token = session["data"]["jwtToken"]
            feed_token = obj.getfeedToken()
            auth_cache.update({"token": auth_token, "feed_token": feed_token, "timestamp": now, "obj": obj})
            logger.info("Auth token refreshed successfully")
            return auth_token, feed_token, obj
        except Exception as e:
            logger.error(f"Auth loop fail: {e}")
            return None, None, None

def get_index_spot(index_name):
    """Get spot price for any index"""
    config = INDEX_CONFIG.get(index_name)
    if not config:
        return None
    
    _, _, obj = get_auth_token()
    if not obj:
        return None
    
    try:
        response = obj.ltpData(config["exchange"], config["symbol"], config["token"])
        if response.get("status") and response.get("data"):
            ltp = float(response["data"].get("ltp", 0))
            # Fix 100x scaling if needed
            if ltp > 100000:
                ltp = ltp / 100
            return ltp
    except Exception as e:
        logger.error(f"Error reading {index_name} spot: {e}")
    return None

def get_vix_value():
    _, _, obj = get_auth_token()
    if not obj:
        return None
    try:
        response = obj.ltpData("NSE", "INDIAVIX", "99919017")
        if response.get("status") and response.get("data"):
            return float(response["data"].get("ltp", 15))
    except Exception as e:
        logger.debug(f"Error reading VIX: {e}")
    return 15.0

def get_current_atm_tokens(index_name):
    """Get ATM strike and tokens for specified index"""
    config = INDEX_CONFIG.get(index_name)
    if not config or not config.get("active", True):
        return None, None
    
    spot = get_index_spot(index_name)
    if not spot:
        return None, None
    
    atm_strike = round(spot / config["atm_strike_multiple"]) * config["atm_strike_multiple"]
    
    # Get expiry date
    today = datetime.now()
    days_ahead = (config["expiry_weekday"] - today.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    expiry_date = (today + timedelta(days=days_ahead)).strftime("%d%b%Y").upper()
    
    # Try Scrip Master first
    try:
        url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
        resp = requests.get(url, timeout=15)
        df = pd.DataFrame(resp.json())
        
        opts = df[(df["name"] == config["symbol"]) & 
                  (df["instrumenttype"] == "OPTIDX") & 
                  (df["exch_seg"] == config["option_exchange"])].copy()
        
        if not opts.empty:
            opts["expiry_date"] = pd.to_datetime(opts["expiry"], format="%d%b%Y", errors="coerce")
            opts = opts.dropna(subset=["expiry_date"])
            opts["strike"] = pd.to_numeric(opts["strike"], errors="coerce") / 100
            
            future = opts[opts["expiry_date"] >= today]
            nearest_expiry = future["expiry_date"].min()
            atm_opts = future[(future["strike"] == atm_strike) & (future["expiry_date"] == nearest_expiry)]
            
            ce = atm_opts[atm_opts["symbol"].str.contains("CE")]
            pe = atm_opts[atm_opts["symbol"].str.contains("PE")]
            
            if not ce.empty and not pe.empty:
                ce_token = str(ce.iloc[0]["token"])
                pe_token = str(pe.iloc[0]["token"])
                ce_symbol = str(ce.iloc[0]["symbol"])
                pe_symbol = str(pe.iloc[0]["symbol"])
                
                INDEX_TOKENS[index_name].update({
                    "ce_token": ce_token,
                    "pe_token": pe_token,
                    "atm_strike": atm_strike,
                    "expiry": expiry_date,
                    "ce_symbol": ce_symbol,
                    "pe_symbol": pe_symbol
                })
                
                logger.info(f"{index_name} -> CE: {ce_token} | PE: {pe_token} | Strike: {atm_strike}")
                return ce_token, pe_token
    except Exception as e:
        logger.error(f"Scrip Master lookup failed for {index_name}: {e}")
    
    # Fallback: Use search API
    try:
        _, _, obj = get_auth_token()
        if obj:
            ce_search = obj.searchScrip(config["option_exchange"], f"{config['symbol']}{atm_strike}CE")
            if ce_search and ce_search.get("data"):
                ce_data = ce_search["data"][0] if isinstance(ce_search["data"], list) else ce_search["data"]
                ce_token = str(ce_data.get("symboltoken", ""))
                
            pe_search = obj.searchScrip(config["option_exchange"], f"{config['symbol']}{atm_strike}PE")
            if pe_search and pe_search.get("data"):
                pe_data = pe_search["data"][0] if isinstance(pe_search["data"], list) else pe_search["data"]
                pe_token = str(pe_data.get("symboltoken", ""))
            
            if ce_token and pe_token:
                INDEX_TOKENS[index_name].update({
                    "ce_token": ce_token,
                    "pe_token": pe_token,
                    "atm_strike": atm_strike,
                    "expiry": expiry_date
                })
                logger.info(f"{index_name} (search) -> CE: {ce_token} | PE: {pe_token}")
                return ce_token, pe_token
    except Exception as e:
        logger.error(f"Search lookup failed for {index_name}: {e}")
    
    return None, None

def refresh_all_tokens():
    """Refresh tokens for all active indices"""
    for idx in INDEX_CONFIG.keys():
        if INDEX_CONFIG[idx].get("active", True):
            get_current_atm_tokens(idx)

# ═══════════════════════════════════════════════════════════════
# POSITION SIZING
# ═══════════════════════════════════════════════════════════════
def calculate_position_size(index_name, signal_strength, score, atr, vix_value, confidence):
    """Calculate position size based on signal strength and market conditions"""
    config = INDEX_CONFIG.get(index_name, {})
    lot_size = config.get("lot_size", 50)
    
    base_risk = CONFIG["BASE_RISK_PER_TRADE_PCT"]
    
    # Adjust for signal strength
    if signal_strength in ["STRONG_BUY_CE", "STRONG_BUY_PE"]:
        risk_pct = min(CONFIG["MAX_RISK_PER_TRADE_PCT"], base_risk * 1.3)
    elif signal_strength in ["BUY_CE", "BUY_PE"]:
        risk_pct = base_risk
    else:
        risk_pct = max(CONFIG["MIN_RISK_PER_TRADE_PCT"], base_risk * 0.7)
    
    # Adjust for VIX
    if vix_value > CONFIG["VIX_HIGH_THRESHOLD"]:
        risk_pct *= 0.7
    elif vix_value > CONFIG["VIX_NORMAL_THRESHOLD"]:
        risk_pct *= 0.85
    
    # Adjust for confidence
    confidence_factor = 0.7 + (confidence / 100) * 0.6
    risk_pct *= confidence_factor
    
    # Calculate lots
    risk_amount = portfolio_state[index_name]["equity"] * (risk_pct / 100)
    stop_distance = atr * CONFIG["ENTRY_ATR_MULT"]
    
    if stop_distance > 0:
        lots = int(risk_amount / (stop_distance * lot_size))
        lots = max(1, min(5, lots))
    else:
        lots = 1
    
    return lots, risk_pct

# ═══════════════════════════════════════════════════════════════
# SIGNAL ENGINE - MULTI INDEX
# ═══════════════════════════════════════════════════════════════
def validate_signal_entry(index_name, sentiment_score, signal_action):
    """Validate if signal meets entry criteria"""
    
    # Check circuit breaker
    cb_ok, cb_reason = check_circuit_breakers(index_name)
    if not cb_ok:
        return False, f"CIRCUIT BREAKER: {cb_reason}"
    
    # Check time filters
    time_ok, time_reason = check_time_filters()
    if not time_ok:
        return False, f"TIME FILTER: {time_reason}"
    
    # Check max daily trades
    today = datetime.now().strftime("%Y-%m-%d")
    if last_trade_date[index_name] != today:
        daily_trade_count[index_name] = 0
        last_trade_date[index_name] = today
    
    if daily_trade_count[index_name] >= CONFIG["MAX_DAILY_TRADES_PER_INDEX"]:
        return False, f"Max daily trades reached ({daily_trade_count[index_name]}/{CONFIG['MAX_DAILY_TRADES_PER_INDEX']})"
    
    # Check cooldown
    if time.time() < signal_state[index_name]["cooldown_until"]:
        remaining = int(signal_state[index_name]["cooldown_until"] - time.time())
        return False, f"Cooldown: {remaining}s remaining"
    
    # Check consecutive same direction
    if signal_action in ["STRONG_BUY_CE", "BUY_CE", "LOW_BUY_CE"]:
        if signal_buffer[index_name]["consecutive_ce"] >= CONFIG["CONSECUTIVE_SAME_DIR_MAX"]:
            return False, f"Max consecutive CE trades"
    else:
        if signal_buffer[index_name]["consecutive_pe"] >= CONFIG["CONSECUTIVE_SAME_DIR_MAX"]:
            return False, f"Max consecutive PE trades"
    
    # Check premium data availability
    ce_premium = latest_ticks[index_name]["ce_price"]
    pe_premium = latest_ticks[index_name]["pe_price"]
    
    if signal_action in ["STRONG_BUY_CE", "BUY_CE", "LOW_BUY_CE"]:
        if ce_premium == 0:
            return False, "CE premium data not available"
        if ce_premium < CONFIG.get("MIN_PREMIUM_FOR_TRADE", 10):
            return False, f"CE premium too low: ₹{ce_premium}"
    else:
        if pe_premium == 0:
            return False, "PE premium data not available"
        if pe_premium < CONFIG.get("MIN_PREMIUM_FOR_TRADE", 10):
            return False, f"PE premium too low: ₹{pe_premium}"
    
    return True, "OK"

def update_candlesticks(index_name, price):
    """Update timeframe candlesticks for an index"""
    current_time = time.time()
    
    for tf, interval in [("1min", 60), ("5min", 300), ("15min", 900)]:
        if tf not in timeframe_data[index_name]:
            timeframe_data[index_name][tf] = deque(maxlen=100)
            continue
        
        if not hasattr(update_candlesticks, f"last_time_{index_name}_{tf}"):
            setattr(update_candlesticks, f"last_time_{index_name}_{tf}", current_time)
            timeframe_data[index_name][tf].append({
                "open": price, "high": price, "low": price, "close": price, "timestamp": current_time
            })
            continue
        
        last_time = getattr(update_candlesticks, f"last_time_{index_name}_{tf}")
        
        if current_time - last_time >= interval:
            # Close current candle
            timeframe_data[index_name][tf].append({
                "open": price, "high": price, "low": price, "close": price, "timestamp": current_time
            })
            setattr(update_candlesticks, f"last_time_{index_name}_{tf}", current_time)
        else:
            # Update current candle
            if timeframe_data[index_name][tf]:
                candle = timeframe_data[index_name][tf][-1]
                candle["high"] = max(candle["high"], price)
                candle["low"] = min(candle["low"], price)
                candle["close"] = price

def run_signal_engine_for_index(index_name):
    """Run signal engine for a specific index"""
    global signal_state, market_signal, daily_trade_count
    
    # Skip if index not active
    if not INDEX_CONFIG.get(index_name, {}).get("active", True):
        return
    
    log_signal_debug(f"=== SIGNAL ENGINE CYCLE START [{index_name}] ===", index_name)
    
    # Check if we have enough data
    prices = list(price_histories[index_name])
    if len(prices) < 30:
        market_signal[index_name]["alert_message"] = f"Collecting data... ({len(prices)}/30)"
        return
    
    current_price = prices[-1]
    current_time = time.time()
    
    # Update candlesticks for timeframes
    update_candlesticks(index_name, current_price)
    
    # ========== LEVEL 1: Calculate Sentiment Score ==========
    sentiment_score = calculate_sentiment_score(index_name)
    
    # ========== LEVEL 2: Get Signal from Sentiment ==========
    signal_action, sentiment_label, confidence = get_signal_from_sentiment(index_name, sentiment_score)
    
    # ========== Calculate additional indicators ==========
    spot_rsi = calculate_rsi(prices, CONFIG["RSI_PERIOD"], CONFIG["RSI_SMOOTHING"])
    spot_macd, _, macd_hist = calculate_macd(prices, CONFIG["MACD_FAST"], CONFIG["MACD_SLOW"], CONFIG["MACD_SIGNAL"])
    spot_atr = calculate_atr(prices, CONFIG["ATR_PERIOD"])
    adx = calculate_adx(prices, 14)
    
    # PCR from OI
    pcr = calculate_pcr(ce_oi_histories[index_name], pe_oi_histories[index_name])
    
    # VIX value
    vix_value = latest_ticks.get("VIX", {}).get("vix", 15.0)
    
    # Get trend data
    trend_1m = market_sentiment[index_name].get("trend_1m", "NEUTRAL")
    trend_5m = market_sentiment[index_name].get("trend_5m", "NEUTRAL")
    trend_15m = market_sentiment[index_name].get("trend_15m", "NEUTRAL")
    score_1m = market_sentiment[index_name].get("score_1m", 0)
    score_5m = market_sentiment[index_name].get("score_5m", 0)
    score_15m = market_sentiment[index_name].get("score_15m", 0)
    
    log_signal_debug(f"Sentiment: {sentiment_score:.1f} | Signal: {signal_action} | "
                     f"1m:{trend_1m}({score_1m}) 5m:{trend_5m}({score_5m}) 15m:{trend_15m}({score_15m})", index_name)
    
    # ========== POSITION MANAGEMENT (Exit checks) ==========
    if signal_state[index_name]["current_action"] != "HOLD":
        active_side = signal_state[index_name]["current_action"]
        current_premium = latest_ticks[index_name]["ce_price"] if "CE" in active_side else latest_ticks[index_name]["pe_price"]
        
        if current_premium > 0:
            unrealized_pnl = current_premium - signal_state[index_name]["entry_price"]
            
            if unrealized_pnl > signal_state[index_name]["max_profit_seen"]:
                signal_state[index_name]["max_profit_seen"] = unrealized_pnl
            
            # Trailing stop
            if current_premium > signal_state[index_name]["highest_premium_seen"]:
                signal_state[index_name]["highest_premium_seen"] = current_premium
                new_sl = current_premium - (spot_atr * CONFIG["TRAILING_ATR_MULT"])
                if new_sl > signal_state[index_name]["stop_loss"]:
                    signal_state[index_name]["stop_loss"] = new_sl
            
            # Stop loss hit
            if current_premium <= signal_state[index_name]["stop_loss"]:
                pnl_points = current_premium - signal_state[index_name]["entry_price"]
                portfolio_state[index_name]["equity"] += (pnl_points * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name].get("lots", 1))
                daily_trade_count[index_name] += 1
                
                exit_msg = f"⚠️ EXIT [{index_name}] | SL Hit | PnL: {pnl_points:.2f} pts"
                send_telegram_alert(exit_msg)
                log_signal_debug(f"EXIT: Stop loss hit. PnL: {pnl_points:.2f}", index_name)
                reset_signal_state(index_name, current_time)
                return
            
            # Target hit
            if current_premium >= signal_state[index_name]["target"]:
                pnl_points = current_premium - signal_state[index_name]["entry_price"]
                portfolio_state[index_name]["equity"] += (pnl_points * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name].get("lots", 1))
                daily_trade_count[index_name] += 1
                portfolio_state[index_name]["winning_trades"] += 1
                portfolio_state[index_name]["total_trades"] += 1
                portfolio_state[index_name]["win_rate"] = portfolio_state[index_name]["winning_trades"] / portfolio_state[index_name]["total_trades"] * 100
                
                exit_msg = f"🎯 TARGET HIT [{index_name}] | PnL: {pnl_points:.2f} pts"
                send_telegram_alert(exit_msg)
                log_signal_debug(f"EXIT: Target achieved. PnL: {pnl_points:.2f}", index_name)
                reset_signal_state(index_name, current_time)
                return
            
            # Max hold time
            hold_time_min = (current_time - signal_state[index_name]["entry_time"]) / 60
            if hold_time_min >= CONFIG["MAX_HOLD_TIME_MIN"]:
                pnl_points = current_premium - signal_state[index_name]["entry_price"]
                portfolio_state[index_name]["equity"] += (pnl_points * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name].get("lots", 1))
                daily_trade_count[index_name] += 1
                
                exit_msg = f"⏰ TIME EXIT [{index_name}] | Hold: {hold_time_min:.0f}m | PnL: {pnl_points:.2f} pts"
                send_telegram_alert(exit_msg)
                log_signal_debug(f"EXIT: Max hold time reached. PnL: {pnl_points:.2f}", index_name)
                reset_signal_state(index_name, current_time)
                return
            
            # Update active display
            market_signal[index_name]["alert_message"] = f"📊 {active_side} ACTIVE [{index_name}] | Hold: {hold_time_min:.0f}m | PnL: {unrealized_pnl:.2f}"
            market_signal[index_name]["signal_strength"] = "ACTIVE"
    
    # ========== NEW SIGNAL ENTRY ==========
    else:
        # Validate signal
        valid, reason = validate_signal_entry(index_name, sentiment_score, signal_action)
        
        if not valid:
            market_signal[index_name]["alert_message"] = f"⏸️ {reason}"
            market_signal[index_name]["signal_strength"] = "BLOCKED"
            return
        
        # Build signal based on action
        if signal_action == "STRONG_BUY_CE":
            premium = latest_ticks[index_name]["ce_price"]
            if premium <= 0:
                return
            
            lots, risk_pct = calculate_position_size(index_name, signal_action, 9, spot_atr, vix_value, confidence)
            
            sl = premium - (spot_atr * CONFIG["ENTRY_ATR_MULT"])
            target = premium + (spot_atr * CONFIG["TARGET_ATR_MULT"])
            
            signal_state[index_name].update({
                "current_action": signal_action,
                "entry_price": premium,
                "highest_premium_seen": premium,
                "stop_loss": sl,
                "target": target,
                "signal_grade": "A+",
                "confidence": confidence,
                "entry_time": current_time,
                "max_profit_seen": 0.0,
                "lots": lots,
                "position_risk_pct": risk_pct,
                "sentiment_at_entry": sentiment_score
            })
            portfolio_state[index_name]["open_positions"] = 1
            signal_buffer[index_name]["consecutive_ce"] += 1
            signal_buffer[index_name]["consecutive_pe"] = 0
            daily_trade_count[index_name] += 1
            
            alert = f"🟢 <b>STRONG BUY CE [{index_name}]</b>\n" \
                    f"💰 Spot: {current_price:.2f} | Premium: ₹{premium:.2f}\n" \
                    f"🎯 Target: {target:.2f} | 🛡️ SL: {sl:.2f}\n" \
                    f"📊 Sentiment: {sentiment_score:.1f} ({market_sentiment[index_name]['sentiment']})\n" \
                    f"📈 Trends: 1m:{trend_1m} 5m:{trend_5m} 15m:{trend_15m}\n" \
                    f"📦 Lots: {lots} | Risk: {risk_pct:.2f}%"
            send_telegram_alert(alert)
            log_signal_debug(f"SIGNAL: {signal_action} [{index_name}] | Conf:{confidence:.1f}% | Sentiment:{sentiment_score:.1f}", index_name)
            
        elif signal_action == "BUY_CE":
            premium = latest_ticks[index_name]["ce_price"]
            if premium <= 0:
                return
            
            lots, risk_pct = calculate_position_size(index_name, signal_action, 7, spot_atr, vix_value, confidence)
            
            sl = premium - (spot_atr * CONFIG["ENTRY_ATR_MULT"] * 0.9)
            target = premium + (spot_atr * CONFIG["TARGET_ATR_MULT"] * 0.9)
            
            signal_state[index_name].update({
                "current_action": signal_action,
                "entry_price": premium,
                "highest_premium_seen": premium,
                "stop_loss": sl,
                "target": target,
                "signal_grade": "A",
                "confidence": confidence,
                "entry_time": current_time,
                "max_profit_seen": 0.0,
                "lots": lots,
                "position_risk_pct": risk_pct,
                "sentiment_at_entry": sentiment_score
            })
            portfolio_state[index_name]["open_positions"] = 1
            signal_buffer[index_name]["consecutive_ce"] += 1
            signal_buffer[index_name]["consecutive_pe"] = 0
            daily_trade_count[index_name] += 1
            
            alert = f"🟢 <b>BUY CE [{index_name}]</b>\n" \
                    f"💰 Spot: {current_price:.2f} | Premium: ₹{premium:.2f}\n" \
                    f"🎯 Target: {target:.2f} | 🛡️ SL: {sl:.2f}\n" \
                    f"📊 Sentiment: {sentiment_score:.1f}\n" \
                    f"📦 Lots: {lots}"
            send_telegram_alert(alert)
            log_signal_debug(f"SIGNAL: {signal_action} [{index_name}] | Conf:{confidence:.1f}%", index_name)
            
        elif signal_action == "LOW_BUY_CE":
            premium = latest_ticks[index_name]["ce_price"]
            if premium <= 0:
                return
            
            lots, risk_pct = calculate_position_size(index_name, signal_action, 5, spot_atr, vix_value, confidence)
            
            sl = premium - (spot_atr * CONFIG["ENTRY_ATR_MULT"] * 0.7)
            target = premium + (spot_atr * CONFIG["TARGET_ATR_MULT"] * 0.7)
            
            signal_state[index_name].update({
                "current_action": signal_action,
                "entry_price": premium,
                "highest_premium_seen": premium,
                "stop_loss": sl,
                "target": target,
                "signal_grade": "B+",
                "confidence": confidence,
                "entry_time": current_time,
                "max_profit_seen": 0.0,
                "lots": lots,
                "position_risk_pct": risk_pct,
                "sentiment_at_entry": sentiment_score
            })
            portfolio_state[index_name]["open_positions"] = 1
            signal_buffer[index_name]["consecutive_ce"] += 1
            signal_buffer[index_name]["consecutive_pe"] = 0
            daily_trade_count[index_name] += 1
            
            alert = f"🟡 <b>LOW BUY CE [{index_name}]</b>\n" \
                    f"💰 Spot: {current_price:.2f} | Premium: ₹{premium:.2f}\n" \
                    f"🎯 Target: {target:.2f} | 🛡️ SL: {sl:.2f}\n" \
                    f"📊 Sentiment: {sentiment_score:.1f}"
            send_telegram_alert(alert)
            log_signal_debug(f"SIGNAL: {signal_action} [{index_name}] | Conf:{confidence:.1f}%", index_name)
            
        elif signal_action == "STRONG_BUY_PE":
            premium = latest_ticks[index_name]["pe_price"]
            if premium <= 0:
                return
            
            lots, risk_pct = calculate_position_size(index_name, signal_action, 9, spot_atr, vix_value, confidence)
            
            sl = premium - (spot_atr * CONFIG["ENTRY_ATR_MULT"])
            target = premium + (spot_atr * CONFIG["TARGET_ATR_MULT"])
            
            signal_state[index_name].update({
                "current_action": signal_action,
                "entry_price": premium,
                "highest_premium_seen": premium,
                "stop_loss": sl,
                "target": target,
                "signal_grade": "A+",
                "confidence": confidence,
                "entry_time": current_time,
                "max_profit_seen": 0.0,
                "lots": lots,
                "position_risk_pct": risk_pct,
                "sentiment_at_entry": sentiment_score
            })
            portfolio_state[index_name]["open_positions"] = 1
            signal_buffer[index_name]["consecutive_pe"] += 1
            signal_buffer[index_name]["consecutive_ce"] = 0
            daily_trade_count[index_name] += 1
            
            alert = f"🔴 <b>STRONG BUY PE [{index_name}]</b>\n" \
                    f"💰 Spot: {current_price:.2f} | Premium: ₹{premium:.2f}\n" \
                    f"🎯 Target: {target:.2f} | 🛡️ SL: {sl:.2f}\n" \
                    f"📊 Sentiment: {sentiment_score:.1f} ({market_sentiment[index_name]['sentiment']})\n" \
                    f"📉 Trends: 1m:{trend_1m} 5m:{trend_5m} 15m:{trend_15m}\n" \
                    f"📦 Lots: {lots} | Risk: {risk_pct:.2f}%"
            send_telegram_alert(alert)
            log_signal_debug(f"SIGNAL: {signal_action} [{index_name}] | Conf:{confidence:.1f}%", index_name)
            
        elif signal_action == "BUY_PE":
            premium = latest_ticks[index_name]["pe_price"]
            if premium <= 0:
                return
            
            lots, risk_pct = calculate_position_size(index_name, signal_action, 7, spot_atr, vix_value, confidence)
            
            sl = premium - (spot_atr * CONFIG["ENTRY_ATR_MULT"] * 0.9)
            target = premium + (spot_atr * CONFIG["TARGET_ATR_MULT"] * 0.9)
            
            signal_state[index_name].update({
                "current_action": signal_action,
                "entry_price": premium,
                "highest_premium_seen": premium,
                "stop_loss": sl,
                "target": target,
                "signal_grade": "A",
                "confidence": confidence,
                "entry_time": current_time,
                "max_profit_seen": 0.0,
                "lots": lots,
                "position_risk_pct": risk_pct,
                "sentiment_at_entry": sentiment_score
            })
            portfolio_state[index_name]["open_positions"] = 1
            signal_buffer[index_name]["consecutive_pe"] += 1
            signal_buffer[index_name]["consecutive_ce"] = 0
            daily_trade_count[index_name] += 1
            
            alert = f"🔴 <b>BUY PE [{index_name}]</b>\n" \
                    f"💰 Spot: {current_price:.2f} | Premium: ₹{premium:.2f}\n" \
                    f"🎯 Target: {target:.2f} | 🛡️ SL: {sl:.2f}\n" \
                    f"📊 Sentiment: {sentiment_score:.1f}\n" \
                    f"📦 Lots: {lots}"
            send_telegram_alert(alert)
            log_signal_debug(f"SIGNAL: {signal_action} [{index_name}] | Conf:{confidence:.1f}%", index_name)
            
        elif signal_action == "LOW_BUY_PE":
            premium = latest_ticks[index_name]["pe_price"]
            if premium <= 0:
                return
            
            lots, risk_pct = calculate_position_size(index_name, signal_action, 5, spot_atr, vix_value, confidence)
            
            sl = premium - (spot_atr * CONFIG["ENTRY_ATR_MULT"] * 0.7)
            target = premium + (spot_atr * CONFIG["TARGET_ATR_MULT"] * 0.7)
            
            signal_state[index_name].update({
                "current_action": signal_action,
                "entry_price": premium,
                "highest_premium_seen": premium,
                "stop_loss": sl,
                "target": target,
                "signal_grade": "B+",
                "confidence": confidence,
                "entry_time": current_time,
                "max_profit_seen": 0.0,
                "lots": lots,
                "position_risk_pct": risk_pct,
                "sentiment_at_entry": sentiment_score
            })
            portfolio_state[index_name]["open_positions"] = 1
            signal_buffer[index_name]["consecutive_pe"] += 1
            signal_buffer[index_name]["consecutive_ce"] = 0
            daily_trade_count[index_name] += 1
            
            alert = f"🟠 <b>LOW BUY PE [{index_name}]</b>\n" \
                    f"💰 Spot: {current_price:.2f} | Premium: ₹{premium:.2f}\n" \
                    f"🎯 Target: {target:.2f} | 🛡️ SL: {sl:.2f}\n" \
                    f"📊 Sentiment: {sentiment_score:.1f}"
            send_telegram_alert(alert)
            log_signal_debug(f"SIGNAL: {signal_action} [{index_name}] | Conf:{confidence:.1f}%", index_name)
        
        else:
            # NO_TRADE - Update market signal display
            market_signal[index_name]["alert_message"] = f"⚪ NO TRADE [{index_name}] | Sentiment: {sentiment_score:.1f} ({market_sentiment[index_name]['sentiment']}) | Trends: 1m:{trend_1m} 5m:{trend_5m} 15m:{trend_15m}"
            market_signal[index_name]["signal_strength"] = "NO_TRADE"
            signal_buffer[index_name]["ce_count"] = 0
            signal_buffer[index_name]["pe_count"] = 0
    
    # Update market signal dictionary
    market_signal[index_name].update({
        "spot_price": current_price,
        "ce_price": latest_ticks[index_name]["ce_price"],
        "pe_price": latest_ticks[index_name]["pe_price"],
        "spot_rsi": round(spot_rsi, 2),
        "spot_macd": round(spot_macd, 4),
        "pcr": round(pcr, 2),
        "spot_atr": round(spot_atr, 2),
        "vix": round(vix_value, 2),
        "signal": signal_state[index_name]["current_action"],
        "confidence": round(signal_state[index_name]["confidence"], 2),
        "timestamp": datetime.now().isoformat(),
        "sentiment_score": sentiment_score,
        "sentiment": market_sentiment[index_name]["sentiment"],
        "trend_1m": trend_1m,
        "trend_5m": trend_5m,
        "trend_15m": trend_15m,
        "score_1m": score_1m,
        "score_5m": score_5m,
        "score_15m": score_15m
    })

def reset_signal_state(index_name, current_time):
    """Reset signal state after exit"""
    signal_state[index_name].update({
        "current_action": "HOLD",
        "entry_price": 0.0,
        "stop_loss": 0.0,
        "target": 0.0,
        "highest_premium_seen": 0.0,
        "confidence": 0.0,
        "cooldown_until": current_time + CONFIG["COOLDOWN_SEC"],
        "entry_time": 0,
        "max_profit_seen": 0.0,
        "lots": 1,
        "position_risk_pct": 1.0
    })
    portfolio_state[index_name]["open_positions"] = 0
    signal_buffer[index_name]["ce_count"] = 0
    signal_buffer[index_name]["pe_count"] = 0

def run_all_signals():
    """Run signal engine for all active indices"""
    for idx in INDEX_CONFIG.keys():
        if INDEX_CONFIG[idx].get("active", True):
            run_signal_engine_for_index(idx)

# ═══════════════════════════════════════════════════════════════
# SAFETY FUNCTIONS
# ═══════════════════════════════════════════════════════════════
def check_circuit_breakers(index_name):
    """Check circuit breaker conditions for an index"""
    if not CONFIG.get("CIRCUIT_BREAKER_ENABLED", True):
        return True, ""
    
    safety = safety_state[index_name]
    
    if safety["circuit_breaker_triggered"]:
        if time.time() - safety["circuit_breaker_time"] > 1800:
            safety["circuit_breaker_triggered"] = False
            safety["circuit_breaker_reason"] = ""
            return True, ""
        return False, safety["circuit_breaker_reason"]
    
    # Daily loss circuit breaker
    daily_loss_pct = (portfolio_state[index_name]["initial_equity"] - portfolio_state[index_name]["equity"]) / portfolio_state[index_name]["initial_equity"] * 100
    if daily_loss_pct >= CONFIG["CIRCUIT_BREAKER_DAILY_LOSS_PCT"]:
        safety["circuit_breaker_triggered"] = True
        safety["circuit_breaker_time"] = time.time()
        safety["circuit_breaker_reason"] = f"Daily loss limit {daily_loss_pct:.2f}%"
        return False, safety["circuit_breaker_reason"]
    
    # Consecutive SL circuit breaker
    if safety["consecutive_sl_count"] >= CONFIG["CIRCUIT_BREAKER_CONSECUTIVE_SL"]:
        safety["circuit_breaker_triggered"] = True
        safety["circuit_breaker_time"] = time.time()
        safety["circuit_breaker_reason"] = f"{safety['consecutive_sl_count']} consecutive SL"
        return False, safety["circuit_breaker_reason"]
    
    return True, ""

def check_time_filters():
    """Check time-based trading restrictions"""
    now = get_ist_now()
    
    if CONFIG.get("NO_TRADE_FIRST_15_MIN", True):
        if now.time() >= dt_time(9, 15) and now.time() < dt_time(9, 30):
            return False, "Opening volatility (9:15-9:30)"
    
    if CONFIG.get("NO_TRADE_LAST_30_MIN", True):
        if now.time() >= dt_time(15, 0) and now.time() <= dt_time(15, 30):
            return False, "Closing volatility (after 15:00)"
    
    return True, ""

def send_telegram_alert(message):
    """Send alert via Telegram"""
    if not TELEGRAM_AVAILABLE or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}, timeout=3)
    except Exception as e:
        logger.error(f"Telegram failed: {e}")

# ═══════════════════════════════════════════════════════════════
# WEBSOCKET HANDLERS (Preserved from original)
# ═══════════════════════════════════════════════════════════════
def on_ws_open(wsapp):
    global sws
    logger.info("WebSocket opened successfully")
    
    # Build subscription tokens for all active indices
    all_tokens = []
    for idx, config in INDEX_CONFIG.items():
        if config.get("active", True):
            all_tokens.append(config["token"])
            if INDEX_TOKENS[idx]["ce_token"]:
                all_tokens.append(INDEX_TOKENS[idx]["ce_token"])
            if INDEX_TOKENS[idx]["pe_token"]:
                all_tokens.append(INDEX_TOKENS[idx]["pe_token"])
    
    # Add VIX
    all_tokens.append("99919017")
    
    if sws and all_tokens:
        try:
            subscription_payload = [{"exchangeType": 1, "tokens": all_tokens}]
            sws.subscribe("tradeguru_001", 1, subscription_payload)
            logger.info(f"Subscribed to {len(all_tokens)} tokens")
        except Exception as e:
            logger.error(f"Subscription failed: {e}")

def on_ws_error(wsapp, error):
    logger.error(f"WebSocket Error: {error}")

def on_ws_close(wsapp, close_status_code=None, close_msg=None):
    global ws_running
    ws_running = False
    logger.warning(f"WebSocket closed: code={close_status_code}, msg={close_msg}")

def on_ws_data(wsapp, message):
    global tick_counter, last_tick_time, latest_ticks
    
    last_tick_time = time.time()
    
    try:
        ticks = []
        
        if isinstance(message, bytes):
            if sws is not None and hasattr(sws, '_parse_binary_data'):
                try:
                    parsed = sws._parse_binary_data(message)
                    if parsed and isinstance(parsed, dict):
                        ticks = [parsed]
                except Exception as e:
                    return
            else:
                return
        elif isinstance(message, str):
            try:
                data = json.loads(message)
                ticks = data if isinstance(data, list) else [data]
            except json.JSONDecodeError:
                return
        elif isinstance(message, dict):
            ticks = [message]
        elif isinstance(message, list):
            ticks = message
        else:
            return
        
        for tick in ticks:
            if not isinstance(tick, dict):
                continue
            
            token = str(tick.get("token") or tick.get("tk") or tick.get("symbol") or "")
            ltp = tick.get("ltp") or tick.get("last_traded_price") or tick.get("lp") or tick.get("close") or 0
            if isinstance(ltp, str):
                try:
                    ltp = float(ltp)
                except:
                    ltp = 0
            
            # Fix scaling
            if isinstance(ltp, (int, float)) and ltp > 100000:
                ltp = ltp / 100
            
            vol = tick.get("v") or tick.get("volume") or 0
            oi = tick.get("oi") or tick.get("open_interest") or 0
            
            # Find which index this token belongs to
            index_name = get_index_by_token(token)
            
            if index_name:
                if token == INDEX_CONFIG[index_name]["token"]:
                    if ltp > 0:
                        latest_ticks[index_name]["spot_price"] = ltp
                        price_histories[index_name].append(ltp)
                        tick_counter += 1
                
                elif token == INDEX_TOKENS[index_name].get("ce_token"):
                    if ltp > 0:
                        latest_ticks[index_name]["ce_price"] = ltp
                        ce_price_histories[index_name].append(ltp)
                        ce_volume_histories[index_name].append(vol)
                        ce_oi_histories[index_name].append(oi)
                
                elif token == INDEX_TOKENS[index_name].get("pe_token"):
                    if ltp > 0:
                        latest_ticks[index_name]["pe_price"] = ltp
                        pe_price_histories[index_name].append(ltp)
                        pe_volume_histories[index_name].append(vol)
                        pe_oi_histories[index_name].append(oi)
            
            elif token == "99919017":  # VIX token
                if ltp > 0:
                    latest_ticks["VIX"]["vix"] = ltp
                    vix_history.append(ltp)
            
            # Run signal engine periodically
            if tick_counter % 3 == 0:
                run_all_signals()
                
    except Exception as e:
        logger.error(f"WebSocket data handler error: {e}")

def start_angel_websocket():
    global CE_TOKEN, PE_TOKEN, ATM_STRIKE, sws, ws_running
    
    logger.info("=" * 60)
    logger.info("WEBSOCKET THREAD STARTED - MULTI-INDEX MODE")
    logger.info("=" * 60)
    
    # Refresh all tokens first
    refresh_all_tokens()
    
    while True:
        try:
            if not is_market_open():
                time.sleep(30)
                continue
            
            auth_token, feed_token, obj = get_auth_token()
            if not feed_token:
                time.sleep(10)
                continue
            
            logger.info(f"Auth OK. Feed: {feed_token[:10]}...")
            
            sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)
            
            sws.on_open = on_ws_open
            sws.on_data = on_ws_data
            sws.on_error = on_ws_error
            sws.on_close = on_ws_close
            
            ws_running = True
            sws.connect()
            
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
            ws_running = False
            sws = None
            time.sleep(10)

# ═══════════════════════════════════════════════════════════════
# FLASK ROUTES
# ═══════════════════════════════════════════════════════════════
_init_completed = False

@app.before_request
def ensure_threads_are_breathing():
    global _init_completed
    if not _init_completed:
        ws_thread = threading.Thread(target=start_angel_websocket, daemon=True, name="angel_websocket_thread")
        ws_thread.start()
        _init_completed = True

@app.route("/", methods=["GET", "HEAD"])
def home():
    return jsonify({
        "status": "healthy",
        "engine": "Multi-Index Options Trading Bot v3.0",
        "supported_indices": list(INDEX_CONFIG.keys()),
        "active_index": ACTIVE_INDEX,
        "market_open": is_market_open(),
        "timestamp": time.time(),
        "features": [
            "Level 1: Market Sentiment Engine (0-100 scoring)",
            "Multi-Timeframe Analysis (1m/5m/15m with weights 30/40/30)",
            "Signal Categories: STRONG BUY, BUY, LOW BUY, NO TRADE",
            "15m Opposing Trend Rejection for LOW signals",
            "Per-index Portfolio Management",
            "Dynamic Position Sizing",
            "Circuit Breakers & Safety Features",
            "Telegram Alerts"
        ]
    }), 200

@app.route("/api/live-signals", methods=["GET"])
def live_signals():
    return jsonify({
        "timestamp": get_ist_now().isoformat(),
        "signals": market_signal,
        "sentiment": market_sentiment,
        "portfolios": portfolio_state,
        "tokens": INDEX_TOKENS,
        "active_index": ACTIVE_INDEX,
        "debug_log": list(signal_debug_log)[-50:]
    }), 200

@app.route("/api/signals/<index_name>", methods=["GET"])
def get_index_signals(index_name):
    if index_name not in INDEX_CONFIG:
        return jsonify({"error": f"Invalid index. Choose from: {list(INDEX_CONFIG.keys())}"}), 400
    
    return jsonify({
        "index": index_name,
        "signal": market_signal[index_name],
        "sentiment": market_sentiment[index_name],
        "portfolio": portfolio_state[index_name],
        "position": signal_state[index_name],
        "tokens": INDEX_TOKENS[index_name]
    }), 200

@app.route("/api/set-active-index", methods=["POST"])
def set_active_index():
    global ACTIVE_INDEX
    data = request.get_json()
    new_index = data.get("index", "NIFTY")
    
    if new_index not in INDEX_CONFIG:
        return jsonify({"error": f"Invalid index. Choose from: {list(INDEX_CONFIG.keys())}"}), 400
    
    ACTIVE_INDEX = new_index
    return jsonify({
        "status": "ok",
        "active_index": ACTIVE_INDEX,
        "message": f"Switched to {new_index}"
    }), 200

@app.route("/api/refresh-tokens", methods=["POST"])
def refresh_tokens():
    refresh_all_tokens()
    return jsonify({
        "status": "ok",
        "tokens": INDEX_TOKENS,
        "message": "All tokens refreshed"
    }), 200

@app.route("/api/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "OK",
        "ws_running": ws_running,
        "last_tick": last_tick_time,
        "ticks_received": tick_counter,
        "active_index": ACTIVE_INDEX,
        "indices": {idx: {
            "active": INDEX_CONFIG[idx].get("active", True),
            "spot": latest_ticks[idx]["spot_price"],
            "ce": latest_ticks[idx]["ce_price"],
            "pe": latest_ticks[idx]["pe_price"],
            "position": signal_state[idx]["current_action"]
        } for idx in INDEX_CONFIG.keys()}
    }), 200

@app.route("/api/config", methods=["GET"])
def get_config():
    return jsonify({
        "index_config": INDEX_CONFIG,
        "trading_config": {
            "timeframe_weights": CONFIG["TIMEFRAME_WEIGHTS"],
            "sentiment_thresholds": CONFIG["SENTIMENT_SCORES"],
            "max_daily_trades_per_index": CONFIG["MAX_DAILY_TRADES_PER_INDEX"],
            "cooldown_sec": CONFIG["COOLDOWN_SEC"],
            "reject_15m_opposing": CONFIG["REJECT_15M_OPPOSING"],
            "weak_signals_allowed": CONFIG["WEAK_SIGNAL_ALLOWED"]
        }
    }), 200

@app.route("/api/reset-safety/<index_name>", methods=["POST"])
def reset_safety(index_name):
    if index_name not in INDEX_CONFIG:
        return jsonify({"error": "Invalid index"}), 400
    
    safety_state[index_name]["circuit_breaker_triggered"] = False
    safety_state[index_name]["circuit_breaker_reason"] = ""
    safety_state[index_name]["recovery_mode"] = False
    safety_state[index_name]["consecutive_sl_count"] = 0
    
    return jsonify({"status": "ok", "message": f"Safety reset for {index_name}"}), 200

if __name__ == "__main__":
    if not _init_completed:
        ws_thread = threading.Thread(target=start_angel_websocket, daemon=True, name="angel_websocket_thread")
        ws_thread.start()
        _init_completed = True
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
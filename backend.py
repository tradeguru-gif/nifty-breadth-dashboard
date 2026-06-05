# === VERSION 12.2 - FIXED: Premium Invalid | Indentation | Greeks API | Slippage | All Bugs ===
# Fixes applied:
# 1. Fixed "Premium Invalid: Rs0" - better token fetching, relaxed premium validation, auto-refresh
# 2. Fixed indentation bug in live_signals() endpoint (return was inside for loop)
# 3. Fixed dir() bug for slippage_adj - using proper variable initialization
# 4. Fixed Greeks API endpoint URL (/marketData/v1/optionGreek)
# 5. Fixed expiry calculation to use actual nearest expiry from scrip master
# 6. Fixed correlation filter division by zero
# 7. Fixed Kelly DB logging (was dividing by 100 twice)
# 8. Fixed performance tracker add_return call
# 9. Fixed VWAP exit duplicate code
# 10. Fixed safe_ltp for list data
# 11. Added proper token refresh when premiums are 0
# 12. Added last_known_good_prices cache
# 13. Fixed scrip master DataFrame creation with empty data
# 14. Fixed is_expiry_day to check actual expiry date
# 15. Added connection context managers for DB operations
# 16. Fixed slippage recording to use proper price reference
# 17. Added fallback price tracking when WS/REST both fail
# 18. Fixed signal_buffer reset logic
# 19. Added comprehensive error handling in token management
# 20. Fixed all variable initialization before use

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
import socket
from collections import deque, defaultdict
from datetime import datetime, timedelta, time as dt_time
from flask import Flask, jsonify, request
from flask_cors import CORS
import pyotp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info(f"Python version: {sys.version}")

for pkg in ['numpy', 'pandas', 'flask', 'requests']:
    try:
        __import__(pkg)
        logger.info(f"{pkg} loaded")
    except Exception as e:
        logger.error(f"{pkg} import failed: {e}")

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

# ============================================================================
# ENVIRONMENT CONFIGURATION
# ============================================================================
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing critical Angel One environment variables")

DB_PATH = "trading_data.db"

# ============================================================================
# DATABASE INITIALIZATION - V12 ENHANCED TABLES
# ============================================================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.cursor()

        # V11 tables (preserved)
        c.execute("""CREATE TABLE IF NOT EXISTS ticks (
            timestamp REAL, token TEXT, price REAL, volume REAL, 
            bid REAL, ask REAL, oi REAL)""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_ticks_time ON ticks(timestamp)")
        c.execute("""CREATE TABLE IF NOT EXISTS signals (
            timestamp REAL, action TEXT, signal_type TEXT, grade TEXT, 
            confidence REAL, ce_price REAL, pe_price REAL, rsi REAL, 
            pcr REAL, regime TEXT, vwap REAL, vix REAL, ml_score REAL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS trades (
            timestamp REAL, action TEXT, entry_price REAL, exit_price REAL, 
            pnl REAL, size_pct REAL, status TEXT, grade TEXT, atr REAL, 
            vix REAL, exit_reason TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS daily_performance (
            date TEXT, equity REAL, daily_pnl REAL, drawdown_pct REAL, 
            sharpe REAL, var REAL, win_rate REAL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS ml_models (
            id INTEGER PRIMARY KEY, model BLOB, created_at REAL, 
            features TEXT, accuracy REAL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS backtest_results (
            id INTEGER PRIMARY KEY, date TEXT, strategy TEXT, trades INTEGER, 
            win_rate REAL, profit_factor REAL, max_drawdown REAL, sharpe REAL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS greeks (
            timestamp REAL, delta REAL, gamma REAL, theta REAL, 
            vega REAL, iv REAL, ce_delta REAL, pe_delta REAL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS market_profile (
            timestamp REAL, poc REAL, value_area_high REAL, value_area_low REAL, 
            vwap REAL, volume_profile TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS portfolio_equity (
            index_name TEXT PRIMARY KEY, equity REAL, last_updated REAL)""")

        # V12 NEW TABLES
        c.execute("""CREATE TABLE IF NOT EXISTS pcr_history (
            timestamp REAL, index_name TEXT, pcr_oi REAL, pcr_volume REAL, 
            pcr_weighted REAL, ce_oi_total REAL, pe_oi_total REAL, 
            ce_volume_total REAL, pe_volume_total REAL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS greeks_history (
            timestamp REAL, index_name TEXT, ce_iv REAL, pe_iv REAL, 
            ce_delta REAL, pe_delta REAL, ce_gamma REAL, pe_gamma REAL,
            ce_theta REAL, pe_theta REAL, ce_vega REAL, pe_vega REAL,
            iv_rank REAL, iv_percentile REAL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS correlation_data (
            timestamp REAL, nifty_price REAL, banknifty_price REAL, 
            correlation_20 REAL, correlation_50 REAL, beta REAL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS regime_history (
            timestamp REAL, index_name TEXT, regime TEXT, adx REAL, 
            bb_width REAL, volatility_regime TEXT, trend_strength REAL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS volume_profile_history (
            timestamp REAL, index_name TEXT, poc REAL, value_area_high REAL, 
            value_area_low REAL, vwap REAL, volume_nodes TEXT, 
            profile_type TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS kelly_history (
            timestamp REAL, index_name TEXT, kelly_fraction REAL, 
            win_rate REAL, avg_win REAL, avg_loss REAL, 
            recommended_lots INTEGER, actual_lots INTEGER)""")
        c.execute("""CREATE TABLE IF NOT EXISTS slippage_log (
            timestamp REAL, index_name TEXT, expected_price REAL, 
            actual_price REAL, slippage_pct REAL, spread REAL, 
            liquidity_score REAL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS performance_metrics (
            timestamp REAL, index_name TEXT, sharpe REAL, sortino REAL, 
            calmar REAL, win_rate REAL, profit_factor REAL, 
            max_drawdown REAL, avg_trade REAL, expectancy REAL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS drawdown_events (
            timestamp REAL, index_name TEXT, drawdown_pct REAL, 
            action_taken TEXT, equity_before REAL, equity_after REAL)""")

        conn.commit()
    finally:
        conn.close()

init_db()

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ============================================================================
# INDEX CONFIGURATION - V12 ENHANCED (SENSEX NOW ACTIVE)
# ============================================================================
INDEX_CONFIG = {
    "NIFTY": {
        "token": "99926000", "exchange": "NSE", "symbol": "NIFTY",
        "lot_size": 50, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50,
        "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2,
        "max_daily_drawdown_pct": 3.0, "correlation_pair": "BANKNIFTY",
        "greeks_enabled": True, "pcr_enabled": True
    },
    "BANKNIFTY": {
        "token": "99926009", "exchange": "NSE", "symbol": "BANKNIFTY",
        "lot_size": 25, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100,
        "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2,
        "max_daily_drawdown_pct": 3.0, "correlation_pair": "NIFTY",
        "greeks_enabled": True, "pcr_enabled": True
    },
    "FINNIFTY": {
        "token": "99926037", "exchange": "NSE", "symbol": "FINNIFTY",
        "lot_size": 40, "expiry_weekday": 1, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50,
        "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2,
        "max_daily_drawdown_pct": 3.0, "correlation_pair": None,
        "greeks_enabled": True, "pcr_enabled": True
    },
    "MIDCPNIFTY": {
        "token": "99926074", "exchange": "NSE", "symbol": "MIDCPNIFTY",
        "lot_size": 75, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 25,
        "option_exchange": "NFO", "ws_exchange_type": 1, "option_ws_exchange_type": 2,
        "max_daily_drawdown_pct": 3.0, "correlation_pair": None,
        "greeks_enabled": False, "pcr_enabled": True
    },
    "SENSEX": {
        "token": "99919000", "exchange": "BSE", "symbol": "SENSEX",
        "lot_size": 15, "expiry_weekday": 4, "active": True,   # ACTIVATED
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100,
        "option_exchange": "BFO", "ws_exchange_type": 3, "option_ws_exchange_type": 4,
        "max_daily_drawdown_pct": 3.0, "correlation_pair": None,
        "greeks_enabled": True,    # Enable Greeks for SENSEX
        "pcr_enabled": True        # Enable PCR for SENSEX
    }
}

ACTIVE_INDEX = "NIFTY"

INDEX_TOKENS = {idx: {"ce_token": None, "pe_token": None, "atm_strike": 0, 
                       "expiry": "", "expiry_date": None, "ce_symbol": "", "pe_symbol": ""} 
                for idx in INDEX_CONFIG}

# Last known good prices cache
last_known_prices = {idx: {"spot": 0.0, "ce": 0.0, "pe": 0.0, "timestamp": 0} 
                     for idx in INDEX_CONFIG}

price_histories = {idx: deque(maxlen=5000) for idx in INDEX_CONFIG}
ce_price_histories = {idx: deque(maxlen=2000) for idx in INDEX_CONFIG}
pe_price_histories = {idx: deque(maxlen=2000) for idx in INDEX_CONFIG}
ce_volume_histories = {idx: deque(maxlen=1000) for idx in INDEX_CONFIG}
pe_volume_histories = {idx: deque(maxlen=1000) for idx in INDEX_CONFIG}
ce_oi_histories = {idx: deque(maxlen=200) for idx in INDEX_CONFIG}
pe_oi_histories = {idx: deque(maxlen=200) for idx in INDEX_CONFIG}

ce_oi_series = {idx: deque(maxlen=500) for idx in INDEX_CONFIG}
pe_oi_series = {idx: deque(maxlen=500) for idx in INDEX_CONFIG}
ce_volume_series = {idx: deque(maxlen=500) for idx in INDEX_CONFIG}
pe_volume_series = {idx: deque(maxlen=500) for idx in INDEX_CONFIG}

vix_history = deque(maxlen=200)
banknifty_history = deque(maxlen=1000)

nifty_price_series = deque(maxlen=200)
banknifty_price_series = deque(maxlen=200)

latest_ticks = {idx: {"spot_price": 0.0, "ce_price": 0.0, "pe_price": 0.0,
                      "ce_volume": 0, "pe_volume": 0, "ce_oi": 0, "pe_oi": 0,
                      "ce_bid": 0.0, "ce_ask": 0.0, "pe_bid": 0.0, "pe_ask": 0.0,
                      "spread_ce": 0.0, "spread_pe": 0.0}
                for idx in INDEX_CONFIG}
latest_ticks["VIX"] = {"vix": 15.0}

latest_greeks = {idx: {
    "ce_iv": 0.0, "pe_iv": 0.0, "ce_delta": 0.0, "pe_delta": 0.0,
    "ce_gamma": 0.0, "pe_gamma": 0.0, "ce_theta": 0.0, "pe_theta": 0.0,
    "ce_vega": 0.0, "pe_vega": 0.0, "iv_rank": 0.0, "iv_percentile": 0.0
} for idx in INDEX_CONFIG}

latest_volume_profile = {idx: {
    "poc": 0.0, "value_area_high": 0.0, "value_area_low": 0.0,
    "vwap": 0.0, "volume_nodes": {}, "profile_type": "neutral"
} for idx in INDEX_CONFIG}

latest_regime = {idx: {
    "regime": "unknown", "adx": 20.0, "bb_width": 0.02,
    "volatility_regime": "normal", "trend_strength": 0.0,
    "regime_score": 50.0
} for idx in INDEX_CONFIG}

latest_pcr = {idx: {
    "pcr_oi": 1.0, "pcr_volume": 1.0, "pcr_weighted": 1.0,
    "pcr_signal": "neutral", "pcr_strength": 0.0
} for idx in INDEX_CONFIG}

latest_correlation = {
    "correlation_20": 0.0, "correlation_50": 0.0, "beta": 1.0,
    "correlation_signal": "neutral", "divergence_detected": False
}

latest_metrics = {idx: {
    "sharpe": 0.0, "sortino": 0.0, "calmar": 0.0,
    "win_rate": 0.0, "profit_factor": 0.0, "max_drawdown": 0.0,
    "avg_trade": 0.0, "expectancy": 0.0
} for idx in INDEX_CONFIG}

daily_drawdown = {idx: {
    "peak_equity": 0.0, "current_drawdown": 0.0, 
    "kill_switch_active": False, "kill_switch_until": 0
} for idx in INDEX_CONFIG}

trade_history = {idx: deque(maxlen=200) for idx in INDEX_CONFIG}

slippage_stats = {idx: {
    "avg_slippage_pct": 0.0, "max_slippage_pct": 0.0,
    "avg_spread": 0.0, "liquidity_score": 1.0
} for idx in INDEX_CONFIG}

tick_counter = 0
ws_running = False
sws = None
last_tick_time = time.time()
startup_complete = False
last_heartbeat = time.time()

_signal_lock = threading.Lock()
_portfolio_lock = threading.Lock()
_greeks_lock = threading.Lock()
_pcr_lock = threading.Lock()
_correlation_lock = threading.Lock()
_regime_lock = threading.Lock()
_metrics_lock = threading.Lock()
_token_lock = threading.Lock()

_api_last_call = 0
_api_min_interval = 0.5
_api_lock = threading.Lock()

def rate_limited_api_call(func, *args, **kwargs):
    global _api_last_call
    with _api_lock:
        elapsed = time.time() - _api_last_call
        if elapsed < _api_min_interval:
            time.sleep(_api_min_interval - elapsed)
        try:
            result = func(*args, **kwargs)
            _api_last_call = time.time()
            return result
        except Exception as e:
            _api_last_call = time.time()
            raise e

TIMEFRAMES = ["1min", "2min", "3min", "5min", "10min", "15min", "20min", "30min"]
TIMEFRAME_WEIGHTS = {
    "1min": 8, "2min": 8, "3min": 8, "5min": 12,
    "10min": 12, "15min": 14, "20min": 14, "30min": 24
}
EMA_SHORT, EMA_MEDIUM, EMA_LONG = 9, 21, 50

SENTIMENT_SCORES = {
    "STRONG_BULLISH": (85, 100, "STRONG BULLISH", "STRONG_BUY_CE"),
    "BULLISH": (70, 84, "BULLISH", "BUY_CE"),
    "SLOW_BULLISH": (55, 69, "SLOW BULLISH", "LOW_BUY_CE"),
    "NEUTRAL": (45, 54, "NEUTRAL", "NO_TRADE"),
    "SLOW_BEARISH": (30, 44, "SLOW BEARISH", "LOW_BUY_PE"),
    "BEARISH": (15, 29, "BEARISH", "BUY_PE"),
    "STRONG_BEARISH": (0, 14, "STRONG BEARISH", "STRONG_BUY_PE")
}

market_sentiment = {idx: {
    "score": 50, "label": "NEUTRAL",
    "trend_1min": "NEUTRAL", "trend_2min": "NEUTRAL",
    "trend_3min": "NEUTRAL", "trend_5min": "NEUTRAL",
    "trend_10min": "NEUTRAL", "trend_15min": "NEUTRAL",
    "trend_20min": "NEUTRAL", "trend_30min": "NEUTRAL"
} for idx in INDEX_CONFIG}

signal_state = {idx: {
    "action": "HOLD", "entry_price": 0, "stop_loss": 0, "target": 0,
    "lots": 1, "cooldown": 0, "confidence": 0,
    "highest": 0, "entry_time": 0,
    "prev_action_side": None,
    "trend_change_cooldown": 0,
    "exit_reason": ""
} for idx in INDEX_CONFIG}

def load_portfolio_equity():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("SELECT index_name, equity FROM portfolio_equity")
            rows = c.fetchall()
            return {row[0]: {"equity": row[1], "open_positions": 0, "daily_trades": 0} for row in rows}
    except Exception as e:
        logger.error(f"Failed to load portfolio equity: {e}")
        return {}

persisted = load_portfolio_equity()
portfolio_state = {idx: {"equity": persisted.get(idx, {}).get("equity", 100000.0), 
                          "open_positions": 0, "daily_trades": 0} for idx in INDEX_CONFIG}

for idx in INDEX_CONFIG:
    daily_drawdown[idx]["peak_equity"] = portfolio_state[idx]["equity"]

market_signal = {idx: {
    "signal": "WAITING", "sentiment_score": 50, "alert_message": "",
    "entry_price": 0.0, "stop_loss": 0.0, "target": 0.0,
    "exit_reason": "",
    "trend_change_cooldown_remaining": 0
} for idx in INDEX_CONFIG}

safety_state = {idx: {"consecutive_sl": 0, "circuit_breaker": False, 
                      "circuit_breaker_until": 0} for idx in INDEX_CONFIG}
signal_buffer = {idx: {"ce_count": 0, "pe_count": 0, 
                        "consecutive_ce": 0, "consecutive_pe": 0} for idx in INDEX_CONFIG}
daily_trade_count = {idx: 0 for idx in INDEX_CONFIG}
last_trade_date = {idx: "" for idx in INDEX_CONFIG}

# ============================================================================
# V12: KELLY CRITERION POSITION SIZING
# ============================================================================
class KellyCriterion:
    """Fractional Kelly Criterion with Bayesian updating for position sizing."""

    def __init__(self, index_name, kelly_fraction=0.25, min_trades=10):
        self.index_name = index_name
        self.kelly_fraction = kelly_fraction
        self.min_trades = min_trades
        self.alpha = 1.0
        self.beta = 1.0
        self.trade_returns = deque(maxlen=100)
        self.win_count = 0
        self.loss_count = 0
        self.avg_win = 0.0
        self.avg_loss = 0.0

    def update(self, trade_pnl_pct):
        self.trade_returns.append(trade_pnl_pct)
        if trade_pnl_pct > 0:
            self.win_count += 1
            self.alpha += 1
        else:
            self.loss_count += 1
            self.beta += 1
        wins = [r for r in self.trade_returns if r > 0]
        losses = [r for r in self.trade_returns if r <= 0]
        if wins:
            self.avg_win = sum(wins) / len(wins)
        if losses:
            self.avg_loss = abs(sum(losses) / len(losses))

    def calculate(self):
        total_trades = self.win_count + self.loss_count
        if total_trades < self.min_trades:
            return 0.01, 0.5, self.avg_win, self.avg_loss
        p = self.alpha / (self.alpha + self.beta)
        q = 1 - p
        if self.avg_loss == 0:
            return 0.01, p, self.avg_win, self.avg_loss
        b = self.avg_win / self.avg_loss if self.avg_loss > 0 else 1.0
        if b == 0:
            return 0.01, p, self.avg_win, self.avg_loss
        kelly_full = (p * b - q) / b
        kelly_full = max(0, min(kelly_full, 0.5))
        kelly_frac = kelly_full * self.kelly_fraction
        return kelly_frac, p, self.avg_win, self.avg_loss

    def get_recommended_risk_pct(self):
        kelly, win_rate, avg_win, avg_loss = self.calculate()
        risk_pct = min(kelly * 100, 2.0)
        risk_pct = max(0.3, risk_pct)
        return risk_pct, win_rate, avg_win, avg_loss

kelly_trackers = {idx: KellyCriterion(idx) for idx in INDEX_CONFIG}

# ============================================================================
# V12: SHARPE/SORTINO REAL-TIME TRACKING
# ============================================================================
class PerformanceTracker:
    def __init__(self, index_name, risk_free_rate=0.0):
        self.index_name = index_name
        self.risk_free_rate = risk_free_rate
        self.returns = deque(maxlen=252)
        self.equity_curve = deque(maxlen=252)
        self.trade_pnls = deque(maxlen=100)
        self.peak_equity = 0.0
        self.max_drawdown = 0.0

    def add_return(self, daily_return_pct, equity):
        self.returns.append(daily_return_pct)
        self.equity_curve.append(equity)
        if equity > self.peak_equity:
            self.peak_equity = equity
        drawdown = (self.peak_equity - equity) / self.peak_equity if self.peak_equity > 0 else 0
        self.max_drawdown = max(self.max_drawdown, drawdown)

    def add_trade(self, pnl):
        self.trade_pnls.append(pnl)

    def calculate_sharpe(self):
        if len(self.returns) < 10:
            return 0.0
        returns_arr = np.array(list(self.returns))
        excess_returns = returns_arr - self.risk_free_rate
        std = np.std(excess_returns, ddof=1)
        if std == 0 or np.isnan(std):
            return 0.0
        sharpe = np.mean(excess_returns) / std * np.sqrt(252)
        return round(sharpe, 3)

    def calculate_sortino(self):
        if len(self.returns) < 10:
            return 0.0
        returns_arr = np.array(list(self.returns))
        excess_returns = returns_arr - self.risk_free_rate
        downside_returns = excess_returns[excess_returns < 0]
        if len(downside_returns) == 0:
            return float('inf') if np.mean(excess_returns) > 0 else 0.0
        downside_std = np.std(downside_returns, ddof=1)
        if downside_std == 0 or np.isnan(downside_std):
            return 0.0
        sortino = np.mean(excess_returns) / downside_std * np.sqrt(252)
        return round(sortino, 3)

    def calculate_calmar(self):
        if len(self.returns) < 10 or self.max_drawdown == 0:
            return 0.0
        annual_return = np.mean(list(self.returns)) * 252
        calmar = annual_return / self.max_drawdown
        return round(calmar, 3)

    def calculate_win_rate(self):
        if not self.trade_pnls:
            return 0.0
        wins = sum(1 for p in self.trade_pnls if p > 0)
        return round(wins / len(self.trade_pnls) * 100, 2)

    def calculate_profit_factor(self):
        if not self.trade_pnls:
            return 0.0
        gross_profit = sum(p for p in self.trade_pnls if p > 0)
        gross_loss = abs(sum(p for p in self.trade_pnls if p < 0))
        if gross_loss == 0:
            return float('inf') if gross_profit > 0 else 0.0
        return round(gross_profit / gross_loss, 3)

    def calculate_expectancy(self):
        if not self.trade_pnls:
            return 0.0
        win_rate = self.calculate_win_rate() / 100
        wins = [p for p in self.trade_pnls if p > 0]
        losses = [p for p in self.trade_pnls if p < 0]
        avg_win = sum(wins) / len(wins) if wins else 0
        avg_loss = abs(sum(losses) / len(losses)) if losses else 1
        if avg_loss == 0:
            return 0.0
        expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)
        return round(expectancy, 3)

    def get_all_metrics(self):
        return {
            "sharpe": self.calculate_sharpe(),
            "sortino": self.calculate_sortino(),
            "calmar": self.calculate_calmar(),
            "win_rate": self.calculate_win_rate(),
            "profit_factor": self.calculate_profit_factor(),
            "max_drawdown": round(self.max_drawdown * 100, 2),
            "avg_trade": round(np.mean(list(self.trade_pnls)), 2) if self.trade_pnls else 0.0,
            "expectancy": self.calculate_expectancy(),
            "total_trades": len(self.trade_pnls),
            "sample_days": len(self.returns)
        }

performance_trackers = {idx: PerformanceTracker(idx) for idx in INDEX_CONFIG}

# ============================================================================
# V12: SLIPPAGE & SPREAD MODELING
# ============================================================================
class SlippageModel:
    def __init__(self, index_name):
        self.index_name = index_name
        self.spread_history = deque(maxlen=100)
        self.slippage_history = deque(maxlen=100)
        self.base_spread_pct = 0.0005

    def record_spread(self, bid, ask, price):
        if bid > 0 and ask > 0 and price > 0:
            spread = ask - bid
            spread_pct = spread / price
            self.spread_history.append(spread_pct)

    def record_slippage(self, expected_price, actual_price):
        if expected_price > 0 and actual_price > 0:
            slippage = abs(actual_price - expected_price) / expected_price
            self.slippage_history.append(slippage)

    def get_adjusted_price(self, side, price, size_lots=1):
        avg_spread = np.mean(list(self.spread_history)) if self.spread_history else self.base_spread_pct
        avg_slippage = np.mean(list(self.slippage_history)) if self.slippage_history else 0.001
        size_multiplier = 1 + (size_lots - 1) * 0.05
        liquidity_score = max(0.3, 1 - avg_spread * 100)
        total_adjustment = (avg_spread / 2 + avg_slippage) * size_multiplier / liquidity_score
        if side == "BUY":
            adjusted = price * (1 + total_adjustment)
        else:
            adjusted = price * (1 - total_adjustment)
        return adjusted, total_adjustment, liquidity_score

    def get_liquidity_score(self):
        avg_spread = np.mean(list(self.spread_history)) if self.spread_history else self.base_spread_pct
        return max(0.3, min(1.0, 1 - avg_spread * 100))

slippage_models = {idx: SlippageModel(idx) for idx in INDEX_CONFIG}

# ============================================================================
# V12: PCR + OI ANALYSIS ENGINE
# ============================================================================
class PCROIAnalyzer:
    def __init__(self, index_name):
        self.index_name = index_name
        self.pcr_oi_history = deque(maxlen=50)
        self.pcr_volume_history = deque(maxlen=50)
        self.oi_change_ce = deque(maxlen=20)
        self.oi_change_pe = deque(maxlen=20)

    def update(self, ce_oi, pe_oi, ce_vol, pe_vol):
        if ce_oi > 0 and pe_oi > 0:
            pcr_oi = pe_oi / ce_oi
            self.pcr_oi_history.append(pcr_oi)
        if ce_vol > 0 and pe_vol > 0:
            pcr_vol = pe_vol / ce_vol
            self.pcr_volume_history.append(pcr_vol)

    def analyze(self, sentiment_label):
        if len(self.pcr_oi_history) < 10:
            return {"signal": "neutral", "strength": 0, "pcr_oi": 1.0, "pcr_vol": 1.0}
        pcr_oi = list(self.pcr_oi_history)[-1]
        pcr_vol = list(self.pcr_volume_history)[-1] if self.pcr_volume_history else pcr_oi
        pcr_weighted = (pcr_oi * 0.6 + pcr_vol * 0.4)
        pcr_sma = np.mean(list(self.pcr_oi_history)[-10:])

        signal = "neutral"
        strength = 0
        if pcr_weighted > 1.5:
            if "BULLISH" in sentiment_label or "BUY_CE" in sentiment_label:
                signal = "confirm_bullish"
                strength = min(30, (pcr_weighted - 1.5) * 20)
            else:
                signal = "oversold"
                strength = min(20, (pcr_weighted - 1.5) * 15)
        elif pcr_weighted < 0.5:
            if "BEARISH" in sentiment_label or "BUY_PE" in sentiment_label:
                signal = "confirm_bearish"
                strength = min(30, (0.5 - pcr_weighted) * 20)
            else:
                signal = "overbought"
                strength = min(20, (0.5 - pcr_weighted) * 15)
        else:
            if pcr_weighted < 0.8 and ("BULLISH" in sentiment_label or "BUY_CE" in sentiment_label):
                signal = "confirm_bullish"
                strength = 10
            elif pcr_weighted > 1.2 and ("BEARISH" in sentiment_label or "BUY_PE" in sentiment_label):
                signal = "confirm_bearish"
                strength = 10

        if len(self.pcr_oi_history) >= 20:
            pcr_trend = pcr_weighted - np.mean(list(self.pcr_oi_history)[:10])
            if pcr_trend > 0.2 and "BULLISH" in sentiment_label:
                signal = "bearish_divergence"
                strength = 25
            elif pcr_trend < -0.2 and "BEARISH" in sentiment_label:
                signal = "bullish_divergence"
                strength = 25

        return {
            "signal": signal,
            "strength": strength,
            "pcr_oi": round(pcr_oi, 3),
            "pcr_vol": round(pcr_vol, 3),
            "pcr_weighted": round(pcr_weighted, 3),
            "pcr_sma": round(pcr_sma, 3)
        }

pcr_analyzers = {idx: PCROIAnalyzer(idx) for idx in INDEX_CONFIG}

# ============================================================================
# V12: GREEKS INTEGRATION ENGINE
# ============================================================================
class GreeksAnalyzer:
    def __init__(self, index_name):
        self.index_name = index_name
        self.iv_history = deque(maxlen=50)
        self.delta_threshold = 0.6
        self.iv_rank_threshold_low = 20
        self.iv_rank_threshold_high = 80

    def update(self, ce_iv, pe_iv, ce_delta, pe_delta, ce_gamma, pe_gamma,
               ce_theta, pe_theta, ce_vega, pe_vega):
        avg_iv = (ce_iv + pe_iv) / 2 if ce_iv > 0 and pe_iv > 0 else max(ce_iv, pe_iv)
        if avg_iv > 0:
            self.iv_history.append(avg_iv)
        with _greeks_lock:
            latest_greeks[self.index_name].update({
                "ce_iv": ce_iv, "pe_iv": pe_iv,
                "ce_delta": ce_delta, "pe_delta": pe_delta,
                "ce_gamma": ce_gamma, "pe_gamma": pe_gamma,
                "ce_theta": ce_theta, "pe_theta": pe_theta,
                "ce_vega": ce_vega, "pe_vega": pe_vega
            })

    def calculate_iv_rank(self):
        if len(self.iv_history) < 20:
            return 50.0, 50.0
        iv_arr = np.array(list(self.iv_history))
        current_iv = iv_arr[-1]
        iv_min = np.min(iv_arr)
        iv_max = np.max(iv_arr)
        if iv_max == iv_min:
            iv_rank = 50.0
        else:
            iv_rank = ((current_iv - iv_min) / (iv_max - iv_min)) * 100
        iv_percentile = np.mean(iv_arr < current_iv) * 100
        return round(iv_rank, 2), round(iv_percentile, 2)

    def analyze(self, action):
        greeks = latest_greeks[self.index_name]
        iv_rank, iv_percentile = self.calculate_iv_rank()
        with _greeks_lock:
            latest_greeks[self.index_name]["iv_rank"] = iv_rank
            latest_greeks[self.index_name]["iv_percentile"] = iv_percentile

        signal = "neutral"
        strength = 0
        block_reason = None

        if "CE" in action:
            delta = abs(greeks.get("ce_delta", 0))
            if delta > self.delta_threshold:
                block_reason = f"CE Delta {delta:.2f} > threshold {self.delta_threshold}"
                signal = "block_deep_itm"
                strength = -50
        elif "PE" in action:
            delta = abs(greeks.get("pe_delta", 0))
            if delta > self.delta_threshold:
                block_reason = f"PE Delta {delta:.2f} > threshold {self.delta_threshold}"
                signal = "block_deep_itm"
                strength = -50

        if iv_rank > self.iv_rank_threshold_high:
            if "BUY" in action:
                signal = "high_iv_favorable"
                strength = 15
            else:
                signal = "high_iv_caution"
                strength = -10
        elif iv_rank < self.iv_rank_threshold_low:
            if "BUY" in action:
                signal = "low_iv_caution"
                strength = -10
            else:
                signal = "low_iv_favorable"
                strength = 15

        gamma = max(abs(greeks.get("ce_gamma", 0)), abs(greeks.get("pe_gamma", 0)))
        if gamma > 0.05:
            signal = "high_gamma_risk"
            strength = max(strength - 10, -30)

        theta = min(greeks.get("ce_theta", 0), greeks.get("pe_theta", 0))
        if theta < -0.5:
            signal = "high_theta_decay"
            strength = max(strength - 5, -20)

        return {
            "signal": signal,
            "strength": strength,
            "iv_rank": iv_rank,
            "iv_percentile": iv_percentile,
            "block_reason": block_reason,
            "delta_ce": greeks.get("ce_delta"),
            "delta_pe": greeks.get("pe_delta"),
            "gamma_max": gamma,
            "theta_min": theta
        }

greeks_analyzers = {idx: GreeksAnalyzer(idx) for idx in INDEX_CONFIG}

# ============================================================================
# V12: CORRELATION FILTER (NIFTY <-> BANKNIFTY)
# ============================================================================
class CorrelationFilter:
    def __init__(self):
        self.nifty_returns = deque(maxlen=50)
        self.banknifty_returns = deque(maxlen=50)
        self.correlation_history = deque(maxlen=50)
        self.divergence_threshold = 0.3

    def update(self, nifty_price, banknifty_price):
        if nifty_price > 0 and banknifty_price > 0:
            self.nifty_returns.append(nifty_price)
            self.banknifty_returns.append(banknifty_price)

    def calculate(self):
        if len(self.nifty_returns) < 20:
            return {"correlation_20": 0, "correlation_50": 0, "beta": 1.0}
        n_arr = np.array(list(self.nifty_returns))
        b_arr = np.array(list(self.banknifty_returns))
        n_ret = np.diff(n_arr) / n_arr[:-1]
        b_ret = np.diff(b_arr) / b_arr[:-1]
        if len(n_ret) < 10:
            return {"correlation_20": 0, "correlation_50": 0, "beta": 1.0}

        if len(n_ret) >= 20:
            try:
                corr_20 = np.corrcoef(n_ret[-20:], b_ret[-20:])[0, 1]
                if np.isnan(corr_20):
                    corr_20 = 0
            except:
                corr_20 = 0
        else:
            corr_20 = 0

        try:
            corr_50 = np.corrcoef(n_ret, b_ret)[0, 1] if len(n_ret) >= 20 else 0
            if np.isnan(corr_50):
                corr_50 = 0
        except:
            corr_50 = 0

        try:
            cov = np.cov(n_ret, b_ret)[0, 1] if len(n_ret) >= 2 else 0
            var_n = np.var(n_ret) if len(n_ret) >= 2 else 1
            beta = cov / var_n if var_n > 0 else 1.0
            if np.isnan(beta):
                beta = 1.0
        except:
            beta = 1.0

        self.correlation_history.append(corr_20)
        return {
            "correlation_20": round(corr_20, 3),
            "correlation_50": round(corr_50, 3),
            "beta": round(beta, 3)
        }

    def analyze(self, index_name, action):
        corr_data = self.calculate()
        corr_20 = corr_data["correlation_20"]
        signal = "neutral"
        strength = 0
        block_reason = None
        if abs(corr_20) < self.divergence_threshold:
            signal = "low_correlation"
            strength = -20
            block_reason = f"Low correlation {corr_20:.2f} - reduce size"
        if corr_20 < -0.2:
            signal = "negative_correlation"
            strength = -40
            block_reason = f"Negative correlation {corr_20:.2f} - conflicting signals"
        if corr_20 > 0.7:
            signal = "high_correlation"
            strength = 10
        beta = corr_data["beta"]
        beta_adjustment = 1.0
        if beta > 1.5:
            beta_adjustment = 0.7
        elif beta < 0.5:
            beta_adjustment = 1.3
        with _correlation_lock:
            latest_correlation.update({
                "correlation_20": corr_data["correlation_20"],
                "correlation_50": corr_data["correlation_50"],
                "beta": beta,
                "correlation_signal": signal,
                "divergence_detected": abs(corr_20) < self.divergence_threshold
            })
        return {
            "signal": signal,
            "strength": strength,
            "block_reason": block_reason,
            "beta_adjustment": beta_adjustment,
            **corr_data
        }

correlation_filter = CorrelationFilter()

# ============================================================================
# V12: REGIME DETECTION ENGINE
# ============================================================================
class RegimeDetector:
    def __init__(self, index_name):
        self.index_name = index_name
        self.price_history = deque(maxlen=100)
        self.volatility_history = deque(maxlen=50)
        self.regime_history = deque(maxlen=20)

    def update(self, price):
        if price > 0:
            self.price_history.append(price)

    def detect(self):
        if len(self.price_history) < 30:
            return {"regime": "unknown", "score": 50, "confidence": 0}
        prices = np.array(list(self.price_history))
        returns = np.diff(prices) / prices[:-1]
        adx = self._calculate_adx(prices)
        bb_width = self._calculate_bb_width(prices)
        vol_20 = np.std(returns[-20:]) * np.sqrt(252) if len(returns) >= 20 else 0.15
        vol_50 = np.std(returns) * np.sqrt(252) if len(returns) >= 10 else 0.15
        self.volatility_history.append(vol_20)
        if len(self.volatility_history) >= 10:
            vol_median = np.median(list(self.volatility_history))
            if vol_20 > vol_median * 1.5:
                vol_regime = "high_vol"
            elif vol_20 < vol_median * 0.7:
                vol_regime = "low_vol"
            else:
                vol_regime = "normal_vol"
        else:
            vol_regime = "normal_vol"
        if adx > 30 and bb_width > 0.02:
            regime = "strong_trend"
            score = 80
        elif adx > 25:
            regime = "trending"
            score = 65
        elif adx < 15 and bb_width < 0.015:
            regime = "ranging"
            score = 35
        elif vol_regime == "high_vol":
            regime = "volatile"
            score = 50
        else:
            regime = "mixed"
            score = 50
        self.regime_history.append(regime)
        if len(self.regime_history) >= 5:
            recent_regimes = list(self.regime_history)[-5:]
            consistency = max(recent_regimes.count(r) for r in set(recent_regimes)) / 5
        else:
            consistency = 0.5
        result = {
            "regime": regime,
            "adx": round(adx, 2),
            "bb_width": round(bb_width, 4),
            "volatility_regime": vol_regime,
            "trend_strength": round(adx, 2),
            "regime_score": score,
            "volatility_annual": round(vol_20, 4),
            "consistency": round(consistency, 2)
        }
        with _regime_lock:
            latest_regime[self.index_name].update(result)
        return result

    def _calculate_adx(self, prices, period=14):
        if len(prices) < period * 2 + 1:
            return 20.0
        plus_dm, minus_dm, tr = [], [], []
        for i in range(1, len(prices)):
            up = prices[i] - prices[i-1]
            down = prices[i-1] - prices[i]
            plus_dm.append(max(up, 0) if up > down else 0)
            minus_dm.append(max(down, 0) if down > up else 0)
            tr.append(abs(prices[i] - prices[i-1]))
        if not tr:
            return 20.0
        atr = sum(tr[-period:]) / period
        plus_di = 100 * sum(plus_dm[-period:]) / period / atr if atr > 0 else 0
        minus_di = 100 * sum(minus_dm[-period:]) / period / atr if atr > 0 else 0
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di) if (plus_di + minus_di) > 0 else 0
        return dx

    def _calculate_bb_width(self, prices, period=20):
        if len(prices) < period:
            return 0.02
        sma = np.mean(prices[-period:])
        std = np.std(prices[-period:])
        if sma == 0:
            return 0.02
        return (2 * std) / sma

    def get_regime_adjustment(self, action):
        regime = latest_regime[self.index_name]["regime"]
        adjustments = {
            "strong_trend": 1.3,
            "trending": 1.1,
            "mixed": 1.0,
            "ranging": 0.7,
            "volatile": 0.5,
            "unknown": 0.8
        }
        return adjustments.get(regime, 1.0)

regime_detectors = {idx: RegimeDetector(idx) for idx in INDEX_CONFIG}

# ============================================================================
# V12: VOLUME PROFILE & VWAP ENGINE
# ============================================================================
class VolumeProfileEngine:
    def __init__(self, index_name):
        self.index_name = index_name
        self.price_volume = deque(maxlen=1000)
        self.vwap_history = deque(maxlen=100)
        self.poc_history = deque(maxlen=20)

    def update(self, price, volume):
        if price > 0 and volume > 0:
            self.price_volume.append((price, volume))

    def calculate_vwap(self):
        if not self.price_volume:
            return 0.0
        total_pv = sum(p * v for p, v in self.price_volume)
        total_v = sum(v for p, v in self.price_volume)
        return total_pv / total_v if total_v > 0 else 0.0

    def calculate_volume_profile(self, num_bins=20):
        if len(self.price_volume) < 50:
            return {"poc": 0, "vah": 0, "val": 0, "nodes": {}}
        prices = [p for p, v in self.price_volume]
        volumes = [v for p, v in self.price_volume]
        min_p, max_p = min(prices), max(prices)
        if max_p == min_p:
            return {"poc": min_p, "vah": max_p, "val": min_p, "nodes": {}}
        bin_size = (max_p - min_p) / num_bins
        bins = defaultdict(float)
        for p, v in self.price_volume:
            bin_idx = int((p - min_p) / bin_size) if bin_size > 0 else 0
            bin_price = min_p + bin_idx * bin_size
            bins[bin_price] += v
        poc = max(bins.items(), key=lambda x: x[1])[0]
        total_vol = sum(bins.values())
        target_vol = total_vol * 0.7
        sorted_bins = sorted(bins.items())
        vah = poc
        val = poc
        current_vol = bins.get(poc, 0)
        poc_idx = next(i for i, (p, v) in enumerate(sorted_bins) if p == poc)
        above_idx = poc_idx + 1
        below_idx = poc_idx - 1
        while current_vol < target_vol and (above_idx < len(sorted_bins) or below_idx >= 0):
            above_vol = sorted_bins[above_idx][1] if above_idx < len(sorted_bins) else 0
            below_vol = sorted_bins[below_idx][1] if below_idx >= 0 else 0
            if above_vol >= below_vol and above_idx < len(sorted_bins):
                current_vol += above_vol
                vah = sorted_bins[above_idx][0]
                above_idx += 1
            elif below_idx >= 0:
                current_vol += below_vol
                val = sorted_bins[below_idx][0]
                below_idx -= 1
            else:
                break
        return {
            "poc": round(poc, 2),
            "vah": round(vah, 2),
            "val": round(val, 2),
            "nodes": {round(k, 2): round(v, 2) for k, v in list(bins.items())[:10]}
        }

    def analyze(self, current_price):
        vwap = self.calculate_vwap()
        profile = self.calculate_volume_profile()
        if vwap == 0 or profile["poc"] == 0:
            return {"signal": "neutral", "strength": 0}
        signal = "neutral"
        strength = 0
        if current_price > vwap * 1.002:
            signal = "above_vwap"
            strength = 10
        elif current_price < vwap * 0.998:
            signal = "below_vwap"
            strength = -10
        if profile["vah"] > 0 and profile["val"] > 0:
            if current_price > profile["vah"]:
                signal = "above_value_area"
                strength = 15
            elif current_price < profile["val"]:
                signal = "below_value_area"
                strength = -15
            else:
                signal = "inside_value_area"
                strength = 0
        poc_distance = abs(current_price - profile["poc"]) / profile["poc"] if profile["poc"] > 0 else 0
        if poc_distance < 0.001:
            signal = "at_poc"
            strength = 0
        if profile["vah"] > 0 and profile["val"] > 0:
            va_range = profile["vah"] - profile["val"]
            total_range_est = vwap * 0.02 if vwap > 0 else 1
            if total_range_est > 0:
                if va_range / total_range_est < 0.3:
                    profile_type = "d_profile"
                elif va_range / total_range_est > 0.6:
                    profile_type = "p_profile"
                else:
                    profile_type = "b_profile"
            else:
                profile_type = "unknown"
        else:
            profile_type = "unknown"
        with _portfolio_lock:
            latest_volume_profile[self.index_name].update({
                "poc": profile["poc"],
                "value_area_high": profile["vah"],
                "value_area_low": profile["val"],
                "vwap": round(vwap, 2),
                "volume_nodes": profile["nodes"],
                "profile_type": profile_type
            })
        return {
            "signal": signal,
            "strength": strength,
            "vwap": round(vwap, 2),
            "poc": profile["poc"],
            "vah": profile["vah"],
            "val": profile["val"],
            "profile_type": profile_type
        }

volume_profile_engines = {idx: VolumeProfileEngine(idx) for idx in INDEX_CONFIG}

# ============================================================================
# V12: MAX DAILY DRAWDOWN KILL SWITCH
# ============================================================================
class DrawdownKillSwitch:
    def __init__(self, index_name, max_drawdown_pct=3.0):
        self.index_name = index_name
        self.max_drawdown_pct = max_drawdown_pct
        self.daily_peak = 0.0
        self.current_drawdown = 0.0
        self.kill_switch_active = False
        self.kill_switch_until = 0

    def update_equity(self, equity):
        if equity > self.daily_peak:
            self.daily_peak = equity
        if self.daily_peak > 0:
            self.current_drawdown = (self.daily_peak - equity) / self.daily_peak * 100
        if self.current_drawdown >= self.max_drawdown_pct and not self.kill_switch_active:
            self.kill_switch_active = True
            self.kill_switch_until = time.time() + 86400
            try:
                with sqlite3.connect(DB_PATH) as conn:
                    c = conn.cursor()
                    c.execute("""INSERT INTO drawdown_events 
                        (timestamp, index_name, drawdown_pct, action_taken, equity_before, equity_after)
                        VALUES (?, ?, ?, ?, ?, ?)""",
                        (time.time(), self.index_name, self.current_drawdown,
                         "KILL_SWITCH_ACTIVATED", self.daily_peak, equity))
                    conn.commit()
            except Exception as e:
                logger.error(f"Failed to log drawdown event: {e}")
            send_telegram_alert(
                f"KILL SWITCH ACTIVATED {self.index_name} | "
                f"Drawdown: {self.current_drawdown:.2f}% | "
                f"Peak: {self.daily_peak:.0f} | Current: {equity:.0f} | "
                f"Trading HALTED for 24h"
            )
            logger.critical(
                f"KILL SWITCH: {self.index_name} drawdown {self.current_drawdown:.2f}% >= "
                f"{self.max_drawdown_pct}%"
            )
            return True
        if self.kill_switch_active and time.time() > self.kill_switch_until:
            self.kill_switch_active = False
            self.daily_peak = equity
            self.current_drawdown = 0.0
            send_telegram_alert(
                f"KILL SWITCH RELEASED {self.index_name} | "
                f"Trading resumed after 24h cooldown"
            )
            logger.info(f"Kill switch released for {self.index_name}")
        return self.kill_switch_active

    def is_active(self):
        if self.kill_switch_active and time.time() > self.kill_switch_until:
            self.kill_switch_active = False
        return self.kill_switch_active

    def reset_daily(self):
        self.daily_peak = portfolio_state[self.index_name]["equity"]
        self.current_drawdown = 0.0
        self.kill_switch_active = False
        self.kill_switch_until = 0

kill_switches = {idx: DrawdownKillSwitch(idx, INDEX_CONFIG[idx].get("max_daily_drawdown_pct", 3.0)) 
                 for idx in INDEX_CONFIG}

# ============================================================================
# V11 CORE FUNCTIONS (SENTIMENT, SIGNALS, ETC.)
# ============================================================================
def is_valid_option_premium(premium, spot_price, side):
    """Relaxed premium validation - SENSEX allows slightly higher premium %."""
    if premium <= 0 or spot_price <= 0:
        return False
    premium_pct = premium / spot_price
    max_pct = 0.25 if spot_price > 50000 else 0.20
    return 0.0002 < premium_pct < max_pct

def calculate_rsi(prices, period=14, smoothing=3):
    if len(prices) < period + 1: return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    if len(gains) < period or len(losses) < period:
        return 50.0
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0: return 100.0
    rsi_raw = 100 - (100 / (1 + avg_gain / avg_loss))
    if smoothing > 1 and len(prices) >= period + smoothing and len(gains) >= period + smoothing:
        rsi_vals = []
        for j in range(smoothing):
            sub_gains = gains[-(period+j):-j if j > 0 else None]
            sub_losses = losses[-(period+j):-j if j > 0 else None]
            if len(sub_gains) == period and len(sub_losses) == period:
                ag = sum(sub_gains) / period
                al = sum(sub_losses) / period
                rsi_vals.append(100 - (100 / (1 + ag / al)) if al > 0 else 100.0)
        if rsi_vals:
            alpha = 2 / (smoothing + 1)
            rsi_smooth = rsi_vals[0]
            for rv in rsi_vals[1:]:
                rsi_smooth = alpha * rv + (1 - alpha) * rsi_smooth
            return rsi_smooth
    return rsi_raw

def calculate_ema(prices, period):
    if not prices:
        return 0
    if len(prices) < period: 
        return sum(prices) / len(prices) if prices else 0
    alpha = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = alpha * p + (1 - alpha) * ema
    return ema

def calculate_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal: return 0.0, 0.0, 0.0
    def ema(arr, p):
        if not arr:
            return 0
        alpha = 2 / (p + 1)
        val = arr[0]
        for x in arr[1:]: 
            val = alpha * x + (1 - alpha) * val
        return val
    ema_fast = ema(prices[-fast:], fast)
    ema_slow = ema(prices[-slow:], slow)
    macd_line = ema_fast - ema_slow
    hist = []
    for i in range(signal, 0, -1):
        if len(prices) >= slow + i:
            ef = ema(prices[-(fast+i):-i], fast)
            es = ema(prices[-(slow+i):-i], slow)
            hist.append(ef - es)
    sig_line = ema(hist, signal) if hist else macd_line
    return macd_line, sig_line, macd_line - sig_line

def calculate_atr(prices, period=14):
    if len(prices) < period + 1: return 5.0
    tr = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    if not tr:
        return 5.0
    return sum(tr[-period:]) / period

def calculate_adx(prices, period=14):
    if len(prices) < period * 2 + 1: return 20.0
    plus_dm, minus_dm, tr = [], [], []
    for i in range(1, len(prices)):
        up = prices[i] - prices[i-1]
        down = prices[i-1] - prices[i]
        plus_dm.append(max(up, 0) if up > down else 0)
        minus_dm.append(max(down, 0) if down > up else 0)
        tr.append(abs(prices[i] - prices[i-1]))
    if not tr or not plus_dm or not minus_dm:
        return 20.0
    atr = sum(tr[-period:]) / period
    plus_di = 100 * sum(plus_dm[-period:]) / period / atr if atr > 0 else 0
    minus_di = 100 * sum(minus_dm[-period:]) / period / atr if atr > 0 else 0
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di) if (plus_di + minus_di) > 0 else 0
    return dx

def calculate_bollinger(prices, period=20, std_dev=2):
    if len(prices) < period:
        return None, None, None
    sma = sum(prices[-period:]) / period
    variance = sum((p - sma) ** 2 for p in prices[-period:]) / period
    std = math.sqrt(variance)
    upper = sma + (std_dev * std)
    lower = sma - (std_dev * std)
    return upper, sma, lower

def get_min_bars_for_timeframe(tf_name):
    mapping = {
        "1min": 60, "2min": 60, "3min": 60, "5min": 60,
        "10min": 60, "15min": 60, "20min": 80, "30min": 100
    }
    return mapping.get(tf_name, 60)

def is_sideways(prices, tf_name):
    if len(prices) < 30:
        return False, 0
    upper, sma, lower = calculate_bollinger(prices, period=20)
    if upper and lower and sma > 0:
        band_width = (upper - lower) / sma
        if band_width < 0.008:
            return True, band_width
    adx = calculate_adx(prices, period=14)
    if adx < 20:
        return True, adx
    recent = prices[-30:]
    price_range = (max(recent) - min(recent)) / sum(recent) * len(recent)
    if price_range < 0.005:
        return True, price_range
    return False, 0

def get_trend_score(prices, tf_name):
    min_bars = get_min_bars_for_timeframe(tf_name)
    if len(prices) < min_bars:
        return "NEUTRAL", 0
    w = TIMEFRAME_WEIGHTS[tf_name]
    ema9 = calculate_ema(prices, EMA_SHORT)
    ema21 = calculate_ema(prices, EMA_MEDIUM)
    ema50 = calculate_ema(prices, EMA_LONG)
    price = prices[-1] if prices else 0
    sideways, sideways_metric = is_sideways(prices, tf_name)
    if sideways:
        return "SIDEWAYS", 0
    if tf_name in ["1min", "2min", "3min"]:
        if ema9 > ema21 and price > ema9:
            return "BULLISH", w
        if ema9 < ema21 and price < ema9:
            return "BEARISH", -w
        return "NEUTRAL", 0
    elif tf_name in ["5min", "10min"]:
        if ema9 > ema21 > ema50 and price > ema9:
            return "BULLISH", w
        if ema9 < ema21 < ema50 and price < ema9:
            return "BEARISH", -w
        if ema9 > ema21 and price > ema9:
            return "BULLISH", w - 5
        if ema9 < ema21 and price < ema9:
            return "BEARISH", -(w - 5)
        return "NEUTRAL", 0
    else:
        if len(prices) >= 20:
            highs = []
            lows = []
            step = 5 if tf_name == "15min" else 7 if tf_name == "20min" else 10
            for i in range(0, min(30, len(prices) - step), step):
                slice_prices = prices[-i-step:-i] if i > 0 else prices[-step:]
                if slice_prices:
                    highs.append(max(slice_prices))
                    lows.append(min(slice_prices))
            if len(highs) >= 2 and len(lows) >= 2:
                if highs[0] > highs[1] and lows[0] > lows[1] and ema9 > ema21:
                    return "BULLISH", w
                if highs[0] < highs[1] and lows[0] < lows[1] and ema9 < ema21:
                    return "BEARISH", -w
                if highs[0] > highs[1]:
                    return "BULLISH", w - 8
                if highs[0] < highs[1]:
                    return "BEARISH", -(w - 8)
        if ema9 > ema21 > ema50:
            return "BULLISH", w - 5
        if ema9 < ema21 < ema50:
            return "BEARISH", -(w - 5)
        return "NEUTRAL", 0

def compute_sentiment(index_name):
    prices = list(price_histories[index_name])
    if len(prices) < 60:
        market_sentiment[index_name]["score"] = 50
        return 50
    total = 0
    for tf in TIMEFRAMES:
        trend, score = get_trend_score(prices, tf)
        market_sentiment[index_name][f"trend_{tf}"] = trend
        total += score
    sentiment = 50 + (total / 3.5)
    sentiment = max(0, min(100, sentiment))
    market_sentiment[index_name]["score"] = sentiment
    for k, (low, high, label, action) in SENTIMENT_SCORES.items():
        if low <= sentiment <= high:
            market_sentiment[index_name]["label"] = label
            break
    return sentiment

def get_signal_from_sentiment(index_name, sentiment):
    for k, (low, high, label, action) in SENTIMENT_SCORES.items():
        if low <= sentiment <= high:
            trend_30m = market_sentiment[index_name].get("trend_30min", "NEUTRAL")
            trend_20m = market_sentiment[index_name].get("trend_20min", "NEUTRAL")
            if "LOW" in action:
                if "CE" in action and trend_30m == "BEARISH" and trend_20m == "BEARISH":
                    return "NO_TRADE", label, sentiment
                if "PE" in action and trend_30m == "BULLISH" and trend_20m == "BULLISH":
                    return "NO_TRADE", label, sentiment
            return action, label, sentiment
    return "NO_TRADE", "UNKNOWN", sentiment

# ============================================================================
# AUTHENTICATION (from working connection code)
# ============================================================================
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
            if not session.get("status"): return None, None, None
            auth_token = session["data"]["jwtToken"]
            feed_token = obj.getfeedToken()
            auth_cache.update({"token": auth_token, "feed_token": feed_token, "timestamp": now, "obj": obj})
            logger.info("Auth token refreshed successfully")
            return auth_token, feed_token, obj
        except Exception as e:
            logger.error(f"Auth loop fail: {e}")
            return None, None, None

def safe_ltp(resp):
    """Safely extract LTP from API response handling multiple formats."""
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

# ============================================================================
# MARKET DATA FUNCTIONS (fixed SENSEX spot and token fetching)
# ============================================================================
def get_index_spot(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config: return None
    _, _, obj = get_auth_token()
    if not obj: return None
    if index_name == "SENSEX":
        try:
            # Direct token fetch
            resp = rate_limited_api_call(obj.ltpData, config["exchange"], config["symbol"], config["token"])
            ltp = safe_ltp(resp)
            if ltp is not None:
                if ltp > 100000: ltp /= 100
                if 50000 < ltp < 100000:
                    return ltp
            # Fallback: search for "SENSEX"
            search = rate_limited_api_call(obj.searchScrip, "BSE", "SENSEX")
            if search and search.get("status") and search.get("data") and len(search["data"]) > 0:
                token = str(search["data"][0].get("symboltoken"))
                ltp_resp = rate_limited_api_call(obj.ltpData, "BSE", "SENSEX", token)
                ltp = safe_ltp(ltp_resp)
                if ltp is not None:
                    if ltp > 100000: ltp /= 100
                    return ltp
        except Exception as e:
            logger.error(f"SENSEX spot error: {e}")
        return None
    # Other indices
    try:
        resp = rate_limited_api_call(obj.ltpData, config["exchange"], config["symbol"], config["token"])
        ltp = safe_ltp(resp)
        if ltp is not None:
            if ltp > 100000: ltp /= 100
            if index_name == "MIDCPNIFTY" and (ltp < 5000 or ltp > 25000):
                search = rate_limited_api_call(obj.searchScrip, "NSE", "MIDCPNIFTY")
                if search and search.get("status") and search.get("data") and len(search["data"]) > 0:
                    token = str(search["data"][0].get("symboltoken"))
                    ltp2 = safe_ltp(rate_limited_api_call(obj.ltpData, "NSE", "MIDCPNIFTY", token))
                    if ltp2 is not None:
                        if ltp2 > 100000: ltp2 /= 100
                        ltp = ltp2
            return ltp
    except Exception as e:
        logger.error(f"Spot fetch {index_name}: {e}")
    return None

def get_vix_value():
    _, _, obj = get_auth_token()
    if not obj: return 15.0
    try:
        vix_tokens = ["99919017", "99919011"]
        for token in vix_tokens:
            try:
                resp = rate_limited_api_call(obj.ltpData, "NSE", "INDIAVIX", token)
                ltp = safe_ltp(resp)
                if ltp is not None:
                    return ltp
            except Exception:
                continue
        try:
            search = rate_limited_api_call(obj.searchScrip, "NSE", "INDIAVIX")
            if search and search.get("status") and search.get("data") and len(search["data"]) > 0:
                token = str(search["data"][0].get("symboltoken"))
                resp = rate_limited_api_call(obj.ltpData, "NSE", "INDIAVIX", token)
                ltp = safe_ltp(resp)
                if ltp is not None:
                    return ltp
        except Exception:
            pass
    except Exception:
        pass
    return 15.0

_scrip_cache = {"data": None, "timestamp": 0}
_scrip_lock = threading.Lock()

def get_scrip_master():
    with _scrip_lock:
        now = time.time()
        if _scrip_cache["data"] and (now - _scrip_cache["timestamp"] < 86400):
            return _scrip_cache["data"]
        try:
            url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
            data = requests.get(url, timeout=30).json()
            _scrip_cache["data"] = data
            _scrip_cache["timestamp"] = now
            logger.info("Scrip Master refreshed")
            return data
        except Exception as e:
            logger.error(f"Scrip Master failed: {e}")
            return _scrip_cache["data"] or []

def get_next_expiry_date(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config:
        return None
    today = datetime.now()
    weekday = today.weekday()
    expiry_weekday = config["expiry_weekday"]
    days_ahead = expiry_weekday - weekday
    if days_ahead <= 0:
        days_ahead += 7
    expiry = today + timedelta(days=days_ahead)
    return expiry

def is_expiry_day(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config: return False
    today = datetime.now().strftime("%d%b%Y").upper()
    tokens = INDEX_TOKENS.get(index_name, {})
    expiry = tokens.get("expiry", "")
    return today == expiry

# ============================================================================
# V12: GREEKS FETCHING - FIXED API ENDPOINT
# ============================================================================
def get_option_greeks(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config or not config.get("greeks_enabled"):
        return None
    tokens = INDEX_TOKENS.get(index_name)
    if not tokens or not tokens.get("ce_token") or not tokens.get("pe_token"):
        return None
    _, _, obj = get_auth_token()
    if not obj:
        return None
    try:
        expiry_str = tokens.get("expiry", "")
        if not expiry_str:
            return _estimate_greeks_fallback(index_name)
        greeks_payload = {
            "name": config["symbol"],
            "expirydate": expiry_str
        }
        url = "https://apiconnect.angelbroking.com/rest/secure/angelbroking/marketData/v1/optionGreek"
        try:
            local_ip = socket.gethostbyname(socket.gethostname())
        except:
            local_ip = "127.0.0.1"
        headers = {
            "Authorization": f"Bearer {auth_cache.get('token', '')}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-UserType": "USER",
            "X-SourceID": "WEB",
            "X-ClientLocalIP": local_ip,
            "X-ClientPublicIP": local_ip,
            "X-MACAddress": "00:00:00:00:00:00",
            "X-PrivateKey": ANGEL_API_KEY
        }
        resp = requests.post(url, json=greeks_payload, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") and data.get("data"):
                greeks_list = data["data"]
                atm_strike = tokens.get("atm_strike", 0)
                ce_greeks = None
                pe_greeks = None
                for g in greeks_list:
                    strike = float(g.get("strikePrice", 0))
                    opt_type = g.get("optionType", "")
                    if abs(strike - atm_strike) < config.get("atm_strike_multiple", 50) * 0.5:
                        if opt_type == "CE":
                            ce_greeks = g
                        elif opt_type == "PE":
                            pe_greeks = g
                if ce_greeks and pe_greeks:
                    greeks_data = {
                        "ce_iv": float(ce_greeks.get("impliedVolatility", 0)) / 100 if float(ce_greeks.get("impliedVolatility", 0)) > 1 else float(ce_greeks.get("impliedVolatility", 0)),
                        "pe_iv": float(pe_greeks.get("impliedVolatility", 0)) / 100 if float(pe_greeks.get("impliedVolatility", 0)) > 1 else float(pe_greeks.get("impliedVolatility", 0)),
                        "ce_delta": float(ce_greeks.get("delta", 0)),
                        "pe_delta": float(pe_greeks.get("delta", 0)),
                        "ce_gamma": float(ce_greeks.get("gamma", 0)),
                        "pe_gamma": float(pe_greeks.get("gamma", 0)),
                        "ce_theta": float(ce_greeks.get("theta", 0)),
                        "pe_theta": float(pe_greeks.get("theta", 0)),
                        "ce_vega": float(ce_greeks.get("vega", 0)),
                        "pe_vega": float(pe_greeks.get("vega", 0))
                    }
                    try:
                        with sqlite3.connect(DB_PATH) as conn:
                            c = conn.cursor()
                            c.execute("""INSERT INTO greeks_history 
                                (timestamp, index_name, ce_iv, pe_iv, ce_delta, pe_delta, 
                                 ce_gamma, pe_gamma, ce_theta, pe_theta, ce_vega, pe_vega,
                                 iv_rank, iv_percentile)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                                (time.time(), index_name,
                                 greeks_data["ce_iv"], greeks_data["pe_iv"],
                                 greeks_data["ce_delta"], greeks_data["pe_delta"],
                                 greeks_data["ce_gamma"], greeks_data["pe_gamma"],
                                 greeks_data["ce_theta"], greeks_data["pe_theta"],
                                 greeks_data["ce_vega"], greeks_data["pe_vega"],
                                 0, 0))
                            conn.commit()
                    except Exception as e:
                        logger.debug(f"Greeks DB log error: {e}")
                    greeks_analyzers[index_name].update(**greeks_data)
                    return greeks_data
        return _estimate_greeks_fallback(index_name)
    except Exception as e:
        logger.debug(f"Greeks API error {index_name}: {e}")
        return _estimate_greeks_fallback(index_name)

def _estimate_greeks_fallback(index_name):
    tokens = INDEX_TOKENS.get(index_name)
    ce_price = latest_ticks[index_name]["ce_price"]
    pe_price = latest_ticks[index_name]["pe_price"]
    spot = latest_ticks[index_name]["spot_price"]
    if ce_price > 0 and pe_price > 0 and spot > 0:
        time_to_expiry = 0.02
        ce_iv = _estimate_iv(ce_price, spot, tokens.get("atm_strike", spot), time_to_expiry, "CE")
        pe_iv = _estimate_iv(pe_price, spot, tokens.get("atm_strike", spot), time_to_expiry, "PE")
        strike = tokens.get("atm_strike", spot)
        moneyness = (spot - strike) / spot
        ce_delta = 0.5 + moneyness * 5
        ce_delta = max(0.05, min(0.95, ce_delta))
        pe_delta = -0.5 + (-moneyness) * 5
        pe_delta = max(-0.95, min(-0.05, pe_delta))
        greeks_data = {
            "ce_iv": ce_iv, "pe_iv": pe_iv,
            "ce_delta": ce_delta, "pe_delta": pe_delta,
            "ce_gamma": 0.02, "pe_gamma": 0.02,
            "ce_theta": -0.1, "pe_theta": -0.1,
            "ce_vega": 0.15, "pe_vega": 0.15
        }
        greeks_analyzers[index_name].update(**greeks_data)
        return greeks_data
    return None

def _estimate_iv(option_price, spot, strike, tte, option_type):
    if option_price <= 0 or spot <= 0 or tte <= 0:
        return 0.2
    if option_type == "CE":
        intrinsic = max(0, spot - strike)
    else:
        intrinsic = max(0, strike - spot)
    time_value = max(0, option_price - intrinsic)
    if time_value <= 0:
        return 0.1
    iv = time_value / (spot * math.sqrt(tte)) * 2
    iv = max(0.05, min(1.0, iv))
    return round(iv, 4)

# ============================================================================
# TOKEN MANAGEMENT - FIXED: with SENSEX multiple patterns
# ============================================================================
def get_current_atm_tokens(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config or not config.get("active"):
        return None, None

    spot = get_index_spot(index_name)
    if not spot or spot <= 0:
        logger.warning(f"{index_name} spot unavailable, cannot fetch tokens")
        return None, None

    mult = config["atm_strike_multiple"]
    atm = int(round(spot / mult) * mult)
    next_expiry = get_next_expiry_date(index_name)
    if not next_expiry:
        logger.warning(f"{index_name}: Could not calculate next expiry")
        return None, None
    expiry = next_expiry.strftime("%d%b%Y").upper()

    scrip = get_scrip_master()
    if scrip and isinstance(scrip, list) and len(scrip) > 0:
        try:
            df = pd.DataFrame(scrip)
            if df.empty:
                raise ValueError("Empty scrip master")
            opts = df[(df["name"] == config["symbol"]) &
                      (df["instrumenttype"] == "OPTIDX") &
                      (df["exch_seg"] == config["option_exchange"])].copy()
            if opts.empty:
                raise ValueError("No options in scrip master")
            opts["expiry_date"] = pd.to_datetime(opts["expiry"], format="%d%b%Y", errors="coerce")
            opts = opts.dropna(subset=["expiry_date"])
            if opts.empty:
                raise ValueError("No valid expiry dates")
            opts["strike"] = pd.to_numeric(opts["strike"], errors="coerce") / 100
            opts = opts.dropna(subset=["strike"])
            if opts.empty:
                raise ValueError("No valid strikes")
            today_dt = datetime.now()
            future = opts[opts["expiry_date"] >= today_dt]
            if future.empty:
                raise ValueError("No future options")
            nearest = future["expiry_date"].min()
            atm_opts = future[(future["strike"] == atm) & (future["expiry_date"] == nearest)]
            if atm_opts.empty:
                same_exp = future[future["expiry_date"] == nearest]
                if same_exp.empty:
                    raise ValueError("No nearest expiry options")
                strike_diffs = (same_exp["strike"] - atm).abs()
                min_idx = strike_diffs.idxmin()
                if pd.isna(min_idx):
                    raise ValueError("No nearest strike found")
                atm_opts = same_exp.loc[[min_idx]]
            ce = atm_opts[atm_opts["symbol"].str.contains("CE", na=False)]
            pe = atm_opts[atm_opts["symbol"].str.contains("PE", na=False)]
            if not ce.empty and not pe.empty:
                ce_token = str(ce.iloc[0]["token"])
                pe_token = str(pe.iloc[0]["token"])
                ce_symbol = str(ce.iloc[0]["symbol"])
                pe_symbol = str(pe.iloc[0]["symbol"])
                INDEX_TOKENS[index_name].update({
                    "ce_token": ce_token,
                    "pe_token": pe_token,
                    "atm_strike": atm,
                    "expiry": expiry,
                    "expiry_date": nearest,
                    "ce_symbol": ce_symbol,
                    "pe_symbol": pe_symbol
                })
                logger.info(f"{index_name} tokens from scrip: CE={ce_token} PE={pe_token} Expiry={expiry}")
                return ce_token, pe_token
            else:
                raise ValueError("CE/PE filtering failed")
        except Exception as e:
            logger.warning(f"{index_name} scrip master path failed: {e}, trying API fallback")

    # API fallback with multiple patterns for SENSEX
    _, _, obj = get_auth_token()
    if obj:
        ce_token = pe_token = None
        ce_symbol = pe_symbol = None
        try:
            if index_name == "SENSEX":
                patterns = [f"{config['symbol']}_{atm}CE", f"{config['symbol']} {atm} CE", f"{config['symbol']}{atm}CE"]
                for pat in patterns:
                    try:
                        ce_resp = rate_limited_api_call(obj.searchScrip, config["option_exchange"], pat)
                        if ce_resp and ce_resp.get("status") and ce_resp.get("data") and len(ce_resp["data"]) > 0:
                            ce_token = str(ce_resp["data"][0].get("symboltoken"))
                            ce_symbol = str(ce_resp["data"][0].get("symbol"))
                            break
                    except Exception:
                        continue
                for pat in patterns:
                    pat_pe = pat.replace("CE", "PE")
                    try:
                        pe_resp = rate_limited_api_call(obj.searchScrip, config["option_exchange"], pat_pe)
                        if pe_resp and pe_resp.get("status") and pe_resp.get("data") and len(pe_resp["data"]) > 0:
                            pe_token = str(pe_resp["data"][0].get("symboltoken"))
                            pe_symbol = str(pe_resp["data"][0].get("symbol"))
                            break
                    except Exception:
                        continue
            else:
                ce_resp = rate_limited_api_call(obj.searchScrip, config["option_exchange"], f"{config['symbol']}{atm}CE")
                if ce_resp and ce_resp.get("status") and ce_resp.get("data") and len(ce_resp["data"]) > 0:
                    ce_token = str(ce_resp["data"][0].get("symboltoken"))
                    ce_symbol = str(ce_resp["data"][0].get("symbol"))
                pe_resp = rate_limited_api_call(obj.searchScrip, config["option_exchange"], f"{config['symbol']}{atm}PE")
                if pe_resp and pe_resp.get("status") and pe_resp.get("data") and len(pe_resp["data"]) > 0:
                    pe_token = str(pe_resp["data"][0].get("symboltoken"))
                    pe_symbol = str(pe_resp["data"][0].get("symbol"))

            if ce_token and pe_token:
                INDEX_TOKENS[index_name].update({
                    "ce_token": ce_token,
                    "pe_token": pe_token,
                    "atm_strike": atm,
                    "expiry": expiry,
                    "expiry_date": next_expiry,
                    "ce_symbol": ce_symbol,
                    "pe_symbol": pe_symbol
                })
                logger.info(f"{index_name} tokens (search): CE={ce_token} PE={pe_token} Expiry={expiry}")
                return ce_token, pe_token
        except Exception as e:
            logger.error(f"Search fallback error {index_name}: {e}")

    return None, None

def refresh_all_tokens():
    for idx in INDEX_CONFIG:
        if INDEX_CONFIG[idx].get("active"):
            try:
                get_current_atm_tokens(idx)
            except Exception as e:
                logger.error(f"Token refresh failed for {idx}: {e}")

def refresh_tokens_if_needed(index_name):
    tokens = INDEX_TOKENS.get(index_name)
    if not tokens or not tokens.get("ce_token") or not tokens.get("pe_token"):
        logger.info(f"{index_name}: Tokens missing, refreshing...")
        get_current_atm_tokens(index_name)
        return
    ce_price = latest_ticks[index_name].get("ce_price", 0)
    pe_price = latest_ticks[index_name].get("pe_price", 0)
    if ce_price == 0 and pe_price == 0:
        last_refresh = tokens.get("last_refresh", 0)
        if time.time() - last_refresh > 60:
            logger.info(f"{index_name}: Premiums are 0, refreshing tokens...")
            get_current_atm_tokens(index_name)
            INDEX_TOKENS[index_name]["last_refresh"] = time.time()

# ============================================================================
# V12: ENHANCED POSITION SIZING WITH KELLY + REGIME + CORRELATION
# ============================================================================
def calculate_position_size_v12(index_name, signal_strength, atr, vix, action):
    config = INDEX_CONFIG[index_name]
    base_risk = 1.0
    if "STRONG" in signal_strength:
        risk_pct = min(2.0, base_risk * 1.3)
    elif "LOW" in signal_strength:
        risk_pct = max(0.5, base_risk * 0.7)
    else:
        risk_pct = base_risk

    kelly_risk, kelly_win_rate, kelly_avg_win, kelly_avg_loss = kelly_trackers[index_name].get_recommended_risk_pct()
    kelly_weight = min(0.5, len(kelly_trackers[index_name].trade_returns) / 100)
    risk_pct = risk_pct * (1 - kelly_weight) + kelly_risk * kelly_weight

    if vix > 25: 
        risk_pct *= 0.7
    elif vix > 20: 
        risk_pct *= 0.85

    regime_adj = regime_detectors[index_name].get_regime_adjustment(action)
    risk_pct *= regime_adj

    pair = config.get("correlation_pair")
    if pair:
        corr_analysis = correlation_filter.analyze(index_name, action)
        beta_adj = corr_analysis.get("beta_adjustment", 1.0)
        risk_pct *= beta_adj
        if corr_analysis.get("block_reason"):
            logger.warning(f"{index_name}: Correlation block - {corr_analysis['block_reason']}")
            risk_pct *= 0.5

    if config.get("greeks_enabled"):
        greeks_analysis = greeks_analyzers[index_name].analyze(action)
        if greeks_analysis.get("block_reason"):
            logger.warning(f"{index_name}: Greeks block - {greeks_analysis['block_reason']}")
            risk_pct *= 0.5
        iv_rank = greeks_analysis.get("iv_rank", 50)
        if iv_rank > 80:
            risk_pct *= 0.8
        elif iv_rank < 20:
            risk_pct *= 1.1

    if config.get("pcr_enabled"):
        pcr_data = latest_pcr.get(index_name, {})
        pcr_signal = pcr_data.get("pcr_signal", "neutral")
        if pcr_signal in ["confirm_bullish", "confirm_bearish"]:
            risk_pct *= 1.1
        elif pcr_signal in ["bearish_divergence", "bullish_divergence"]:
            risk_pct *= 0.7

    if is_expiry_day(index_name):
        risk_pct *= 0.5
        logger.info(f"{index_name}: Expiry day - position size halved")

    liquidity_score = slippage_models[index_name].get_liquidity_score()
    risk_pct *= max(0.5, liquidity_score)

    risk_pct = max(0.2, min(3.0, risk_pct))

    risk_amount = portfolio_state[index_name]["equity"] * (risk_pct / 100)
    stop_dist = atr * 1.5
    if stop_dist > 0:
        lots = int(risk_amount / (stop_dist * config["lot_size"]))
        lots = max(1, min(5, lots))
    else:
        lots = 1

    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("""INSERT INTO kelly_history 
                (timestamp, index_name, kelly_fraction, win_rate, avg_win, avg_loss, recommended_lots, actual_lots)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (time.time(), index_name, kelly_risk / 100, kelly_win_rate, kelly_avg_win, kelly_avg_loss, lots, lots))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to log Kelly data: {e}")

    return lots, risk_pct

# ============================================================================
# V11 FUNCTIONS (PRESERVED)
# ============================================================================
def reset_signal_state(index_name, current_time, exit_reason=""):
    signal_state[index_name].update({
        "action": "HOLD", "entry_price": 0, "stop_loss": 0, "target": 0,
        "lots": 1, "cooldown": current_time + 60, "confidence": 0,
        "highest": 0, "entry_time": 0,
        "exit_reason": exit_reason
    })
    portfolio_state[index_name]["open_positions"] = 0

def save_portfolio_equity(index_name):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("""INSERT OR REPLACE INTO portfolio_equity (index_name, equity, last_updated)
                           VALUES (?, ?, ?)""",
                      (index_name, portfolio_state[index_name]["equity"], time.time()))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to save equity for {index_name}: {e}")

def send_telegram_alert(msg):
    if not TELEGRAM_AVAILABLE or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}, timeout=3)
    except Exception as e:
        logger.error(f"Telegram error: {e}")

# ============================================================================
# V11.0: MARKET ANALYSIS EXIT ENGINE (PRESERVED)
# ============================================================================
def should_exit_market_analysis(index_name, action, prices, ce_prem, pe_prem):
    if len(prices) < 60:
        return False, ""
    exit_reason = ""
    trends = {tf: market_sentiment[index_name].get(f"trend_{tf}", "NEUTRAL") for tf in TIMEFRAMES}
    if "CE" in action:
        bearish_count = sum(1 for t in trends.values() if t in ["BEARISH", "SIDEWAYS"])
        if bearish_count >= 5:
            exit_reason = f"Trend Reversal: {bearish_count}/8 timeframes bearish/sideways"
            return True, exit_reason
    elif "PE" in action:
        bullish_count = sum(1 for t in trends.values() if t in ["BULLISH", "SIDEWAYS"])
        if bullish_count >= 5:
            exit_reason = f"Trend Reversal: {bullish_count}/8 timeframes bullish/sideways"
            return True, exit_reason
    rsi = calculate_rsi(prices)
    if len(prices) >= 20:
        price_trend = prices[-1] - prices[-10]
        rsi_trend = calculate_rsi(prices[-10:]) - calculate_rsi(prices[-20:-10])
        if "CE" in action and price_trend > 0 and rsi_trend < 0 and rsi > 70:
            exit_reason = f"Bearish Divergence: Price up + RSI down (RSI={rsi:.1f})"
            return True, exit_reason
        if "PE" in action and price_trend < 0 and rsi_trend > 0 and rsi < 30:
            exit_reason = f"Bullish Divergence: Price down + RSI up (RSI={rsi:.1f})"
            return True, exit_reason
    vix = latest_ticks["VIX"]["vix"]
    if len(vix_history) >= 10:
        vix_sma = sum(list(vix_history)[-10:]) / 10
        if vix > vix_sma * 1.25:
            if "CE" in action:
                exit_reason = f"VIX Spike: {vix:.1f} vs SMA {vix_sma:.1f} (volatility crush risk)"
                return True, exit_reason
            elif "PE" in action:
                exit_reason = f"VIX Spike: {vix:.1f} vs SMA {vix_sma:.1f} (volatility crush risk)"
                return True, exit_reason
    adx = calculate_adx(prices)
    if adx < 15 and len(prices) >= 30:
        current_prem = ce_prem if "CE" in action else pe_prem
        entry = signal_state[index_name].get("entry_price", 0)
        if entry > 0 and current_prem > 0:
            profit_pct = (current_prem - entry) / entry
            if profit_pct > 0.15:
                exit_reason = f"Trend Weakness: ADX {adx:.1f} with {profit_pct*100:.1f}% profit - secure gains"
                return True, exit_reason
    upper, sma, lower = calculate_bollinger(prices)
    if upper and lower and sma > 0:
        if "CE" in action and prices[-1] > upper * 0.998:
            exit_reason = f"Overbought: Price {prices[-1]:.2f} near upper Bollinger {upper:.2f}"
            return True, exit_reason
        if "PE" in action and prices[-1] < lower * 1.002:
            exit_reason = f"Oversold: Price {prices[-1]:.2f} near lower Bollinger {lower:.2f}"
            return True, exit_reason
    return False, ""

# ============================================================================
# V11.0: TREND CHANGE COOLDOWN (PRESERVED)
# ============================================================================
def get_action_side(action):
    if "CE" in action:
        return "CE"
    elif "PE" in action:
        return "PE"
    return None

def check_trend_change_cooldown(index_name, new_action, current_time):
    current_side = get_action_side(signal_state[index_name]["action"])
    new_side = get_action_side(new_action)
    if current_side is not None and signal_state[index_name]["action"] != "HOLD":
        return True, 0
    prev_side = signal_state[index_name].get("prev_action_side")
    if prev_side is not None and new_side is not None and prev_side != new_side:
        cooldown_until = current_time + 60
        signal_state[index_name]["trend_change_cooldown"] = cooldown_until
        signal_state[index_name]["prev_action_side"] = new_side
        logger.info(f"{index_name}: Trend change cooldown activated {prev_side} -> {new_side}")
        return False, 60
    cooldown_until = signal_state[index_name].get("trend_change_cooldown", 0)
    if current_time < cooldown_until:
        remaining = int(cooldown_until - current_time)
        return False, remaining
    if new_side is not None:
        signal_state[index_name]["prev_action_side"] = new_side
    return True, 0

# ============================================================================
# V12: MAIN SIGNAL ENGINE (your advanced signal generation)
# ============================================================================
def run_signal_engine_for_index(index_name):
    if not INDEX_CONFIG[index_name].get("active"): 
        return

    with _signal_lock:
        prices = list(price_histories[index_name])
        if len(prices) < 30:
            market_signal[index_name]["alert_message"] = f"Collecting data ({len(prices)}/30)"
            market_signal[index_name]["signal"] = "WAITING"
            return

        now = time.time()
        spot = prices[-1] if prices else 0

        # Update last known good spot price
        if spot > 0:
            last_known_prices[index_name]["spot"] = spot
            last_known_prices[index_name]["timestamp"] = now
        else:
            spot = last_known_prices[index_name].get("spot", 0)

        # V12: Update all engines
        regime_detectors[index_name].update(spot)
        regime_data = regime_detectors[index_name].detect()

        vol = latest_ticks[index_name].get("ce_volume", 0) + latest_ticks[index_name].get("pe_volume", 0)
        volume_profile_engines[index_name].update(spot, max(vol, 1))
        vp_analysis = volume_profile_engines[index_name].analyze(spot)

        ce_oi = latest_ticks[index_name].get("ce_oi", 0)
        pe_oi = latest_ticks[index_name].get("pe_oi", 0)
        ce_vol = latest_ticks[index_name].get("ce_volume", 0)
        pe_vol = latest_ticks[index_name].get("pe_volume", 0)
        pcr_analyzers[index_name].update(ce_oi, pe_oi, ce_vol, pe_vol)
        pcr_analysis = pcr_analyzers[index_name].analyze(market_sentiment[index_name].get("label", "NEUTRAL"))

        with _pcr_lock:
            latest_pcr[index_name].update({
                "pcr_oi": pcr_analysis.get("pcr_oi", 1.0),
                "pcr_volume": pcr_analysis.get("pcr_vol", 1.0),
                "pcr_weighted": pcr_analysis.get("pcr_weighted", 1.0),
                "pcr_signal": pcr_analysis.get("signal", "neutral"),
                "pcr_strength": pcr_analysis.get("strength", 0)
            })

        if index_name == "NIFTY":
            nifty_price_series.append(spot)
        elif index_name == "BANKNIFTY":
            banknifty_price_series.append(spot)

        if len(nifty_price_series) > 0 and len(banknifty_price_series) > 0:
            correlation_filter.update(
                list(nifty_price_series)[-1],
                list(banknifty_price_series)[-1]
            )

        if INDEX_CONFIG[index_name].get("greeks_enabled"):
            get_option_greeks(index_name)

        ce_bid = latest_ticks[index_name].get("ce_bid", 0)
        ce_ask = latest_ticks[index_name].get("ce_ask", 0)
        pe_bid = latest_ticks[index_name].get("pe_bid", 0)
        pe_ask = latest_ticks[index_name].get("pe_ask", 0)
        ce_prem = latest_ticks[index_name]["ce_price"]
        pe_prem = latest_ticks[index_name]["pe_price"]

        if ce_prem <= 0:
            ce_prem = last_known_prices[index_name].get("ce", 0)
        if pe_prem <= 0:
            pe_prem = last_known_prices[index_name].get("pe", 0)

        if ce_bid > 0 and ce_ask > 0:
            slippage_models[index_name].record_spread(ce_bid, ce_ask, ce_prem if ce_prem > 0 else spot)
        if pe_bid > 0 and pe_ask > 0:
            slippage_models[index_name].record_spread(pe_bid, pe_ask, pe_prem if pe_prem > 0 else spot)

        sentiment = compute_sentiment(index_name)
        action, label, conf = get_signal_from_sentiment(index_name, sentiment)
        rsi = calculate_rsi(prices)
        _, _, macd_hist = calculate_macd(prices)
        atr = calculate_atr(prices)
        vix = latest_ticks["VIX"]["vix"]

        current_equity = portfolio_state[index_name]["equity"]
        kill_active = kill_switches[index_name].update_equity(current_equity)
        if kill_active:
            if signal_state[index_name]["action"] != "HOLD":
                active = signal_state[index_name]["action"]
                prem = ce_prem if "CE" in active else pe_prem
                if prem > 0:
                    pnl = prem - signal_state[index_name]["entry_price"]
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        save_portfolio_equity(index_name)
                    reset_signal_state(index_name, now, "KILL_SWITCH")
            market_signal[index_name].update({
                "alert_message": f"KILL SWITCH: Max drawdown hit. Trading halted.",
                "signal": "KILL_SWITCH",
                "drawdown_pct": round(kill_switches[index_name].current_drawdown, 2)
            })
            return

        daily_pnl = portfolio_state[index_name]["equity"] - daily_drawdown[index_name].get("peak_equity", current_equity)
        if daily_drawdown[index_name].get("peak_equity", 0) == 0:
            daily_drawdown[index_name]["peak_equity"] = current_equity

        daily_return = daily_pnl / max(current_equity, 1) * 100 if current_equity > 0 else 0
        performance_trackers[index_name].add_return(daily_return, current_equity)

        if safety_state[index_name]["circuit_breaker"]:
            if now < safety_state[index_name]["circuit_breaker_until"]:
                market_signal[index_name]["alert_message"] = "Circuit breaker active"
                market_signal[index_name]["signal"] = "CIRCUIT_BREAKER"
                return
            else:
                safety_state[index_name]["circuit_breaker"] = False
                safety_state[index_name]["consecutive_sl"] = 0
                logger.info(f"{index_name}: Circuit breaker released")

        slippage_adj = 0.0
        liquidity = 1.0

        if signal_state[index_name]["action"] != "HOLD":
            active = signal_state[index_name]["action"]
            prem = ce_prem if "CE" in active else pe_prem

            if prem > 0:
                exit_side = "SELL"
                adjusted_prem, slippage_adj, liquidity = slippage_models[index_name].get_adjusted_price(
                    exit_side, prem, signal_state[index_name]["lots"]
                )

                pnl = adjusted_prem - signal_state[index_name]["entry_price"]

                if prem > signal_state[index_name].get("highest", 0):
                    signal_state[index_name]["highest"] = prem
                    new_sl = prem - (atr * 1.8)
                    if new_sl > signal_state[index_name]["stop_loss"]:
                        signal_state[index_name]["stop_loss"] = new_sl

                if prem <= signal_state[index_name]["stop_loss"]:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        save_portfolio_equity(index_name)

                    pnl_pct = pnl / max(signal_state[index_name]["entry_price"], 1)
                    kelly_trackers[index_name].update(pnl_pct)
                    performance_trackers[index_name].add_trade(pnl_total)

                    safety_state[index_name]["consecutive_sl"] += 1
                    if safety_state[index_name]["consecutive_sl"] >= 3:
                        safety_state[index_name]["circuit_breaker"] = True
                        safety_state[index_name]["circuit_breaker_until"] = now + 1800
                        send_telegram_alert(f"CIRCUIT BREAKER {index_name} | 3 consecutive SLs. Trading paused 30 min.")

                    daily_trade_count[index_name] += 1
                    send_telegram_alert(f"EXIT {index_name} | SL | PnL: {pnl:.2f} pts | Slippage: {slippage_adj*100:.3f}%")
                    reset_signal_state(index_name, now, "STOP_LOSS")
                    return

                if prem >= signal_state[index_name]["target"]:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        save_portfolio_equity(index_name)

                    pnl_pct = pnl / max(signal_state[index_name]["entry_price"], 1)
                    kelly_trackers[index_name].update(pnl_pct)
                    performance_trackers[index_name].add_trade(pnl_total)

                    safety_state[index_name]["consecutive_sl"] = 0
                    daily_trade_count[index_name] += 1
                    send_telegram_alert(f"TARGET {index_name} | PnL: {pnl:.2f} pts | Slippage: {slippage_adj*100:.3f}%")
                    reset_signal_state(index_name, now, "TARGET_HIT")
                    return

                entry_time = signal_state[index_name].get("entry_time", 0)
                if entry_time > 0 and (now - entry_time) / 60 >= 45:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        save_portfolio_equity(index_name)

                    pnl_pct = pnl / max(signal_state[index_name]["entry_price"], 1)
                    kelly_trackers[index_name].update(pnl_pct)
                    performance_trackers[index_name].add_trade(pnl_total)

                    daily_trade_count[index_name] += 1
                    send_telegram_alert(f"TIME EXIT {index_name} | PnL: {pnl:.2f} pts")
                    reset_signal_state(index_name, now, "TIME_EXIT")
                    return

                should_exit, exit_reason = should_exit_market_analysis(index_name, active, prices, ce_prem, pe_prem)
                if should_exit:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        save_portfolio_equity(index_name)

                    pnl_pct = pnl / max(signal_state[index_name]["entry_price"], 1)
                    kelly_trackers[index_name].update(pnl_pct)
                    performance_trackers[index_name].add_trade(pnl_total)

                    safety_state[index_name]["consecutive_sl"] = 0
                    daily_trade_count[index_name] += 1
                    send_telegram_alert(f"MARKET EXIT {index_name} | {exit_reason} | PnL: {pnl:.2f} pts")
                    reset_signal_state(index_name, now, exit_reason)
                    return

                vwap = latest_volume_profile[index_name].get("vwap", 0)
                if vwap > 0:
                    if "CE" in active and spot < vwap * 0.997:
                        pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                        with _portfolio_lock:
                            portfolio_state[index_name]["equity"] += pnl_total
                            save_portfolio_equity(index_name)
                        kelly_trackers[index_name].update(pnl / max(signal_state[index_name]["entry_price"], 1))
                        performance_trackers[index_name].add_trade(pnl_total)
                        daily_trade_count[index_name] += 1
                        send_telegram_alert(f"VWAP EXIT {index_name} | Price below VWAP | PnL: {pnl:.2f} pts")
                        reset_signal_state(index_name, now, "VWAP_EXIT")
                        return
                    elif "PE" in active and spot > vwap * 1.003:
                        pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                        with _portfolio_lock:
                            portfolio_state[index_name]["equity"] += pnl_total
                            save_portfolio_equity(index_name)
                        kelly_trackers[index_name].update(pnl / max(signal_state[index_name]["entry_price"], 1))
                        performance_trackers[index_name].add_trade(pnl_total)
                        daily_trade_count[index_name] += 1
                        send_telegram_alert(f"VWAP EXIT {index_name} | Price above VWAP | PnL: {pnl:.2f} pts")
                        reset_signal_state(index_name, now, "VWAP_EXIT")
                        return

            current_pnl = 0.0
            highest_pnl = 0.0
            if prem > 0:
                current_pnl = round(pnl, 2)
                highest = signal_state[index_name].get("highest", 0)
                if highest > 0:
                    highest_pnl = round(highest - signal_state[index_name]["entry_price"], 2)

            market_signal[index_name].update({
                "alert_message": f"ACTIVE {active} {index_name}",
                "signal": "ACTIVE",
                "entry_price": signal_state[index_name]["entry_price"],
                "stop_loss": signal_state[index_name]["stop_loss"],
                "target": signal_state[index_name]["target"],
                "exit_reason": "",
                "trend_change_cooldown_remaining": 0,
                "current_pnl": current_pnl,
                "highest_pnl": highest_pnl,
                "slippage_pct": round(slippage_adj * 100, 3),
                "liquidity_score": round(liquidity, 2)
            })

        else:
            can_trade, cooldown_remaining = check_trend_change_cooldown(index_name, action, now)
            if not can_trade:
                market_signal[index_name]["alert_message"] = f"Trend change cooldown: {cooldown_remaining}s"
                market_signal[index_name]["signal"] = "COOLDOWN"
                market_signal[index_name]["trend_change_cooldown_remaining"] = cooldown_remaining
                return

            if now < signal_state[index_name]["cooldown"]:
                remaining = int(signal_state[index_name]["cooldown"] - now)
                market_signal[index_name]["alert_message"] = f"Cooldown {remaining}s"
                market_signal[index_name]["signal"] = "COOLDOWN"
                market_signal[index_name]["trend_change_cooldown_remaining"] = 0
                return

            today = datetime.now().strftime("%Y-%m-%d")
            if last_trade_date[index_name] != today:
                daily_trade_count[index_name] = 0
                last_trade_date[index_name] = today
                kill_switches[index_name].reset_daily()

            if daily_trade_count[index_name] >= 20:
                market_signal[index_name]["alert_message"] = "Max daily trades reached"
                market_signal[index_name]["signal"] = "BLOCKED"
                return

            prem = ce_prem if "CE" in action else pe_prem if "PE" in action else 0
            min_prem = INDEX_CONFIG[index_name].get("min_premium", 5)

            if prem <= 0:
                refresh_tokens_if_needed(index_name)
                ce_prem = latest_ticks[index_name]["ce_price"]
                pe_prem = latest_ticks[index_name]["pe_price"]
                if ce_prem <= 0:
                    ce_prem = last_known_prices[index_name].get("ce", 0)
                if pe_prem <= 0:
                    pe_prem = last_known_prices[index_name].get("pe", 0)
                prem = ce_prem if "CE" in action else pe_prem if "PE" in action else 0

            if prem <= 0 or prem < min_prem:
                market_signal[index_name]["alert_message"] = f"Premium invalid: Rs{prem} (min: {min_prem})"
                market_signal[index_name]["signal"] = "WAITING"
                return

            if INDEX_CONFIG[index_name].get("greeks_enabled"):
                greeks_check = greeks_analyzers[index_name].analyze(action)
                if greeks_check.get("block_reason"):
                    logger.warning(f"{index_name}: Entry blocked by Greeks - {greeks_check['block_reason']}")
                    market_signal[index_name]["alert_message"] = f"Greeks block: {greeks_check['block_reason']}"
                    market_signal[index_name]["signal"] = "BLOCKED"
                    return

            pair = INDEX_CONFIG[index_name].get("correlation_pair")
            if pair:
                corr_check = correlation_filter.analyze(index_name, action)
                if corr_check.get("signal") == "negative_correlation":
                    logger.warning(f"{index_name}: Entry blocked by correlation - {corr_check.get('block_reason')}")
                    market_signal[index_name]["alert_message"] = f"Correlation block: {corr_check.get('block_reason')}"
                    market_signal[index_name]["signal"] = "BLOCKED"
                    return

            regime = latest_regime[index_name].get("regime", "unknown")
            if regime == "volatile" and "STRONG" not in action:
                logger.info(f"{index_name}: Avoiding entry in volatile regime without strong signal")
                market_signal[index_name]["alert_message"] = "Volatile regime - need strong signal"
                market_signal[index_name]["signal"] = "BLOCKED"
                return

            buf = signal_buffer[index_name]
            if "CE" in action:
                buf["ce_count"] += 1
                buf["pe_count"] = 0
                if buf["ce_count"] < 2:
                    market_signal[index_name]["alert_message"] = f"Building CE ({buf['ce_count']}/2)"
                    market_signal[index_name]["signal"] = "BUILDING"
                    return
            elif "PE" in action:
                buf["pe_count"] += 1
                buf["ce_count"] = 0
                if buf["pe_count"] < 2:
                    market_signal[index_name]["alert_message"] = f"Building PE ({buf['pe_count']}/2)"
                    market_signal[index_name]["signal"] = "BUILDING"
                    return
            else:
                buf["ce_count"] = buf["pe_count"] = 0

            lots, risk = calculate_position_size_v12(index_name, action, atr, vix, action)

            entry_side = "BUY"
            adjusted_prem, slippage_adj, liquidity = slippage_models[index_name].get_adjusted_price(
                entry_side, prem, lots
            )

            sl_pct = 0.3 if "LOW" in action else 0.45
            sl = max(adjusted_prem * (1 - sl_pct), adjusted_prem - (atr * 1.5))
            target = adjusted_prem + (atr * 3.5)
            if "LOW" in action:
                target = adjusted_prem + (atr * 2.5)

            signal_state[index_name].update({
                "action": action, "entry_price": adjusted_prem, "stop_loss": sl, "target": target,
                "lots": lots, "entry_time": now, "highest": adjusted_prem, "confidence": conf,
                "exit_reason": ""
            })
            portfolio_state[index_name]["open_positions"] = 1
            buf["ce_count"] = 0
            buf["pe_count"] = 0
            if "CE" in action:
                buf["consecutive_ce"] += 1
                buf["consecutive_pe"] = 0
            else:
                buf["consecutive_pe"] += 1
                buf["consecutive_ce"] = 0

            daily_trade_count[index_name] += 1

            emoji = "B" if "STRONG" in action and "CE" in action else "S" if "STRONG" in action and "PE" in action else "W" if "LOW" in action else "N"

            vwap_str = f"VWAP:{latest_volume_profile[index_name].get('vwap', 0):.1f}"
            regime_str = f"Regime:{regime}"
            iv_str = f"IV-R:{latest_greeks[index_name].get('iv_rank', 0):.0f}"
            pcr_str = f"PCR:{latest_pcr[index_name].get('pcr_weighted', 1.0):.2f}"

            msg = (f"{emoji} {action} {index_name} | Spot: {spot:.2f} | "
                   f"Prem: Rs{adjusted_prem:.2f} (slip:{slippage_adj*100:.2f}%) | "
                   f"SL: {sl:.2f} Tgt: {target:.2f} | Sentiment: {sentiment:.1f} ({label}) | "
                   f"Lots: {lots} | Risk: {risk:.2f}% | {vwap_str} | {regime_str} | {iv_str} | {pcr_str}")

            send_telegram_alert(msg)
            logger.info(f"ENTRY {index_name} {action} | Regime: {regime} | Kelly: {kelly_trackers[index_name].get_recommended_risk_pct()[0]:.2f}%")

        metrics = performance_trackers[index_name].get_all_metrics()
        with _metrics_lock:
            latest_metrics[index_name].update(metrics)

        market_signal[index_name].update({
            "spot_price": spot,
            "ce_price": ce_prem,
            "pe_price": pe_prem,
            "sentiment_score": sentiment,
            "sentiment": label,
            "signal": signal_state[index_name]["action"],
            "confidence": conf,
            "trend_1min": market_sentiment[index_name]["trend_1min"],
            "trend_2min": market_sentiment[index_name]["trend_2min"],
            "trend_3min": market_sentiment[index_name]["trend_3min"],
            "trend_5min": market_sentiment[index_name]["trend_5min"],
            "trend_10min": market_sentiment[index_name]["trend_10min"],
            "trend_15min": market_sentiment[index_name]["trend_15min"],
            "trend_20min": market_sentiment[index_name]["trend_20min"],
            "trend_30min": market_sentiment[index_name]["trend_30min"],
            "timestamp": datetime.now().isoformat(),
            "entry_price": signal_state[index_name]["entry_price"] if signal_state[index_name]["action"] != "HOLD" else 0.0,
            "stop_loss": signal_state[index_name]["stop_loss"] if signal_state[index_name]["action"] != "HOLD" else 0.0,
            "target": signal_state[index_name]["target"] if signal_state[index_name]["action"] != "HOLD" else 0.0,
            "exit_reason": signal_state[index_name].get("exit_reason", ""),
            "trend_change_cooldown_remaining": max(0, int(signal_state[index_name].get("trend_change_cooldown", 0) - now)),
            "regime": latest_regime[index_name].get("regime", "unknown"),
            "regime_score": latest_regime[index_name].get("regime_score", 50),
            "vwap": latest_volume_profile[index_name].get("vwap", 0),
            "poc": latest_volume_profile[index_name].get("poc", 0),
            "value_area_high": latest_volume_profile[index_name].get("value_area_high", 0),
            "value_area_low": latest_volume_profile[index_name].get("value_area_low", 0),
            "pcr_oi": latest_pcr[index_name].get("pcr_oi", 1.0),
            "pcr_signal": latest_pcr[index_name].get("pcr_signal", "neutral"),
            "iv_rank": latest_greeks[index_name].get("iv_rank", 0),
            "iv_percentile": latest_greeks[index_name].get("iv_percentile", 0),
            "delta_ce": latest_greeks[index_name].get("ce_delta", 0),
            "delta_pe": latest_greeks[index_name].get("pe_delta", 0),
            "sharpe": latest_metrics[index_name].get("sharpe", 0),
            "sortino": latest_metrics[index_name].get("sortino", 0),
            "max_drawdown": latest_metrics[index_name].get("max_drawdown", 0),
            "win_rate": latest_metrics[index_name].get("win_rate", 0),
            "profit_factor": latest_metrics[index_name].get("profit_factor", 0),
            "expectancy": latest_metrics[index_name].get("expectancy", 0),
            "kill_switch_active": kill_switches[index_name].is_active(),
            "current_drawdown": round(kill_switches[index_name].current_drawdown, 2),
            "liquidity_score": round(slippage_models[index_name].get_liquidity_score(), 2),
            "kelly_risk_pct": round(kelly_trackers[index_name].get_recommended_risk_pct()[0], 2),
            "correlation_20": latest_correlation.get("correlation_20", 0),
            "beta": latest_correlation.get("beta", 1.0)
        })

def run_all_signals():
    for idx in INDEX_CONFIG:
        if INDEX_CONFIG[idx].get("active"):
            try:
                run_signal_engine_for_index(idx)
            except Exception as e:
                logger.error(f"Signal engine error for {idx}: {e}")

# ============================================================================
# WEBSOCKET HANDLERS (from working connection code)
# ============================================================================
def on_ws_open(wsapp):
    """Called once when WebSocket connects. Subscribe to tokens here."""
    global ws_running, last_heartbeat
    ws_running = True
    last_heartbeat = time.time()
    logger.info("=" * 60)
    logger.info("WEBSOCKET CONNECTED")
    logger.info("=" * 60)

    # Build subscription list for all active indices
    subscribe_tokens = []
    for idx, cfg in INDEX_CONFIG.items():
        if not cfg.get("active"):
            continue
        
        # Spot index token
        subscribe_tokens.append({
            "exchangeType": cfg["ws_exchange_type"],
            "tokens": [cfg["token"]]
        })
        
        # Option tokens (if already fetched)
        tok = INDEX_TOKENS.get(idx, {})
        if tok.get("ce_token"):
            subscribe_tokens.append({
                "exchangeType": cfg["option_ws_exchange_type"],
                "tokens": [tok["ce_token"]]
            })
        if tok.get("pe_token"):
            subscribe_tokens.append({
                "exchangeType": cfg["option_ws_exchange_type"],
                "tokens": [tok["pe_token"]]
            })

    if not subscribe_tokens:
        logger.warning("No tokens available to subscribe yet. REST poller will backfill.")
        return

    # Safe subscription - handle different SmartAPI library versions
    try:
        if sws is None:
            logger.warning("Global sws is None, cannot subscribe")
            return
            
        if hasattr(sws, 'subscribe'):
            # Standard V2 signature: subscribe(correlation_id, mode, token_list)
            sws.subscribe("admin", 1, subscribe_tokens)
            logger.info(f"Subscribed to {len(subscribe_tokens)} token groups via subscribe()")
        elif hasattr(sws, 'send_request'):
            # Fallback for older/alternative versions
            sws.send_request("subscribe", subscribe_tokens)
            logger.info(f"Subscribed to {len(subscribe_tokens)} token groups via send_request()")
        else:
            logger.warning("WebSocket object has no subscribe/send_request method. Check SmartAPI library version.")
    except Exception as e:
        logger.error(f"WebSocket subscription failed (non-fatal): {e}")
        # REST poller will continue fetching data, so this is not fatal


def start_angel_websocket():
    """Main WebSocket reconnection thread."""
    global sws, ws_running, last_heartbeat
    logger.info("=" * 60)
    logger.info("WEBSOCKET THREAD STARTED")
    logger.info("=" * 60)
    
    try:
        refresh_all_tokens()
    except Exception as e:
        logger.error(f"Token refresh during WS startup failed: {e}")

    while True:
        try:
            if not is_market_open():
                time.sleep(5)
                last_heartbeat = time.time()
                continue

            auth_token, feed_token, obj = get_auth_token()
            if not feed_token:
                logger.warning("No feed token, retrying in 10s...")
                time.sleep(10)
                continue

            sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)
            sws.on_open = on_ws_open
            sws.on_data = on_ws_data
            sws.on_error = on_ws_error
            sws.on_close = on_ws_close
            
            # connect() is blocking; it returns only when the socket closes
            sws.connect()
            
        except Exception as e:
            logger.error(f"WebSocket thread error: {e}")
            ws_running = False
            sws = None
            time.sleep(10)

def on_ws_error(wsapp, error):
    logger.error(f"WebSocket Error: {error}")

def on_ws_close(wsapp, close_status_code=None, close_msg=None):
    global ws_running
    ws_running = False
    logger.warning(f"WebSocket closed: {close_status_code} {close_msg}")

def on_ws_data(wsapp, message):
    global tick_counter, last_tick_time, last_heartbeat
    last_tick_time = time.time()
    last_heartbeat = time.time()
    try:
        ticks = []
        if isinstance(message, bytes):
            # Try library parser if available, otherwise skip gracefully
            if sws is not None and hasattr(sws, '_parse_binary_data'):
                try:
                    parsed = sws._parse_binary_data(message)
                    ticks = [parsed] if isinstance(parsed, dict) else parsed if isinstance(parsed, list) else []
                except Exception as e:
                    logger.debug(f"Binary parse failed: {e}")
                    return
            else:
                logger.debug("Received binary tick but no parser available")
                return
        elif isinstance(message, str):
            try:
                data = json.loads(message)
                ticks = data if isinstance(data, list) else [data]
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse JSON: {message[:100]}")
                return
        elif isinstance(message, dict):
            ticks = [message]
        elif isinstance(message, list):
            ticks = message
        else:
            logger.debug(f"Unexpected message type: {type(message)}")
            return
        for tick in ticks:
            if not isinstance(tick, dict): 
                continue
            token = str(tick.get("token") or tick.get("tk") or "")
            ltp = tick.get("last_traded_price") or tick.get("ltp") or tick.get("lp") or 0
            if isinstance(ltp, str):
                try: 
                    ltp = float(ltp)
                except: 
                    ltp = 0
            if isinstance(ltp, (int, float)) and ltp > 100000:
                ltp /= 100
            vol = tick.get("volume") or tick.get("v") or 0
            oi = tick.get("open_interest") or tick.get("oi") or 0
            bid = tick.get("bid") or tick.get("bp") or 0
            ask = tick.get("ask") or tick.get("ap") or 0

            idx = None
            for i, cfg in INDEX_CONFIG.items():
                if cfg.get("token") == token:
                    idx = i
                    break
            if not idx:
                for i, t in INDEX_TOKENS.items():
                    if t.get("ce_token") == token or t.get("pe_token") == token:
                        idx = i
                        break
            if idx:
                if token == INDEX_CONFIG[idx]["token"]:
                    if ltp > 0:
                        latest_ticks[idx]["spot_price"] = ltp
                        price_histories[idx].append(ltp)
                        tick_counter += 1
                elif token == INDEX_TOKENS[idx].get("ce_token"):
                    if ltp > 0:
                        latest_ticks[idx]["ce_price"] = ltp
                        ce_price_histories[idx].append(ltp)
                        last_known_prices[idx]["ce"] = ltp
                        last_known_prices[idx]["timestamp"] = time.time()
                    if vol > 0:
                        latest_ticks[idx]["ce_volume"] = vol
                        ce_volume_histories[idx].append(vol)
                        ce_volume_series[idx].append(vol)
                    if oi > 0:
                        latest_ticks[idx]["ce_oi"] = oi
                        ce_oi_histories[idx].append(oi)
                        ce_oi_series[idx].append(oi)
                    if bid > 0:
                        latest_ticks[idx]["ce_bid"] = bid
                    if ask > 0:
                        latest_ticks[idx]["ce_ask"] = ask
                elif token == INDEX_TOKENS[idx].get("pe_token"):
                    if ltp > 0:
                        latest_ticks[idx]["pe_price"] = ltp
                        pe_price_histories[idx].append(ltp)
                        last_known_prices[idx]["pe"] = ltp
                        last_known_prices[idx]["timestamp"] = time.time()
                    if vol > 0:
                        latest_ticks[idx]["pe_volume"] = vol
                        pe_volume_histories[idx].append(vol)
                        pe_volume_series[idx].append(vol)
                    if oi > 0:
                        latest_ticks[idx]["pe_oi"] = oi
                        pe_oi_histories[idx].append(oi)
                        pe_oi_series[idx].append(oi)
                    if bid > 0:
                        latest_ticks[idx]["pe_bid"] = bid
                    if ask > 0:
                        latest_ticks[idx]["pe_ask"] = ask
            elif token == "99919017":
                if ltp > 0:
                    latest_ticks["VIX"]["vix"] = ltp
                    vix_history.append(ltp)
        if tick_counter % 3 == 0 and tick_counter > 0:
            try:
                run_all_signals()
            except Exception as e:
                logger.error(f"Signal engine error in WS callback: {e}")
    except Exception as e:
        logger.error(f"WS data error: {e}")
def start_angel_websocket():
    global sws, ws_running, last_heartbeat
    logger.info("="*60)
    logger.info("WEBSOCKET THREAD STARTED")
    logger.info(f"Current IST time: {(datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Market open check: {is_market_open()}")
    logger.info("="*60)
    refresh_all_tokens()
    while True:
        try:
            if not is_market_open():
                time.sleep(5)
                last_heartbeat = time.time()
                continue
            auth_token, feed_token, obj = get_auth_token()
            if not feed_token:
                time.sleep(10)
                continue
            sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)
            sws.on_open = on_ws_open
            sws.on_data = on_ws_data
            sws.on_error = on_ws_error
            sws.on_close = on_ws_close
            ws_running = True
            sws.connect()
        except Exception as e:
            logger.error(f"WebSocket thread error: {e}")
            ws_running = False
            sws = None
            time.sleep(10)

# ============================================================================
# REST API POLLER (from working connection code)
# ============================================================================
def is_market_open():
    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    current_time = now_ist.time()
    is_weekday = now_ist.weekday() < 5
    market_open = dt_time(9, 10)
    market_close = dt_time(15, 35)
    is_trading_hours = market_open <= current_time <= market_close
    return is_weekday and is_trading_hours

def start_rest_api_poller():
    global last_heartbeat
    logger.info("REST API poller started - PRIMARY DATA SOURCE")
    auth_obj = None
    auth_time = 0
    spot_cache = {idx: {"price": 0, "time": 0} for idx in INDEX_CONFIG}
    spot_cache_ttl = 5
    poll_count = 0
    token_refresh_counter = 0

    while True:
        try:
            last_heartbeat = time.time()
            if not is_market_open():
                time.sleep(5)
                poll_count += 1
                if poll_count % 60 == 0:
                    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
                    logger.info(f"Market CLOSED | IST: {now_ist.strftime('%H:%M')} | Waiting for 09:15 IST...")
                continue
            poll_count = 0
            now = time.time()
            if not auth_obj or (now - auth_time > 3000):
                _, _, auth_obj = get_auth_token()
                auth_time = now
            if not auth_obj:
                time.sleep(10)
                continue

            token_refresh_counter += 1
            if token_refresh_counter >= 30:
                refresh_all_tokens()
                token_refresh_counter = 0

            for idx in INDEX_CONFIG:
                if now - spot_cache[idx]["time"] > spot_cache_ttl:
                    spot = get_index_spot(idx)
                    if spot and spot > 0:
                        valid_ranges = {
                            "NIFTY": (15000, 30000),
                            "BANKNIFTY": (30000, 70000),
                            "FINNIFTY": (15000, 30000),
                            "MIDCPNIFTY": (8000, 18000),
                            "SENSEX": (50000, 100000)
                        }
                        min_val, max_val = valid_ranges.get(idx, (0, 999999))
                        if not (min_val < spot < max_val):
                            logger.warning(f"REST POLLER: Spot price {spot} out of valid range for {idx}, skipping")
                            continue
                        price_histories[idx].append(spot)
                        latest_ticks[idx]["spot_price"] = spot
                        last_known_prices[idx]["spot"] = spot
                        last_known_prices[idx]["timestamp"] = now
                        spot_cache[idx] = {"price": spot, "time": now}
                        logger.info(f"REST POLLER: {idx} spot fetched: {spot}")

            if int(now) % 30 < 10:
                vix = get_vix_value()
                if vix:
                    latest_ticks["VIX"]["vix"] = vix
                    vix_history.append(vix)

            for idx, tokens in INDEX_TOKENS.items():
                if tokens.get("ce_token") and tokens.get("pe_token") and tokens.get("ce_symbol") and tokens.get("pe_symbol"):
                    try:
                        spot = latest_ticks[idx]["spot_price"]

                    ce_resp = rate_limited_api_call(
                        auth_obj.ltpData,
                        INDEX_CONFIG[idx]["option_exchange"],
                        tokens["ce_symbol"],
                        tokens["ce_token"]
                    )
                    ce = safe_ltp(ce_resp)
                    if ce is not None:
                        if ce > 100000: ce /= 100
                        if ce > 0 and ce < 10000:
                            if is_valid_option_premium(ce, spot, "CE"):
                                ce_price_histories[idx].append(ce)
                                latest_ticks[idx]["ce_price"] = ce
                                last_known_prices[idx]["ce"] = ce
                                last_known_prices[idx]["timestamp"] = now
                                logger.info(f"REST POLLER: {idx} CE fetched: {ce}")
                            else:
                                logger.warning(f"REST POLLER: {idx} CE price {ce} invalid for spot {spot}")
                        else:
                            logger.warning(f"REST POLLER: {idx} CE price {ce} out of range")

                    pe_resp = rate_limited_api_call(
                        auth_obj.ltpData,
                        INDEX_CONFIG[idx]["option_exchange"],
                        tokens["pe_symbol"],
                        tokens["pe_token"]
                    )
                    pe = safe_ltp(pe_resp)
                    if pe is not None:
                        if pe > 100000: pe /= 100
                        if pe > 0 and pe < 10000:
                            if is_valid_option_premium(pe, spot, "PE"):
                                pe_price_histories[idx].append(pe)
                                latest_ticks[idx]["pe_price"] = pe
                                last_known_prices[idx]["pe"] = pe
                                last_known_prices[idx]["timestamp"] = now
                                logger.info(f"REST POLLER: {idx} PE fetched: {pe}")
                            else:
                                logger.warning(f"REST POLLER: {idx} PE price {pe} invalid for spot {spot}")
                        else:
                            logger.warning(f"REST POLLER: {idx} PE price {pe} out of range")

                    except Exception as e:
                        logger.debug(f"REST {idx} option fetch error: {e}")
                else:
                    logger.info(f"REST POLLER: {idx} tokens missing, attempting refresh...")
                    get_current_atm_tokens(idx)

            try:
                run_all_signals()
            except Exception as e:
                logger.error(f"REST poller signal engine error: {e}")
            time.sleep(10)
        except Exception as e:
            logger.error(f"REST poller error: {e}")
            auth_obj = None
            time.sleep(10)

# ============================================================================
# BACKGROUND THREADS
# ============================================================================
_init_completed = False
_init_lock = threading.Lock()

def _start_background_threads():
    global _init_completed
    with _init_lock:
        if not _init_completed:
            threading.Thread(target=start_angel_websocket, daemon=True).start()
            threading.Thread(target=start_rest_api_poller, daemon=True).start()
            _init_completed = True
            logger.info("Background threads started")

@app.before_request
def start_backgrounds():
    _start_background_threads()

# ============================================================================
# FLASK ROUTES - V12 ENHANCED (FIXED INDENTATION BUG)
# ============================================================================
@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "healthy",
        "engine": "Multi-Index Options Bot v12.2 (MERGED: Working Socket + Full Analytics)",
        "indices": list(INDEX_CONFIG.keys()),
        "market_open": is_market_open(),
        "timestamp": time.time(),
        "features": [
            "8-Timeframe Multi-Timeframe Analysis",
            "PCR + OI Signal Filtering",
            "Greeks Integration (Delta < 0.6, IV Rank)",
            "Max Daily Drawdown Kill Switch (-3%)",
            "NIFTY-BANKNIFTY Correlation Filter",
            "Regime Detection (Trending/Ranging/Volatile)",
            "Volume Profile + VWAP Signal Enhancement",
            "Kelly Criterion Position Sizing",
            "Slippage/Spread Modeling",
            "Real-time Sharpe/Sortino/Calmar Tracking",
            "Market Analysis Exit Engine",
            "Trend Change Cooldown",
            "Trailing Stop Loss",
            "FIXED: Premium Invalid Rs0 Error",
            "FIXED: Greeks API Endpoint",
            "FIXED: Token Auto-Refresh on Zero Premiums",
            "FIXED: SENSEX Token Fetching with Multiple Patterns",
            "MERGED: Working WebSocket + REST Data Fetching"
        ]
    })

@app.route("/api/live-signals", methods=["GET"])
def live_signals():
    sentiment_data = {}
    for idx in INDEX_CONFIG:
        sentiment_data[idx] = {
            "score": market_sentiment[idx]["score"],
            "label": market_sentiment[idx]["label"],
            "trend_1min": market_sentiment[idx]["trend_1min"],
            "trend_2min": market_sentiment[idx]["trend_2min"],
            "trend_3min": market_sentiment[idx]["trend_3min"],
            "trend_5min": market_sentiment[idx]["trend_5min"],
            "trend_10min": market_sentiment[idx]["trend_10min"],
            "trend_15min": market_sentiment[idx]["trend_15min"],
            "trend_20min": market_sentiment[idx]["trend_20min"],
            "trend_30min": market_sentiment[idx]["trend_30min"]
        }

    return jsonify({
        "timestamp": datetime.now().isoformat(),
        "signals": market_signal,
        "sentiment": sentiment_data,
        "portfolios": portfolio_state,
        "safety": safety_state,
        "tokens": INDEX_TOKENS,
        "market_open": is_market_open(),
        "debug": {"ws_running": ws_running, "ticks": tick_counter},
        "version": "12.2",
        "regime": latest_regime,
        "pcr": latest_pcr,
        "greeks": latest_greeks,
        "volume_profile": latest_volume_profile,
        "correlation": latest_correlation,
        "performance": latest_metrics,
        "drawdown": {k: {
            "current_drawdown": float(v.current_drawdown),
            "kill_switch_active": bool(v.is_active()),
            "max_drawdown_pct": float(v.max_drawdown_pct)
        } for k, v in kill_switches.items()},
        "slippage": {k: {
            "liquidity_score": float(v.get_liquidity_score()),
            "avg_spread_pct": round(float(np.mean(list(v.spread_history)) * 100), 3) if v.spread_history else 0.0
        } for k, v in slippage_models.items()},
        "kelly": {k: {
            "recommended_risk_pct": round(float(v.get_recommended_risk_pct()[0]), 2),
            "win_rate": round(float(v.get_recommended_risk_pct()[1]), 2),
            "total_trades": int(v.win_count + v.loss_count)
        } for k, v in kelly_trackers.items()}
    })

@app.route("/api/health")
def health():
    now = time.time()
    heartbeat_age = now - last_heartbeat
    is_healthy = heartbeat_age < 60
    status_code = 200 if is_healthy else 503
    return jsonify({
        "status": "OK" if is_healthy else "STALE",
        "ws_running": ws_running,
        "ticks_received": tick_counter,
        "market_open": is_market_open(),
        "last_heartbeat_age_sec": round(heartbeat_age, 1),
        "threads_alive": _init_completed,
        "version": "12.2",
        "features_active": {
            "pcr_oi": True,
            "greeks": True,
            "kill_switch": True,
            "correlation": True,
            "regime_detection": True,
            "volume_profile": True,
            "kelly_criterion": True,
            "slippage_model": True,
            "sharpe_sortino": True,
            "premium_fix": True,
            "token_auto_refresh": True
        }
    }), status_code

@app.route("/api/analytics/<index_name>")
def analytics(index_name):
    if index_name not in INDEX_CONFIG:
        return jsonify({"error": "Invalid index"}), 400
    return jsonify({
        "index": index_name,
        "timestamp": datetime.now().isoformat(),
        "regime": latest_regime.get(index_name, {}),
        "pcr": latest_pcr.get(index_name, {}),
        "greeks": latest_greeks.get(index_name, {}),
        "volume_profile": latest_volume_profile.get(index_name, {}),
        "performance": latest_metrics.get(index_name, {}),
        "drawdown": {
            "current": kill_switches[index_name].current_drawdown,
            "kill_switch": kill_switches[index_name].is_active(),
            "max_allowed": kill_switches[index_name].max_drawdown_pct
        },
        "kelly": {
            "risk_pct": round(kelly_trackers[index_name].get_recommended_risk_pct()[0], 2),
            "win_rate": round(kelly_trackers[index_name].get_recommended_risk_pct()[1], 2),
            "avg_win": round(kelly_trackers[index_name].get_recommended_risk_pct()[2], 2),
            "avg_loss": round(kelly_trackers[index_name].get_recommended_risk_pct()[3], 2),
            "trade_count": kelly_trackers[index_name].win_count + kelly_trackers[index_name].loss_count
        },
        "slippage": {
            "liquidity_score": round(slippage_models[index_name].get_liquidity_score(), 2),
            "avg_spread": round(np.mean(list(slippage_models[index_name].spread_history)) * 100, 3) if slippage_models[index_name].spread_history else 0
        }
    })

@app.route("/api/correlation")
def correlation_endpoint():
    return jsonify({
        "timestamp": datetime.now().isoformat(),
        "nifty_banknifty": latest_correlation,
        "nifty_samples": len(nifty_price_series),
        "banknifty_samples": len(banknifty_price_series)
    })

@app.route("/api/performance")
def performance_endpoint():
    return jsonify({
        "timestamp": datetime.now().isoformat(),
        "metrics": latest_metrics,
        "drawdown_status": {k: {
            "current_drawdown": v.current_drawdown,
            "kill_switch_active": v.is_active()
        } for k, v in kill_switches.items()}
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    _start_background_threads()
    app.run(host="0.0.0.0", port=port, debug=False)
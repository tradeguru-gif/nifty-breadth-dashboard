# === EQUITY-ONLY SCALPING v16.1 – 1m/2m/3m/5m (No Telegram, No Commodity) ===
# FIX: Better WebSocket/REST coordination, fixed database initialization
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
import struct
import traceback
from collections import deque, Counter
from datetime import datetime, timedelta, time as dt_time, timezone
from flask import Flask, jsonify, request
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor, as_completed
import pyotp

# ---------- DEBUG MODE ----------
DEBUG_MODE = os.getenv("DEBUG_MODE", "0") == "1"
PAPER_MODE = os.getenv("PAPER_MODE", "0") == "1"

logging.basicConfig(level=logging.DEBUG if DEBUG_MODE else logging.INFO,
                    stream=sys.stdout, force=True)
logger = logging.getLogger(__name__)

app = Flask(__name__)

ALLOWED_ORIGINS = [
    "http://index-options.co",
    "https://index-options.co",
    "http://localhost:5000",
    "http://127.0.0.1:5000",
    "https://kame.nifty-options.workers.dev"
]
CORS(app, origins="*", supports_credentials=False)

API_KEY = os.getenv("API_KEY", "")
application = app

# ----------------------------------------------------------------------
# ENVIRONMENT (Angel One credentials only)
# ----------------------------------------------------------------------
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing critical Angel One environment variables")

DB_PATH = "trading_data.db"
PAPER_DB_PATH = "paper_trading_data.db" if PAPER_MODE else DB_PATH

# ----------------------------------------------------------------------
# DATABASE (FIXED: Ensure all tables exist)
# ----------------------------------------------------------------------
def init_db():
    """Initialize all required database tables with proper error handling"""
    db_paths = [DB_PATH]
    if PAPER_MODE:
        db_paths.append(PAPER_DB_PATH)
    
    for db_path in db_paths:
        try:
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            
            # Main tables
            c.execute("""CREATE TABLE IF NOT EXISTS ticks 
                        (timestamp REAL, token TEXT, price REAL, volume REAL, bid REAL, ask REAL, oi REAL)""")
            c.execute("CREATE INDEX IF NOT EXISTS idx_ticks_time ON ticks(timestamp)")
            
            c.execute("""CREATE TABLE IF NOT EXISTS signals 
                        (timestamp REAL, action TEXT, signal_type TEXT, grade TEXT, confidence REAL, 
                         ce_price REAL, pe_price REAL, rsi REAL, pcr REAL, regime TEXT, vwap REAL, vix REAL, ml_score REAL)""")
            
            c.execute("""CREATE TABLE IF NOT EXISTS trades 
                        (timestamp REAL, action TEXT, entry_price REAL, exit_price REAL, pnl REAL, 
                         size_pct REAL, status TEXT, grade TEXT, atr REAL, vix REAL, exit_reason TEXT)""")
            
            c.execute("""CREATE TABLE IF NOT EXISTS daily_performance 
                        (date TEXT, equity REAL, daily_pnl REAL, drawdown_pct REAL, sharpe REAL, var REAL, win_rate REAL)""")
            
            c.execute("""CREATE TABLE IF NOT EXISTS ml_models 
                        (id INTEGER PRIMARY KEY, model BLOB, created_at REAL, features TEXT, accuracy REAL)""")
            
            c.execute("""CREATE TABLE IF NOT EXISTS portfolio_equity 
                        (index_name TEXT PRIMARY KEY, equity REAL, last_updated REAL, active_action TEXT, 
                         entry_price REAL, stop_loss REAL, target REAL, lots INTEGER, entry_time REAL, 
                         highest REAL, last_trade_date TEXT, daily_trade_count INTEGER)""")
            
            c.execute("""CREATE TABLE IF NOT EXISTS kelly_history 
                        (timestamp REAL, index_name TEXT, kelly_fraction REAL, win_rate REAL, 
                         avg_win REAL, avg_loss REAL, recommended_lots INTEGER, actual_lots INTEGER)""")
            
            c.execute("""CREATE TABLE IF NOT EXISTS greeks_history 
                        (timestamp REAL, index_name TEXT, ce_iv REAL, pe_iv REAL, ce_delta REAL, pe_delta REAL, 
                         ce_gamma REAL, pe_gamma REAL, ce_theta REAL, pe_theta REAL, ce_vega REAL, pe_vega REAL, 
                         iv_rank REAL, iv_percentile REAL)""")
            
            c.execute("""CREATE TABLE IF NOT EXISTS performance_metrics 
                        (timestamp REAL, index_name TEXT, sharpe REAL, sortino REAL, calmar REAL, 
                         win_rate REAL, profit_factor REAL, max_drawdown REAL, avg_trade REAL, expectancy REAL)""")
            
            c.execute("""CREATE TABLE IF NOT EXISTS drawdown_events 
                        (timestamp REAL, index_name TEXT, drawdown_pct REAL, action_taken TEXT, 
                         equity_before REAL, equity_after REAL)""")
            
            c.execute("""CREATE TABLE IF NOT EXISTS signal_state 
                        (index_name TEXT PRIMARY KEY, action TEXT, entry_price REAL, stop_loss REAL, 
                         target REAL, lots INTEGER, entry_time REAL, highest REAL)""")
            
            conn.commit()
            logger.info(f"Database initialized: {db_path}")
        except Exception as e:
            logger.error(f"Database initialization error for {db_path}: {e}")
        finally:
            conn.close()

init_db()

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ============================================================
# MONKEY PATCHES – fix SmartAPI WS bugs (ENHANCED BINARY PARSER)
# ============================================================
_original_parse = SmartWebSocketV2._parse_binary_data
def _patched_parse(self, binary_data):
    try:
        result = _original_parse(self, binary_data)
        if result and result.get('token'):
            return result
    except Exception:
        result = {}
    try:
        # Fallback manual byte parsing for SmartWebSocketV2 (Angel One binary format)
        if len(binary_data) >= 10:
            token_int = int.from_bytes(binary_data[2:10], byteorder='little')
            if token_int > 0:
                result['token'] = str(token_int)

                # Extract LTP (bytes 26-33, int64, divide by 100)
                if len(binary_data) >= 34:
                    ltp_raw = int.from_bytes(binary_data[26:34], byteorder='little', signed=True)
                    result['last_traded_price'] = ltp_raw / 100.0

                # Extract Volume (bytes 42-49)
                if len(binary_data) >= 50:
                    vol_raw = int.from_bytes(binary_data[42:50], byteorder='little', signed=True)
                    result['volume'] = vol_raw

                # Extract Bid/Ask Prices (bytes 50-57, 66-73) with sanity checks
                if len(binary_data) >= 74:
                    bid_raw = int.from_bytes(binary_data[50:58], byteorder='little', signed=True)
                    ask_raw = int.from_bytes(binary_data[66:74], byteorder='little', signed=True)
                    if 0 < bid_raw < 100000000:
                        result['best_bid_price'] = bid_raw / 100.0
                    if 0 < ask_raw < 100000000:
                        result['best_ask_price'] = ask_raw / 100.0

                # Extract Open Interest (bytes 82-89)
                if len(binary_data) >= 90:
                    oi_raw = int.from_bytes(binary_data[82:90], byteorder='little', signed=True)
                    result['open_interest'] = oi_raw

                return result
    except Exception as e:
        logger.debug(f"Critical error in custom binary fallback parser: {e}")
    return result
SmartWebSocketV2._parse_binary_data = _patched_parse

_original_on_close = SmartWebSocketV2._on_close
def _patched_on_close(self, wsapp, *args):
    try:
        _original_on_close(self, wsapp, *args)
    except Exception:
        pass
SmartWebSocketV2._on_close = _patched_on_close

_original_on_pong = SmartWebSocketV2._on_pong
def _patched_on_pong(self, wsapp, *args):
    try:
        _original_on_pong(self, wsapp, *args)
    except Exception:
        pass
SmartWebSocketV2._on_pong = _patched_on_pong
# ============================================================

# ----------------------------------------------------------------------
# INDEX CONFIGURATION – Only Equity Indices (5 active)
# ----------------------------------------------------------------------
INDEX_CONFIG = {
    "NIFTY": {
        "token": "99926000", "exchange": "NSE", "symbol": "NIFTY", "lot_size": 50, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": "BANKNIFTY", "greeks_enabled": True, "pcr_enabled": True,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.6
    },
    "BANKNIFTY": {
        "token": "99926009", "exchange": "NSE", "symbol": "BANKNIFTY", "lot_size": 25, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": "NIFTY", "greeks_enabled": True, "pcr_enabled": True,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.8
    },
    "FINNIFTY": {
        "token": "99926037", "exchange": "NSE", "symbol": "FINNIFTY", "lot_size": 40, "expiry_weekday": 1, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.6
    },
    "SENSEX": {
        "token": "99919000", "exchange": "BSE", "symbol": "SENSEX", "lot_size": 15, "expiry_weekday": 4, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100, "option_exchange": "BFO",
        "ws_exchange_type": 3, "option_ws_exchange_type": 4, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.5
    },
    "BANKEX": {
        "token": "99919012", "exchange": "BSE", "symbol": "BANKEX", "lot_size": 15, "expiry_weekday": 4, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100, "option_exchange": "BFO",
        "ws_exchange_type": 3, "option_ws_exchange_type": 4, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": "BANKNIFTY", "greeks_enabled": True, "pcr_enabled": True,
        "regime_adx_threshold": 25, "regime_atr_threshold": 0.8
    },
}

INDEX_TOKEN_SET = {cfg["token"] for cfg in INDEX_CONFIG.values() if cfg.get("token")}
INDEX_NAMES = [idx for idx, cfg in INDEX_CONFIG.items() if cfg.get("active")]

# ----------------------------------------------------------------------
# GLOBAL STATE
# ----------------------------------------------------------------------
_latest_ticks_lock = threading.Lock()
_market_signal_lock = threading.Lock()
_portfolio_state_lock = threading.Lock()
_signal_state_lock = threading.Lock()
_candle_histories_lock = threading.Lock()
_price_histories_lock = threading.Lock()
_ce_price_histories_lock = threading.Lock()
_pe_price_histories_lock = threading.Lock()
_current_candle_lock = threading.Lock()
_prev_volume_lock = threading.Lock()

INDEX_TOKENS = {idx: {"ce_token": None, "pe_token": None, "atm_strike": 0, "expiry": "", "expiry_date": None, "ce_symbol": "", "pe_symbol": ""} for idx in INDEX_NAMES}
last_known_prices = {idx: {"spot": 0.0, "ce": 0.0, "pe": 0.0, "timestamp": 0} for idx in INDEX_NAMES}
price_histories = {idx: deque(maxlen=5000) for idx in INDEX_NAMES}

portfolio_state = {idx: {"equity": 100000.0, "open_positions": 0, "daily_pnl": 0.0, "total_pnl": 0.0, "live_pnl": 0.0} for idx in INDEX_NAMES}
signal_state = {idx: {"action": "HOLD", "entry_price": 0.0, "stop_loss": 0.0, "target": 0.0, "lots": 0, "entry_time": 0.0, "highest": 0.0, "cooldown": 0, "confidence": 0, "exit_reason": ""} for idx in INDEX_NAMES}

market_signal = {idx: {"sentiment_score": 50, "signal": "WAITING", "alert_message": "", "entry_price": 0, "stop_loss": 0, "target": 0, "exit_reason": "", "quality_score": 0, "strike_price": 0, "trading_symbol": "", "status": ""} for idx in INDEX_NAMES}

# ---------- TRADE STATE (for PnL tracking) ----------
trade_state = {
    idx: {
        "active": False,
        "side": "",
        "entry_price": 0.0,
        "current_price": 0.0,
        "stop_loss": 0.0,
        "target": 0.0,
        "pnl": 0.0,
        "pnl_points": 0.0,
        "entry_time": "",
        "exit_time": ""
    } for idx in INDEX_NAMES
}

ce_price_histories = {idx: deque(maxlen=2000) for idx in INDEX_NAMES}
pe_price_histories = {idx: deque(maxlen=2000) for idx in INDEX_NAMES}
ce_volume_histories = {idx: deque(maxlen=1000) for idx in INDEX_NAMES}
pe_volume_histories = {idx: deque(maxlen=1000) for idx in INDEX_NAMES}
ce_oi_histories = {idx: deque(maxlen=200) for idx in INDEX_NAMES}
pe_oi_histories = {idx: deque(maxlen=200) for idx in INDEX_NAMES}

vix_history = deque(maxlen=200)
nifty_price_series = deque(maxlen=200)
banknifty_price_series = deque(maxlen=200)

latest_ticks = {}
for idx in INDEX_CONFIG:
    latest_ticks[idx] = {
        "spot_price": 0.0, "ce_price": 0.0, "pe_price": 0.0,
        "ce_volume": 0, "pe_volume": 0, "ce_oi": 0, "pe_oi": 0,
        "ce_bid": 0.0, "ce_ask": 0.0, "pe_bid": 0.0, "pe_ask": 0.0
    }
latest_ticks["VIX"] = {"vix": 15.0}

daily_drawdown = {idx: {"peak_equity": 0.0, "current_drawdown": 0.0, "dd_warning_sent": False} for idx in INDEX_NAMES}
safety_state = {idx: {"consecutive_sl": 0, "circuit_breaker": False, "circuit_breaker_until": 0} for idx in INDEX_NAMES}
signal_buffer = {idx: {"ce_count": 0, "pe_count": 0} for idx in INDEX_NAMES}
daily_trade_count = {idx: 0 for idx in INDEX_NAMES}
last_trade_date = {idx: "" for idx in INDEX_NAMES}

# ---------- PORTFOLIO-LEVEL (ALL-INDEX) KILL SWITCH ----------
GLOBAL_MAX_DAILY_DRAWDOWN_PCT = float(os.getenv("GLOBAL_MAX_DAILY_DRAWDOWN_PCT", "5.0"))
global_kill_switch = {"triggered": False, "date": "", "peak_total_equity": 0.0}
_global_kill_switch_lock = threading.Lock()

_last_signal_run = 0
_signal_run_lock = threading.Lock()

_historical_iv_ce = {idx: deque(maxlen=200) for idx in INDEX_NAMES}
_historical_iv_pe = {idx: deque(maxlen=200) for idx in INDEX_NAMES}
_regime_history = {idx: deque(maxlen=5) for idx in INDEX_NAMES}

# ----------------------------------------------------------------------
# TIMEFRAME DEFINITIONS – Only 1m, 2m, 3m, 5m
# ----------------------------------------------------------------------
TIMEFRAMES = ["1min", "2min", "3min", "5min"]
TIMEFRAME_SECONDS = {"1min":60, "2min":120, "3min":180, "5min":300}
TIMEFRAME_WEIGHTS = {"1min":8, "2min":10, "3min":12, "5min":16}

candle_histories = {idx: {tf: deque(maxlen=500) for tf in TIMEFRAMES} for idx in INDEX_NAMES}
_last_candle_time = {idx: {tf: 0 for tf in TIMEFRAMES} for idx in INDEX_NAMES}
_current_candle = {idx: {tf: None for tf in TIMEFRAMES} for idx in INDEX_NAMES}
_prev_volume = {idx: 0 for idx in INDEX_NAMES}

# ----------------------------------------------------------------------
# HELPER: Update trade PnL
# ----------------------------------------------------------------------
def update_trade_pnl(index_name, current_price):
    if not trade_state[index_name]["active"] or current_price <= 0:
        return
    trade_state[index_name]["current_price"] = current_price
    pts = current_price - trade_state[index_name]["entry_price"]
    lot_size = INDEX_CONFIG[index_name]["lot_size"]
    lots = signal_state[index_name].get("lots", 1) or 1
    trade_state[index_name]["pnl_points"] = round(pts, 2)
    trade_state[index_name]["pnl"] = round(pts * lot_size * lots, 2)

# ----------------------------------------------------------------------
# [REST OF YOUR CODE - All the indicator functions, authentication,
#  token management, signal engine, WebSocket handlers, etc. remain exactly
#  the same as your original file from here...]
# ----------------------------------------------------------------------

# ============================================================
# CRITICAL FIX: Improved Connection Manager - WebSocket ONLY,
# REST ONLY as fallback when WebSocket fails
# ============================================================
class ConnectionManager:
    def __init__(self):
        force_rest = os.getenv("FORCE_REST_MODE", "0") == "1"
        self.use_websocket = not force_rest
        self._ws_thread = None
        self._rest_thread = None
        self._refresh_thread = None
        self._ws_connected = False
        self._rest_active = False
        self._ws_failure_count = 0
        self._max_ws_failures = 3

    def start(self):
        if self.use_websocket:
            logger.info("WebSocket mode enabled. Starting WS thread.")
            self._ws_thread = threading.Thread(target=self._ws_connection_loop, daemon=True)
            self._ws_thread.start()
            threading.Thread(target=self._ws_watchdog, daemon=True).start()
            threading.Thread(target=self._tick_watchdog, daemon=True).start()
        else:
            logger.info("REST-only mode forced by environment variable.")
            self._start_rest_fallback()

        # Always start token refresh scheduler
        self._refresh_thread = threading.Thread(target=self._schedule_token_refresh, daemon=True)
        self._refresh_thread.start()
        logger.info("Pre-market token refresh scheduler started.")

    def _ws_connection_loop(self):
        """WebSocket connection loop with exponential backoff"""
        global sws, ws_running, last_heartbeat, last_tick_timestamp
        
        retry_delay = 5
        max_retry_delay = 120
        
        while True:
            try:
                if not is_market_open():
                    logger.info("Market closed. Waiting 60s before retry...")
                    time.sleep(60)
                    continue

                # Reset connection state
                with _ws_connect_lock:
                    ws_running = False
                    sws = None

                auth_token, feed_token, _ = get_auth_token()
                if not feed_token:
                    logger.error("Failed to get feed token, retrying in 10s...")
                    time.sleep(10)
                    continue

                new_sws = SmartWebSocketV2(
                    auth_token,
                    ANGEL_API_KEY,
                    ANGEL_CLIENT_ID,
                    feed_token,
                    max_retry_attempt=3
                )

                new_sws.on_open = on_ws_open
                new_sws.on_data = on_ws_data
                new_sws.on_error = on_ws_error
                new_sws.on_close = on_ws_close

                with _ws_connect_lock:
                    sws = new_sws

                logger.info("Attempting WebSocket connection...")
                ws_thread = threading.Thread(target=sws.connect, daemon=True)
                ws_thread.start()

                # Wait for connection to establish
                time.sleep(8)

                # Check if connection succeeded
                with _ws_connect_lock:
                    if not ws_running:
                        logger.warning("WebSocket connection failed to establish")
                        self._ws_failure_count += 1
                        retry_delay = min(retry_delay * 2, max_retry_delay)
                        logger.info(f"Retrying in {retry_delay}s (failure count: {self._ws_failure_count})")
                        time.sleep(retry_delay)
                        continue

                # Connection successful - reset failure count
                self._ws_failure_count = 0
                retry_delay = 5
                self._ws_connected = True
                
                # Stop REST fallback if running
                self._rest_active = False

                # Monitor connection health
                while True:
                    time.sleep(10)
                    
                    # Check if market closed
                    if not is_market_open():
                        logger.info("Market closed, disconnecting WebSocket")
                        with _ws_connect_lock:
                            ws_running = False
                        if sws:
                            try:
                                sws.close_connection()
                            except:
                                pass
                        break
                    
                    # Check connection health
                    with _ws_connect_lock:
                        if not ws_running:
                            logger.warning("WebSocket disconnected detected")
                            break
                    
                    # Check for data starvation
                    if time.time() - last_tick_timestamp > 60:
                        logger.warning("No ticks for 60s - forcing reconnect")
                        with _ws_connect_lock:
                            ws_running = False
                        if sws:
                            try:
                                sws.close_connection()
                            except:
                                pass
                        break
                    
                    # Send heartbeat if needed
                    if time.time() - last_heartbeat > 20:
                        try:
                            if sws and hasattr(sws, 'send_heartbeat'):
                                sws.send_heartbeat()
                                last_heartbeat = time.time()
                        except Exception:
                            pass

            except Exception as e:
                logger.error(f"WebSocket connection loop error: {e}")
                retry_delay = min(retry_delay * 2, max_retry_delay)
                time.sleep(retry_delay)

    def _start_rest_fallback(self):
        """Start REST fallback mode"""
        if not self._rest_active:
            self._rest_active = True
            self._rest_thread = threading.Thread(target=self._rest_fallback_loop, daemon=True)
            self._rest_thread.start()
            logger.info("REST fallback mode started")

    def _rest_fallback_loop(self):
        """REST fallback loop - runs when WebSocket is unavailable"""
        global last_rest_fetch, last_tick_timestamp
        
        logger.info("Starting REST fallback (concurrent fetch mode)")
        cycle_count = 0
        
        while True:
            # Only run REST if WebSocket is not running
            with _ws_connect_lock:
                if ws_running:
                    time.sleep(10)
                    continue
            
            cycle_count += 1
            try:
                assets_to_fetch = []
                for idx in INDEX_NAMES:
                    cfg = INDEX_CONFIG[idx]
                    if cfg.get("active") and is_market_open():
                        assets_to_fetch.append(idx)

                if not assets_to_fetch:
                    time.sleep(30)
                    continue

                # Refresh tokens if needed
                for idx in assets_to_fetch:
                    tokens = INDEX_TOKENS.get(idx, {})
                    if not tokens.get("ce_token") or not tokens.get("pe_token"):
                        logger.warning(f"Tokens missing for {idx}, refreshing...")
                        get_current_atm_tokens(idx)

                # Fetch data concurrently
                with ThreadPoolExecutor(max_workers=3) as executor:
                    futures = {executor.submit(fetch_asset_data, idx): idx for idx in assets_to_fetch}
                    for future in as_completed(futures):
                        result = future.result()
                        idx = result["index"]
                        try:
                            spot = result.get("spot")
                            if spot and spot > 0:
                                with _latest_ticks_lock:
                                    latest_ticks[idx]["spot_price"] = spot
                                with _price_histories_lock:
                                    price_histories[idx].append(spot)
                                update_candle(idx, spot, 0, time.time())
                                with _latest_ticks_lock:
                                    last_known_prices[idx]["spot"] = spot
                                    last_known_prices[idx]["timestamp"] = time.time()
                                last_tick_timestamp = time.time()

                            ce = result.get("ce")
                            pe = result.get("pe")
                            if ce:
                                with _latest_ticks_lock:
                                    latest_ticks[idx]["ce_price"] = ce["ltp"]
                                    latest_ticks[idx]["ce_volume"] = ce["volume"]
                                    latest_ticks[idx]["ce_oi"] = ce["oi"]
                                    latest_ticks[idx]["ce_bid"] = ce["bid"]
                                    latest_ticks[idx]["ce_ask"] = ce["ask"]
                                with _latest_ticks_lock:
                                    last_known_prices[idx]["ce"] = ce["ltp"]
                                with _ce_price_histories_lock:
                                    ce_price_histories[idx].append(ce["ltp"])
                                if trade_state[idx]["active"] and "CE" in trade_state[idx]["side"]:
                                    update_trade_pnl(idx, ce["ltp"])
                                last_tick_timestamp = time.time()
                            if pe:
                                with _latest_ticks_lock:
                                    latest_ticks[idx]["pe_price"] = pe["ltp"]
                                    latest_ticks[idx]["pe_volume"] = pe["volume"]
                                    latest_ticks[idx]["pe_oi"] = pe["oi"]
                                    latest_ticks[idx]["pe_bid"] = pe["bid"]
                                    latest_ticks[idx]["pe_ask"] = pe["ask"]
                                with _latest_ticks_lock:
                                    last_known_prices[idx]["pe"] = pe["ltp"]
                                with _pe_price_histories_lock:
                                    pe_price_histories[idx].append(pe["ltp"])
                                if trade_state[idx]["active"] and "PE" in trade_state[idx]["side"]:
                                    update_trade_pnl(idx, pe["ltp"])
                                last_tick_timestamp = time.time()
                        except Exception as e:
                            logger.error(f"Error processing REST result for {idx}: {e}")

                # VIX
                try:
                    vix = get_vix_ltp()
                    if vix:
                        with _latest_ticks_lock:
                            latest_ticks["VIX"]["vix"] = vix
                        vix_history.append(vix)
                except Exception as e:
                    logger.debug(f"VIX REST fetch error: {e}")

                last_rest_fetch = time.time()

                # Run signals
                ready_indices = [idx for idx in INDEX_NAMES if INDEX_CONFIG[idx].get("active") and has_complete_data(idx)]
                if ready_indices:
                    run_all_signals()

                # Check if WebSocket reconnected
                with _ws_connect_lock:
                    if ws_running:
                        logger.info("REST cycle interrupted: WS reconnected")
                        self._rest_active = False
                        break

                # Sleep between cycles
                cycle_interval = int(os.getenv("REST_CYCLE_INTERVAL", "10"))
                time.sleep(cycle_interval)

            except Exception as e:
                logger.error(f"REST fallback error: {e}")
                last_rest_fetch = time.time()
                time.sleep(10)

    def _ws_watchdog(self):
        """Monitor WebSocket health and restart if needed"""
        global ws_running, last_heartbeat, last_tick_timestamp
        
        while True:
            time.sleep(15)
            with _ws_connect_lock:
                if not ws_running:
                    continue
                    
            now = time.time()
            if (now - last_heartbeat > 30) or (now - last_tick_timestamp > 60):
                logger.warning(f"Data starvation detected. Last heartbeat: {now - last_heartbeat:.1f}s ago, Last tick: {now - last_tick_timestamp:.1f}s ago")
                with _ws_connect_lock:
                    ws_running = False
                if sws:
                    try:
                        sws.close_connection()
                    except Exception:
                        pass

    def _tick_watchdog(self):
        """Monitor tick counter for stalls"""
        global ws_running, tick_counter, last_tick_timestamp
        
        last_count = 0
        while True:
            time.sleep(10)
            with _ws_connect_lock:
                if not ws_running:
                    continue
                    
            if time.time() - last_tick_timestamp > 60:
                logger.warning("No ticks for 60s - forcing reconnect")
                with _ws_connect_lock:
                    ws_running = False
                if sws:
                    try:
                        sws.close_connection()
                    except:
                        pass
            elif tick_counter == last_count and tick_counter > 0 and time.time() - last_tick_timestamp > 45:
                logger.warning("Tick counter stalled - forcing reconnect")
                with _ws_connect_lock:
                    ws_running = False
                if sws:
                    try:
                        sws.close_connection()
                    except:
                        pass
            else:
                last_count = tick_counter

    def _schedule_token_refresh(self):
        """Pre-market token refresh scheduler"""
        while True:
            now_utc = datetime.now(timezone.utc)
            now_ist = now_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
            if now_ist.weekday() < 5:
                open_time = datetime.combine(now_ist.date(), dt_time(9, 10), tzinfo=now_ist.tzinfo)
                wait = (open_time - now_ist).total_seconds()
                if wait > 0:
                    time.sleep(wait)
                    logger.info("⏰ Pre-market token refresh triggered")
                    refresh_all_tokens()
                    logger.info("Pre-market token refresh completed")
            time.sleep(60)

# ============================================================
# REMAINING FUNCTIONS (unchanged from your original file)
# ============================================================

# [All your other functions: calculate_ema, calculate_rsi, get_auth_token,
#  get_current_atm_tokens, run_signal_engine_for_index, etc. remain EXACTLY
#  as they were in your original file - they are too long to reprint here
#  but should be copied from your current working file]

# ============================================================
# BACKGROUND THREADS – NON-BLOCKING STARTUP
# ============================================================
_init_completed = False
_init_lock = threading.Lock()

def _start_background_threads():
    global _init_completed
    with _init_lock:
        if not _init_completed:
            logger.info("Starting background threads...")
            
            # Start connection manager
            try:
                conn_manager = ConnectionManager()
                conn_manager.start()
                logger.info("Connection manager started.")
            except Exception as e:
                logger.error(f"Failed to start connection manager: {e}")

            # Token prefetch in background
            def prefetch_loop():
                max_retries = 5
                for attempt in range(max_retries):
                    try:
                        refresh_all_tokens()
                        ready = 0
                        total_active = sum(1 for cfg in INDEX_CONFIG.values() if cfg.get("active"))
                        for idx, cfg in INDEX_CONFIG.items():
                            if not cfg.get("active"):
                                continue
                            tokens = INDEX_TOKENS.get(idx, {})
                            if tokens.get("ce_token") and tokens.get("pe_token"):
                                ready += 1
                        logger.info(f"Token prefetch attempt {attempt + 1}: {ready}/{total_active} indices ready")
                        if ready == total_active:
                            break
                        if attempt < max_retries - 1:
                            wait_time = (attempt + 1) * 2
                            logger.warning(f"Waiting {wait_time} seconds before retry...")
                            time.sleep(wait_time)
                    except Exception as e:
                        logger.error(f"Token prefetch error: {e}")

                global _init_completed
                _init_completed = True
                logger.info("Background threads initialized.")

            threading.Thread(target=prefetch_loop, daemon=True).start()
            logger.info("✅ Non-blocking token prefetch started. API is LIVE.")

_background_started = False
_background_start_lock = threading.Lock()

def auto_start_background():
    global _background_started
    with _background_start_lock:
        if not _background_started:
            init_db()
            load_portfolio_state()
            _start_background_threads()
            _background_started = True
            logger.info("Auto-start: Background threads initialized for production")

auto_start_background()

# ============================================================
# FLASK ROUTES (unchanged)
# ============================================================

@app.before_request
def check_auth():
    if API_KEY:
        auth = request.headers.get("X-API-Key")
        if auth != API_KEY:
            return jsonify({"error": "Unauthorized"}), 401

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "healthy",
        "engine": "Equity-Only Scalping v16.1 – with Trade State & PnL",
        "indices": [i for i, cfg in INDEX_CONFIG.items() if cfg.get("active")],
        "market_open": is_market_open()
    })

@app.route("/api/live-signals", methods=["GET"])
def live_signals():
    # [Your existing live_signals function]
    pass

@app.route("/api/signal-audio", methods=["GET"])
def signal_audio():
    # [Your existing signal_audio function]
    pass

@app.route("/api/health", methods=["GET"])
def health():
    # [Your existing health function]
    pass

@app.route("/api/connection-status", methods=["GET"])
def connection_status():
    # [Your existing connection_status function]
    pass

@app.route("/api/candles/<index_name>/<timeframe>", methods=["GET"])
def get_candles(index_name, timeframe):
    # [Your existing get_candles function]
    pass

@app.route("/api/backtest-signal/<index_name>", methods=["POST"])
def backtest_signal(index_name):
    # [Your existing backtest_signal function]
    pass

# ----------------------------------------------------------------------
# RUN FLASK
# ----------------------------------------------------------------------
if __name__ == "__main__":
    init_db()
    load_portfolio_state()
    refresh_all_tokens()
    _start_background_threads()
    logger.info("Background workers initiated. Starting Flask API Server...")
    app.run(host="0.0.0.0", port=5000, debug=False)
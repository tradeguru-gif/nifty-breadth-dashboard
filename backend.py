"""
backend.py — Institutional-Grade Nifty Options Signal Engine v5.0
====================================================================
PROFESSIONAL FEATURES:
- Fixed signal confirmation logic (persistent across ticks)
- Separate CE/PE price history for accurate signal generation
- Real Black-Scholes Greeks with actual IV from option chain
- Redis/PostgreSQL state persistence
- Paper trading + Live trading toggle
- Order management system with position tracking
- Backtesting framework integration
- Proper WebSocket binary parsing (Angel One protobuf)
- Portfolio heat, correlation checks, max daily loss circuit breakers
- Institutional risk engine with Kelly criterion sizing
- Signal journal with P&L tracking

ENVIRONMENT VARIABLES REQUIRED:
  ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET
  DATABASE_URL (PostgreSQL) — optional, falls back to SQLite
  REDIS_URL — optional, falls back to in-memory
  TRADING_MODE — "PAPER" or "LIVE" (default: PAPER)
  MAX_DAILY_LOSS_PCT — max portfolio drawdown % (default: 3.0)
  PORTFOLIO_HEAT_MAX_PCT — max total exposure % (default: 50.0)
"""

import os
import sys
import time
import json
import logging
import threading
import signal
import math
import fcntl
import struct
import sqlite3
import hashlib
import gc
import uuid
from collections import deque
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, List, Tuple, Any
from enum import Enum
from decimal import Decimal, ROUND_HALF_UP

import requests
import pandas as pd
import numpy as np
import pyotp
from flask import Flask, jsonify, request
from flask_cors import CORS

# Try importing Redis, fallback to dict
redis_available = False
try:
    import redis
    redis_available = True
except ImportError:
    pass

# Try importing psycopg2, fallback to SQLite
postgres_available = False
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    postgres_available = True
except ImportError:
    pass

# SmartAPI imports
from libs.SmartApi import SmartConnect
from libs.SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ============================================================
# LOGGING — Structured, Rotating, Async-safe
# ============================================================
from logging.handlers import RotatingFileHandler

os.makedirs("logs", exist_ok=True)

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(threadName)-12s | %(message)s"
)

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

# File handler (rotating, 10MB max, 5 backups)
file_handler = RotatingFileHandler(
    "logs/nifty_engine.log", maxBytes=10*1024*1024, backupCount=5
)
file_handler.setFormatter(formatter)

# Signal journal handler
journal_handler = RotatingFileHandler(
    "logs/signal_journal.log", maxBytes=5*1024*1024, backupCount=10
)
journal_handler.setFormatter(logging.Formatter(
    "%(asctime)s | SIGNAL | %(message)s"
))

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(console_handler)
logger.addHandler(file_handler)

journal_logger = logging.getLogger("signal_journal")
journal_logger.setLevel(logging.INFO)
journal_logger.addHandler(journal_handler)

# ============================================================
# FLASK APP
# ============================================================
app = Flask(__name__)
CORS(app)
application = app

# ============================================================
# ENVIRONMENT & CONFIGURATION
# ============================================================
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///nifty_engine.db")
REDIS_URL = os.getenv("REDIS_URL", None)
TRADING_MODE = os.getenv("TRADING_MODE", "PAPER").upper()
MAX_DAILY_LOSS_PCT = float(os.getenv("MAX_DAILY_LOSS_PCT", "3.0"))
PORTFOLIO_HEAT_MAX_PCT = float(os.getenv("PORTFOLIO_HEAT_MAX_PCT", "50.0"))

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    raise ValueError("Missing Angel One credentials. Set ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET")

logger.info(f"Trading Mode: {TRADING_MODE}")
logger.info(f"Max Daily Loss: {MAX_DAILY_LOSS_PCT}%")
logger.info(f"Max Portfolio Heat: {PORTFOLIO_HEAT_MAX_PCT}%")

# ============================================================
# CONFIGURATION CONSTANTS
# ============================================================
class Config:
    """Institutional-grade configuration parameters."""
    # Technical Indicators
    RSI_PERIOD = 14
    MACD_FAST = 12
    MACD_SLOW = 26
    MACD_SIGNAL = 9
    ATR_PERIOD = 14
    BB_PERIOD = 20
    BB_STD = 2.0
    ADX_PERIOD = 14
    VWAP_WINDOW = 50
    EMA_FAST = 9
    EMA_SLOW = 21
    
    # PCR
    PCR_EMA_PERIOD = 10
    PCR_BULLISH = 0.9
    PCR_BEARISH = 1.2
    
    # Signal Thresholds
    STRONG_BUY_THRESHOLD = 85
    BUY_THRESHOLD = 70
    CONSIDER_THRESHOLD = 55
    
    # Signal Management
    SIGNAL_CONFIRMATION_BARS = 2
    SIGNAL_MIN_DURATION_SEC = 180
    SIGNAL_MAX_AGE_SEC = 1800
    COOLDOWN_AFTER_FLIP_SEC = 30
    MAX_FLIPS_PER_HOUR = 3
    
    # Risk Management
    POSITION_SIZE_BASE_PCT = 10
    POSITION_SIZE_MAX_PCT = 25
    STOP_LOSS_ATR_MULT = 1.5
    TARGET_ATR_MULT = 3.0
    MAX_DRAWDOWN_PCT = 5.0
    TRAILING_STOP_ATR_MULT = 2.0
    
    # Volatility Filters
    VOLUME_FILTER_RATIO = 0.6
    SPREAD_ATR_MULTIPLIER = 1.5
    MIN_VOLATILITY_PCT = 0.3
    
    # Portfolio Risk
    RISK_FREE_RATE = 0.06
    DAYS_TO_EXPIRY = 7
    KELLY_FRACTION = 0.25  # Half-Kelly for safety
    
    # Operational
    TOKEN_REFRESH_SEC = 300
    WS_HEARTBEAT_SEC = 25
    REST_POLL_INTERVAL_SEC = 30
    MARKET_OPEN = "09:15"
    MARKET_CLOSE = "15:30"
    
    # Nifty Lot Size
    NIFTY_LOT_SIZE = 75

CONFIG = Config()

# ============================================================
# ENUMS & DATA CLASSES
# ============================================================
class SignalAction(Enum):
    HOLD = "HOLD"
    STRONG_BUY_CE = "STRONG BUY CE"
    BUY_CE = "BUY CE"
    CONSIDER_CE_BUY = "CONSIDER CE BUY"
    STRONG_BUY_PE = "STRONG BUY PE"
    BUY_PE = "BUY PE"
    CONSIDER_PE_BUY = "CONSIDER PE BUY"
    EXIT_CE = "EXIT CE"
    EXIT_PE = "EXIT PE"

class SignalGrade(Enum):
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    F = "F"

class TradingMode(Enum):
    PAPER = "PAPER"
    LIVE = "LIVE"

class OrderStatus(Enum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    FILLED = "FILLED"
    PARTIAL = "PARTIAL"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"

@dataclass
class Greeks:
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0
    vega: float = 0.0
    iv: float = 0.0
    
@dataclass
class Signal:
    action: str = "HOLD"
    signal_type: str = "NONE"
    grade: str = "D"
    confidence: float = 0.0
    entry_price: float = 0.0
    stop_loss: float = 0.0
    target: float = 0.0
    position_size_pct: float = 0.0
    risk_reward: float = 0.0
    timestamp: str = ""
    regime: str = "RANGING"
    
@dataclass
class Position:
    symbol: str = ""
    token: str = ""
    option_type: str = ""
    entry_price: float = 0.0
    current_price: float = 0.0
    quantity: int = 0
    stop_loss: float = 0.0
    target: float = 0.0
    trailing_stop: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    entry_time: str = ""
    status: str = "OPEN"
    order_id: str = ""
    mode: str = "PAPER"
    highest_price_since_entry: float = 0.0      # ← ADD THIS
    lowest_price_since_entry: float = float("inf")  # ← ADD THIS
    
@dataclass
class TradeJournal:
    trade_id: str = ""
    timestamp: str = ""
    action: str = ""
    symbol: str = ""
    option_type: str = ""
    entry_price: float = 0.0
    exit_price: float = 0.0
    quantity: int = 0
    pnl: float = 0.0
    pnl_pct: float = 0.0
    grade: str = ""
    confidence: float = 0.0
    regime: str = ""
    exit_reason: str = ""
    mode: str = "PAPER"

# ============================================================
# DATABASE LAYER (PostgreSQL/SQLite)
# ============================================================
class DatabaseManager:
    """Persistent storage for signals, positions, and trade journal."""
    
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.is_postgres = db_url.startswith("postgresql://") and postgres_available
        self._init_db()
    
    def _get_conn(self):
        if self.is_postgres:
            return psycopg2.connect(self.db_url)
        else:
            path = self.db_url.replace("sqlite:///", "")
            return sqlite3.connect(path, check_same_thread=False)
    
    def _init_db(self):
        conn = self._get_conn()
        cursor = conn.cursor()
        
        # Signals table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                action TEXT,
                signal_type TEXT,
                grade TEXT,
                confidence REAL,
                ce_price REAL,
                pe_price REAL,
                spread REAL,
                rsi REAL,
                macd REAL,
                pcr REAL,
                vwap REAL,
                atr REAL,
                delta REAL,
                gamma REAL,
                theta REAL,
                vega REAL,
                iv REAL,
                adx REAL,
                bb_position REAL,
                iv_rank REAL,
                regime TEXT,
                session_phase TEXT,
                entry_price REAL,
                stop_loss REAL,
                target REAL,
                position_size_pct REAL,
                risk_reward REAL,
                UNIQUE(timestamp, action)
            )
        """)
        
        # Positions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT UNIQUE,
                symbol TEXT,
                token TEXT,
                option_type TEXT,
                entry_price REAL,
                current_price REAL,
                quantity INTEGER,
                stop_loss REAL,
                target REAL,
                trailing_stop REAL,
                unrealized_pnl REAL,
                realized_pnl REAL,
                entry_time TEXT,
                status TEXT,
                mode TEXT
            )
        """)
        
        # Trade journal table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trade_journal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id TEXT UNIQUE,
                timestamp TEXT,
                action TEXT,
                symbol TEXT,
                option_type TEXT,
                entry_price REAL,
                exit_price REAL,
                quantity INTEGER,
                pnl REAL,
                pnl_pct REAL,
                grade TEXT,
                confidence REAL,
                regime TEXT,
                exit_reason TEXT,
                mode TEXT
            )
        """)
        
        # Daily P&L tracking
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_pnl (
                date TEXT PRIMARY KEY,
                starting_equity REAL,
                current_equity REAL,
                realized_pnl REAL,
                unrealized_pnl REAL,
                max_drawdown_pct REAL,
                num_trades INTEGER,
                num_wins INTEGER,
                num_losses INTEGER,
                win_rate REAL
            )
        """)
        
        # Backtest results
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS backtest_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT UNIQUE,
                start_date TEXT,
                end_date TEXT,
                total_return_pct REAL,
                sharpe_ratio REAL,
                max_drawdown_pct REAL,
                win_rate REAL,
                profit_factor REAL,
                num_trades INTEGER,
                avg_trade_return REAL,
                config TEXT,
                created_at TEXT
            )
        """)
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized: {'PostgreSQL' if self.is_postgres else 'SQLite'}")
    
    def save_signal(self, signal_data: dict):
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO signals 
                (timestamp, action, signal_type, grade, confidence, ce_price, pe_price, spread,
                 rsi, macd, pcr, vwap, atr, delta, gamma, theta, vega, iv,
                 adx, bb_position, iv_rank, regime, session_phase,
                 entry_price, stop_loss, target, position_size_pct, risk_reward)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal_data.get("timestamp"), signal_data.get("action"), 
                signal_data.get("signal_type"), signal_data.get("grade"),
                signal_data.get("confidence"), signal_data.get("ce_price"),
                signal_data.get("pe_price"), signal_data.get("spread"),
                signal_data.get("rsi"), signal_data.get("macd"),
                signal_data.get("pcr"), signal_data.get("vwap"),
                signal_data.get("atr"), signal_data.get("delta"),
                signal_data.get("gamma"), signal_data.get("theta"),
                signal_data.get("vega"), signal_data.get("iv"),
                signal_data.get("adx"), signal_data.get("bb_position"),
                signal_data.get("iv_rank"), signal_data.get("regime"),
                signal_data.get("session_phase"), signal_data.get("entry_price"),
                signal_data.get("stop_loss"), signal_data.get("target"),
                signal_data.get("position_size_pct"), signal_data.get("risk_reward")
            ))
            conn.commit()
        except Exception as e:
            logger.error(f"DB save_signal error: {e}")
        finally:
            conn.close()
    
    def save_position(self, pos: Position):
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO positions (order_id, symbol, token, option_type, entry_price,
                    current_price, quantity, stop_loss, target, trailing_stop,
                    unrealized_pnl, realized_pnl, entry_time, status, mode)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    current_price=excluded.current_price,
                    unrealized_pnl=excluded.unrealized_pnl,
                    realized_pnl=excluded.realized_pnl,
                    status=excluded.status,
                    trailing_stop=excluded.trailing_stop
            """, (pos.order_id, pos.symbol, pos.token, pos.option_type, pos.entry_price,
                  pos.current_price, pos.quantity, pos.stop_loss, pos.target, pos.trailing_stop,
                  pos.unrealized_pnl, pos.realized_pnl, pos.entry_time, pos.status, pos.mode))
            conn.commit()
        except Exception as e:
            logger.error(f"DB save_position error: {e}")
        finally:
            conn.close()
    
    def save_trade(self, trade: TradeJournal):
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO trade_journal (trade_id, timestamp, action, symbol, option_type,
                    entry_price, exit_price, quantity, pnl, pnl_pct, grade, confidence,
                    regime, exit_reason, mode)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_id) DO UPDATE SET
                    exit_price=excluded.exit_price,
                    pnl=excluded.pnl,
                    pnl_pct=excluded.pnl_pct,
                    exit_reason=excluded.exit_reason
            """, (trade.trade_id, trade.timestamp, trade.action, trade.symbol, trade.option_type,
                  trade.entry_price, trade.exit_price, trade.quantity, trade.pnl, trade.pnl_pct,
                  trade.grade, trade.confidence, trade.regime, trade.exit_reason, trade.mode))
            conn.commit()
        except Exception as e:
            logger.error(f"DB save_trade error: {e}")
        finally:
            conn.close()
    
    def get_open_positions(self) -> List[Position]:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM positions WHERE status = 'OPEN'")
            rows = cursor.fetchall()
            positions = []
            for row in rows:
                pos = Position(
                    order_id=row[1], symbol=row[2], token=row[3], option_type=row[4],
                    entry_price=row[5], current_price=row[6], quantity=row[7],
                    stop_loss=row[8], target=row[9], trailing_stop=row[10],
                    unrealized_pnl=row[11], realized_pnl=row[12],
                    entry_time=row[13], status=row[14], mode=row[15]
                )
                positions.append(pos)
            return positions
        except Exception as e:
            logger.error(f"DB get_open_positions error: {e}")
            return []
        finally:
            conn.close()
    
    def get_today_pnl(self) -> dict:
        today = datetime.now().strftime("%Y-%m-%d")
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT SUM(pnl), COUNT(*), 
                       SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                       SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losses
                FROM trade_journal 
                WHERE date(timestamp) = date(?)
            """, (today,))
            row = cursor.fetchone()
            return {
                "realized_pnl": row[0] or 0,
                "num_trades": row[1] or 0,
                "wins": row[2] or 0,
                "losses": row[3] or 0
            }
        except Exception as e:
            logger.error(f"DB get_today_pnl error: {e}")
            return {"realized_pnl": 0, "num_trades": 0, "wins": 0, "losses": 0}
        finally:
            conn.close()

# Initialize database
db = DatabaseManager(DATABASE_URL)

# ============================================================
# REDIS / STATE MANAGER
# ============================================================
class StateManager:
    """Manages runtime state with Redis fallback to in-memory."""
    
    def __init__(self):
        self._redis = None
        self._memory = {}
        self._lock = threading.RLock()
        
        if redis_available and REDIS_URL:
            try:
                self._redis = redis.from_url(REDIS_URL, decode_responses=True)
                self._redis.ping()
                logger.info("Redis connected")
            except Exception as e:
                logger.warning(f"Redis connection failed: {e}. Using in-memory state.")
                self._redis = None
    
    def set(self, key: str, value: Any, ttl: int = 3600):
        with self._lock:
            if self._redis:
                self._redis.setex(key, ttl, json.dumps(value))
            else:
                self._memory[key] = {"value": value, "expires": time.time() + ttl}
    
    def get(self, key: str, default=None):
        with self._lock:
            if self._redis:
                val = self._redis.get(key)
                return json.loads(val) if val else default
            else:
                entry = self._memory.get(key)
                if entry and entry["expires"] > time.time():
                    return entry["value"]
                return default
    
    def delete(self, key: str):
        with self._lock:
            if self._redis:
                self._redis.delete(key)
            else:
                self._memory.pop(key, None)

state_mgr = StateManager()

# ============================================================
# GLOBAL STATE
# ============================================================
spot_cache = {"value": None, "timestamp": 0}
CE_TOKEN = None
PE_TOKEN = None
CE_SYMBOL = ""
PE_SYMBOL = ""
ATM_STRIKE = 0
EXPIRY_DATE = ""

# Separate price histories for CE and PE
ce_price_history = deque(maxlen=500)
pe_price_history = deque(maxlen=500)
ce_volume_history = deque(maxlen=500)
pe_volume_history = deque(maxlen=500)

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

_LOCK_FILE = "/tmp/nifty_signal_engine.lock"
_is_primary_worker = False
_worker_lock_fd = None

_reconnecting = False
_reconnect_lock = threading.Lock()
_ws_connect_lock = threading.Lock()
_last_429_time = 0

# Timeframe snapshots
timeframe_history = {
    "1min": deque(maxlen=60),
    "5min": deque(maxlen=20),
    "10min": deque(maxlen=15),
    "15min": deque(maxlen=10),
    "20min": deque(maxlen=10)
}
last_minute_snapshot = {"time": 0, "price": 0, "volume": 0}

# Signal state — PERSISTENT (survives across ticks)
signal_state = {
    "current_action": "HOLD",
    "current_signal_type": "NONE",
    "pending_action": None,
    "pending_signal_type": None,
    "confirmation_count": 0,
    "required_confirmations": CONFIG.SIGNAL_CONFIRMATION_BARS,
    "signal_start_time": None,
    "last_confirmed_action": "HOLD",
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
    "lowest_price_since_entry": float("inf"),
}

# Portfolio state
portfolio_state = {
    "equity": 100000.0,
    "available_cash": 100000.0,
    "total_exposure": 0.0,
    "total_exposure_pct": 0.0,
    "daily_pnl": 0.0,
    "daily_pnl_pct": 0.0,
    "max_drawdown_today": 0.0,
    "starting_equity_today": 100000.0,
    "circuit_breaker_triggered": False,
    "positions": [],
}

# Market data
market_signal = {
    "signal": "WAITING", "ce_price": 0.0, "pe_price": 0.0, "spread": 0.0,
    "rsi": 50, "macd": 0.0, "pcr": 1.0, "vwap": 0.0, "atr": 0.0,
    "ema_fast": 0.0, "ema_slow": 0.0, "delta": 0.0, "gamma": 0.0,
    "theta": 0.0, "vega": 0.0, "volume": 0, "timestamp": "",
    "atr_pct": 0.0, "adx": 0.0, "bb_position": 50.0, "rsi_divergence": "NONE",
    "iv_rank": 50, "signal_grade": "D", "iv": 0.0
}

market_state = {
    "rsi": 50, "momentum": "NEUTRAL", "strength": "LOW", "trend": "SIDEWAYS",
    "action": "HOLD", "confidence": 0, "volatility": "NORMAL", "alert": "NONE",
    "regime": "UNKNOWN", "session_phase": "UNKNOWN",
    "trend_1min": "SIDEWAYS", "trend_5min": "SIDEWAYS", "trend_10min": "SIDEWAYS",
    "trend_15min": "SIDEWAYS", "trend_20min": "SIDEWAYS", "timeframe_agreement": 0
}

institutional_state = {
    "vwap": 0, "ema_fast": 0, "ema_slow": 0, "ema_signal": "NEUTRAL", "atr": 0,
    "oi_buildup": "NEUTRAL", "iv_state": "NORMAL", "candle_structure": "SIDEWAYS",
    "market_breadth": "BALANCED", "volume_profile": "NORMAL", "smart_money_flow": "NEUTRAL",
    "delta": 0, "gamma": 0, "theta": 0, "vega": 0, "iv": 0,
    "institutional_signal": "HOLD", "institutional_confidence": 0,
    "signal_grade": "D", "position_size_pct": 0, "risk_reward": 0.0,
    "entry_price": 0.0, "stop_loss": 0.0, "target": 0.0, "max_drawdown_pct": 0.0
}

# PCR cache
pcr_cache = {"value": 1.0, "time": 0, "source": "default"}
pcr_history = deque(maxlen=20)

# Option chain cache for Greeks
option_chain_cache = {"data": None, "timestamp": 0}

# Auth cache
auth_cache = {"token": None, "feed_token": None, "timestamp": 0, "obj": None}
AUTH_CACHE_TTL = 3600

# ============================================================
# HELPER FUNCTIONS
# ============================================================
def is_market_open():
    """Check if Indian equity markets are open."""
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    start = datetime.strptime(CONFIG.MARKET_OPEN, "%H:%M").time()
    end = datetime.strptime(CONFIG.MARKET_CLOSE, "%H:%M").time()
    return start <= now.time() <= end

def get_market_phase():
    """Get current market session phase."""
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
    """Fetch NIFTY 50 spot price from NSE."""
    try:
        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/market-data/live-equity-market"
        }
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        time.sleep(0.5)
        resp = session.get(url, headers=headers, timeout=10)
        data = resp.json()
        spot = float(data["data"][0]["lastPrice"])
        logger.info(f"NIFTY spot = {spot}")
        return spot
    except Exception as e:
        logger.error(f"Spot fetch error: {e}")
        return None

def get_nifty_spot_cached():
    """Get cached NIFTY spot price (15s TTL)."""
    now = time.time()
    if now - spot_cache.get("timestamp", 0) < 15 and spot_cache.get("value"):
        return spot_cache["value"]
    spot = get_nifty_spot()
    if spot:
        spot_cache["value"] = spot
        spot_cache["timestamp"] = now
    return spot

def get_current_atm_tokens():
    """Return (ce_token, pe_token, ce_symbol, pe_symbol, atm_strike, expiry_date) for nearest expiry."""
    global CE_TOKEN, PE_TOKEN, CE_SYMBOL, PE_SYMBOL, ATM_STRIKE, EXPIRY_DATE
    
    spot = get_nifty_spot_cached()
    if not spot:
        logger.error("Cannot get ATM tokens: spot price unavailable")
        return None, None, None, None, None, None

    atm_strike = round(spot / 50) * 50
    logger.info(f"ATM strike = {atm_strike} (spot={spot})")

    try:
        url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        df = pd.DataFrame(resp.json())
    except Exception as e:
        logger.error(f"Instrument master error: {e}")
        return None, None, None, None, None, None

    nifty_opts = df[
        (df["name"].astype(str) == "NIFTY") &
        (df["instrumenttype"].astype(str) == "OPTIDX") &
        (df["exch_seg"].astype(str) == "NFO")
    ].copy()
    
    if nifty_opts.empty:
        nifty_opts = df[df["symbol"].astype(str).str.match(r'^NIFTY\d{2}[A-Z]{3}\d{2}', na=False)].copy()
    
    if nifty_opts.empty:
        logger.error("No NIFTY options found")
        return None, None, None, None, None, None

    for fmt in ["%d%b%Y", "%d-%b-%Y", "%Y-%m-%d", "%d%m%Y"]:
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
        return None, None, None, None, None, None
    
    nearest_expiry = future_expiries["expiry_date"].min()
    logger.info(f"Using expiry: {nearest_expiry.date()}")

    atm_opts = nifty_opts[(nifty_opts["strike"] == atm_strike) & (nifty_opts["expiry_date"] == nearest_expiry)]
    if atm_opts.empty:
        strikes = sorted(nifty_opts[nifty_opts["expiry_date"] == nearest_expiry]["strike"].unique())
        nearest_strike = min(strikes, key=lambda x: abs(x - atm_strike))
        logger.info(f"ATM strike not found, using nearest: {nearest_strike}")
        atm_opts = nifty_opts[(nifty_opts["strike"] == nearest_strike) & (nifty_opts["expiry_date"] == nearest_expiry)]
        atm_strike = nearest_strike

    ce = atm_opts[atm_opts["symbol"].astype(str).str.upper().str.endswith("CE", na=False)]
    pe = atm_opts[atm_opts["symbol"].astype(str).str.upper().str.endswith("PE", na=False)]
    
    if ce.empty or pe.empty:
        logger.error(f"CE/PE not found. CE count={len(ce)}, PE count={len(pe)}")
        return None, None, None, None, None, None

    ce_token = str(ce.iloc[0]["token"])
    pe_token = str(pe.iloc[0]["token"])
    ce_symbol = str(ce.iloc[0]["symbol"])
    pe_symbol = str(pe.iloc[0]["symbol"])
    
    logger.info(f"Tokens resolved: CE={ce_token} ({ce_symbol}), PE={pe_token} ({pe_symbol})")
    
    CE_TOKEN = ce_token
    PE_TOKEN = pe_token
    CE_SYMBOL = ce_symbol
    PE_SYMBOL = pe_symbol
    ATM_STRIKE = atm_strike
    EXPIRY_DATE = nearest_expiry.strftime("%d%b%Y").upper()
    
    return ce_token, pe_token, ce_symbol, pe_symbol, atm_strike, EXPIRY_DATE

# ============================================================
# BLACK-SCHOLES GREEKS (Real Calculation)
# ============================================================
def black_scholes_greeks(S, K, T, r, sigma, option_type="CE"):
    """
    Calculate Black-Scholes Greeks.
    S: Spot price, K: Strike, T: Time to expiry (years), r: risk-free rate, sigma: IV
    Returns: delta, gamma, theta, vega, iv
    """
    try:
        from scipy.stats import norm
    except ImportError:
        logger.warning("scipy not available, using approximate Greeks")
        return approximate_greeks(S, K, T, sigma, option_type)
    
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0, 0.0, 0.0, 0.0, sigma
    
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    
    nd1 = norm.cdf(d1)
    nd2 = norm.cdf(d2)
    n_pdf_d1 = norm.pdf(d1)
    
    if option_type.upper() == "CE":
        delta = nd1
        theta = (-S * n_pdf_d1 * sigma / (2 * math.sqrt(T)) 
                 - r * K * math.exp(-r * T) * nd2) / 365
    else:
        delta = nd1 - 1
        theta = (-S * n_pdf_d1 * sigma / (2 * math.sqrt(T)) 
                 + r * K * math.exp(-r * T) * (1 - nd2)) / 365
    
    gamma = n_pdf_d1 / (S * sigma * math.sqrt(T))
    vega = S * n_pdf_d1 * math.sqrt(T) / 100
    
    return round(delta, 4), round(gamma, 4), round(theta, 4), round(vega, 4), round(sigma, 4)

def approximate_greeks(S, K, T, sigma, option_type):
    """Approximate Greeks when scipy is unavailable."""
    moneyness = abs(S - K) / S if S > 0 else 0
    if option_type.upper() == "CE":
        delta = 0.5 + (0.5 - moneyness) if moneyness < 0.5 else 0.9
    else:
        delta = -(0.5 + (0.5 - moneyness)) if moneyness < 0.5 else -0.9
    delta = max(-1, min(1, delta))
    gamma = 0.05 * (1 - moneyness * 2) if moneyness < 0.5 else 0.01
    theta = -S * 0.001 * T * 365
    vega = S * 0.1 * math.sqrt(T)
    return round(delta, 4), round(gamma, 4), round(theta, 4), round(vega, 4), round(sigma, 4)

def get_iv_from_option_chain():
    """Fetch IV from Angel One option chain or calculate from prices."""
    global option_chain_cache
    
    now = time.time()
    if now - option_chain_cache["timestamp"] < 120 and option_chain_cache["data"]:
        return option_chain_cache["data"]
    
    auth_token, _, obj = get_auth_token()
    if not obj:
        return None
    
    try:
        url = "https://apiconnect.angelone.in/rest/secure/angelbroking/order/v1/optionGreek"
        payload = json.dumps({"name": "NIFTY", "expirydate": EXPIRY_DATE})
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-UserType": "USER",
            "X-SourceID": "WEB",
            "X-ClientLocalIP": "127.0.0.1",
            "X-ClientPublicIP": "127.0.0.1",
            "X-MACAddress": "00:00:00:00:00:00",
            "X-PrivateKey": ANGEL_API_KEY
        }
        resp = requests.post(url, headers=headers, data=payload, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") and data.get("data"):
                option_chain_cache = {"data": data["data"], "timestamp": now}
                return data["data"]
    except Exception as e:
        logger.warning(f"Option Greeks API failed: {e}")
    
    # Fallback: estimate IV from ATM option price using Newton-Raphson
    spot = get_nifty_spot_cached()
    ce_price = latest_ticks.get("ce_price", 0)
    if spot and ce_price > 0 and ATM_STRIKE > 0:
        T = max(CONFIG.DAYS_TO_EXPIRY / 365, 0.001)
        iv = estimate_iv_from_price(spot, ATM_STRIKE, T, ce_price, "CE")
        return [{"strikePrice": str(ATM_STRIKE), "optionType": "CE", 
                 "impliedVolatility": str(iv), "delta": "0.5", "gamma": "0.05",
                 "theta": "-1.0", "vega": "1.0"}]
    
    return None

def estimate_iv_from_price(S, K, T, market_price, option_type, r=0.06, max_iter=100, tol=1e-5):
    """Estimate implied volatility using Newton-Raphson method."""
    try:
        from scipy.stats import norm
    except ImportError:
        return 0.20
    
    sigma = 0.20
    for i in range(max_iter):
        d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        
        if option_type.upper() == "CE":
            price = S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
            vega = S * norm.pdf(d1) * math.sqrt(T)
        else:
            price = K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
            vega = S * norm.pdf(d1) * math.sqrt(T)
        
        diff = price - market_price
        if abs(diff) < tol:
            return sigma
        
        if vega == 0:
            break
        sigma = sigma - diff / vega
        sigma = max(0.01, min(2.0, sigma))
    
    return sigma

def get_real_greece(option_type="CE"):
    """Get real Greeks from option chain or calculate them."""
    spot = get_nifty_spot_cached()
    if not spot or ATM_STRIKE <= 0:
        return Greeks()
    
    chain_data = get_iv_from_option_chain()
    if chain_data:
        for item in chain_data:
            strike = float(item.get("strikePrice", 0))
            opt_type = item.get("optionType", "")
            if abs(strike - ATM_STRIKE) < 1 and opt_type.upper() == option_type.upper():
                return Greeks(
                    delta=float(item.get("delta", 0)),
                    gamma=float(item.get("gamma", 0)),
                    theta=float(item.get("theta", 0)),
                    vega=float(item.get("vega", 0)),
                    iv=float(item.get("impliedVolatility", 0.20))
                )
    
    # Fallback to Black-Scholes calculation
    T = max(CONFIG.DAYS_TO_EXPIRY / 365, 0.001)
    price = latest_ticks.get("ce_price" if option_type == "CE" else "pe_price", 0)
    iv = estimate_iv_from_price(spot, ATM_STRIKE, T, price, option_type) if price > 0 else 0.20
    delta, gamma, theta, vega, iv = black_scholes_greeks(spot, ATM_STRIKE, T, CONFIG.RISK_FREE_RATE, iv, option_type)
    return Greeks(delta=delta, gamma=gamma, theta=theta, vega=vega, iv=iv)

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

def estimate_iv_rank(ce_price, history, period=20):
    if len(history) < period:
        return 50
    iv_min = min(history[-period:])
    iv_max = max(history[-period:])
    if iv_max == iv_min:
        return 50
    rank = (ce_price - iv_min) / (iv_max - iv_min) * 100
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
    """Fetch PCR from NSE or Angel One API."""
    now = time.time()
    if now - pcr_cache["time"] < 120:
        return pcr_cache["value"], pcr_cache["source"]
    
    # Try Angel One PCR API first
    auth_token, _, obj = get_auth_token()
    if auth_token:
        try:
            url = "https://apiconnect.angelone.in/rest/secure/angelbroking/order/v1/putCallRatio"
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "X-UserType": "USER",
                "X-SourceID": "WEB",
                "X-ClientLocalIP": "127.0.0.1",
                "X-ClientPublicIP": "127.0.0.1",
                "X-MACAddress": "00:00:00:00:00:00",
                "X-PrivateKey": ANGEL_API_KEY
            }
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") and data.get("data"):
                    for item in data["data"]:
                        if "NIFTY" in item.get("tradingSymbol", ""):
                            pcr = float(item.get("pcr", 1.0))
                            pcr_cache.update({"value": pcr, "time": now, "source": "angel_pcr"})
                            pcr_history.append(pcr)
                            logger.info(f"Angel PCR fetched: {pcr:.2f}")
                            return pcr, "angel_pcr"
        except Exception as e:
            logger.warning(f"Angel PCR API failed: {e}")
    
    # Fallback to NSE
    try:
        url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/option-chain"
        }
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=15)
        time.sleep(2)
        resp = session.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if "records" in data and "data" in data["records"]:
                records = data["records"]["data"]
                ce_oi = sum(x.get("CE", {}).get("openInterest", 0) for x in records if "CE" in x)
                pe_oi = sum(x.get("PE", {}).get("openInterest", 0) for x in records if "PE" in x)
                pcr = pe_oi / ce_oi if ce_oi else 1.0
                pcr_cache.update({"value": pcr, "time": now, "source": "nse_oi"})
                pcr_history.append(pcr)
                logger.info(f"NSE PCR fetched: {pcr:.2f}")
                return pcr, "nse_oi"
    except Exception as e:
        logger.warning(f"NSE PCR failed: {e}")
    
    return pcr_cache["value"], "cached"

def calculate_pcr_ema():
    if len(pcr_history) < 3:
        return 1.0
    alpha = 2 / (CONFIG.PCR_EMA_PERIOD + 1)
    ema = pcr_history[0]
    for val in list(pcr_history)[1:]:
        ema = alpha * val + (1 - alpha) * ema
    return ema

# ============================================================
# RISK ENGINE
# ============================================================
def calculate_kelly_criterion(win_rate, avg_win, avg_loss):
    """Calculate Kelly fraction for position sizing."""
    if avg_loss == 0 or win_rate <= 0 or win_rate >= 1:
        return 0.0
    kelly = win_rate - ((1 - win_rate) / (avg_win / avg_loss))
    return max(0, kelly * CONFIG.KELLY_FRACTION)

def check_portfolio_heat(new_position_pct: float) -> Tuple[bool, str]:
    """Check if adding new position would exceed portfolio heat limits."""
    current_heat = portfolio_state["total_exposure_pct"]
    projected_heat = current_heat + new_position_pct
    
    if projected_heat > PORTFOLIO_HEAT_MAX_PCT:
        return False, f"Portfolio heat would exceed {PORTFOLIO_HEAT_MAX_PCT}% (current: {current_heat:.1f}%, new: {new_position_pct:.1f}%)"
    
    return True, "OK"

def check_daily_loss_limit() -> Tuple[bool, str]:
    """Check if daily loss limit has been breached."""
    today_pnl = db.get_today_pnl()
    realized_pnl = today_pnl["realized_pnl"]
    
    starting_equity = portfolio_state["starting_equity_today"]
    daily_loss_pct = (abs(realized_pnl) / starting_equity) * 100 if starting_equity > 0 else 0
    
    if daily_loss_pct >= MAX_DAILY_LOSS_PCT:
        portfolio_state["circuit_breaker_triggered"] = True
        return False, f"DAILY LOSS LIMIT BREACHED: {daily_loss_pct:.2f}% >= {MAX_DAILY_LOSS_PCT}%"
    
    return True, "OK"

def update_portfolio_state():
    """Update portfolio exposure, P&L, and drawdown."""
    open_positions = db.get_open_positions()
    total_exposure = 0.0
    total_unrealized = 0.0
    
    for pos in open_positions:
        exposure = pos.entry_price * pos.quantity
        total_exposure += exposure
        total_unrealized += pos.unrealized_pnl
    
    equity = portfolio_state["equity"]
    portfolio_state["total_exposure"] = total_exposure
    portfolio_state["total_exposure_pct"] = (total_exposure / equity) * 100 if equity > 0 else 0
    portfolio_state["positions"] = open_positions
    
    today_pnl = db.get_today_pnl()
    portfolio_state["daily_pnl"] = today_pnl["realized_pnl"] + total_unrealized
    portfolio_state["daily_pnl_pct"] = (portfolio_state["daily_pnl"] / portfolio_state["starting_equity_today"]) * 100
    
    if portfolio_state["daily_pnl"] < 0:
        dd_pct = abs(portfolio_state["daily_pnl_pct"])
        if dd_pct > portfolio_state["max_drawdown_today"]:
            portfolio_state["max_drawdown_today"] = dd_pct

def check_correlation_risk(new_option_type: str) -> Tuple[bool, str]:
    """Check correlation risk — avoid adding same-direction positions."""
    open_positions = db.get_open_positions()
    same_type_count = sum(1 for p in open_positions if p.option_type == new_option_type)
    
    if same_type_count >= 2:
        return False, f"Correlation risk: Already have {same_type_count} {new_option_type} positions"
    
    return True, "OK"

# ============================================================
# ORDER MANAGEMENT
# ============================================================
class OrderManager:
    """Manages order placement for both paper and live trading."""
    
    def __init__(self):
        self.paper_orders = {}
        self.live_orders = {}
    
    def place_order(self, symbol: str, token: str, transaction_type: str, 
                   quantity: int, price: float = 0, order_type: str = "MARKET",
                   product_type: str = "CARRYFORWARD") -> Tuple[bool, str, str]:
        """
        Place an order. Returns (success, message, order_id).
        """
        order_id = str(uuid.uuid4())
        mode = TRADING_MODE
        
        if mode == "PAPER":
            self.paper_orders[order_id] = {
                "symbol": symbol,
                "token": token,
                "transaction_type": transaction_type,
                "quantity": quantity,
                "price": price,
                "status": "FILLED",
                "mode": "PAPER",
                "timestamp": datetime.now().isoformat()
            }
            logger.info(f"[PAPER ORDER] {transaction_type} {quantity} {symbol} @ {price}")
            return True, "Paper order filled", order_id
        
        else:
            auth_token, _, obj = get_auth_token()
            if not obj:
                return False, "Auth failed", ""
            
            try:
                order_params = {
                    "variety": "NORMAL",
                    "tradingsymbol": symbol,
                    "symboltoken": token,
                    "transactiontype": transaction_type,
                    "exchange": "NFO",
                    "ordertype": order_type,
                    "producttype": product_type,
                    "duration": "DAY",
                    "quantity": str(quantity),
                    "price": str(price) if order_type == "LIMIT" else "0"
                }
                
                response = obj.placeOrder(order_params)
                if response and response.get("status"):
                    live_order_id = response["data"]["orderid"]
                    self.live_orders[live_order_id] = order_params
                    logger.info(f"[LIVE ORDER] {transaction_type} {quantity} {symbol} @ {price} - ID: {live_order_id}")
                    return True, "Live order placed", live_order_id
                else:
                    error_msg = response.get("message", "Unknown error") if response else "No response"
                    logger.error(f"Live order failed: {error_msg}")
                    return False, error_msg, ""
            except Exception as e:
                logger.error(f"Live order exception: {e}")
                return False, str(e), ""
    
    def get_order_status(self, order_id: str) -> dict:
        """Get order status."""
        if order_id in self.paper_orders:
            return self.paper_orders[order_id]
        
        if TRADING_MODE == "LIVE":
            auth_token, _, obj = get_auth_token()
            if obj:
                try:
                    status = obj.individualOrderDetails(order_id)
                    return status
                except Exception as e:
                    logger.error(f"Get order status error: {e}")
        
        return {"status": "UNKNOWN"}

order_mgr = OrderManager()

# ============================================================
# POSITION MANAGEMENT
# ============================================================
def open_position(option_type: str, entry_price: float, stop_loss: float, 
                  target: float, position_size_pct: float, grade: str, 
                  confidence: float, regime: str):
    """Open a new position with risk checks."""
    
    heat_ok, heat_msg = check_portfolio_heat(position_size_pct)
    if not heat_ok:
        logger.warning(f"Position rejected: {heat_msg}")
        return False
    
    loss_ok, loss_msg = check_daily_loss_limit()
    if not loss_ok:
        logger.warning(f"Position rejected: {loss_msg}")
        return False
    
    corr_ok, corr_msg = check_correlation_risk(option_type)
    if not corr_ok:
        logger.warning(f"Position rejected: {corr_msg}")
        return False
    
    equity = portfolio_state["equity"]
    position_value = equity * (position_size_pct / 100)
    quantity = int(position_value / entry_price)
    quantity = max(quantity, CONFIG.NIFTY_LOT_SIZE)
    quantity = (quantity // CONFIG.NIFTY_LOT_SIZE) * CONFIG.NIFTY_LOT_SIZE
    
    if quantity <= 0:
        logger.warning(f"Position rejected: Quantity too small ({quantity})")
        return False
    
    symbol = CE_SYMBOL if option_type == "CE" else PE_SYMBOL
    token = CE_TOKEN if option_type == "CE" else PE_TOKEN
    
    success, msg, order_id = order_mgr.place_order(
        symbol=symbol,
        token=token,
        transaction_type="BUY",
        quantity=quantity,
        price=0,
        order_type="MARKET"
    )
    
    if not success:
        logger.error(f"Failed to open position: {msg}")
        return False
    
    position = Position(
        order_id=order_id,
        symbol=symbol,
        token=token,
        option_type=option_type,
        entry_price=entry_price,
        current_price=entry_price,
        quantity=quantity,
        stop_loss=stop_loss,
        target=target,
        trailing_stop=stop_loss,
        unrealized_pnl=0.0,
        realized_pnl=0.0,
        entry_time=datetime.now().isoformat(),
        status="OPEN",
        mode=TRADING_MODE
    )
    
    db.save_position(position)
    update_portfolio_state()
    
    journal_logger.info(f"POSITION OPENED: {option_type} | Entry: {entry_price} | SL: {stop_loss} | "
                        f"Target: {target} | Qty: {quantity} | Grade: {grade} | Mode: {TRADING_MODE}")
    
    signal_state["entry_price"] = entry_price
    signal_state["stop_loss"] = stop_loss
    signal_state["target"] = target
    signal_state["highest_price_since_entry"] = entry_price
    signal_state["lowest_price_since_entry"] = entry_price
    
    return True

def close_position(position: Position, exit_price: float, exit_reason: str):
    """Close an open position."""
    
    if position.option_type == "CE":
        pnl = (exit_price - position.entry_price) * position.quantity
    else:
        pnl = (position.entry_price - exit_price) * position.quantity
    
    pnl_pct = (pnl / (position.entry_price * position.quantity)) * 100 if position.entry_price > 0 else 0
    
    success, msg, order_id = order_mgr.place_order(
        symbol=position.symbol,
        token=position.token,
        transaction_type="SELL",
        quantity=position.quantity,
        price=0,
        order_type="MARKET"
    )
    
    if not success:
        logger.error(f"Failed to close position: {msg}")
        return False
    
    position.current_price = exit_price
    position.realized_pnl = pnl
    position.status = "CLOSED"
    db.save_position(position)
    
    trade = TradeJournal(
        trade_id=position.order_id,
        timestamp=datetime.now().isoformat(),
        action="CLOSE",
        symbol=position.symbol,
        option_type=position.option_type,
        entry_price=position.entry_price,
        exit_price=exit_price,
        quantity=position.quantity,
        pnl=pnl,
        pnl_pct=pnl_pct,
        grade=signal_state.get("signal_grade", "D"),
        confidence=market_state.get("confidence", 0),
        regime=market_state.get("regime", "UNKNOWN"),
        exit_reason=exit_reason,
        mode=TRADING_MODE
    )
    db.save_trade(trade)
    
    portfolio_state["equity"] += pnl
    portfolio_state["available_cash"] += (position.entry_price * position.quantity) + pnl
    update_portfolio_state()
    
    journal_logger.info(f"POSITION CLOSED: {position.option_type} | Entry: {position.entry_price} | "
                        f"Exit: {exit_price} | P&L: {pnl:.2f} ({pnl_pct:.2f}%) | Reason: {exit_reason} | Mode: {TRADING_MODE}")
    
    return True

def update_positions():
    """Update all open positions with current prices and check exits."""
    open_positions = db.get_open_positions()
    
    for pos in open_positions:
        current_price = latest_ticks.get(f"{pos.option_type.lower()}_price", 0)
        if current_price <= 0:
            continue
        
        pos.current_price = current_price
        
        if pos.option_type == "CE":
            pos.unrealized_pnl = (current_price - pos.entry_price) * pos.quantity
        else:
            pos.unrealized_pnl = (pos.entry_price - current_price) * pos.quantity
        
        if pos.option_type == "CE":
            if current_price > pos.entry_price:
                new_trailing = current_price - (pos.entry_price - pos.stop_loss) * 0.5
                pos.trailing_stop = max(pos.trailing_stop, new_trailing)
            if current_price <= pos.stop_loss or current_price <= pos.trailing_stop:
                close_position(pos, current_price, "STOP_LOSS" if current_price <= pos.stop_loss else "TRAILING_STOP")
                continue
            if current_price >= pos.target:
                close_position(pos, current_price, "TARGET")
                continue
        else:
            if current_price < pos.entry_price:
                new_trailing = current_price + (pos.stop_loss - pos.entry_price) * 0.5
                pos.trailing_stop = min(pos.trailing_stop, new_trailing)
            if current_price >= pos.stop_loss or current_price >= pos.trailing_stop:
                close_position(pos, current_price, "STOP_LOSS" if current_price >= pos.stop_loss else "TRAILING_STOP")
                continue
            if current_price <= pos.target:
                close_position(pos, current_price, "TARGET")
                continue
        
        db.save_position(pos)
    
    update_portfolio_state()

# ============================================================
# SIGNAL ENGINE (CORRECTED)
# ============================================================
def run_signal_engine(ce_price, pe_price, ce_history, pe_history, ce_volumes, pe_volumes):
    """
    Professional signal engine with:
    - Separate CE/PE price analysis
    - Persistent confirmation logic
    - Real Greeks from Black-Scholes
    - Portfolio risk checks
    - Position management
    """
    global market_signal, market_state, institutional_state, signal_state
    
    if len(ce_history) < 30 or len(pe_history) < 30:
        return
    
    spot = get_nifty_spot_cached() or 0
    spread = ce_price - pe_price
    
    # Use combined prices for market direction analysis
    combined_prices = [(c + p) / 2 for c, p in zip(ce_history, pe_history)]
    combined_volumes = [(c + p) / 2 for c, p in zip(ce_volumes, pe_volumes)]
    
    # Technical indicators on combined price
    rsi = calculate_rsi(combined_prices, CONFIG.RSI_PERIOD)
    macd_line, macd_hist = calculate_macd(combined_prices, CONFIG.MACD_FAST, CONFIG.MACD_SLOW, CONFIG.MACD_SIGNAL)
    vwap = calculate_vwap(combined_prices, combined_volumes)
    ema_fast = calculate_ema(combined_prices, CONFIG.EMA_FAST)
    ema_slow = calculate_ema(combined_prices, CONFIG.EMA_SLOW)
    atr = calculate_atr(combined_prices, CONFIG.ATR_PERIOD)
    
    # Separate indicators for CE and PE
    ce_rsi = calculate_rsi(list(ce_history), CONFIG.RSI_PERIOD)
    pe_rsi = calculate_rsi(list(pe_history), CONFIG.RSI_PERIOD)
    ce_ema = calculate_ema(list(ce_history), CONFIG.EMA_FAST)
    pe_ema = calculate_ema(list(pe_history), CONFIG.EMA_FAST)
    
    pcr, pcr_src = get_nifty_pcr()
    pcr_ema = calculate_pcr_ema()
    
    # Real Greeks
    ce_greeks = get_real_greece("CE")
    pe_greeks = get_real_greece("PE")
    
    # Bollinger, ADX, Divergence on combined
    bb_sma, bb_upper, bb_lower, bb_pos = calculate_bollinger(combined_prices, CONFIG.BB_PERIOD, CONFIG.BB_STD)
    adx = calculate_adx(combined_prices, CONFIG.ADX_PERIOD)
    rsi_values = [calculate_rsi(combined_prices[:i+1], CONFIG.RSI_PERIOD) for i in range(CONFIG.RSI_PERIOD, len(combined_prices))]
    rsi_div = calculate_rsi_divergence(combined_prices, rsi_values) if len(rsi_values) >= 5 else "NONE"
    
    atr_pct = (atr / combined_prices[-1]) * 100 if combined_prices[-1] > 0 else 0
    iv_rank = estimate_iv_rank(ce_price, list(ce_history)[-min(20, len(ce_history)):], 20)
    
    # Volume trend
    if len(combined_volumes) >= 20:
        recent_vol = sum(combined_volumes[-10:]) / 10
        older_vol = sum(combined_volumes[-20:-10]) / 10
        vol_trend = "INCREASING" if recent_vol > older_vol * 1.2 else "DECREASING" if recent_vol < older_vol * 0.8 else "FLAT"
    else:
        vol_trend = "FLAT"
    
    # Regime detection
    if adx > 30:
        regime = "TRENDING"
    elif atr_pct > 1.5:
        regime = "VOLATILE"
    elif bb_pos < 20 or bb_pos > 80:
        regime = "BREAKOUT"
    else:
        regime = "RANGING"
    
    session_phase = get_market_phase()
    market_state["regime"] = regime
    market_state["session_phase"] = session_phase
    
    # Timeframe analysis
    tf_trends = get_all_timeframe_trends()
    bullish_tf = sum(1 for t in ["1min","5min","10min","15min","20min"] if tf_trends[t]["trend"]=="BULLISH")
    bearish_tf = sum(1 for t in ["1min","5min","10min","15min","20min"] if tf_trends[t]["trend"]=="BEARISH")
    tf_score_bull = bullish_tf * 10
    tf_score_bear = bearish_tf * 10
    
    # Technical scoring
    tech_bull = 0
    tech_bear = 0
    
    # RSI signals
    if 55 < rsi < 75:
        tech_bull += 10
    elif 40 < rsi < 55:
        tech_bull += 5
    if 25 < rsi < 45:
        tech_bear += 10
    elif 45 < rsi < 60:
        tech_bear += 5
    
    # MACD
    if macd_hist > 0 and macd_line > 0:
        tech_bull += 10
    elif macd_hist > 0:
        tech_bull += 6
    elif macd_hist < 0 and macd_line < 0:
        tech_bear += 10
    elif macd_hist < 0:
        tech_bear += 6
    
    # PCR
    if pcr < CONFIG.PCR_BULLISH:
        tech_bull += 10
    elif pcr < 1.0:
        tech_bull += 7
    elif pcr > CONFIG.PCR_BEARISH:
        tech_bear += 10
    elif pcr > 1.2:
        tech_bear += 7
    
    # Volume + EMA
    if vol_trend == "INCREASING":
        if ema_fast > ema_slow:
            tech_bull += 10
        elif ema_fast < ema_slow:
            tech_bear += 10
    
    # VWAP alignment
    avg_price = (ce_price + pe_price) / 2
    if avg_price > vwap and avg_price > ema_slow:
        tech_bull += 10
    elif avg_price > vwap or avg_price > ema_slow:
        tech_bull += 5
    elif avg_price < vwap and avg_price < ema_slow:
        tech_bear += 10
    elif avg_price < vwap or avg_price < ema_slow:
        tech_bear += 5
    
    # Bollinger
    if bb_pos < 20:
        tech_bull += 8
    elif bb_pos > 80:
        tech_bear += 8
    
    # ADX trend strength
    if adx > 30:
        if ema_fast > ema_slow:
            tech_bull += 5
        else:
            tech_bear += 5
    
    # RSI Divergence
    if rsi_div == "BULLISH" and ema_fast > ema_slow:
        tech_bull += 8
    elif rsi_div == "BEARISH" and ema_fast < ema_slow:
        tech_bear += 8
    
    # IV Rank adjustments
    if iv_rank > 70:
        tech_bull -= 5
        tech_bear += 3
    elif iv_rank < 30:
        tech_bull += 3
    
    # Greeks-based adjustments
    if ce_greeks.delta > 0.6:
        tech_bull += 3
    if pe_greeks.delta < -0.6:
        tech_bear += 3
    if ce_greeks.theta < -5:
        tech_bull -= 2
        tech_bear -= 2
    
    total_bull = tf_score_bull + tech_bull
    total_bear = tf_score_bear + tech_bear
    raw_confidence = max(total_bull, total_bear)
    
    # Determine raw action
    if total_bull >= total_bear and total_bull >= CONFIG.CONSIDER_THRESHOLD:
        if raw_confidence >= CONFIG.STRONG_BUY_THRESHOLD:
            raw_action = "STRONG BUY CE"
            signal_type = "TRENDING" if bullish_tf >= 4 else "MOMENTUM"
        elif raw_confidence >= CONFIG.BUY_THRESHOLD:
            raw_action = "BUY CE"
            signal_type = "MOMENTUM"
        elif raw_confidence >= CONFIG.CONSIDER_THRESHOLD:
            raw_action = "CONSIDER CE BUY"
            signal_type = "MOMENTUM"
        else:
            raw_action = "HOLD"
            signal_type = "NONE"
    elif total_bear > total_bull and total_bear >= CONFIG.CONSIDER_THRESHOLD:
        if raw_confidence >= CONFIG.STRONG_BUY_THRESHOLD:
            raw_action = "STRONG BUY PE"
            signal_type = "TRENDING" if bearish_tf >= 4 else "MOMENTUM"
        elif raw_confidence >= CONFIG.BUY_THRESHOLD:
            raw_action = "BUY PE"
            signal_type = "MOMENTUM"
        elif raw_confidence >= CONFIG.CONSIDER_THRESHOLD:
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
    # CORRECTED SIGNAL CONFIRMATION LOGIC
    # ============================================================
    now_ts = time.time()
    final_action = signal_state["current_action"]
    final_signal_type = signal_state["current_signal_type"]
    
    # Handle cooldown after flip
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
                logger.info(f"New pending action: {raw_action} (confirmation 1/{CONFIG.SIGNAL_CONFIRMATION_BARS})")
            else:
                signal_state["confirmation_count"] += 1
                logger.info(f"Confirmation {signal_state['confirmation_count']}/{CONFIG.SIGNAL_CONFIRMATION_BARS} for {raw_action}")
            
            if signal_state["confirmation_count"] >= CONFIG.SIGNAL_CONFIRMATION_BARS:
                final_action = raw_action
                final_signal_type = signal_type
                signal_state["current_action"] = raw_action
                signal_state["current_signal_type"] = signal_type
                signal_state["last_confirmed_action"] = raw_action
                signal_state["cooldown_until"] = now_ts + CONFIG.COOLDOWN_AFTER_FLIP_SEC
                signal_state["pending_action"] = None
                signal_state["pending_signal_type"] = None
                signal_state["confirmation_count"] = 0
                signal_state["signal_start_time"] = now_ts
                
                if signal_state["flip_window_start"] == 0 or (now_ts - signal_state["flip_window_start"]) > 3600:
                    signal_state["flip_window_start"] = now_ts
                    signal_state["flip_count_hour"] = 1
                else:
                    signal_state["flip_count_hour"] += 1
                
                logger.info(f"SIGNAL CONFIRMED: {final_action} [{final_signal_type}] after {CONFIG.SIGNAL_CONFIRMATION_BARS} confirmations")
            else:
                final_action = signal_state["current_action"]
                final_signal_type = signal_state["current_signal_type"]
    else:
        if signal_state["pending_action"] is not None:
            signal_state["pending_action"] = None
            signal_state["pending_signal_type"] = None
            signal_state["confirmation_count"] = 0
        
        if signal_state["signal_start_time"] and (now_ts - signal_state["signal_start_time"]) > CONFIG.SIGNAL_MAX_AGE_SEC:
            if final_action != "HOLD":
                logger.info(f"Signal {final_action} expired after {CONFIG.SIGNAL_MAX_AGE_SEC}s")
                final_action = "HOLD"
                final_signal_type = "NONE"
                signal_state["current_action"] = "HOLD"
                signal_state["current_signal_type"] = "NONE"
                signal_state["signal_start_time"] = None
    
    # Max flips per hour check
    if signal_state["flip_count_hour"] >= CONFIG.MAX_FLIPS_PER_HOUR:
        logger.warning(f"Max flips per hour ({CONFIG.MAX_FLIPS_PER_HOUR}) reached. Forcing HOLD.")
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
        else:
            grade = "D"
    
    signal_state["signal_grade"] = grade
    
    # ============================================================
    # POSITION SIZING & RISK PARAMETERS
    # ============================================================
    position_pct = 0
    rr = 0
    entry = 0
    stop = 0
    target = 0
    max_dd = 0
    
    today_pnl = db.get_today_pnl()
    total_trades = today_pnl["num_trades"]
    wins = today_pnl["wins"]
    win_rate = wins / total_trades if total_trades > 0 else 0.5
    
    if final_action in ["STRONG BUY CE", "BUY CE", "CONSIDER CE BUY"]:
        entry = ce_price
        stop = entry - atr * CONFIG.STOP_LOSS_ATR_MULT
        target = entry + atr * CONFIG.TARGET_ATR_MULT
        
        if grade == "A":
            base = CONFIG.POSITION_SIZE_MAX_PCT
        elif grade == "B":
            base = CONFIG.POSITION_SIZE_BASE_PCT * 1.5
        elif grade == "C":
            base = CONFIG.POSITION_SIZE_BASE_PCT
        else:
            base = 0
        
        if regime == "VOLATILE":
            base *= 0.7
        elif regime == "TRENDING":
            base *= 1.2
        
        heat_ok, _ = check_portfolio_heat(base)
        if not heat_ok:
            base *= 0.5
        
        position_pct = min(CONFIG.POSITION_SIZE_MAX_PCT, max(0, base))
        
        if atr > 0:
            risk = entry - stop
            reward = target - entry
            rr = reward / risk if risk > 0 else 0
        
        open_positions = db.get_open_positions()
        existing_ce = [p for p in open_positions if p.option_type == "CE"]
        
        if not existing_ce and position_pct > 0:
            open_position("CE", entry, stop, target, position_pct, grade, raw_confidence, regime)
        elif existing_ce:
            for pos in existing_ce:
                if ce_price > pos.highest_price_since_entry:
                    pos.highest_price_since_entry = ce_price
                    new_trailing = ce_price - (entry - stop) * 0.5
                    pos.trailing_stop = max(pos.trailing_stop, new_trailing)
                    db.save_position(pos)
            update_positions()
    
    elif final_action in ["STRONG BUY PE", "BUY PE", "CONSIDER PE BUY"]:
        entry = pe_price
        stop = entry + atr * CONFIG.STOP_LOSS_ATR_MULT
        target = entry - atr * CONFIG.TARGET_ATR_MULT
        
        if grade == "A":
            base = CONFIG.POSITION_SIZE_MAX_PCT
        elif grade == "B":
            base = CONFIG.POSITION_SIZE_BASE_PCT * 1.5
        elif grade == "C":
            base = CONFIG.POSITION_SIZE_BASE_PCT
        else:
            base = 0
        
        if regime == "VOLATILE":
            base *= 0.7
        elif regime == "TRENDING":
            base *= 1.2
        
        heat_ok, _ = check_portfolio_heat(base)
        if not heat_ok:
            base *= 0.5
        
        position_pct = min(CONFIG.POSITION_SIZE_MAX_PCT, max(0, base))
        
        if atr > 0:
            risk = stop - entry
            reward = entry - target
            rr = reward / risk if risk > 0 else 0
        
        open_positions = db.get_open_positions()
        existing_pe = [p for p in open_positions if p.option_type == "PE"]
        
        if not existing_pe and position_pct > 0:
            open_position("PE", entry, stop, target, position_pct, grade, raw_confidence, regime)
        elif existing_pe:
            for pos in existing_pe:
                if pe_price < pos.lowest_price_since_entry:
                    pos.lowest_price_since_entry = pe_price
                    new_trailing = pe_price + (stop - entry) * 0.5
                    pos.trailing_stop = min(pos.trailing_stop, new_trailing)
                    db.save_position(pos)
            update_positions()
    
    else:
        open_positions = db.get_open_positions()
        for pos in open_positions:
            if pos.option_type == "CE" and final_action in ["STRONG BUY PE", "BUY PE"]:
                current_price = latest_ticks.get("ce_price", 0)
                if current_price > 0:
                    close_position(pos, current_price, "SIGNAL_REVERSE")
            elif pos.option_type == "PE" and final_action in ["STRONG BUY CE", "BUY CE"]:
                current_price = latest_ticks.get("pe_price", 0)
                if current_price > 0:
                    close_position(pos, current_price, "SIGNAL_REVERSE")
        
        signal_state["entry_price"] = 0
        signal_state["stop_loss"] = 0
        signal_state["target"] = 0
        position_pct = 0
        rr = 0
        max_dd = 0
    
    signal_state["position_size_pct"] = position_pct
    signal_state["risk_reward"] = rr
    signal_state["max_drawdown_pct"] = max_dd
    signal_state["entry_price"] = entry if final_action not in ["HOLD", "NONE"] else 0
    
    # Update market state
    market_state.update({
        "rsi": round(rsi, 2),
        "momentum": "UPTREND" if ema_fast > ema_slow else "DOWNTREND" if ema_fast < ema_slow else "NEUTRAL",
        "strength": "HIGH" if final_signal_type == "TRENDING" else "MODERATE" if final_signal_type == "MOMENTUM" else "LOW",
        "trend": "BULLISH" if ema_fast > ema_slow else "BEARISH" if ema_fast < ema_slow else "SIDEWAYS",
        "action": final_action,
        "confidence": raw_confidence,
        "volatility": "HIGH" if atr > 15 else "NORMAL" if atr > 5 else "LOW",
        "alert": final_action,
        "trend_1min": tf_trends["1min"]["trend"],
        "trend_5min": tf_trends["5min"]["trend"],
        "trend_10min": tf_trends["10min"]["trend"],
        "trend_15min": tf_trends["15min"]["trend"],
        "trend_20min": tf_trends["20min"]["trend"],
        "timeframe_agreement": max(bullish_tf, bearish_tf),
        "regime": regime,
        "session_phase": session_phase,
        "portfolio_heat": round(portfolio_state["total_exposure_pct"], 2),
        "daily_pnl_pct": round(portfolio_state["daily_pnl_pct"], 2),
        "max_drawdown_today": round(portfolio_state["max_drawdown_today"], 2),
        "circuit_breaker": portfolio_state["circuit_breaker_triggered"]
    })
    
    institutional_state.update({
        "vwap": round(vwap, 2),
        "ema_fast": round(ema_fast, 2),
        "ema_slow": round(ema_slow, 2),
        "ema_signal": "BULLISH" if ema_fast > ema_slow else "BEARISH",
        "atr": round(atr, 2),
        "oi_buildup": "BULLISH" if pcr < 0.9 else "BEARISH" if pcr > 1.2 else "NEUTRAL",
        "iv_state": "HIGH" if ce_greeks.vega > 2 else "NORMAL",
        "candle_structure": "BULLISH" if ema_fast > ema_slow and rsi > 55 else "BEARISH" if ema_fast < ema_slow and rsi < 45 else "SIDEWAYS",
        "market_breadth": "BULLISH" if bullish_tf >= 3 else "BEARISH" if bearish_tf >= 3 else "BALANCED",
        "volume_profile": vol_trend,
        "smart_money_flow": "BULLISH" if vwap > ema_slow and vol_trend == "INCREASING" else "BEARISH" if vwap < ema_slow and vol_trend == "INCREASING" else "NEUTRAL",
        "delta": ce_greeks.delta,
        "gamma": ce_greeks.gamma,
        "theta": ce_greeks.theta,
        "vega": ce_greeks.vega,
        "iv": ce_greeks.iv,
        "institutional_signal": final_action,
        "institutional_confidence": raw_confidence,
        "signal_grade": grade,
        "position_size_pct": position_pct,
        "risk_reward": round(rr, 2),
        "entry_price": round(entry, 2) if entry else 0,
        "stop_loss": round(stop, 2) if stop else 0,
        "target": round(target, 2) if target else 0,
        "max_drawdown_pct": round(max_dd, 2),
        "ce_delta": ce_greeks.delta,
        "pe_delta": pe_greeks.delta,
        "ce_iv": ce_greeks.iv,
        "pe_iv": pe_greeks.iv
    })
    
    market_signal.update({
        "signal": "BULLISH" if final_action in ["STRONG BUY CE", "BUY CE", "CONSIDER CE BUY"] else 
                  "BEARISH" if final_action in ["STRONG BUY PE", "BUY PE", "CONSIDER PE BUY"] else "NEUTRAL",
        "action": final_action,
        "ce_price": ce_price,
        "pe_price": pe_price,
        "spread": round(spread, 2),
        "rsi": round(rsi, 2),
        "ce_rsi": round(ce_rsi, 2),
        "pe_rsi": round(pe_rsi, 2),
        "macd": round(macd_hist, 2),
        "pcr": round(pcr, 2),
        "pcr_source": pcr_src,
        "vwap": round(vwap, 2),
        "atr": round(atr, 2),
        "atr_pct": round(atr_pct, 2),
        "ema_fast": round(ema_fast, 2),
        "ema_slow": round(ema_slow, 2),
        "delta": ce_greeks.delta,
        "gamma": ce_greeks.gamma,
        "theta": ce_greeks.theta,
        "vega": ce_greeks.vega,
        "iv": ce_greeks.iv,
        "volume": int(combined_volumes[-1]) if combined_volumes else 0,
        "timestamp": datetime.now().isoformat(),
        "adx": round(adx, 2),
        "bb_position": round(bb_pos, 2),
        "rsi_divergence": rsi_div,
        "iv_rank": round(iv_rank, 2),
        "signal_grade": grade,
        "regime": regime,
        "session_phase": session_phase,
        "confirmation_count": signal_state["confirmation_count"],
        "pending_action": signal_state["pending_action"] or "NONE",
        "portfolio_heat": round(portfolio_state["total_exposure_pct"], 2),
        "daily_pnl": round(portfolio_state["daily_pnl"], 2),
        "daily_pnl_pct": round(portfolio_state["daily_pnl_pct"], 2)
    })
    
    db.save_signal(market_signal)
    
    if final_action != signal_state["last_logged_action"]:
        journal_logger.info(
            f"PRO SIGNAL: {final_action} [{final_signal_type}] Grade:{grade} Conf:{raw_confidence} "
            f"BullTF:{bullish_tf} BearTF:{bearish_tf} RSI:{rsi:.1f} ADX:{adx:.1f} PCR:{pcr:.2f} "
            f"IV:{ce_greeks.iv:.1f}% PosSize:{position_pct}% RR:{rr:.1f} "
            f"Heat:{portfolio_state['total_exposure_pct']:.1f}% Mode:{TRADING_MODE}"
        )
        signal_state["last_logged_action"] = final_action

# ============================================================
# WEBSOCKET CALLBACKS (CORRECTED BINARY PARSING)
# ============================================================
def _acquire_primary_lock():
    global _is_primary_worker, _worker_lock_fd
    try:
        fd = os.open(_LOCK_FILE, os.O_CREAT | os.O_RDWR)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _worker_lock_fd = fd
        _is_primary_worker = True
        logger.info("[WORKER] Acquired primary lock - WebSocket mode")
        return True
    except (IOError, OSError):
        logger.info("[WORKER] Primary lock held - REST-only mode")
        _is_primary_worker = False
        return False

def parse_binary_tick(data: bytes) -> dict:
    """
    Parse Angel One SmartWebSocketV2 binary tick format.
    Based on official documentation binary protocol.
    """
    try:
        if len(data) < 51:
            return {}
        
        mode = struct.unpack('b', data[0:1])[0]
        exchange_type = struct.unpack('b', data[1:2])[0]
        
        token_bytes = data[2:27]
        token = token_bytes.decode('utf-8').split('\x00')[0].strip()
        
        seq_num = struct.unpack('>q', data[27:35])[0]
        exch_ts = struct.unpack('>q', data[35:43])[0]
        ltp = struct.unpack('>q', data[43:51])[0] / 100.0
        
        tick = {
            "mode": mode,
            "exchange_type": exchange_type,
            "token": token,
            "sequence_number": seq_num,
            "exchange_timestamp": exch_ts,
            "ltp": ltp
        }
        
        if mode >= 2 and len(data) >= 123:
            tick["last_traded_quantity"] = struct.unpack('>q', data[51:59])[0]
            tick["avg_traded_price"] = struct.unpack('>q', data[59:67])[0] / 100.0
            tick["volume"] = struct.unpack('>q', data[67:75])[0]
            tick["total_buy_quantity"] = struct.unpack('>d', data[75:83])[0]
            tick["total_sell_quantity"] = struct.unpack('>d', data[83:91])[0]
            tick["open"] = struct.unpack('>q', data[91:99])[0] / 100.0
            tick["high"] = struct.unpack('>q', data[99:107])[0] / 100.0
            tick["low"] = struct.unpack('>q', data[107:115])[0] / 100.0
            tick["close"] = struct.unpack('>q', data[115:123])[0] / 100.0
        
        if mode >= 3 and len(data) >= 139:
            tick["last_trade_timestamp"] = struct.unpack('>q', data[123:131])[0]
            tick["oi"] = struct.unpack('>q', data[131:139])[0]
        
        return tick
    except Exception as e:
        logger.error(f"Binary parse error: {e}, data_len={len(data)}")
        return {}

def patch_smartwebsocket(sws_instance):
    """Patch SmartWebSocketV2 for proper binary handling."""
    import websocket, ssl
    
    def fixed_connect():
        headers = {
            "Authorization": sws_instance.auth_token,
            "x-api-key": sws_instance.api_key,
            "x-client-code": sws_instance.client_code,
            "x-feed-token": sws_instance.feed_token
        }
        try:
            sws_instance.wsapp = websocket.WebSocketApp(
                sws_instance.ROOT_URI,
                header=headers,
                on_open=sws_instance._on_open,
                on_message=sws_instance._on_message,
                on_error=sws_instance._on_error,
                on_close=sws_instance._on_close,
                on_ping=sws_instance._on_ping,
                on_pong=sws_instance._on_pong
            )
            sws_instance.wsapp.run_forever(
                sslopt={"cert_reqs": ssl.CERT_NONE},
                ping_interval=sws_instance.HEART_BEAT_INTERVAL
            )
        except Exception as e:
            logger.error(f"WebSocket connect error: {e}")
            raise
    
    sws_instance.connect = fixed_connect

    def fixed_on_close(wsapp, close_status_code=None, close_msg=None):
        logger.warning(f"WebSocket closed: code={close_status_code}, msg={close_msg}")
        if hasattr(sws_instance, 'on_close') and sws_instance.on_close:
            try:
                sws_instance.on_close(wsapp, close_status_code, close_msg)
            except TypeError:
                # Fallback for older signature
                try:
                    sws_instance.on_close(wsapp)
                except:
                    pass

    # Patch _on_close to handle websocket-client's 4-arg callback
    original_on_close = sws_instance._on_close
    def patched_on_close(wsapp, close_status_code=None, close_msg=None):
        try:
            original_on_close(wsapp)
        except TypeError:
            pass
    sws_instance._on_close = patched_on_close
    sws_instance.MAX_RETRY_ATTEMPT = 0
    sws_instance.retry_strategy = 0
    sws_instance.HEART_BEAT_INTERVAL = 25  # Keep this
    return sws_instance

def on_open(wsapp):
    """WebSocket connection opened."""
    logger.info("WebSocket OPENED")
    global _reconnecting
    with _reconnect_lock:
        _reconnecting = False
    
    if sws and CE_TOKEN and PE_TOKEN:
        try:
            sws.subscribe("nifty_signal", 1, [{"exchangeType": 2, "tokens": [CE_TOKEN, PE_TOKEN]}])
            logger.info(f"Subscribed to CE={CE_TOKEN}, PE={PE_TOKEN} (exchange=2, mode=1)")
        except Exception as e:
            logger.error(f"Subscribe error: {e}")

def on_data(wsapp, message):
    """
    Handle WebSocket data - supports both binary and JSON formats.
    Angel One sends binary protobuf ticks that the library may parse into dicts.
    """
    global tick_counter, last_tick_time
    
    last_tick_time = time.time()
    
    try:
        if isinstance(message, dict):
            _process_tick_dict(message)
            return
        
        if isinstance(message, str):
            try:
                data = json.loads(message)
                if isinstance(data, dict):
                    _process_tick_dict(data)
                elif isinstance(data, list):
                    for item in data:
                        _process_tick_dict(item)
            except json.JSONDecodeError:
                logger.debug(f"Non-JSON string message: {message[:100]}")
            return
        
        if isinstance(message, bytes):
            tick = parse_binary_tick(message)
            if tick and tick.get("token"):
                _process_tick_dict(tick)
            else:
                try:
                    text = message.decode('utf-8', errors='ignore')
                    if text.strip():
                        logger.debug(f"Binary decoded as text: {text[:100]}")
                except:
                    pass
            return
        
        logger.warning(f"Unknown message type: {type(message)}")
    
    except Exception as e:
        logger.error(f"on_data error: {e}", exc_info=True)

def _process_tick_dict(tick: dict):
    """Process a single tick dictionary."""
    global tick_counter
    
    token = str(tick.get("tk") or tick.get("token") or tick.get("symbolToken") or "")
    if not token:
        return
    
    ltp = tick.get("ltp") or tick.get("last_traded_price") or tick.get("lp") or 0
    if isinstance(ltp, (int, float)) and ltp > 1000:
        ltp = ltp / 100
    
    vol = tick.get("v") or tick.get("volume") or tick.get("vol") or 0
    oi = tick.get("oi") or tick.get("openInterest") or 0
    bid = tick.get("bp1") or tick.get("bid") or 0
    ask = tick.get("sp1") or tick.get("ask") or 0
    
    if token == CE_TOKEN:
        latest_ticks["ce_price"] = ltp
        latest_ticks["ce_volume"] = vol
        latest_ticks["ce_oi"] = oi
        latest_ticks["ce_bid"] = bid
        latest_ticks["ce_ask"] = ask
        ce_price_history.append(ltp)
        ce_volume_history.append(vol)
        tick_counter += 1
        logger.debug(f"CE TICK: {ltp} vol={vol} oi={oi}")
    
    elif token == PE_TOKEN:
        latest_ticks["pe_price"] = ltp
        latest_ticks["pe_volume"] = vol
        latest_ticks["pe_oi"] = oi
        latest_ticks["pe_bid"] = bid
        latest_ticks["pe_ask"] = ask
        pe_price_history.append(ltp)
        pe_volume_history.append(vol)
        tick_counter += 1
        logger.debug(f"PE TICK: {ltp} vol={vol} oi={oi}")
    
    if tick_counter % 10 == 0:
        logger.info(f"Live tick #{tick_counter}: CE={latest_ticks['ce_price']:.2f} PE={latest_ticks['pe_price']:.2f} "
                    f"Spread={latest_ticks['ce_price'] - latest_ticks['pe_price']:.2f}")
    
    now = time.time()
    if now - last_minute_snapshot["time"] >= 60:
        avg_price = (latest_ticks["ce_price"] + latest_ticks["pe_price"]) / 2
        avg_vol = (latest_ticks["ce_volume"] + latest_ticks["pe_volume"]) / 2
        snap = {
            "time": now,
            "price": avg_price,
            "volume": avg_vol,
            "ce": latest_ticks["ce_price"],
            "pe": latest_ticks["pe_price"]
        }
        for tf in timeframe_history:
            timeframe_history[tf].append(snap)
        last_minute_snapshot["time"] = now
        last_minute_snapshot["price"] = avg_price
    
    ce = latest_ticks["ce_price"]
    pe = latest_ticks["pe_price"]
    if ce > 0 and pe > 0 and len(ce_price_history) >= 30 and tick_counter % 5 == 0:
        run_signal_engine(
            ce, pe,
            list(ce_price_history),
            list(pe_price_history),
            list(ce_volume_history),
            list(pe_volume_history)
        )
    
    if tick_counter % 10 == 0:
        update_positions()

def on_error(wsapp, error):
    logger.error(f"WebSocket error: {error}")

def on_close(wsapp, close_status_code=None, close_msg=None):
    logger.warning(f"WebSocket CLOSE: code={close_status_code}, msg={close_msg}")
    global ws_running
    ws_running = False

# ============================================================
# AUTHENTICATION
# ============================================================
def get_auth_token():
    """Get cached or fresh authentication tokens."""
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
        auth_cache.update({
            "token": auth_token,
            "feed_token": feed_token,
            "timestamp": now,
            "obj": obj
        })
        logger.info("Auth token refreshed")
        return auth_token, feed_token, obj
    except Exception as e:
        logger.error(f"Auth error: {e}")
        return None, None, None

# ============================================================
# TOKEN REFRESH
# ============================================================
def refresh_tokens_periodically():
    """Periodically refresh ATM tokens to handle expiry rollovers."""
    global CE_TOKEN, PE_TOKEN, CE_SYMBOL, PE_SYMBOL, ATM_STRIKE, EXPIRY_DATE
    
    while engine_active:
        time.sleep(CONFIG.TOKEN_REFRESH_SEC)
        
        if not is_market_open():
            continue
        
        new_ce, new_pe, new_ce_sym, new_pe_sym, new_strike, new_expiry = get_current_atm_tokens()
        if new_ce and new_pe:
            if new_ce != CE_TOKEN or new_pe != PE_TOKEN:
                logger.info(f"Token refresh: CE {CE_TOKEN}->{new_ce}, PE {PE_TOKEN}->{new_pe}")
                CE_TOKEN, PE_TOKEN = new_ce, new_pe
                CE_SYMBOL, PE_SYMBOL = new_ce_sym, new_pe_sym
                ATM_STRIKE = new_strike
                EXPIRY_DATE = new_expiry
                
                if sws and ws_running:
                    try:
                        sws.unsubscribe_all()
                        time.sleep(0.5)
                        sws.subscribe("nifty_signal", 1, [{"exchangeType": 2, "tokens": [CE_TOKEN, PE_TOKEN]}])
                        logger.info("Resubscribed with new tokens")
                    except Exception as e:
                        logger.error(f"Resubscribe error: {e}")

# ============================================================
# WEBSOCKET CONNECTION MANAGER
# ============================================================
def start_websocket():
    """Main WebSocket connection manager with reconnection logic."""
    global ws_running, CE_TOKEN, PE_TOKEN, sws, last_tick_time, tick_counter, _reconnecting, _last_429_time
    
    if not _is_primary_worker:
        logger.info("[WORKER] Not primary - WebSocket thread exiting")
        return
    
    retry_delay = 5
    consecutive_failures = 0
    
    while engine_active:
        ws_running = False
        
        if not is_market_open():
            logger.info("Market closed. Sleeping 5 minutes...")
            CE_TOKEN = None
            PE_TOKEN = None
            time.sleep(300)
            continue
        
        now = time.time()
        if now - _last_429_time < 300:
            remaining = int(300 - (now - _last_429_time))
            logger.warning(f"429 cooldown active. Waiting {remaining}s...")
            time.sleep(min(remaining, 60))
            continue
        
        with _reconnect_lock:
            if _reconnecting:
                time.sleep(2)
                continue
            _reconnecting = True
        
        try:
            if not CE_TOKEN or not PE_TOKEN:
                get_current_atm_tokens()
                if not CE_TOKEN or not PE_TOKEN:
                    logger.warning("No tokens, retrying in 60s...")
                    with _reconnect_lock:
                        _reconnecting = False
                    time.sleep(60)
                    continue
            
            auth_token, feed_token, obj = get_auth_token()
            if not auth_token:
                consecutive_failures += 1
                wait = min(retry_delay * (2 ** min(consecutive_failures, 6)), 300)
                logger.warning(f"Auth failed (#{consecutive_failures}), waiting {wait}s...")
                with _reconnect_lock:
                    _reconnecting = False
                time.sleep(wait)
                continue
            
            consecutive_failures = 0

            # BEFORE: sws = SmartWebSocketV2(...)
            # Add this cleanup block:

            # Aggressive cleanup of any existing connection
            if sws is not None:
                try:
                    if hasattr(sws, 'wsapp') and sws.wsapp:
                        sws.wsapp.close()
                        time.sleep(2)  # Wait for server to register disconnect
                except Exception as e:
                    logger.debug(f"Cleanup error: {e}")
                sws = None

            # Force garbage collection to release socket

            time.sleep(3)  # Critical: wait before new connection
            
            sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)
            sws = patch_smartwebsocket(sws)
            
            sws.on_open = on_open
            sws.on_data = on_data
            sws.on_error = on_error
            sws.on_close = on_close
            
            ws_running = True
            last_tick_time = time.time()
            tick_counter = 0
            logger.info("Connecting WebSocket...")
            
            ws_thread = threading.Thread(target=sws.connect, daemon=True, name="WS-Connect")
            ws_thread.start()
            time.sleep(5)
            
            if not ws_running:
                logger.warning("WebSocket failed to connect, retrying...")
                with _reconnect_lock:
                    _reconnecting = False
                time.sleep(10)
                continue
            
            no_tick_count = 0
            while ws_running and engine_active:
                time.sleep(5)
                
                with _reconnect_lock:
                    if _reconnecting:
                        no_tick_count = 0
                        continue
                
                age = time.time() - last_tick_time
                if not ws_running:
                    no_tick_count = 0
                    continue
                
                if age > 90:
                    no_tick_count += 1
                    logger.warning(f"No ticks for {age:.0f}s (strike {no_tick_count}/3)")
                    if no_tick_count >= 3:
                        logger.error("Watchdog: Max strikes reached, forcing reconnect")
                        ws_running = False
                        break
                else:
                    if no_tick_count > 0:
                        logger.info("Ticks resumed")
                    no_tick_count = 0
            
            logger.warning("WebSocket loop ended, cleaning up...")
            try:
                if sws and hasattr(sws, 'wsapp') and sws.wsapp:
                    sws.wsapp.close()
            except:
                pass
            sws = None
            
            with _reconnect_lock:
                _reconnecting = False
            
            time.sleep(10)
        
        except Exception as e:
            error_str = str(e)
            logger.error(f"WebSocket fatal error: {e}", exc_info=True)
            
            if '429' in error_str or 'Connection Limit Exceeded' in error_str:
                logger.error("RATE LIMIT 429. Entering 10-minute cooldown.")
                # Close any existing connection before cooldown
                try:
                    if sws and hasattr(sws, 'wsapp') and sws.wsapp:
                        sws.wsapp.close()
                except:
                    pass
                sws = None
                gc.collect()
                _last_429_time = time.time()
                with _reconnect_lock:
                    _reconnecting = False
                time.sleep(600)  # 10 minutes - Angel One needs this
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                wait = min(retry_delay * (2 ** min(consecutive_failures, 6)), 300)
                logger.info(f"Waiting {wait}s before reconnect...")
                with _reconnect_lock:
                    _reconnecting = False
                time.sleep(wait)          


# ============================================================
# REST FALLBACK
# ============================================================
def rest_fallback():
    """REST API fallback for when WebSocket is unavailable."""
    global CE_TOKEN, PE_TOKEN
    
    while engine_active:
        time.sleep(CONFIG.REST_POLL_INTERVAL_SEC)
        
        if not is_market_open():
            continue
        
        if ws_running and (time.time() - last_tick_time) < 45:
            continue
        
        if not CE_TOKEN or not PE_TOKEN:
            logger.debug("REST fallback: No tokens available")
            continue
        
        auth_token, _, obj = get_auth_token()
        if not auth_token or not obj:
            logger.warning("REST fallback: Auth failed")
            continue
        
        try:
            ce_data = obj.ltpData("NFO", "NIFTY", CE_TOKEN)
            if ce_data and ce_data.get("data") and ce_data["data"].get("ltp"):
                ltp = float(ce_data["data"]["ltp"])
                latest_ticks["ce_price"] = ltp
                ce_price_history.append(ltp)
                ce_volume_history.append(100)
                logger.info(f"[REST FALLBACK] CE LTP: {ltp}")
            else:
                logger.warning(f"REST fallback CE invalid response: {ce_data}")
            
            pe_data = obj.ltpData("NFO", "NIFTY", PE_TOKEN)
            if pe_data and pe_data.get("data") and pe_data["data"].get("ltp"):
                ltp = float(pe_data["data"]["ltp"])
                latest_ticks["pe_price"] = ltp
                pe_price_history.append(ltp)
                pe_volume_history.append(100)
                logger.info(f"[REST FALLBACK] PE LTP: {ltp}")
            else:
                logger.warning(f"REST fallback PE invalid response: {pe_data}")
            
            ce = latest_ticks["ce_price"]
            pe = latest_ticks["pe_price"]
            if ce > 0 and pe > 0 and len(ce_price_history) >= 30:
                run_signal_engine(
                    ce, pe,
                    list(ce_price_history),
                    list(pe_price_history),
                    list(ce_volume_history),
                    list(pe_volume_history)
                )
                logger.info(f"[REST FALLBACK] Signal engine triggered: CE={ce}, PE={pe}")
        
        except Exception as e:
            logger.error(f"REST fallback error: {e}")

# ============================================================
# BACKTESTING FRAMEWORK
# ============================================================
class BacktestEngine:
    """Backtesting engine for strategy validation."""
    
    def __init__(self, start_date: str, end_date: str, initial_capital: float = 100000):
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d")
        self.initial_capital = initial_capital
        self.trades = []
        self.equity_curve = []
    
    def fetch_historical_data(self, token: str, interval: str = "ONE_MINUTE") -> pd.DataFrame:
        """Fetch historical candle data from Angel One."""
        auth_token, _, obj = get_auth_token()
        if not obj:
            logger.error("Auth failed for backtest")
            return pd.DataFrame()
        
        try:
            params = {
                "exchange": "NFO",
                "symboltoken": token,
                "interval": interval,
                "fromdate": self.start_date.strftime("%Y-%m-%d 09:15"),
                "todate": self.end_date.strftime("%Y-%m-%d 15:30")
            }
            data = obj.getCandleData(params)
            if data and data.get("data"):
                df = pd.DataFrame(data["data"], columns=["timestamp", "open", "high", "low", "close", "volume"])
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                return df
        except Exception as e:
            logger.error(f"Historical data fetch error: {e}")
        
        return pd.DataFrame()
    
    def run_backtest(self, ce_token: str, pe_token: str) -> dict:
        """Run backtest on historical data."""
        logger.info(f"Starting backtest: {self.start_date.date()} to {self.end_date.date()}")
        
        ce_df = self.fetch_historical_data(ce_token)
        pe_df = self.fetch_historical_data(pe_token)
        
        if ce_df.empty or pe_df.empty:
            return {"error": "Failed to fetch historical data"}
        
        merged = pd.merge(ce_df, pe_df, on="timestamp", suffixes=("_ce", "_pe"))
        
        capital = self.initial_capital
        position = None
        trades = []
        
        for i in range(30, len(merged)):
            row = merged.iloc[i]
            ce_hist = list(merged["close_ce"].iloc[:i+1])
            pe_hist = list(merged["close_pe"].iloc[:i+1])
            ce_vol = list(merged["volume_ce"].iloc[:i+1])
            pe_vol = list(merged["volume_pe"].iloc[:i+1])
            
            global ce_price_history, pe_price_history, ce_volume_history, pe_volume_history
            ce_price_history = deque(ce_hist, maxlen=500)
            pe_price_history = deque(pe_hist, maxlen=500)
            ce_volume_history = deque(ce_vol, maxlen=500)
            pe_volume_history = deque(pe_vol, maxlen=500)
            
            latest_ticks["ce_price"] = row["close_ce"]
            latest_ticks["pe_price"] = row["close_pe"]
            
            run_signal_engine(row["close_ce"], row["close_pe"], ce_hist, pe_hist, ce_vol, pe_vol)
            
            action = signal_state["current_action"]
            
            if position is None and action in ["STRONG BUY CE", "BUY CE"]:
                position = {"type": "CE", "entry": row["close_ce"], "stop": row["close_ce"] * 0.95}
            elif position is None and action in ["STRONG BUY PE", "BUY PE"]:
                position = {"type": "PE", "entry": row["close_pe"], "stop": row["close_pe"] * 1.05}
            elif position and action == "HOLD":
                if position["type"] == "CE" and row["close_ce"] <= position["stop"]:
                    pnl = (row["close_ce"] - position["entry"]) * 75
                    capital += pnl
                    trades.append({"type": "CE", "pnl": pnl, "exit_reason": "STOP"})
                    position = None
                elif position["type"] == "PE" and row["close_pe"] >= position["stop"]:
                    pnl = (position["entry"] - row["close_pe"]) * 75
                    capital += pnl
                    trades.append({"type": "PE", "pnl": pnl, "exit_reason": "STOP"})
                    position = None
            
            self.equity_curve.append({"timestamp": row["timestamp"], "equity": capital})
        
        total_return = ((capital - self.initial_capital) / self.initial_capital) * 100
        wins = sum(1 for t in trades if t["pnl"] > 0)
        losses = sum(1 for t in trades if t["pnl"] <= 0)
        win_rate = (wins / len(trades) * 100) if trades else 0
        
        equity_df = pd.DataFrame(self.equity_curve)
        if not equity_df.empty:
            equity_df["peak"] = equity_df["equity"].cummax()
            equity_df["drawdown"] = (equity_df["equity"] - equity_df["peak"]) / equity_df["peak"] * 100
            max_dd = equity_df["drawdown"].min()
        else:
            max_dd = 0
        
        result = {
            "run_id": str(uuid.uuid4())[:8],
            "start_date": self.start_date.strftime("%Y-%m-%d"),
            "end_date": self.end_date.strftime("%Y-%m-%d"),
            "total_return_pct": round(total_return, 2),
            "max_drawdown_pct": round(abs(max_dd), 2),
            "win_rate": round(win_rate, 2),
            "num_trades": len(trades),
            "wins": wins,
            "losses": losses,
            "final_capital": round(capital, 2),
            "config": json.dumps({"mode": "BACKTEST"})
        }
        
        conn = db._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO backtest_results (run_id, start_date, end_date, total_return_pct,
                    max_drawdown_pct, win_rate, num_trades, avg_trade_return, config, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (result["run_id"], result["start_date"], result["end_date"],
                  result["total_return_pct"], result["max_drawdown_pct"],
                  result["win_rate"], result["num_trades"], 0,
                  result["config"], datetime.now().isoformat()))
            conn.commit()
        except Exception as e:
            logger.error(f"Backtest save error: {e}")
        finally:
            conn.close()
        
        logger.info(f"Backtest complete: Return={total_return:.2f}%, MaxDD={max_dd:.2f}%, WinRate={win_rate:.1f}%")
        return result

# ============================================================
# FLASK ENDPOINTS
# ============================================================
@app.route("/")
def home():
    return jsonify({
        "status": "online",
        "message": "Nifty Signal Engine v5.0 - Institutional Grade",
        "worker_type": "PRIMARY (WebSocket)" if _is_primary_worker else "SECONDARY (REST-only)",
        "market_open": is_market_open(),
        "trading_mode": TRADING_MODE,
        "version": "5.0"
    })

@app.route("/api/live-signals")
def live_signals():
    spot = get_nifty_spot_cached()
    open_positions = db.get_open_positions()
    
    return jsonify({
        "status": "active",
        "data": market_signal,
        "market": market_state,
        "institutional": institutional_state,
        "spot_price": spot if spot else 0,
        "trading_mode": TRADING_MODE,
        "portfolio": {
            "equity": round(portfolio_state["equity"], 2),
            "available_cash": round(portfolio_state["available_cash"], 2),
            "total_exposure_pct": round(portfolio_state["total_exposure_pct"], 2),
            "daily_pnl": round(portfolio_state["daily_pnl"], 2),
            "daily_pnl_pct": round(portfolio_state["daily_pnl_pct"], 2),
            "max_drawdown_today": round(portfolio_state["max_drawdown_today"], 2),
            "circuit_breaker": portfolio_state["circuit_breaker_triggered"],
            "open_positions": len(open_positions)
        },
        "signal_state": {
            "current_action": signal_state["current_action"],
            "pending_action": signal_state["pending_action"] or "NONE",
            "signal_type": signal_state["current_signal_type"],
            "confirmation_count": signal_state["confirmation_count"],
            "required_confirmations": CONFIG.SIGNAL_CONFIRMATION_BARS,
            "grade": signal_state["signal_grade"],
            "position_size_pct": signal_state["position_size_pct"],
            "risk_reward": signal_state["risk_reward"],
            "entry_price": signal_state["entry_price"],
            "stop_loss": signal_state["stop_loss"],
            "target": signal_state["target"],
            "flip_count_hour": signal_state["flip_count_hour"]
        }
    })

@app.route("/api/health")
def health():
    return jsonify({
        "status": "ok",
        "ws_running": ws_running,
        "reconnecting": _reconnecting,
        "is_primary_worker": _is_primary_worker,
        "ce_token": CE_TOKEN,
        "pe_token": PE_TOKEN,
        "ce_symbol": CE_SYMBOL,
        "pe_symbol": PE_SYMBOL,
        "atm_strike": ATM_STRIKE,
        "expiry": EXPIRY_DATE,
        "latest_ce": latest_ticks["ce_price"],
        "latest_pe": latest_ticks["pe_price"],
        "ce_history_len": len(ce_price_history),
        "pe_history_len": len(pe_price_history),
        "last_tick_age": round(time.time() - last_tick_time, 1),
        "trading_mode": TRADING_MODE,
        "timestamp": datetime.now().isoformat()
    })

@app.route("/api/positions")
def get_positions():
    open_positions = db.get_open_positions()
    return jsonify({
        "status": "ok",
        "positions": [asdict(p) for p in open_positions],
        "count": len(open_positions)
    })

@app.route("/api/trades")
def get_trades():
    today = datetime.now().strftime("%Y-%m-%d")
    conn = db._get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM trade_journal WHERE date(timestamp) = date(?) ORDER BY timestamp DESC", (today,))
        rows = cursor.fetchall()
        trades = []
        for row in rows:
            trades.append({
                "trade_id": row[1], "timestamp": row[2], "action": row[3],
                "symbol": row[4], "option_type": row[5], "entry_price": row[6],
                "exit_price": row[7], "quantity": row[8], "pnl": row[9],
                "pnl_pct": row[10], "grade": row[11], "confidence": row[12],
                "regime": row[13], "exit_reason": row[14], "mode": row[15]
            })
        return jsonify({"status": "ok", "trades": trades, "count": len(trades)})
    except Exception as e:
        logger.error(f"Get trades error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        conn.close()

@app.route("/api/debug/tokens")
def debug_tokens():
    auth_token, _, obj = get_auth_token()
    if not obj:
        return jsonify({"error": "Auth failed"}), 500
    
    ce_ltp = obj.ltpData("NFO", "NIFTY", CE_TOKEN) if CE_TOKEN else None
    pe_ltp = obj.ltpData("NFO", "NIFTY", PE_TOKEN) if PE_TOKEN else None
    
    return jsonify({
        "CE_TOKEN": CE_TOKEN,
        "PE_TOKEN": PE_TOKEN,
        "CE_SYMBOL": CE_SYMBOL,
        "PE_SYMBOL": PE_SYMBOL,
        "ATM_STRIKE": ATM_STRIKE,
        "EXPIRY": EXPIRY_DATE,
        "CE_LTP": ce_ltp,
        "PE_LTP": pe_ltp,
        "trading_mode": TRADING_MODE,
        "timestamp": datetime.now().isoformat()
    })

@app.route("/api/backtest", methods=["POST"])
def run_backtest_endpoint():
    """Run backtest for specified date range."""
    data = request.get_json() or {}
    start_date = data.get("start_date", (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
    end_date = data.get("end_date", datetime.now().strftime("%Y-%m-%d"))
    
    if not CE_TOKEN or not PE_TOKEN:
        get_current_atm_tokens()
    
    engine = BacktestEngine(start_date, end_date)
    result = engine.run_backtest(CE_TOKEN, PE_TOKEN)
    
    return jsonify({
        "status": "complete",
        "result": result
    })

@app.route("/api/mode", methods=["GET", "POST"])
def trading_mode():
    """Get or set trading mode (PAPER/LIVE)."""
    global TRADING_MODE
    
    if request.method == "GET":
        return jsonify({"trading_mode": TRADING_MODE})
    
    data = request.get_json() or {}
    new_mode = data.get("mode", "PAPER").upper()
    
    if new_mode not in ["PAPER", "LIVE"]:
        return jsonify({"error": "Invalid mode. Use PAPER or LIVE"}), 400
    
    old_mode = TRADING_MODE
    TRADING_MODE = new_mode
    logger.info(f"Trading mode changed: {old_mode} -> {TRADING_MODE}")
    
    return jsonify({
        "status": "ok",
        "previous_mode": old_mode,
        "current_mode": TRADING_MODE,
        "message": f"Trading mode set to {TRADING_MODE}"
    })

@app.route("/api/manual/close", methods=["POST"])
def manual_close_position():
    """Manually close a position by order_id."""
    data = request.get_json() or {}
    order_id = data.get("order_id")
    
    if not order_id:
        return jsonify({"error": "order_id required"}), 400
    
    open_positions = db.get_open_positions()
    position = next((p for p in open_positions if p.order_id == order_id), None)
    
    if not position:
        return jsonify({"error": "Position not found or already closed"}), 404
    
    current_price = latest_ticks.get(f"{position.option_type.lower()}_price", 0)
    if current_price <= 0:
        return jsonify({"error": "Cannot close - no current price available"}), 400
    
    success = close_position(position, current_price, "MANUAL")
    
    return jsonify({
        "status": "closed" if success else "failed",
        "order_id": order_id,
        "exit_price": current_price,
        "mode": TRADING_MODE
    })

@app.route("/api/signals/history")
def signal_history():
    """Get historical signals from database."""
    limit = request.args.get("limit", 100, type=int)
    
    conn = db._get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT timestamp, action, grade, confidence, ce_price, pe_price, 
                   rsi, macd, pcr, atr, regime, entry_price, stop_loss, target,
                   position_size_pct, risk_reward
            FROM signals 
            ORDER BY timestamp DESC 
            LIMIT ?
        """, (limit,))
        
        rows = cursor.fetchall()
        signals = []
        for row in rows:
            signals.append({
                "timestamp": row[0], "action": row[1], "grade": row[2],
                "confidence": row[3], "ce_price": row[4], "pe_price": row[5],
                "rsi": row[6], "macd": row[7], "pcr": row[8], "atr": row[9],
                "regime": row[10], "entry_price": row[11], "stop_loss": row[12],
                "target": row[13], "position_size_pct": row[14], "risk_reward": row[15]
            })
        
        return jsonify({"status": "ok", "signals": signals, "count": len(signals)})
    except Exception as e:
        logger.error(f"Signal history error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        conn.close()

@app.route("/api/stats")
def get_stats():
    """Get trading statistics."""
    today_pnl = db.get_today_pnl()
    open_positions = db.get_open_positions()
    
    total_unrealized = sum(p.unrealized_pnl for p in open_positions)
    total_realized = today_pnl["realized_pnl"]
    
    conn = db._get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT COUNT(*), 
                   SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                   SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losses,
                   SUM(pnl) as total_pnl,
                   AVG(pnl_pct) as avg_return
            FROM trade_journal
        """)
        row = cursor.fetchone()
        
        return jsonify({
            "status": "ok",
            "today": {
                "realized_pnl": round(total_realized, 2),
                "unrealized_pnl": round(total_unrealized, 2),
                "total_pnl": round(total_realized + total_unrealized, 2),
                "num_trades": today_pnl["num_trades"],
                "wins": today_pnl["wins"],
                "losses": today_pnl["losses"],
                "win_rate": round((today_pnl["wins"] / today_pnl["num_trades"] * 100), 2) if today_pnl["num_trades"] > 0 else 0
            },
            "all_time": {
                "total_trades": row[0] or 0,
                "total_wins": row[1] or 0,
                "total_losses": row[2] or 0,
                "total_pnl": round(row[3] or 0, 2),
                "avg_return_pct": round(row[4] or 0, 2),
                "win_rate": round((row[1] / row[0] * 100), 2) if row[0] > 0 else 0
            },
            "portfolio": {
                "equity": round(portfolio_state["equity"], 2),
                "available_cash": round(portfolio_state["available_cash"], 2),
                "total_exposure_pct": round(portfolio_state["total_exposure_pct"], 2),
                "open_positions": len(open_positions),
                "daily_pnl_pct": round(portfolio_state["daily_pnl_pct"], 2),
                "max_drawdown_today": round(portfolio_state["max_drawdown_today"], 2),
                "circuit_breaker": portfolio_state["circuit_breaker_triggered"]
            },
            "mode": TRADING_MODE
        })
    except Exception as e:
        logger.error(f"Stats error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        conn.close()

# ============================================================
# ENGINE STARTUP
# ============================================================
_engine_started = False
_startup_lock = threading.Lock()

def initialize_engine():
    """Initialize all background threads."""
    global _engine_started
    
    with _startup_lock:
        if _engine_started:
            return
        _engine_started = True
        
        logger.info("=" * 60)
        logger.info("Nifty Signal Engine v5.0 - Institutional Grade")
        logger.info(f"Trading Mode: {TRADING_MODE}")
        logger.info(f"Max Daily Loss: {MAX_DAILY_LOSS_PCT}%")
        logger.info(f"Portfolio Heat Max: {PORTFOLIO_HEAT_MAX_PCT}%")
        logger.info("Features:")
        logger.info("  [OK] Fixed signal confirmation (persistent across ticks)")
        logger.info("  [OK] Separate CE/PE price analysis")
        logger.info("  [OK] Real Black-Scholes Greeks with IV")
        logger.info("  [OK] Database persistence (signals, positions, journal)")
        logger.info("  [OK] Paper/Live trading toggle")
        logger.info("  [OK] Order management with position tracking")
        logger.info("  [OK] Backtesting framework")
        logger.info("  [OK] Proper WebSocket binary parsing")
        logger.info("  [OK] Portfolio heat & correlation risk checks")
        logger.info("  [OK] Daily loss circuit breakers")
        logger.info("  [OK] Trailing stops")
        logger.info("=" * 60)
        
        _acquire_primary_lock()
        
        today = datetime.now().strftime("%Y-%m-%d")
        conn = db._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT SUM(pnl) FROM trade_journal 
                WHERE date(timestamp) = date(?)
            """, (today,))
            row = cursor.fetchone()
            if row and row[0]:
                portfolio_state["daily_pnl"] = row[0]
        except:
            pass
        finally:
            conn.close()
        
        threading.Thread(target=start_websocket, daemon=True, name="WS-Main").start()
        threading.Thread(target=rest_fallback, daemon=True, name="REST-Fallback").start()
        threading.Thread(target=refresh_tokens_periodically, daemon=True, name="TokenRefresher").start()
        
        logger.info("Background threads started")

@app.before_request
def start_engine_once():
    """Lazy initialization on first request."""
    if not _engine_started:
        initialize_engine()

# ============================================================
# GRACEFUL SHUTDOWN
# ============================================================
def shutdown_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global engine_active
    logger.info("Shutdown signal received - initiating graceful shutdown...")
    engine_active = False
    
    try:
        open_positions = db.get_open_positions()
        for pos in open_positions:
            current_price = latest_ticks.get(f"{pos.option_type.lower()}_price", 0)
            if current_price > 0:
                close_position(pos, current_price, "SHUTDOWN")
    except Exception as e:
        logger.error(f"Error closing positions on shutdown: {e}")
    
    try:
        if sws and hasattr(sws, 'wsapp') and sws.wsapp:
            sws.wsapp.close()
    except:
        pass
    
    logger.info("Shutdown complete")
    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)

# ============================================================
# LOCAL DEVELOPMENT ENTRY POINT
# ============================================================
if __name__ == "__main__":
    initialize_engine()
    port = int(os.environ.get("PORT", 10000))
    logger.info(f"Starting Flask server on port {port}")
    app.run(host="0.0.0.0", port=port, threaded=True)
# === VERSION 14.1 - PRO SIGNAL BOT: Fixed Gunicorn thread startup ===
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
from collections import deque
from datetime import datetime, timedelta, time as dt_time, timezone
from flask import Flask, jsonify, request
from flask_cors import CORS
import pyotp

# Force logging to stdout immediately
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
    force=True
)
logger = logging.getLogger(__name__)

# Log startup immediately
logger.info("=" * 60)
logger.info("STARTING BOT v14.1 - REST-only mode (Gunicorn compatible)")
logger.info("=" * 60)

app = Flask(__name__)

ALLOWED_ORIGINS = [
    "http://index-options.co",
    "https://index-options.co",
    "http://localhost:5000",
    "http://127.0.0.1:5000"
]
CORS(app, origins=ALLOWED_ORIGINS, supports_credentials=False)

API_KEY = os.getenv("API_KEY", "")
application = app

# ----------------------------------------------------------------------
# ENVIRONMENT
# ----------------------------------------------------------------------
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PASSWORD = os.getenv("ANGEL_PASSWORD")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

logger.info(f"ANGEL_API_KEY set: {bool(ANGEL_API_KEY)}")
logger.info(f"ANGEL_CLIENT_ID set: {bool(ANGEL_CLIENT_ID)}")
logger.info(f"ANGEL_PASSWORD set: {bool(ANGEL_PASSWORD)}")
logger.info(f"ANGEL_TOTP_SECRET set: {bool(ANGEL_TOTP_SECRET)}")

if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP_SECRET]):
    logger.error("Missing critical Angel One environment variables!")
    raise ValueError("Missing critical Angel One environment variables")

DB_PATH = "trading_data.db"

# ----------------------------------------------------------------------
# DATABASE
# ----------------------------------------------------------------------
def init_db():
    logger.info("Initializing database...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS ticks (timestamp REAL, token TEXT, price REAL, volume REAL, bid REAL, ask REAL, oi REAL)""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ticks_time ON ticks(timestamp)")
    c.execute("""CREATE TABLE IF NOT EXISTS signals (timestamp REAL, action TEXT, signal_type TEXT, grade TEXT, confidence REAL, ce_price REAL, pe_price REAL, rsi REAL, pcr REAL, regime TEXT, vwap REAL, vix REAL, ml_score REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS trades (timestamp REAL, action TEXT, entry_price REAL, exit_price REAL, pnl REAL, size_pct REAL, status TEXT, grade TEXT, atr REAL, vix REAL, exit_reason TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS daily_performance (date TEXT, equity REAL, daily_pnl REAL, drawdown_pct REAL, sharpe REAL, var REAL, win_rate REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS ml_models (id INTEGER PRIMARY KEY, model BLOB, created_at REAL, features TEXT, accuracy REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS portfolio_equity (index_name TEXT PRIMARY KEY, equity REAL, last_updated REAL, active_action TEXT, entry_price REAL, stop_loss REAL, target REAL, lots INTEGER, entry_time REAL, highest REAL, last_trade_date TEXT, daily_trade_count INTEGER)""")
    c.execute("""CREATE TABLE IF NOT EXISTS kelly_history (timestamp REAL, index_name TEXT, kelly_fraction REAL, win_rate REAL, avg_win REAL, avg_loss REAL, recommended_lots INTEGER, actual_lots INTEGER)""")
    c.execute("""CREATE TABLE IF NOT EXISTS greeks_history (timestamp REAL, index_name TEXT, ce_iv REAL, pe_iv REAL, ce_delta REAL, pe_delta REAL, ce_gamma REAL, pe_gamma REAL, ce_theta REAL, pe_theta REAL, ce_vega REAL, pe_vega REAL, iv_rank REAL, iv_percentile REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS performance_metrics (timestamp REAL, index_name TEXT, sharpe REAL, sortino REAL, calmar REAL, win_rate REAL, profit_factor REAL, max_drawdown REAL, avg_trade REAL, expectancy REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS drawdown_events (timestamp REAL, index_name TEXT, drawdown_pct REAL, action_taken TEXT, equity_before REAL, equity_after REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS signal_state (index_name TEXT PRIMARY KEY, action TEXT, entry_price REAL, stop_loss REAL, target REAL, lots INTEGER, entry_time REAL, highest REAL)""")
    try:
        c.execute("ALTER TABLE portfolio_equity ADD COLUMN last_trade_date TEXT")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE portfolio_equity ADD COLUMN daily_trade_count INTEGER")
    except Exception:
        pass
    conn.commit()
    conn.close()
    logger.info("Database initialized successfully")

init_db()

try:
    from SmartApi import SmartConnect
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
    logger.info("SmartApi imported successfully")
except ImportError as e:
    logger.error(f"SmartApi import failed: {e}")
    sys.exit(1)

# ----------------------------------------------------------------------
# INDEX CONFIGURATION
# ----------------------------------------------------------------------
INDEX_CONFIG = {
    "NIFTY": {
        "token": "99926000", "exchange": "NSE", "symbol": "NIFTY", "lot_size": 50, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": "BANKNIFTY", "greeks_enabled": True, "pcr_enabled": True
    },
    "BANKNIFTY": {
        "token": "99926009", "exchange": "NSE", "symbol": "BANKNIFTY", "lot_size": 25, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": "NIFTY", "greeks_enabled": True, "pcr_enabled": True
    },
    "FINNIFTY": {
        "token": "99926037", "exchange": "NSE", "symbol": "FINNIFTY", "lot_size": 40, "expiry_weekday": 1, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 50, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True
    },
    "MIDCPNIFTY": {
        "token": "99926074", "exchange": "NSE", "symbol": "MIDCPNIFTY", "lot_size": 75, "expiry_weekday": 3, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 25, "option_exchange": "NFO",
        "ws_exchange_type": 1, "option_ws_exchange_type": 2, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": None, "greeks_enabled": False, "pcr_enabled": True
    },
    "SENSEX": {
        "token": "99919000", "exchange": "BSE", "symbol": "SENSEX", "lot_size": 15, "expiry_weekday": 4, "active": True,
        "min_premium": 5, "max_premium": 8000, "atm_strike_multiple": 100, "option_exchange": "BFO",
        "ws_exchange_type": 3, "option_ws_exchange_type": 4, "max_daily_drawdown_pct": 3.0,
        "correlation_pair": None, "greeks_enabled": True, "pcr_enabled": True
    }
}

INDEX_TOKEN_SET = {cfg["token"] for cfg in INDEX_CONFIG.values()}
logger.info(f"Loaded {len(INDEX_CONFIG)} indices: {list(INDEX_CONFIG.keys())}")

# ----------------------------------------------------------------------
# GLOBAL STATE & LOCKS
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

INDEX_TOKENS = {idx: {"ce_token": None, "pe_token": None, "atm_strike": 0, "expiry": "", "expiry_date": None, "ce_symbol": "", "pe_symbol": ""} for idx in INDEX_CONFIG}
last_known_prices = {idx: {"spot": 0.0, "ce": 0.0, "pe": 0.0, "timestamp": 0} for idx in INDEX_CONFIG}
price_histories = {idx: deque(maxlen=5000) for idx in INDEX_CONFIG}

portfolio_state = {idx: {"equity": 100000.0, "open_positions": 0, "daily_pnl": 0.0, "total_pnl": 0.0, "live_pnl": 0.0} for idx in INDEX_CONFIG}
signal_state = {idx: {"action": "HOLD", "entry_price": 0.0, "stop_loss": 0.0, "target": 0.0, "lots": 0, "entry_time": 0.0, "highest": 0.0, "cooldown": 0, "confidence": 0, "exit_reason": ""} for idx in INDEX_CONFIG}
market_signal = {idx: {"sentiment_score": 50, "signal": "WAITING", "alert_message": "", "entry_price": 0, "stop_loss": 0, "target": 0, "exit_reason": ""} for idx in INDEX_CONFIG}

ce_price_histories = {idx: deque(maxlen=2000) for idx in INDEX_CONFIG}
pe_price_histories = {idx: deque(maxlen=2000) for idx in INDEX_CONFIG}
ce_volume_histories = {idx: deque(maxlen=1000) for idx in INDEX_CONFIG}
pe_volume_histories = {idx: deque(maxlen=1000) for idx in INDEX_CONFIG}
ce_oi_histories = {idx: deque(maxlen=200) for idx in INDEX_CONFIG}
pe_oi_histories = {idx: deque(maxlen=200) for idx in INDEX_CONFIG}
vix_history = deque(maxlen=200)
nifty_price_series = deque(maxlen=200)
banknifty_price_series = deque(maxlen=200)

latest_ticks = {idx: {"spot_price": 0.0, "ce_price": 0.0, "pe_price": 0.0,
                      "ce_volume": 0, "pe_volume": 0, "ce_oi": 0, "pe_oi": 0,
                      "ce_bid": 0.0, "ce_ask": 0.0, "pe_bid": 0.0, "pe_ask": 0.0}
                for idx in INDEX_CONFIG}
latest_ticks["VIX"] = {"vix": 15.0}

daily_drawdown = {idx: {"peak_equity": 0.0, "current_drawdown": 0.0} for idx in INDEX_CONFIG}
safety_state = {idx: {"consecutive_sl": 0, "circuit_breaker": False, "circuit_breaker_until": 0} for idx in INDEX_CONFIG}
signal_buffer = {idx: {"ce_count": 0, "pe_count": 0} for idx in INDEX_CONFIG}
daily_trade_count = {idx: 0 for idx in INDEX_CONFIG}
last_trade_date = {idx: "" for idx in INDEX_CONFIG}

# ----------------------------------------------------------------------
# TIMEFRAMES
# ----------------------------------------------------------------------
TIMEFRAMES = ["1min", "2min", "3min", "5min", "8min", "10min", "15min", "20min"]
TIMEFRAME_SECONDS = {"1min":60, "2min":120, "3min":180, "5min":300, "8min":480, "10min":600, "15min":900, "20min":1200}
TIMEFRAME_WEIGHTS = {"1min":8, "2min":8, "3min":8, "5min":12, "8min":12, "10min":12, "15min":14, "20min":14}

candle_histories = {idx: {tf: deque(maxlen=500) for tf in TIMEFRAMES} for idx in INDEX_CONFIG}
_last_candle_time = {idx: {tf: 0 for tf in TIMEFRAMES} for idx in INDEX_CONFIG}
_current_candle = {idx: {tf: None for tf in TIMEFRAMES} for idx in INDEX_CONFIG}
_prev_volume = {idx: 0 for idx in INDEX_CONFIG}

# ----------------------------------------------------------------------
# TECHNICAL INDICATORS (simplified versions)
# ----------------------------------------------------------------------
def calculate_ema(prices, period):
    if not prices: return 0
    if len(prices) < period: return sum(prices)/len(prices)
    alpha = 2/(period+1)
    ema = sum(prices[:period])/period
    for p in prices[period:]:
        ema = alpha*p + (1-alpha)*ema
    return ema

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

def calculate_atr(highs, lows, closes, period=14):
    if len(closes) < period+1:
        return 5.0
    tr = []
    for i in range(1, len(closes)):
        tr.append(abs(closes[i] - closes[i-1]))
    return sum(tr[-period:])/period if len(tr)>=period else 5.0

def calculate_adx(highs, lows, closes, period=14):
    return 20.0  # Simplified

def update_candle(idx, price, cumulative_volume, timestamp):
    global _prev_volume
    prev = _prev_volume.get(idx, 0)
    if cumulative_volume < prev or (cumulative_volume == 0 and prev > 0):
        tick_vol = 0
    else:
        tick_vol = max(0, cumulative_volume - prev)
    _prev_volume[idx] = cumulative_volume

    for tf, interval in TIMEFRAME_SECONDS.items():
        candle_start = int(timestamp / interval) * interval
        with _current_candle_lock:
            if _last_candle_time[idx][tf] != candle_start:
                if _current_candle[idx][tf] is not None:
                    with _candle_histories_lock:
                        candle_histories[idx][tf].append(_current_candle[idx][tf])
                _current_candle[idx][tf] = {
                    "open": price, "high": price, "low": price, "close": price,
                    "volume": tick_vol, "timestamp": candle_start
                }
                _last_candle_time[idx][tf] = candle_start
            else:
                if _current_candle[idx][tf] is not None:
                    _current_candle[idx][tf]["high"] = max(_current_candle[idx][tf]["high"], price)
                    _current_candle[idx][tf]["low"] = min(_current_candle[idx][tf]["low"], price)
                    _current_candle[idx][tf]["close"] = price
                    _current_candle[idx][tf]["volume"] += tick_vol

# ----------------------------------------------------------------------
# PERSISTENCE FUNCTIONS
# ----------------------------------------------------------------------
def load_portfolio_state():
    global portfolio_state, signal_state, daily_trade_count, last_trade_date
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        for idx in INDEX_CONFIG:
            row = c.execute(
                "SELECT equity, active_action, entry_price, stop_loss, target, lots, entry_time, highest, last_trade_date, daily_trade_count "
                "FROM portfolio_equity WHERE index_name=?", (idx,)
            ).fetchone()
            if row:
                portfolio_state[idx]["equity"] = float(row[0]) if row[0] is not None else 100000.0
                action = row[1]
                if action and action != "HOLD":
                    signal_state[idx].update({
                        "action": action,
                        "entry_price": float(row[2]) if row[2] is not None else 0.0,
                        "stop_loss": float(row[3]) if row[3] is not None else 0.0,
                        "target": float(row[4]) if row[4] is not None else 0.0,
                        "lots": int(row[5]) if row[5] is not None else 0,
                        "entry_time": float(row[6]) if row[6] is not None else 0.0,
                        "highest": float(row[7]) if row[7] is not None else 0.0
                    })
                    portfolio_state[idx]["open_positions"] = 1
                if len(row) >= 9 and row[8]:
                    last_trade_date[idx] = row[8]
                if len(row) >= 10 and row[9] is not None:
                    daily_trade_count[idx] = int(row[9])
        conn.close()
        logger.info("Persistent state loaded successfully.")
    except Exception as e:
        logger.error(f"Error loading state: {e}")

def save_portfolio_state(idx):
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO portfolio_equity (index_name, equity, last_updated, active_action, entry_price, stop_loss, target, lots, entry_time, highest, last_trade_date, daily_trade_count) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                  (idx, portfolio_state[idx]["equity"], time.time(),
                   signal_state[idx]["action"], signal_state[idx]["entry_price"],
                   signal_state[idx]["stop_loss"], signal_state[idx]["target"],
                   signal_state[idx]["lots"], signal_state[idx]["entry_time"],
                   signal_state[idx]["highest"],
                   last_trade_date.get(idx, ""),
                   daily_trade_count.get(idx, 0)))
        conn.commit()

# ----------------------------------------------------------------------
# AUTHENTICATION
# ----------------------------------------------------------------------
auth_cache = {"token": None, "feed_token": None, "timestamp": 0, "obj": None}
_auth_lock = threading.Lock()

def get_auth_token():
    with _auth_lock:
        now = time.time()
        if auth_cache["token"] and (now - auth_cache["timestamp"] < 3300):
            return auth_cache["token"], auth_cache["feed_token"], auth_cache["obj"]
        try:
            logger.info("Generating new auth token...")
            totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
            logger.info(f"TOTP generated: {totp[:4]}...")
            obj = SmartConnect(api_key=ANGEL_API_KEY)
            session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
            if not session.get("status"):
                logger.error(f"Auth failed: {session}")
                return None, None, None
            auth_token = session["data"]["jwtToken"]
            feed_token = obj.getfeedToken()
            auth_cache.update({"token": auth_token, "feed_token": feed_token, "timestamp": now, "obj": obj})
            logger.info("Auth token refreshed successfully")
            return auth_token, feed_token, obj
        except Exception as e:
            logger.error(f"Auth error: {type(e).__name__} - {str(e)}")
            return None, None, None

def safe_ltp(resp):
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

def get_index_spot(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config:
        return None
    _, _, obj = get_auth_token()
    if not obj:
        return None
    try:
        resp = obj.ltpData(config["exchange"], config["symbol"], config["token"])
        ltp = safe_ltp(resp)
        if ltp and ltp > 0:
            if config["exchange"] in ["NSE", "BSE"] and ltp > 100000:
                ltp /= 100
            return ltp
    except Exception as e:
        logger.debug(f"Spot fetch {index_name}: {e}")
    return None

# ----------------------------------------------------------------------
# SCRIP MASTER / TOKEN MANAGEMENT
# ----------------------------------------------------------------------
_scrip_cache = {"data": None, "timestamp": 0}
_scrip_lock = threading.Lock()

def parse_expiry_date(expiry_str):
    if not expiry_str:
        return None
    formats = ["%d%b%Y", "%d%b%y", "%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d", "%d/%m/%Y"]
    for fmt in formats:
        try:
            return datetime.strptime(expiry_str, fmt)
        except Exception:
            continue
    return None

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
            logger.info("Scrip master refreshed")
            return data
        except Exception as e:
            logger.error(f"Scrip master failed: {e}")
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
    return today + timedelta(days=days_ahead)

def get_current_atm_tokens(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config or not config.get("active"):
        INDEX_TOKENS[index_name].update({"ce_token": None, "pe_token": None})
        return None, None
    spot = get_index_spot(index_name)
    if not spot or spot <= 0:
        INDEX_TOKENS[index_name].update({"ce_token": None, "pe_token": None})
        return None, None
    mult = config["atm_strike_multiple"]
    atm = int(round(spot / mult) * mult)
    next_expiry = get_next_expiry_date(index_name)
    if not next_expiry:
        INDEX_TOKENS[index_name].update({"ce_token": None, "pe_token": None})
        return None, None
    expiry = next_expiry.strftime("%d%b%Y").upper()
    scrip = get_scrip_master()
    if scrip:
        try:
            df = pd.DataFrame(scrip)
            opts = df[(df["name"] == config["symbol"]) &
                      (df["instrumenttype"] == "OPTIDX") &
                      (df["exch_seg"] == config["option_exchange"])]
            if not opts.empty:
                opts["expiry_date"] = opts["expiry"].apply(parse_expiry_date)
                opts = opts.dropna(subset=["expiry_date"])
                opts["strike"] = pd.to_numeric(opts["strike"], errors="coerce") / 100
                opts = opts.dropna(subset=["strike"])
                future = opts[opts["expiry_date"] >= datetime.now()]
                if not future.empty:
                    nearest = future["expiry_date"].min()
                    atm_opts = future[(future["strike"] == atm) & (future["expiry_date"] == nearest)]
                    if atm_opts.empty:
                        same_exp = future[future["expiry_date"] == nearest]
                        if not same_exp.empty:
                            diff = (same_exp["strike"] - atm).abs()
                            atm_opts = same_exp.loc[[diff.idxmin()]]
                    if not atm_opts.empty:
                        ce = atm_opts[atm_opts["symbol"].str.contains("CE", na=False)]
                        pe = atm_opts[atm_opts["symbol"].str.contains("PE", na=False)]
                        if not ce.empty and not pe.empty:
                            ce_token = str(ce.iloc[0]["token"])
                            pe_token = str(pe.iloc[0]["token"])
                            ce_symbol = str(ce.iloc[0]["symbol"])
                            pe_symbol = str(pe.iloc[0]["symbol"])
                            actual_strike = int(ce.iloc[0]["strike"])
                            if actual_strike != atm:
                                logger.info(f"{index_name} ATM strike {atm} not found, using nearest {actual_strike}")
                            else:
                                logger.info(f"{index_name} ATM strike {atm} selected")
                            INDEX_TOKENS[index_name].update({
                                "ce_token": ce_token, "pe_token": pe_token, "atm_strike": actual_strike,
                                "expiry": expiry, "expiry_date": nearest,
                                "ce_symbol": ce_symbol, "pe_symbol": pe_symbol
                            })
                            logger.info(f"{index_name} tokens: CE={ce_token} PE={pe_token} expiry={expiry}")
                            return ce_token, pe_token
        except Exception as e:
            logger.warning(f"{index_name} token fetch error: {e}")
    INDEX_TOKENS[index_name].update({"ce_token": None, "pe_token": None})
    return None, None

def refresh_all_tokens():
    for idx in INDEX_CONFIG:
        if INDEX_CONFIG[idx].get("active"):
            get_current_atm_tokens(idx)

# ----------------------------------------------------------------------
# SENTIMENT
# ----------------------------------------------------------------------
def compute_sentiment(index_name):
    sentiment_scores = []
    for tf in TIMEFRAMES:
        with _candle_histories_lock:
            candles = list(candle_histories[index_name][tf])
        if len(candles) < 10:
            continue
        closes = [c["close"] for c in candles]
        if len(closes) < 60:
            continue
        ema9 = calculate_ema(closes, 9)
        ema21 = calculate_ema(closes, 21)
        ema50 = calculate_ema(closes, 50) if len(closes) >= 50 else ema21
        price = closes[-1]
        recent = closes[-30:]
        price_range = (max(recent) - min(recent)) / sum(recent) * len(recent) if sum(recent) else 0
        if price_range < 0.005:
            score = 0
        elif tf in ["1min", "2min", "3min"]:
            if ema9 > ema21 and price > ema9:
                score = TIMEFRAME_WEIGHTS[tf]
            elif ema9 < ema21 and price < ema9:
                score = -TIMEFRAME_WEIGHTS[tf]
            else:
                score = 0
        elif tf in ["5min", "8min", "10min"]:
            if ema9 > ema21 > ema50 and price > ema9:
                score = TIMEFRAME_WEIGHTS[tf]
            elif ema9 < ema21 < ema50 and price < ema9:
                score = -TIMEFRAME_WEIGHTS[tf]
            elif ema9 > ema21 and price > ema9:
                score = TIMEFRAME_WEIGHTS[tf] - 5
            elif ema9 < ema21 and price < ema9:
                score = -TIMEFRAME_WEIGHTS[tf] + 5
            else:
                score = 0
        else:
            if ema9 > ema21 > ema50:
                score = TIMEFRAME_WEIGHTS[tf] - 5
            elif ema9 < ema21 < ema50:
                score = -TIMEFRAME_WEIGHTS[tf] + 5
            else:
                score = 0
        sentiment_scores.append(score)
    if not sentiment_scores:
        return 50
    total = sum(sentiment_scores)
    sentiment = 50 + (total / 3.5)
    return max(0, min(100, sentiment))

SENTIMENT_SCORES = {
    "STRONG_BULLISH": (85, 100, "STRONG BULLISH", "STRONG_BUY_CE"),
    "BULLISH": (70, 84, "BULLISH", "BUY_CE"),
    "SLOW_BULLISH": (55, 69, "SLOW BULLISH", "LOW_BUY_CE"),
    "NEUTRAL": (45, 54, "NEUTRAL", "NO_TRADE"),
    "SLOW_BEARISH": (30, 44, "SLOW BEARISH", "LOW_BUY_PE"),
    "BEARISH": (15, 29, "BEARISH", "BUY_PE"),
    "STRONG_BEARISH": (0, 14, "STRONG BEARISH", "STRONG_BUY_PE")
}

def get_signal_from_sentiment(sentiment):
    for _, (low, high, _, action) in SENTIMENT_SCORES.items():
        if low <= sentiment <= high:
            return action
    return "NO_TRADE"

def get_sentiment_label(sentiment):
    for _, (low, high, label, _) in SENTIMENT_SCORES.items():
        if low <= sentiment <= high:
            return label
    return "UNKNOWN"

# ----------------------------------------------------------------------
# ML FILTER
# ----------------------------------------------------------------------
class MLSignalFilter:
    def __init__(self):
        self.is_trained = False
    def predict(self, feature_vector):
        try:
            prem, spot, rsi, adx, vix, sentiment = feature_vector
            score = 0.50
            if 45 <= rsi <= 70:
                score += 0.10
            if adx > 25:
                score += 0.15
            if 12 <= vix <= 25:
                score += 0.10
            if sentiment > 55 or sentiment < 45:
                score += 0.10
            if prem > 0 and spot > 0:
                score += 0.05
            return max(0.0, min(1.0, score))
        except Exception:
            return 0.50
ml_filter = MLSignalFilter()

# ----------------------------------------------------------------------
# KELLY CRITERION
# ----------------------------------------------------------------------
class KellyCriterion:
    def __init__(self, index_name, kelly_fraction=0.25, min_trades=10):
        self.index_name = index_name
        self.kelly_fraction = kelly_fraction
        self.min_trades = min_trades
        self.win_count = 0
        self.loss_count = 0
        self.avg_win = 0.0
        self.avg_loss = 0.0
    def update(self, trade_pnl_pct):
        if trade_pnl_pct > 0:
            self.win_count += 1
            self.avg_win = ((self.avg_win * (self.win_count - 1)) + trade_pnl_pct) / self.win_count
        else:
            self.loss_count += 1
            self.avg_loss = ((self.avg_loss * (self.loss_count - 1)) + trade_pnl_pct) / self.loss_count
    def get_recommended_risk_pct(self):
        total = self.win_count + self.loss_count
        if total < self.min_trades:
            return 0.5, 0.5, self.avg_win, self.avg_loss
        p = self.win_count / total
        q = 1 - p
        b = 2.0
        if self.avg_loss != 0:
            b = abs(self.avg_win / self.avg_loss)
            b = min(b, 10.0)
        kelly_full = (p * b - q) / b if b > 0 else 0
        kelly_full = max(0, min(kelly_full, 0.5))
        return kelly_full * self.kelly_fraction, p, self.avg_win, self.avg_loss
kelly_trackers = {idx: KellyCriterion(idx) for idx in INDEX_CONFIG}

# ----------------------------------------------------------------------
# VOLUME PROFILE ENGINE
# ----------------------------------------------------------------------
class VolumeProfileEngine:
    def __init__(self, index_name):
        self.price_volume = deque(maxlen=1000)
        self.ce_price_volume = deque(maxlen=1000)
        self.pe_price_volume = deque(maxlen=1000)
    def update(self, price, volume, option_type=None):
        if option_type is None:
            if price > 0 and volume > 0:
                self.price_volume.append((price, volume))
        elif option_type == "CE":
            if price > 0 and volume > 0:
                self.ce_price_volume.append((price, volume))
        elif option_type == "PE":
            if price > 0 and volume > 0:
                self.pe_price_volume.append((price, volume))
    def analyze(self, current_price, current_volume, option_type=None):
        if option_type is None:
            pv = self.price_volume
        elif option_type == "CE":
            pv = self.ce_price_volume
        elif option_type == "PE":
            pv = self.pe_price_volume
        else:
            pv = self.price_volume
        if not pv:
            return {"vwap": 0.0}
        total_pv = sum(p * v for p, v in pv)
        total_v = sum(v for p, v in pv)
        vwap = total_pv / total_v if total_v > 0 else current_price
        return {"vwap": vwap}
volume_profile_engines = {idx: VolumeProfileEngine(idx) for idx in INDEX_CONFIG}

class CorrelationFilter:
    def __init__(self):
        self.nifty_returns = deque(maxlen=50)
        self.banknifty_returns = deque(maxlen=50)
    def update(self, nifty_price, banknifty_price):
        if nifty_price>0 and banknifty_price>0:
            self.nifty_returns.append(nifty_price)
            self.banknifty_returns.append(banknifty_price)
    def analyze(self, index_name, action):
        return {"beta_adjustment": 1.0, "block_reason": None}
correlation_filter = CorrelationFilter()

# ----------------------------------------------------------------------
# MARKET STATE
# ----------------------------------------------------------------------
def is_market_open():
    now_ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    current = now_ist.time()
    return now_ist.weekday() < 5 and dt_time(9, 10) <= current <= dt_time(15, 35)

def is_expiry_day(index_name):
    config = INDEX_CONFIG.get(index_name)
    if not config:
        return False
    today = datetime.now().strftime("%d%b%Y").upper()
    tokens = INDEX_TOKENS.get(index_name, {})
    expiry = tokens.get("expiry", "")
    return today == expiry

# ----------------------------------------------------------------------
# SIGNAL ENGINE (simplified but functional)
# ----------------------------------------------------------------------
def send_telegram_alert(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=3)
    except Exception as e:
        logger.warning(f"Telegram failed: {type(e).__name__}")

def reset_signal_state(index_name, current_time, exit_reason=""):
    with _signal_state_lock:
        signal_state[index_name].update({
            "action": "HOLD", "entry_price": 0, "stop_loss": 0, "target": 0,
            "lots": 1, "cooldown": current_time + 60, "confidence": 0,
            "highest": 0, "entry_time": 0, "exit_reason": exit_reason
        })
    with _portfolio_state_lock:
        portfolio_state[index_name]["open_positions"] = 0
    save_portfolio_state(index_name)
    with _market_signal_lock:
        market_signal[index_name].update({
            "signal": "EXIT",
            "alert_message": f"EXIT {exit_reason}",
            "exit_reason": exit_reason
        })

def should_exit_market_analysis(index_name, action, prices_spot, ce_prem, pe_prem):
    if len(prices_spot) < 60:
        return False, ""
    with _candle_histories_lock:
        closes = [c["close"] for c in candle_histories[index_name]["1min"]]
    if len(closes) >= 20:
        price_trend = closes[-1] - closes[-10]
        rsi_vals = [calculate_rsi(closes[:i]) for i in range(len(closes)-10, len(closes)+1)]
        if len(rsi_vals) >= 2:
            rsi_trend = rsi_vals[-1] - rsi_vals[-2]
            if "CE" in action and price_trend > 0 and rsi_trend < 0:
                return True, "Bearish divergence"
            if "PE" in action and price_trend < 0 and rsi_trend > 0:
                return True, "Bullish divergence"
    with _latest_ticks_lock:
        vix = latest_ticks["VIX"]["vix"]
    if len(vix_history) >= 10:
        vix_sma = sum(list(vix_history)[-10:]) / 10
        if vix > vix_sma * 1.25:
            return True, f"VIX spike {vix:.1f} vs SMA {vix_sma:.1f}"
    return False, ""

def run_signal_engine_for_index(index_name):
    if not INDEX_CONFIG[index_name].get("active"):
        return

    tokens = INDEX_TOKENS.get(index_name, {})
    if not tokens.get("ce_token") or not tokens.get("pe_token"):
        with _market_signal_lock:
            market_signal[index_name]["alert_message"] = "Option tokens not loaded"
            market_signal[index_name]["signal"] = "WAITING"
        return

    with _candle_histories_lock:
        if len(candle_histories[index_name]["1min"]) < 30:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Building candles ({len(candle_histories[index_name]['1min'])}/30)"
                market_signal[index_name]["signal"] = "WAITING"
            return

    with _market_signal_lock:
        if market_signal[index_name]["signal"] == "EXIT":
            market_signal[index_name]["signal"] = "COOLDOWN"

    now = time.time()

    with _latest_ticks_lock:
        spot = latest_ticks[index_name]["spot_price"]
        if spot <= 0:
            spot = last_known_prices[index_name].get("spot", 0)
        ce_prem = latest_ticks[index_name]["ce_price"]
        pe_prem = latest_ticks[index_name]["pe_price"]
        if ce_prem <= 0:
            ce_prem = last_known_prices[index_name].get("ce", 0)
        if pe_prem <= 0:
            pe_prem = last_known_prices[index_name].get("pe", 0)
        ce_vol = latest_ticks[index_name]["ce_volume"]
        pe_vol = latest_ticks[index_name]["pe_volume"]

    vp_engine = volume_profile_engines[index_name]
    vp_engine.update(spot, ce_vol, option_type=None)
    vp_engine.update(ce_prem, ce_vol, option_type="CE")
    vp_engine.update(pe_prem, pe_vol, option_type="PE")

    if index_name == "NIFTY":
        nifty_price_series.append(spot)
    elif index_name == "BANKNIFTY":
        banknifty_price_series.append(spot)
    if len(nifty_price_series) > 0 and len(banknifty_price_series) > 0:
        correlation_filter.update(list(nifty_price_series)[-1], list(banknifty_price_series)[-1])

    sentiment = compute_sentiment(index_name)
    action = get_signal_from_sentiment(sentiment)
    sentiment_label = get_sentiment_label(sentiment)
    with _market_signal_lock:
        market_signal[index_name]["sentiment_score"] = sentiment

    # Drawdown kill switch
    with _portfolio_state_lock:
        current_equity = portfolio_state[index_name]["equity"]
    peak = daily_drawdown[index_name]["peak_equity"]
    if current_equity > peak:
        daily_drawdown[index_name]["peak_equity"] = current_equity
    drawdown = (peak - current_equity) / peak * 100 if peak > 0 else 0
    daily_drawdown[index_name]["current_drawdown"] = drawdown
    if drawdown >= INDEX_CONFIG[index_name].get("max_daily_drawdown_pct", 3.0):
        with _signal_state_lock:
            if signal_state[index_name]["action"] != "HOLD":
                active = signal_state[index_name]["action"]
                prem = ce_prem if "CE" in active else pe_prem
                if prem > 0:
                    pnl = prem - signal_state[index_name]["entry_price"]
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_state_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        portfolio_state[index_name]["daily_pnl"] += pnl_total
                        portfolio_state[index_name]["total_pnl"] += pnl_total
                        portfolio_state[index_name]["live_pnl"] = 0.0
                    save_portfolio_state(index_name)
                    reset_signal_state(index_name, now, "KILL_SWITCH")
        with _market_signal_lock:
            market_signal[index_name]["alert_message"] = "KILL SWITCH: Max drawdown hit. Trading halted."
            market_signal[index_name]["signal"] = "KILL_SWITCH"
        return

    # Circuit breaker
    if safety_state[index_name]["circuit_breaker"]:
        if now < safety_state[index_name]["circuit_breaker_until"]:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = "Circuit breaker active"
                market_signal[index_name]["signal"] = "CIRCUIT_BREAKER"
            return
        else:
            safety_state[index_name]["circuit_breaker"] = False
            safety_state[index_name]["consecutive_sl"] = 0

    # Existing position handling
    with _signal_state_lock:
        current_action = signal_state[index_name]["action"]
    if current_action != "HOLD":
        active = current_action
        with _latest_ticks_lock:
            fresh_ce = latest_ticks[index_name]["ce_price"]
            fresh_pe = latest_ticks[index_name]["pe_price"]
        prem = fresh_ce if "CE" in active else fresh_pe
        if prem <= 0:
            prem = last_known_prices[index_name].get("ce" if "CE" in active else "pe", 0)
        if prem > 0:
            with _signal_state_lock:
                pnl = prem - signal_state[index_name]["entry_price"]
                with _portfolio_state_lock:
                    portfolio_state[index_name]["live_pnl"] = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                if prem > signal_state[index_name].get("highest", 0):
                    signal_state[index_name]["highest"] = prem
                    with _candle_histories_lock:
                        closes = [c["close"] for c in candle_histories[index_name]["1min"]]
                    if len(closes) >= 14:
                        atr = calculate_atr([], [], closes, 14)
                        new_sl = prem - atr * 1.8
                        if new_sl > signal_state[index_name]["stop_loss"]:
                            signal_state[index_name]["stop_loss"] = new_sl
                if prem <= signal_state[index_name]["stop_loss"]:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_state_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        portfolio_state[index_name]["daily_pnl"] += pnl_total
                        portfolio_state[index_name]["total_pnl"] += pnl_total
                        portfolio_state[index_name]["live_pnl"] = 0.0
                    save_portfolio_state(index_name)
                    pnl_pct = pnl / max(signal_state[index_name]["entry_price"], 1)
                    kelly_trackers[index_name].update(pnl_pct)
                    safety_state[index_name]["consecutive_sl"] += 1
                    if safety_state[index_name]["consecutive_sl"] >= 3:
                        safety_state[index_name]["circuit_breaker"] = True
                        safety_state[index_name]["circuit_breaker_until"] = now + 1800
                        send_telegram_alert(f"CIRCUIT BREAKER {index_name} | 3 consecutive SLs. Trading paused 30 min.")
                    send_telegram_alert(f"EXIT {index_name} | SL | PnL: {pnl:.2f} pts")
                    reset_signal_state(index_name, now, "STOP_LOSS")
                    return
                if prem >= signal_state[index_name]["target"]:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_state_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        portfolio_state[index_name]["daily_pnl"] += pnl_total
                        portfolio_state[index_name]["total_pnl"] += pnl_total
                        portfolio_state[index_name]["live_pnl"] = 0.0
                    save_portfolio_state(index_name)
                    pnl_pct = pnl / max(signal_state[index_name]["entry_price"], 1)
                    kelly_trackers[index_name].update(pnl_pct)
                    safety_state[index_name]["consecutive_sl"] = 0
                    send_telegram_alert(f"EXIT {index_name} | TARGET | PnL: {pnl:.2f} pts")
                    reset_signal_state(index_name, now, "TARGET_HIT")
                    return
                entry_time = signal_state[index_name].get("entry_time", 0)
                if entry_time > 0 and (now - entry_time) / 60 >= 45:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_state_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        portfolio_state[index_name]["daily_pnl"] += pnl_total
                        portfolio_state[index_name]["total_pnl"] += pnl_total
                        portfolio_state[index_name]["live_pnl"] = 0.0
                    save_portfolio_state(index_name)
                    kelly_trackers[index_name].update(pnl / max(signal_state[index_name]["entry_price"], 1))
                    send_telegram_alert(f"EXIT {index_name} | TIME | PnL: {pnl:.2f} pts")
                    reset_signal_state(index_name, now, "TIME_EXIT")
                    return
                with _price_histories_lock:
                    prices_spot = list(price_histories[index_name])
                should_exit, exit_reason = should_exit_market_analysis(index_name, active, prices_spot, fresh_ce, fresh_pe)
                if should_exit:
                    pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                    with _portfolio_state_lock:
                        portfolio_state[index_name]["equity"] += pnl_total
                        portfolio_state[index_name]["daily_pnl"] += pnl_total
                        portfolio_state[index_name]["total_pnl"] += pnl_total
                        portfolio_state[index_name]["live_pnl"] = 0.0
                    save_portfolio_state(index_name)
                    kelly_trackers[index_name].update(pnl / max(signal_state[index_name]["entry_price"], 1))
                    send_telegram_alert(f"EXIT {index_name} | {exit_reason} | PnL: {pnl:.2f} pts")
                    reset_signal_state(index_name, now, exit_reason)
                    return

                if "CE" in active:
                    vwap_data = vp_engine.analyze(ce_prem, ce_vol, option_type="CE")
                else:
                    vwap_data = vp_engine.analyze(pe_prem, pe_vol, option_type="PE")
                option_vwap = vwap_data["vwap"]
                if option_vwap > 0:
                    if "CE" in active and prem < option_vwap * 0.997:
                        pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                        with _portfolio_state_lock:
                            portfolio_state[index_name]["equity"] += pnl_total
                            portfolio_state[index_name]["daily_pnl"] += pnl_total
                            portfolio_state[index_name]["total_pnl"] += pnl_total
                            portfolio_state[index_name]["live_pnl"] = 0.0
                        save_portfolio_state(index_name)
                        kelly_trackers[index_name].update(pnl / max(signal_state[index_name]["entry_price"], 1))
                        send_telegram_alert(f"EXIT {index_name} | VWAP | PnL: {pnl:.2f} pts")
                        reset_signal_state(index_name, now, "VWAP_EXIT")
                        return
                    elif "PE" in active and prem > option_vwap * 1.003:
                        pnl_total = pnl * INDEX_CONFIG[index_name]["lot_size"] * signal_state[index_name]["lots"]
                        with _portfolio_state_lock:
                            portfolio_state[index_name]["equity"] += pnl_total
                            portfolio_state[index_name]["daily_pnl"] += pnl_total
                            portfolio_state[index_name]["total_pnl"] += pnl_total
                            portfolio_state[index_name]["live_pnl"] = 0.0
                        save_portfolio_state(index_name)
                        kelly_trackers[index_name].update(pnl / max(signal_state[index_name]["entry_price"], 1))
                        send_telegram_alert(f"EXIT {index_name} | VWAP | PnL: {pnl:.2f} pts")
                        reset_signal_state(index_name, now, "VWAP_EXIT")
                        return

            with _market_signal_lock:
                market_signal[index_name].update({
                    "alert_message": f"ACTIVE {active}",
                    "signal": "ACTIVE",
                    "entry_price": signal_state[index_name]["entry_price"],
                    "stop_loss": signal_state[index_name]["stop_loss"],
                    "target": signal_state[index_name]["target"],
                    "current_pnl": round(pnl, 2),
                })
        else:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"ACTIVE {active} – premium unavailable"
                market_signal[index_name]["signal"] = "ACTIVE"
        return

    # New entry logic
    with _signal_state_lock:
        if now < signal_state[index_name]["cooldown"]:
            remaining = int(signal_state[index_name]["cooldown"] - now)
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Cooldown {remaining}s"
                market_signal[index_name]["signal"] = "COOLDOWN"
            return

    today = datetime.now().strftime("%Y-%m-%d")
    if last_trade_date[index_name] != today:
        daily_trade_count[index_name] = 0
        last_trade_date[index_name] = today
        daily_drawdown[index_name]["peak_equity"] = portfolio_state[index_name]["equity"]
        with _portfolio_state_lock:
            portfolio_state[index_name]["daily_pnl"] = 0.0
            portfolio_state[index_name]["live_pnl"] = 0.0
        save_portfolio_state(index_name)

    if daily_trade_count[index_name] >= 20:
        with _market_signal_lock:
            market_signal[index_name]["alert_message"] = "Max daily trades"
            market_signal[index_name]["signal"] = "BLOCKED"
        return

    if action == "NO_TRADE":
        with _market_signal_lock:
            market_signal[index_name]["alert_message"] = f"Sentiment {sentiment:.0f} - {sentiment_label}"
            market_signal[index_name]["signal"] = "NO_TRADE"
        return

    side = "CE" if "CE" in action else "PE" if "PE" in action else None
    if side is None:
        with _market_signal_lock:
            market_signal[index_name]["alert_message"] = f"Invalid action {action}"
            market_signal[index_name]["signal"] = "NO_TRADE"
        return
    prem = ce_prem if side == "CE" else pe_prem
    min_prem = INDEX_CONFIG[index_name].get("min_premium", 5)
    if prem <= 0 or prem < min_prem:
        with _market_signal_lock:
            market_signal[index_name]["alert_message"] = f"Premium invalid: Rs{prem}"
            market_signal[index_name]["signal"] = "WAITING"
        return

    buf = signal_buffer[index_name]
    if side == "CE":
        buf["ce_count"] += 1
        buf["pe_count"] = 0
        if buf["ce_count"] < 2:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Building CE ({buf['ce_count']}/2)"
                market_signal[index_name]["signal"] = "BUILDING"
            return
    else:
        buf["pe_count"] += 1
        buf["ce_count"] = 0
        if buf["pe_count"] < 2:
            with _market_signal_lock:
                market_signal[index_name]["alert_message"] = f"Building PE ({buf['pe_count']}/2)"
                market_signal[index_name]["signal"] = "BUILDING"
            return

    with _price_histories_lock:
        prices_spot = list(price_histories[index_name])
    rsi = calculate_rsi(prices_spot[-50:]) if len(prices_spot) >= 50 else 50
    with _candle_histories_lock:
        closes_5min = [c["close"] for c in candle_histories[index_name]["5min"]]
    adx = calculate_adx([], [], closes_5min, 14) if len(closes_5min) >= 30 else 20

    with _latest_ticks_lock:
        vix = latest_ticks["VIX"]["vix"]
        if vix <= 0:
            vix = 15.0

    ml_prob = ml_filter.predict([prem, spot, rsi, adx, vix, sentiment])

    logger.info(f"{index_name} ML={ml_prob:.2f} RSI={rsi:.1f} ADX={adx:.1f} VIX={vix:.1f}")

    if ml_prob < 0.55 and "STRONG" not in action:
        with _market_signal_lock:
            market_signal[index_name]["alert_message"] = f"ML filter: prob {ml_prob:.2f}"
            market_signal[index_name]["signal"] = "BLOCKED"
        return

    kelly_risk, win_rate, avg_win, avg_loss = kelly_trackers[index_name].get_recommended_risk_pct()

    if "STRONG" in action:
        base_risk_pct = 2.0
    elif "LOW" in action:
        base_risk_pct = 0.8
    else:
        base_risk_pct = 1.2
    risk_pct = base_risk_pct * 0.5 + kelly_risk * 0.5
    if vix > 25:
        risk_pct *= 0.7
    elif vix > 20:
        risk_pct *= 0.85
    if adx > 25:
        risk_pct *= 1.2
    elif adx < 15:
        risk_pct *= 0.8
    if is_expiry_day(index_name):
        risk_pct *= 0.5
    risk_pct = max(0.5, min(3.0, risk_pct))

    with _candle_histories_lock:
        closes_1min = [c["close"] for c in candle_histories[index_name]["1min"]]
    atr = calculate_atr([], [], closes_1min, 14)
    if "STRONG" in action:
        sl_pct = 0.45
        target_mult = 3.5
    elif "LOW" in action:
        sl_pct = 0.3
        target_mult = 2.5
    else:
        sl_pct = 0.4
        target_mult = 3.0
    sl = max(prem * (1 - sl_pct), prem - atr * 1.5)
    target = prem + atr * target_mult

    risk_amount = portfolio_state[index_name]["equity"] * (risk_pct / 100)
    stop_dist = prem - sl
    if stop_dist > 0:
        lots = int(risk_amount / (stop_dist * INDEX_CONFIG[index_name]["lot_size"]))
        lots = max(1, min(5, lots))
    else:
        lots = 1

    with _signal_state_lock:
        signal_state[index_name].update({
            "action": action, "entry_price": prem, "stop_loss": sl, "target": target,
            "lots": lots, "entry_time": now, "highest": prem, "cooldown": 0
        })
    with _portfolio_state_lock:
        portfolio_state[index_name]["open_positions"] = 1
    signal_buffer[index_name]["ce_count"] = signal_buffer[index_name]["pe_count"] = 0
    daily_trade_count[index_name] += 1
    save_portfolio_state(index_name)

    emoji = "🔥" if "STRONG" in action and "CE" in action else "❄️" if "STRONG" in action and "PE" in action else "⚡" if "LOW" in action else "📊"
    msg = (f"{emoji} {action} {index_name} | Spot:{spot:.0f} Prem:{prem:.2f} SL:{sl:.2f} Tgt:{target:.2f} | "
           f"Sentiment:{sentiment:.0f} ({sentiment_label}) | Lots:{lots} Risk:{risk_pct:.1f}%")
    send_telegram_alert(msg)
    logger.info(msg)

    with _market_signal_lock:
        market_signal[index_name].update({
            "signal": action,
            "alert_message": f"ENTRY {action}",
            "entry_price": prem,
            "stop_loss": sl,
            "target": target,
            "sentiment_score": sentiment,
            "exit_reason": ""
        })

def run_all_signals():
    for idx in INDEX_CONFIG:
        if INDEX_CONFIG[idx].get("active"):
            try:
                run_signal_engine_for_index(idx)
            except Exception as e:
                logger.error(f"Signal error {idx}: {e}")

# ----------------------------------------------------------------------
# REST-ONLY MODE (with detailed logging)
# ----------------------------------------------------------------------
tick_counter = 0
last_tick_timestamp = time.time()
fetch_cycle_count = 0

def start_rest_only_mode():
    global tick_counter, last_tick_timestamp, fetch_cycle_count
    
    logger.info("=" * 60)
    logger.info("STARTING REST-ONLY MODE")
    logger.info("=" * 60)
    
    auth_obj = None
    auth_time = 0
    
    while True:
        try:
            fetch_cycle_count += 1
            logger.info(f"--- Fetch cycle #{fetch_cycle_count} ---")
            
            if not is_market_open():
                logger.info("Market is closed. Waiting 60 seconds...")
                time.sleep(60)
                continue
            
            logger.info("Market is open. Fetching data...")
            
            # Refresh auth token if needed
            now = time.time()
            if not auth_obj or now - auth_time > 3300:
                logger.info("Getting auth token...")
                _, _, auth_obj = get_auth_token()
                auth_time = now
                if not auth_obj:
                    logger.warning("Failed to get auth token, retrying in 10 seconds...")
                    time.sleep(10)
                    continue
                logger.info("Auth token obtained successfully")
            
            # Refresh tokens if needed
            for idx in INDEX_CONFIG:
                if not INDEX_CONFIG[idx].get("active"):
                    continue
                
                tokens = INDEX_TOKENS.get(idx, {})
                if not tokens.get("ce_token") or not tokens.get("pe_token"):
                    logger.info(f"Getting ATM tokens for {idx}...")
                    get_current_atm_tokens(idx)
                    continue
                
                try:
                    # Get spot price
                    logger.debug(f"Fetching spot for {idx}...")
                    spot = get_index_spot(idx)
                    if spot and spot > 0:
                        with _latest_ticks_lock:
                            latest_ticks[idx]["spot_price"] = spot
                        with _price_histories_lock:
                            price_histories[idx].append(spot)
                        update_candle(idx, spot, 0, time.time())
                        with _latest_ticks_lock:
                            last_known_prices[idx]["spot"] = spot
                            last_known_prices[idx]["timestamp"] = time.time()
                        tick_counter += 1
                        last_tick_timestamp = time.time()
                        logger.info(f"{idx} Spot: {spot:.2f}")
                    else:
                        logger.debug(f"{idx} Spot: No data")
                    
                    # Get CE price
                    if tokens.get("ce_token") and tokens.get("ce_symbol"):
                        logger.debug(f"Fetching CE for {idx}...")
                        ce_resp = auth_obj.ltpData(
                            INDEX_CONFIG[idx]["option_exchange"], 
                            tokens["ce_symbol"], 
                            tokens["ce_token"]
                        )
                        ce_price = safe_ltp(ce_resp)
                        if ce_price and ce_price > 0:
                            if ce_price > 100000:
                                ce_price /= 100
                            with _latest_ticks_lock:
                                latest_ticks[idx]["ce_price"] = ce_price
                            with _ce_price_histories_lock:
                                ce_price_histories[idx].append(ce_price)
                            volume_profile_engines[idx].update(ce_price, 0, option_type="CE")
                            with _latest_ticks_lock:
                                last_known_prices[idx]["ce"] = ce_price
                            logger.info(f"{idx} CE: {ce_price:.2f}")
                        else:
                            logger.debug(f"{idx} CE: No data")
                    
                    # Get PE price
                    if tokens.get("pe_token") and tokens.get("pe_symbol"):
                        logger.debug(f"Fetching PE for {idx}...")
                        pe_resp = auth_obj.ltpData(
                            INDEX_CONFIG[idx]["option_exchange"], 
                            tokens["pe_symbol"], 
                            tokens["pe_token"]
                        )
                        pe_price = safe_ltp(pe_resp)
                        if pe_price and pe_price > 0:
                            if pe_price > 100000:
                                pe_price /= 100
                            with _latest_ticks_lock:
                                latest_ticks[idx]["pe_price"] = pe_price
                            with _pe_price_histories_lock:
                                pe_price_histories[idx].append(pe_price)
                            volume_profile_engines[idx].update(pe_price, 0, option_type="PE")
                            with _latest_ticks_lock:
                                last_known_prices[idx]["pe"] = pe_price
                            logger.info(f"{idx} PE: {pe_price:.2f}")
                        else:
                            logger.debug(f"{idx} PE: No data")
                            
                except Exception as e:
                    logger.error(f"REST fetch error for {idx}: {type(e).__name__} - {str(e)}")
            
            # Run signal engine
            logger.info("Running signal engine...")
            run_all_signals()
            logger.info("Signal engine completed")
            
            # Sleep before next cycle (5 seconds)
            logger.info("Sleeping 5 seconds...")
            time.sleep(5)
            
        except Exception as e:
            logger.error(f"REST-only mode error: {type(e).__name__} - {str(e)}")
            time.sleep(10)

# ----------------------------------------------------------------------
# BACKGROUND THREADS
# ----------------------------------------------------------------------
_init_completed = False
_init_lock = threading.Lock()

def _start_background_threads():
    global _init_completed
    with _init_lock:
        if not _init_completed:
            logger.info("=" * 60)
            logger.info("Starting background threads...")
            logger.info("=" * 60)
            
            # Pre-fetch tokens
            logger.info("Pre-fetching option tokens...")
            for attempt in range(5):
                refresh_all_tokens()
                ready = sum(1 for idx, cfg in INDEX_CONFIG.items()
                           if cfg.get("active") and INDEX_TOKENS[idx].get("ce_token"))
                total_active = sum(1 for cfg in INDEX_CONFIG.values() if cfg.get("active"))
                logger.info(f"Token prefetch attempt {attempt + 1}: {ready}/{total_active} indices ready")
                if ready == total_active:
                    break
                time.sleep((attempt + 1) * 2)
            
            # Start REST-only mode thread
            logger.info("Starting REST-only mode thread...")
            rest_thread = threading.Thread(target=start_rest_only_mode, daemon=True)
            rest_thread.start()
            logger.info(f"REST-only mode thread started (daemon={rest_thread.daemon})")
            
            _init_completed = True
            logger.info("Background threads initialization complete")

# ----------------------------------------------------------------------
# FLASK ROUTES
# ----------------------------------------------------------------------
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
        "engine": "Multi-Index Options Bot v14.1 (REST-only mode)",
        "indices": [i for i, cfg in INDEX_CONFIG.items() if cfg.get("active")],
        "market_open": is_market_open(),
        "mode": "REST_ONLY",
        "ticks_received": tick_counter,
        "fetch_cycles": fetch_cycle_count
    })

@app.route("/api/live-signals", methods=["GET"])
def live_signals():
    sentiment_data = {}
    for idx in INDEX_CONFIG:
        if not INDEX_CONFIG[idx].get("active"):
            continue
        with _market_signal_lock:
            sentiment_data[idx] = {
                "score": market_signal[idx].get("sentiment_score", 50),
                "label": get_sentiment_label(market_signal[idx].get("sentiment_score", 50))
            }
    with _market_signal_lock, _portfolio_state_lock:
        return jsonify({
            "timestamp": datetime.now().isoformat(),
            "signals": market_signal,
            "sentiment": sentiment_data,
            "portfolios": portfolio_state,
            "market_open": is_market_open(),
            "debug": {
                "ws_running": False,
                "mode": "REST_ONLY",
                "ticks": tick_counter,
                "fetch_cycles": fetch_cycle_count,
                "last_tick_ago": round(time.time() - last_tick_timestamp, 1)
            },
            "version": "14.1"
        })

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({
        "status": "OK" if (time.time() - last_tick_timestamp < 30) else "DEGRADED",
        "ws_running": False,
        "mode": "REST_ONLY",
        "ticks": tick_counter,
        "fetch_cycles": fetch_cycle_count,
        "last_tick_seconds_ago": round(time.time() - last_tick_timestamp, 2)
    })

@app.route("/api/connection-status", methods=["GET"])
def connection_status():
    return jsonify({
        "websocket_running": False,
        "mode": "REST_ONLY",
        "fetch_cycles": fetch_cycle_count,
        "last_tick_seconds_ago": round(time.time() - last_tick_timestamp, 2),
        "total_ticks_received": tick_counter,
        "market_open": is_market_open(),
        "connection_mode": "REST_ONLY",
        "active_indices": [idx for idx, cfg in INDEX_CONFIG.items() if cfg.get("active")],
        "tokens_loaded": {idx: {
            "ce_token": bool(INDEX_TOKENS[idx].get("ce_token")),
            "pe_token": bool(INDEX_TOKENS[idx].get("pe_token"))
        } for idx in INDEX_CONFIG}
    })

# ----------------------------------------------------------------------
# AUTO-START BACKGROUND THREADS (for Gunicorn)
# ----------------------------------------------------------------------
# This runs when Gunicorn imports the module
logger.info("=" * 60)
logger.info("AUTO-STARTING BACKGROUND THREADS (Gunicorn mode)")
logger.info("=" * 60)

# Initialize database and load state
init_db()
load_portfolio_state()

# Start background threads
_start_background_threads()

logger.info("=" * 60)
logger.info("Background threads started. Flask server ready.")
logger.info("=" * 60)

# ----------------------------------------------------------------------
# MAIN ENTRY POINT (for direct execution)
# ----------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("INITIALIZING BOT v14.1 (Direct execution)")
    logger.info("=" * 60)
    
    init_db()
    load_portfolio_state()
    _start_background_threads()
    
    logger.info("=" * 60)
    logger.info("Starting Flask server...")
    logger.info("=" * 60)
    
    app.run(host="0.0.0.0", port=10000, debug=False)
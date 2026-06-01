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
SPOT_TOKEN = "99926000"  # Nifty 50 Index Institutional Token Identifier
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

timeframe_history = {tf: deque(maxlen=50) for tf in ["1min", "5min", "10min", "15min", "20min"]}
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
    "cooldown_until": 0
}

portfolio_state = {
    "equity": 100000.0, "initial_equity": 100000.0, "daily_pnl": 0.0,
    "max_drawdown_today": 0.0, "open_positions": 0, "daily_peak": 100000.0,
    "daily_loss_limit_pct": 2.0, "var_95": 0.0, "sharpe_ratio": 0.0
}

market_signal = {
    "signal": "WAITING", "spot_price": 0.0, "ce_price": 0.0, "pe_price": 0.0,
    "spot_rsi": 50.0, "spot_macd": 0.0, "pcr": 1.0, "spot_atr": 0.0,
    "regime": "RANGING", "confidence": 50.0, "timestamp": ""
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

CONFIG = {
    "SPOT_RSI_PERIOD": 14,
    "SPOT_MACD_FAST": 12,
    "SPOT_MACD_SLOW": 26,
    "SPOT_MACD_SIGNAL": 9,
    "SPOT_ATR_PERIOD": 14,
    "BULLISH_TREND_RSI": 53,
    "BEARISH_TREND_RSI": 47,
    "CONSIDER_THRESHOLD": 52,
    "ENTRY_ATR_MULT": 1.5,
    "TRAILING_ATR_MULT": 1.8,
    "TARGET_ATR_MULT": 4.0,
    "COOLDOWN_SEC": 120,
}

# ============================================================
# TECHNICAL ANALYSIS ENGINE MATHEMATICS
# ============================================================
def calculate_rsi(prices, period=14):
    if len(prices) < period + 1: return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0: return 100.0
    return 100 - (100 / (1 + avg_gain / avg_loss))

def calculate_macd(prices, fast=12, slow=26):
    if len(prices) < slow: return 0.0, 0.0
    def ema(arr, p):
        alpha = 2 / (p + 1)
        val = arr[0]
        for x in arr[1:]: val = alpha * x + (1 - alpha) * val
        return val
    ema_f = ema(prices[-fast:], fast)
    ema_s = ema(prices[-slow:], slow)
    return (ema_f - ema_s), (ema_f - ema_s)

def calculate_atr(prices, period=14):
    if len(prices) < period + 1: return 5.0
    tr = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    return sum(tr[-period:]) / period

# ============================================================
# RISK ENVIRONMENT MATRICES
# ============================================================
class RiskManager:
    def check_daily_loss_limit(self):
        loss_pct = (portfolio_state["initial_equity"] - portfolio_state["equity"]) / portfolio_state["initial_equity"] * 100
        return loss_pct >= portfolio_state["daily_loss_limit_pct"]

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
# PROFESSIONAL DISCIPLINED SIGNAL EXECUTION ENGINE
# ============================================================
def run_signal_engine():
    global market_signal, market_state, signal_state, portfolio_state, latest_ticks
    
    if risk_manager.check_daily_loss_limit():
        market_state["action"] = "HALTED_LOSS_LIMIT"
        return

    spot_history_list = list(spot_price_history)
    if len(spot_history_list) < 30: return

    # 1. Base Nifty Spot Trend Calculations
    spot_rsi = calculate_rsi(spot_history_list, CONFIG["SPOT_RSI_PERIOD"])
    spot_macd, macd_signal = calculate_macd(spot_history_list, CONFIG["SPOT_MACD_FAST"], CONFIG["SPOT_MACD_SLOW"])
    spot_atr = calculate_atr(spot_history_list, CONFIG["SPOT_ATR_PERIOD"])
    pcr = get_nifty_pcr()

    current_time = time.time()
    
    # --------------------------------------------------------
    # STATE A: ACTIVE POSITION MANAGEMENT WORKFLOW (TRAILING ENGINE)
    # --------------------------------------------------------
    if signal_state["current_action"] != "HOLD":
        active_side = signal_state["current_action"]
        current_premium = latest_ticks["ce_price"] if active_side == "BUY_CE" else latest_ticks["pe_price"]
        
        if current_premium == 0: return 

        if current_premium > signal_state["highest_premium_seen"]:
            signal_state["highest_premium_seen"] = current_premium
            new_sl = current_premium - (spot_atr * CONFIG["TRAILING_ATR_MULT"])
            if new_sl > signal_state["stop_loss"]:
                signal_state["stop_loss"] = new_sl

        # Exit Check 1: Trailing Stop Loss Breached
        if current_premium <= signal_state["stop_loss"]:
            pnl_points = current_premium - signal_state["entry_price"]
            portfolio_state["equity"] += (pnl_points * 50)
            send_telegram_alert(f"📉 <b>TRAILING SL HIT:</b> Out of {active_side} @ {current_premium} | PnL: {pnl_points:.2f} pts")
            reset_signal_state(current_time)
            return

        # Exit Check 2: Hard Target Taken
        if current_premium >= signal_state["target"]:
            pnl_points = current_premium - signal_state["entry_price"]
            portfolio_state["equity"] += (pnl_points * 50)
            send_telegram_alert(f"🎯 <b>TARGET ACHIEVED:</b> Profit booked on {active_side} @ {current_premium} | PnL: {pnl_points:.2f} pts")
            reset_signal_state(current_time)
            return

        # Exit Check 3: Institutional Momentum Structural Trend Reversal Signal
        if active_side == "BUY_CE" and (spot_rsi < 45 or spot_macd < macd_signal):
            pnl_points = current_premium - signal_state["entry_price"]
            portfolio_state["equity"] += (pnl_points * 50)
            send_telegram_alert(f"🔄 <b>MOMENTUM EXIT:</b> Bullish trend softening. Liquidating CE @ {current_premium} | PnL: {pnl_points:.2f} pts")
            reset_signal_state(current_time)
            return
            
        if active_side == "BUY_PE" and (spot_rsi > 55 or spot_macd > macd_signal):
            pnl_points = current_premium - signal_state["entry_price"]
            portfolio_state["equity"] += (pnl_points * 50)
            send_telegram_alert(f"🔄 <b>MOMENTUM EXIT:</b> Bearish trend softening. Liquidating PE @ {current_premium} | PnL: {pnl_points:.2f} pts")
            reset_signal_state(current_time)
            return

    # --------------------------------------------------------
    # STATE B: POSITION DISCOVERY WORKFLOW (SCANNER CORE)
    # --------------------------------------------------------
    else:
        if current_time < signal_state["cooldown_until"]: return

        # Strategy Trigger 1: Spot Bullish Breakout or Slow Grinding Uptrend Verified
        if spot_rsi >= CONFIG["BULLISH_TREND_RSI"] and spot_macd > macd_signal:
            ce_premium = latest_ticks["ce_price"]
            if ce_premium == 0: return
            
            signal_state.update({
                "current_action": "BUY_CE",
                "entry_price": ce_premium,
                "highest_premium_seen": ce_premium,
                "stop_loss": ce_premium - (spot_atr * CONFIG["ENTRY_ATR_MULT"]),
                "target": ce_premium + (spot_atr * CONFIG["TARGET_ATR_MULT"]),
                "signal_grade": "A" if spot_rsi > 60 else "B",
                "confidence": float(spot_rsi)
            })
            portfolio_state["open_positions"] = 1
            send_telegram_alert(f"🚀 <b>SIGNAL INITIATED:</b> BUY NIFTY ATM CALL (CE) @ {ce_premium} | Spot: {latest_ticks['spot_price']} | SL: {signal_state['stop_loss']:.2f}")

        # Strategy Trigger 2: Spot Bearish Breakdown or Slow Grinding Downtrend Verified
        elif spot_rsi <= CONFIG["BEARISH_TREND_RSI"] and spot_macd < macd_signal:
            pe_premium = latest_ticks["pe_price"]
            if pe_premium == 0: return
            
            signal_state.update({
                "current_action": "BUY_PE",
                "entry_price": pe_premium,
                "highest_premium_seen": pe_premium,
                "stop_loss": pe_premium - (spot_atr * CONFIG["ENTRY_ATR_MULT"]),
                "target": pe_premium + (spot_atr * CONFIG["TARGET_ATR_MULT"]),
                "signal_grade": "A" if spot_rsi < 40 else "B",
                "confidence": float(100 - spot_rsi)
            })
            portfolio_state["open_positions"] = 1
            send_telegram_alert(f"🩸 <b>SIGNAL INITIATED:</b> BUY NIFTY ATM PUT (PE) @ {pe_premium} | Spot: {latest_ticks['spot_price']} | SL: {signal_state['stop_loss']:.2f}")

    # Global Reporting Layer Synchronization
    market_signal.update({
        "spot_price": latest_ticks["spot_price"], "ce_price": latest_ticks["ce_price"], "pe_price": latest_ticks["pe_price"],
        "spot_rsi": round(spot_rsi, 2), "spot_macd": round(spot_macd, 4), "spot_atr": round(spot_atr, 2),
        "signal": signal_state["current_action"], "confidence": round(signal_state["confidence"], 2), "timestamp": datetime.now().isoformat()
    })

def reset_signal_state(current_time):
    global signal_state, portfolio_state
    signal_state.update({
        "current_action": "HOLD", "entry_price": 0.0, "stop_loss": 0.0,
        "target": 0.0, "highest_premium_seen": 0.0, "confidence": 0.0,
        "cooldown_until": current_time + CONFIG["COOLDOWN_SEC"]
    })
    portfolio_state["open_positions"] = 0

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
# WEBSOCKET SUBSCRIPTION STREAM DATA INTERFACES (SmartWebSocketV2)
# ============================================================
def on_ws_open(wsapp, open_message):
    global sws
    logger.info(f"WebSocket opened: {open_message}")
    if sws and CE_TOKEN and PE_TOKEN:
        try:
            # SmartWebSocketV2 subscribe signature: subscribe(correlation_id, mode, tokens)
            # mode 1 = LTP, 2 = Quote, 3 = SnapQuote
            subscription_payload = [
                {"exchangeType": 1, "tokens": [SPOT_TOKEN]},      # NSE Spot
                {"exchangeType": 2, "tokens": [CE_TOKEN, PE_TOKEN]}  # NFO Options
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

def on_ws_data(wsapp, message):
    global tick_counter, last_tick_time, latest_ticks, spot_price_history, ce_price_history, pe_price_history
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
            
            # Auto-correct float notation anomalies transmitted across NFO segments
            if isinstance(ltp, (int, float)) and ltp > 50000 and token != SPOT_TOKEN:
                ltp = ltp / 100
                
            vol = tick.get("v") or tick.get("volume_trade_for_the_day", 0)
            oi = tick.get("oi") or tick.get("open_interest", 0)

            if token == SPOT_TOKEN:
                latest_ticks["spot_price"] = ltp
                spot_price_history.append(ltp)
                tick_counter += 1
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

            # Engine cycle firing block mapped to processing density
            if tick_counter % 3 == 0:
                run_signal_engine()
                
    except Exception as e: 
        logger.error(f"Callback data parser exception: {e}")

# ============================================================
# SUPERVISOR BACKGROUND LIFECYCLE DAEMON (FIXED)
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
            
            if not CE_TOKEN or not PE_TOKEN:
                logger.info("Option tokens not resolved yet. Executing token lookup sequence...")
                get_current_atm_tokens()
                
            if not CE_TOKEN or not PE_TOKEN:
                logger.error("Could not resolve option tokens. Retrying in 10s...")
                time.sleep(10)
                continue
            
            logger.info(f"Initializing SmartWebSocketV2 for ATM Strike {ATM_STRIKE}")
            
            # SmartWebSocketV2: auth_token, feed_token, client_code
            sws = SmartWebSocketV2(auth_token, feed_token, ANGEL_CLIENT_ID)
            
            # Assign callbacks
            sws.on_open = on_ws_open
            sws.on_data = on_ws_data
            sws.on_error = on_ws_error
            sws.on_close = on_ws_close
            
            ws_running = True
            
            # Connect starts the blocking loop
            sws.connect()
            
        except Exception as e:
            logger.error(f"Critical error in supervisor daemon loop: {str(e)}")
            ws_running = False
            sws = None
            time.sleep(10)

# ============================================================
# INITIALIZATION RUNNER HOOK
# ============================================================
def init_background_threads():
    """
    Guarantees the background streaming daemon runs reliably 
    under Gunicorn worker process topologies.
    """
    logger.info("Initializing framework-bound background pipelines...")
    ws_thread = threading.Thread(target=start_angel_websocket, daemon=True)
    ws_thread.start()
    logger.info("Background streaming thread successfully bound and deployed.")

# Flag to prevent multiple invocations if Gunicorn reloads contexts internally
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
        "status": "healthy", "engine": "Institutional Trend-Following Engine Core",
        "market_open": is_market_open(), "timestamp": time.time()
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
        "tokens": {"atm_strike": ATM_STRIKE, "expiry": EXPIRY_DATE, "ce_token": CE_TOKEN, "pe_token": PE_TOKEN}
    }), 200

@app.route("/api/health", methods=["GET"])
def health_check():
    return jsonify({"status": "OK", "alive": True}), 200

if __name__ == "__main__":
    # Local development fallback
    if not _init_completed:
        init_background_threads()
        _init_completed = True
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
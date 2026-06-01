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
import queue
import uuid
from collections import deque
from datetime import datetime, timedelta, time as dt_time
from flask import Flask, jsonify, request
from flask_cors import CORS
import pyotp

# ============================================================
# INITIALIZATION, LOGGING & DEPENDENCIES
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app

try:
    from sklearn.ensemble import RandomForestClassifier
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logger.warning("Scikit-learn not available. ML features disabled.")

try:
    import telebot
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False
    logger.warning("Telegram not available. Alert features disabled.")

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
    """Initialize database with all required tables"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Existing tables
    c.execute("""CREATE TABLE IF NOT EXISTS ticks
                 (timestamp REAL, token TEXT, price REAL, volume REAL, 
                  bid REAL, ask REAL, oi REAL, id TEXT PRIMARY KEY)""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ticks_time ON ticks(timestamp)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ticks_token ON ticks(token)")
    
    c.execute("""CREATE TABLE IF NOT EXISTS signals
                 (timestamp REAL, action TEXT, signal_type TEXT, grade TEXT, 
                  confidence REAL, ce_price REAL, pe_price REAL, rsi REAL, 
                  pcr REAL, regime TEXT, id TEXT PRIMARY KEY)""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_signals_time ON signals(timestamp)")
    
    c.execute("""CREATE TABLE IF NOT EXISTS trades
                 (timestamp REAL, action TEXT, entry_price REAL, exit_price REAL, 
                  pnl REAL, size_pct REAL, status TEXT, grade TEXT, 
                  exit_reason TEXT, id TEXT PRIMARY KEY)""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(timestamp)")
    
    c.execute("""CREATE TABLE IF NOT EXISTS daily_performance
                 (date TEXT PRIMARY KEY, equity REAL, daily_pnl REAL, 
                  drawdown_pct REAL, sharpe REAL, var REAL, trades_count INTEGER)""")
    
    c.execute("""CREATE TABLE IF NOT EXISTS ml_models
                 (id INTEGER PRIMARY KEY, model BLOB, created_at REAL, 
                  features TEXT, version TEXT)""")
    
    # New tables for enhanced features
    c.execute("""CREATE TABLE IF NOT EXISTS metrics
                 (timestamp REAL, metric_name TEXT, metric_value REAL,
                  tags TEXT, id TEXT PRIMARY KEY)""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_metrics_name_time ON metrics(metric_name, timestamp)")
    
    c.execute("""CREATE TABLE IF NOT EXISTS market_regimes
                 (timestamp REAL, regime TEXT, confidence REAL,
                  volatility REAL, trend_strength REAL, id TEXT PRIMARY KEY)""")
    
    c.execute("""CREATE TABLE IF NOT EXISTS strategy_performance
                 (date TEXT, strategy_name TEXT, pnl REAL, trades_count INTEGER,
                  win_rate REAL, avg_win REAL, avg_loss REAL, sharpe REAL,
                  PRIMARY KEY (date, strategy_name))""")
    
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
        token_int = int.from_bytes(token_bytes, byteorder="little")
        result["token"] = str(token_int)
        ltp = int.from_bytes(binary_data[26:34], byteorder="little") / 100
        result["ltp"] = ltp
        volume = int.from_bytes(binary_data[34:42], byteorder="little")
        result["v"] = volume
    except Exception as e:
        logger.error(f"Binary parse error: {e}")
    
    return result

SmartWebSocketV2._parse_binary_data = _patched_parse

_original_on_close = SmartWebSocketV2._on_close

def _patched_on_close(self, wsapp, *args):
    try:
        _original_on_close(self, wsapp)
    except:
        pass

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

# Price and volume histories
spot_price_history = deque(maxlen=1000)
ce_price_history = deque(maxlen=1000)
pe_price_history = deque(maxlen=1000)
ce_volume_history = deque(maxlen=1000)
pe_volume_history = deque(maxlen=1000)
ce_oi_history = deque(maxlen=50)
pe_oi_history = deque(maxlen=50)

# Timeframe configurations
TIMEFRAMES = ["1min", "2min", "3min", "5min", "10min", "15min", "20min"]
timeframe_history = {tf: deque(maxlen=50) for tf in TIMEFRAMES}
last_timeframe_update = {tf: 0 for tf in TIMEFRAMES}
timeframe_candles = {
    tf: {"open": 0, "high": 0, "low": float("inf"), "close": 0, "active": False}
    for tf in TIMEFRAMES
}

# Latest market data
latest_ticks = {
    "spot_price": 0.0, "ce_price": 0.0, "pe_price": 0.0,
    "ce_volume": 0, "pe_volume": 0, "ce_oi": 0, "pe_oi": 0,
    "ce_bid": 0.0, "ce_ask": 0.0, "pe_bid": 0.0, "pe_ask": 0.0
}

# Tick processing queue (for async processing)
tick_queue = queue.Queue(maxsize=10000)
tick_counter = 0
ws_running = False
sws = None
last_tick_time = time.time()
engine_active = True
last_minute_snapshot = {"time": 0, "price": 0}

# Signal and trading state
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
    "max_profit_seen": 0.0,
    "trade_id": None
}

portfolio_state = {
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
    "total_pnl": 0.0
}

# Market analysis state
market_signal = {
    "signal": "WAITING", 
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
    "signal_strength": "NONE",
    "regime_confidence": 0.0,
    "volatility_regime": "NORMAL"
}

market_state = {
    "rsi": 50, 
    "momentum": "NEUTRAL", 
    "strength": "LOW", 
    "trend": "SIDEWAYS",
    "action": "HOLD", 
    "confidence": 0, 
    "volatility": "NORMAL", 
    "regime": "UNKNOWN",
    "adx": 20,
    "macd_hist": 0
}

institutional_state = {
    "vwap": 0.0, 
    "ema_fast": 0.0, 
    "ema_slow": 0.0, 
    "atr": 0.0,
    "delta": 0.0, 
    "gamma": 0.0, 
    "theta": 0.0, 
    "vega": 0.0, 
    "iv": 0.20
}

# Cache and buffers
pcr_cache = {"value": 1.0, "time": 0}
spot_cache = {"value": None, "timestamp": 0}
CACHE_TTL = 15

# Metrics collection
metrics_buffer = deque(maxlen=1000)
reconnection_attempts = 0
last_reconnection_time = 0

# ============================================================
# ENHANCED CONFIGURATION - PROFESSIONAL GRADE
# ============================================================
CONFIG = {
    # Technical indicators
    "SPOT_RSI_PERIOD": 14,
    "SPOT_RSI_SMOOTHING": 3,
    "SPOT_MACD_FAST": 12,
    "SPOT_MACD_SLOW": 26,
    "SPOT_MACD_SIGNAL": 9,
    "SPOT_ATR_PERIOD": 14,
    "ATR_SMOOTHING": True,
    
    # Signal thresholds
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
    
    # Risk management
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
    "MAX_DD_PCT": 5.0,
    "RISK_PER_TRADE_PCT": 2.0,
    
    # WebSocket and connection
    "WS_RECONNECT_DELAY_BASE": 5,
    "WS_RECONNECT_DELAY_MAX": 60,
    "WS_HEARTBEAT_INTERVAL": 30,
    "TICK_PROCESSING_TIMEOUT": 0.1,
    
    # Machine Learning
    "ML_UPDATE_INTERVAL": 3600,  # Update ML model every hour
    "ML_CONFIDENCE_THRESHOLD": 0.65,
    "ML_LOOKBACK_PERIODS": 100,
    
    # Performance monitoring
    "METRICS_COLLECTION_INTERVAL": 60,
    "PERFORMANCE_SNAPSHOT_INTERVAL": 300
}

# Signal buffer for confirmation
signal_buffer = {
    "ce_count": 0, 
    "pe_count": 0,
    "last_signal_time": 0, 
    "consecutive_ce": 0, 
    "consecutive_pe": 0,
    "signal_queue": deque(maxlen=10)
}

# Daily trade tracking
daily_trade_count = 0
last_trade_date = ""

# ============================================================
# ENHANCED TECHNICAL ANALYSIS ENGINE
# ============================================================
def calculate_rsi(prices, period=14, smoothing=3):
    """Calculate RSI with optional smoothing"""
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

def calculate_macd(prices, fast=12, slow=26, signal_period=9):
    """Calculate MACD indicators"""
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
    
    if macd_history:
        signal_line = ema(macd_history, signal_period)
    else:
        signal_line = macd_line
    
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def calculate_atr(prices, period=14):
    """Calculate Average True Range"""
    if len(prices) < period + 1:
        return 5.0
    
    tr_list = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    
    if CONFIG.get("ATR_SMOOTHING", True) and len(tr_list) >= period:
        atr = sum(tr_list[:period]) / period
        for tr in tr_list[period:]:
            atr = ((period - 1) * atr + tr) / period
        return atr
    
    return sum(tr_list[-period:]) / period

def calculate_adx(prices, period=14):
    """Calculate ADX for trend strength"""
    if len(prices) < period * 2 + 1:
        return 20.0
    
    plus_dm = []
    minus_dm = []
    tr_list = []
    
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

def calculate_ema(prices, period):
    """Calculate Exponential Moving Average"""
    if len(prices) < period:
        return sum(prices) / len(prices) if prices else 0
    
    alpha = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = alpha * p + (1 - alpha) * ema
    return ema

def calculate_vwap(prices, volumes):
    """Calculate Volume Weighted Average Price"""
    if not prices or not volumes or len(prices) != len(volumes):
        return 0.0
    
    typical_prices = prices  # Using close prices as typical price
    vwap = sum(p * v for p, v in zip(typical_prices, volumes)) / sum(volumes) if sum(volumes) > 0 else 0
    return vwap

def calculate_bollinger_bands(prices, period=20, num_std=2):
    """Calculate Bollinger Bands"""
    if len(prices) < period:
        return 0, 0, 0
    
    sma = sum(prices[-period:]) / period
    variance = sum((p - sma) ** 2 for p in prices[-period:]) / period
    std_dev = math.sqrt(variance)
    
    upper_band = sma + (std_dev * num_std)
    lower_band = sma - (std_dev * num_std)
    
    return upper_band, sma, lower_band

# ============================================================
# ENHANCED RISK MANAGEMENT
# ============================================================
class RiskManager:
    def __init__(self):
        self.daily_loss_limit_reached = False
        self.var_history = deque(maxlen=252)
        self.position_sizing_model = "adaptive"
        
    def check_daily_loss_limit(self):
        """Check if daily loss limit has been reached"""
        loss_pct = (portfolio_state["initial_equity"] - portfolio_state["equity"]) / portfolio_state["initial_equity"] * 100
        limit_reached = loss_pct >= portfolio_state["daily_loss_limit_pct"]
        
        if limit_reached and not self.daily_loss_limit_reached:
            self.daily_loss_limit_reached = True
            send_telegram_alert(f"🚨 DAILY LOSS LIMIT REACHED: {loss_pct:.2f}%")
            
        return limit_reached
    
    def check_max_daily_trades(self):
        """Check if max daily trades limit has been reached"""
        global daily_trade_count, last_trade_date
        today = datetime.now().strftime("%Y-%m-%d")
        if today != last_trade_date:
            daily_trade_count = 0
            last_trade_date = today
            self.daily_loss_limit_reached = False
        
        return daily_trade_count >= CONFIG["MAX_DAILY_TRADES"]
    
    def calculate_position_size(self, signal_confidence, atr, account_equity):
        """Calculate optimal position size based on risk parameters"""
        base_risk_pct = CONFIG["RISK_PER_TRADE_PCT"]
        
        # Adjust position size based on confidence
        confidence_multiplier = signal_confidence / 50.0
        confidence_multiplier = max(0.5, min(1.5, confidence_multiplier))
        
        # Adjust based on market volatility
        volatility_regime = market_signal.get("volatility_regime", "NORMAL")
        volatility_multiplier = {
            "LOW": 1.2,
            "NORMAL": 1.0,
            "HIGH": 0.7,
            "EXTREME": 0.5
        }.get(volatility_regime, 1.0)
        
        # Calculate risk amount
        risk_amount = account_equity * (base_risk_pct / 100) * confidence_multiplier * volatility_multiplier
        
        # Calculate position size based on ATR
        if atr > 0:
            position_size = risk_amount / atr
        else:
            position_size = risk_amount / 50  # Default lot size
        
        # Cap position size
        max_position = account_equity * 0.1 / 50  # Max 10% of equity
        position_size = min(position_size, max_position)
        
        return max(1, int(position_size))
    
    def calculate_var(self, returns, confidence_level=0.95):
        """Calculate Value at Risk"""
        if len(returns) < 2:
            return 0.0
        
        sorted_returns = sorted(returns)
        index = int((1 - confidence_level) * len(sorted_returns))
        return abs(sorted_returns[index])
    
    def update_var_metrics(self):
        """Update VaR and Sharpe ratio"""
        if len(portfolio_state.get("returns_history", [])) > 1:
            returns = list(portfolio_state["returns_history"])
            self.var_95 = self.calculate_var(returns, 0.95)
            portfolio_state["var_95"] = self.var_95
            
            # Calculate Sharpe ratio
            if len(returns) > 1:
                avg_return = np.mean(returns)
                std_return = np.std(returns)
                if std_return > 0:
                    sharpe = (avg_return * 252) / (std_return * np.sqrt(252))
                    portfolio_state["sharpe_ratio"] = sharpe

risk_manager = RiskManager()

# ============================================================
# ENHANCED TELEGRAM ALERTS
# ============================================================
def send_telegram_alert(message, parse_mode="HTML"):
    """Send formatted alert to Telegram with retry logic"""
    if not TELEGRAM_AVAILABLE or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    
    for attempt in range(3):
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            response = requests.post(
                url, 
                json={
                    "chat_id": TELEGRAM_CHAT_ID, 
                    "text": message, 
                    "parse_mode": parse_mode
                }, 
                timeout=5
            )
            if response.status_code == 200:
                return True
            else:
                logger.warning(f"Telegram attempt {attempt + 1} failed: {response.status_code}")
                time.sleep(1)
        except Exception as e:
            logger.error(f"Telegram attempt {attempt + 1} failed: {e}")
            time.sleep(1)
    
    return False

def send_performance_update():
    """Send daily performance summary"""
    if datetime.now().hour == 15 and datetime.now().minute < 30:  # After market close
        total_trades = portfolio_state["total_trades"]
        win_rate = (portfolio_state["winning_trades"] / total_trades * 100) if total_trades > 0 else 0
        total_pnl = portfolio_state["total_pnl"]
        
        message = (f"📊 <b>Daily Performance Summary</b>\n"
                  f"Trades: {total_trades}\n"
                  f"Win Rate: {win_rate:.1f}%\n"
                  f"Total P&L: ₹{total_pnl:,.2f}\n"
                  f"Sharpe: {portfolio_state['sharpe_ratio']:.2f}\n"
                  f"Max DD: {portfolio_state['max_drawdown_today']:.2f}%")
        
        send_telegram_alert(message)

# ============================================================
# ENHANCED MARKET REGIME DETECTION
# ============================================================
def detect_market_regime(rsi, adx, macd_hist, volatility, pcr):
    """Detect current market regime with confidence scoring"""
    regime_scores = {
        "TRENDING_UP": 0,
        "TRENDING_DOWN": 0,
        "RANGING": 0,
        "VOLATILE": 0,
        "BREAKOUT": 0
    }
    
    # ADX based trend strength
    if adx > CONFIG["STRONG_TREND_MIN"]:
        if rsi > 55 and macd_hist > 0:
            regime_scores["TRENDING_UP"] += 3
        elif rsi < 45 and macd_hist < 0:
            regime_scores["TRENDING_DOWN"] += 3
        regime_scores["BREAKOUT"] += 2
    elif adx < 20:
        regime_scores["RANGING"] += 2
    
    # RSI based momentum
    if rsi > 70:
        regime_scores["TRENDING_UP"] += 2
        regime_scores["BREAKOUT"] += 1
    elif rsi < 30:
        regime_scores["TRENDING_DOWN"] += 2
        regime_scores["BREAKOUT"] += 1
    elif 40 <= rsi <= 60:
        regime_scores["RANGING"] += 2
    
    # Volatility regime
    if volatility > 1.5:  # High volatility threshold
        regime_scores["VOLATILE"] += 3
        regime_scores["TRENDING_UP"] -= 1
        regime_scores["TRENDING_DOWN"] -= 1
    
    # PCR based sentiment
    if pcr < 0.8:
        regime_scores["TRENDING_UP"] += 1
    elif pcr > 1.2:
        regime_scores["TRENDING_DOWN"] += 1
    
    # Determine dominant regime
    dominant_regime = max(regime_scores, key=regime_scores.get)
    confidence = (regime_scores[dominant_regime] / sum(regime_scores.values())) * 100 if sum(regime_scores.values()) > 0 else 0
    
    # Volatility classification
    if volatility > 2.0:
        volatility_regime = "EXTREME"
    elif volatility > 1.2:
        volatility_regime = "HIGH"
    elif volatility > 0.6:
        volatility_regime = "NORMAL"
    else:
        volatility_regime = "LOW"
    
    return dominant_regime, confidence, volatility_regime

# ============================================================
# GRADE 1 PRO SIGNAL CLASSIFICATION ENGINE
# ============================================================
def classify_signal_strength(rsi, macd_hist, adx, pcr, volume_ratio, trend_direction, volatility_regime="NORMAL"):
    """Enhanced signal classification with volatility adjustment"""
    score = 0
    factors = []
    
    # Base score calculation
    if trend_direction == "BULLISH":
        if rsi >= CONFIG["STRONG_CE_RSI"]:
            score += 2
            factors.append("RSI_STRONG")
        elif rsi >= CONFIG["CONSIDER_CE_RSI"]:
            score += 1
            factors.append("RSI_CONSIDER")
        
        if macd_hist > CONFIG["MACD_CONFIRM_THRESHOLD"]:
            score += 2
            factors.append("MACD_CONFIRM")
        elif macd_hist > 0:
            score += 1
            factors.append("MACD_WEAK")
        
        if adx >= CONFIG["STRONG_TREND_MIN"]:
            score += 2
            factors.append("TREND_STRONG")
        elif adx >= 20:
            score += 1
            factors.append("TREND_MODERATE")
        
        if pcr <= CONFIG["PCR_BULLISH_THRESHOLD"]:
            score += 1
            factors.append("PCR_BULLISH")
        
        if volume_ratio >= CONFIG["VOLUME_SPIKE_RATIO"]:
            score += 1
            factors.append("VOLUME_SPIKE")
            
    else:  # BEARISH
        if rsi <= CONFIG["STRONG_PE_RSI"]:
            score += 2
            factors.append("RSI_STRONG")
        elif rsi <= CONFIG["CONSIDER_PE_RSI"]:
            score += 1
            factors.append("RSI_CONSIDER")
        
        if macd_hist < -CONFIG["MACD_CONFIRM_THRESHOLD"]:
            score += 2
            factors.append("MACD_CONFIRM")
        elif macd_hist < 0:
            score += 1
            factors.append("MACD_WEAK")
        
        if adx >= CONFIG["STRONG_TREND_MIN"]:
            score += 2
            factors.append("TREND_STRONG")
        elif adx >= 20:
            score += 1
            factors.append("TREND_MODERATE")
        
        if pcr >= CONFIG["PCR_BEARISH_THRESHOLD"]:
            score += 1
            factors.append("PCR_BEARISH")
        
        if volume_ratio >= CONFIG["VOLUME_SPIKE_RATIO"]:
            score += 1
            factors.append("VOLUME_SPIKE")
    
    # Volatility adjustment
    if volatility_regime == "HIGH":
        score = int(score * 0.8)  # Reduce score in high volatility
        factors.append("VOLATILITY_REDUCTION")
    elif volatility_regime == "LOW":
        score = int(score * 1.1)  # Boost score in low volatility
        factors.append("LOW_VOL_BOOST")
    
    # Classification
    if score >= 7:
        return "STRONG", score, factors
    elif score >= 4:
        return "CONSIDER", score, factors
    else:
        return "WEAK", score, factors

def generate_alert_message(action, strength, spot_price, premium, sl, target, factors, confidence):
    """Generate formatted alert message"""
    factor_str = " | ".join(factors) if factors else "Basic"
    
    if action == "BUY_CE":
        if strength == "STRONG":
            return (f"🟢 <b>STRONG CE BUY</b>\n"
                    f"💰 Spot: {spot_price} | Premium: {premium:.2f}\n"
                    f"🎯 Target: {target:.2f} | 🛡️ SL: {sl:.2f}\n"
                    f"📊 Confidence: {confidence:.1f}% | Factors: {factor_str}")
        else:
            return (f"🟡 <b>CONSIDER CE BUY</b>\n"
                    f"💰 Spot: {spot_price} | Premium: {premium:.2f}\n"
                    f"🎯 Target: {target:.2f} | 🛡️ SL: {sl:.2f}\n"
                    f"📊 Confidence: {confidence:.1f}% | Factors: {factor_str}")
    
    elif action == "BUY_PE":
        if strength == "STRONG":
            return (f"🔴 <b>STRONG PE BUY</b>\n"
                    f"💰 Spot: {spot_price} | Premium: {premium:.2f}\n"
                    f"🎯 Target: {target:.2f} | 🛡️ SL: {sl:.2f}\n"
                    f"📊 Confidence: {confidence:.1f}% | Factors: {factor_str}")
        else:
            return (f"🟠 <b>CONSIDER PE BUY</b>\n"
                    f"💰 Spot: {spot_price} | Premium: {premium:.2f}\n"
                    f"🎯 Target: {target:.2f} | 🛡️ SL: {sl:.2f}\n"
                    f"📊 Confidence: {confidence:.1f}% | Factors: {factor_str}")
    
    elif action == "EXIT":
        return (f"⚠️ <b>EXIT SIGNAL</b>\n"
                f"💰 Spot: {spot_price} | Premium: {premium:.2f}\n"
                f"📊 Reason: {factor_str}")
    
    elif action == "HOLD":
        return (f"⏸️ <b>HOLD</b>\n"
                f"💰 Spot: {spot_price:.2f}\n"
                f"📊 Market: {market_signal.get('regime', 'Ranging')} | Confidence: {confidence:.1f}%")
    
    return (f"📊 <b>WAITING</b>\n"
            f"💰 Spot: {spot_price:.2f}")

# ============================================================
# METRICS COLLECTION AND STORAGE
# ============================================================
def collect_metrics():
    """Collect and store performance metrics"""
    metric_id = str(uuid.uuid4())
    timestamp = time.time()
    
    metrics = [
        ("equity", portfolio_state["equity"], "portfolio"),
        ("daily_pnl", portfolio_state["daily_pnl"], "portfolio"),
        ("sharpe_ratio", portfolio_state["sharpe_ratio"], "portfolio"),
        ("var_95", portfolio_state["var_95"], "risk"),
        ("spot_rsi", market_state.get("rsi", 0), "technical"),
        ("adx", market_state.get("adx", 0), "technical"),
        ("pcr", market_signal.get("pcr", 0), "sentiment"),
        ("signal_confidence", signal_state.get("confidence", 0), "signals"),
        ("tick_rate", tick_counter / 60 if tick_counter > 0 else 0, "performance"),
        ("ws_connected", 1 if ws_running else 0, "system")
    ]
    
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        for metric_name, metric_value, tags in metrics:
            c.execute("""INSERT INTO metrics (timestamp, metric_name, metric_value, tags, id)
                        VALUES (?, ?, ?, ?, ?)""",
                     (timestamp, metric_name, metric_value, tags, str(uuid.uuid4())))
        
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to collect metrics: {e}")

def run_metrics_collector():
    """Background thread for metrics collection"""
    while True:
        try:
            time.sleep(CONFIG["METRICS_COLLECTION_INTERVAL"])
            collect_metrics()
        except Exception as e:
            logger.error(f"Metrics collector error: {e}")

# ============================================================
# GRADE 1 PRO SIGNAL EXECUTION ENGINE
# ============================================================
def run_signal_engine():
    """Enhanced signal engine with regime detection and improved risk management"""
    global market_signal, market_state, signal_state, portfolio_state, latest_ticks
    global signal_buffer, daily_trade_count
    
    # Risk checks
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
    
    # Data validation
    spot_history_list = list(spot_price_history)
    if len(spot_history_list) < 50:
        market_signal["alert_message"] = "⏳ Collecting market data..."
        market_signal["signal_strength"] = "WAITING"
        return
    
    # Calculate technical indicators
    spot_rsi = calculate_rsi(spot_history_list, CONFIG["SPOT_RSI_PERIOD"], CONFIG["SPOT_RSI_SMOOTHING"])
    spot_macd, macd_signal_line, macd_hist = calculate_macd(
        spot_history_list, CONFIG["SPOT_MACD_FAST"], CONFIG["SPOT_MACD_SLOW"], CONFIG["SPOT_MACD_SIGNAL"]
    )
    spot_atr = calculate_atr(spot_history_list, CONFIG["SPOT_ATR_PERIOD"])
    adx = calculate_adx(spot_history_list, CONFIG["TREND_STRENGTH_PERIOD"])
    pcr = get_nifty_pcr()
    
    # Calculate EMAs
    ema_fast = calculate_ema(spot_history_list, 20)
    ema_slow = calculate_ema(spot_history_list, 50)
    price_above_fast = spot_history_list[-1] > ema_fast if spot_history_list else False
    price_above_slow = spot_history_list[-1] > ema_slow if spot_history_list else False
    
    # Volume analysis
    ce_vol_list = list(ce_volume_history)
    pe_vol_list = list(pe_volume_history)
    avg_ce_vol = sum(ce_vol_list[-CONFIG["VOLUME_MA_PERIOD"]:]) / min(len(ce_vol_list), CONFIG["VOLUME_MA_PERIOD"]) if ce_vol_list else 1
    avg_pe_vol = sum(pe_vol_list[-CONFIG["VOLUME_MA_PERIOD"]:]) / min(len(pe_vol_list), CONFIG["VOLUME_MA_PERIOD"]) if pe_vol_list else 1
    current_ce_vol = ce_vol_list[-1] if ce_vol_list else 0
    current_pe_vol = pe_vol_list[-1] if pe_vol_list else 0
    ce_vol_ratio = current_ce_vol / avg_ce_vol if avg_ce_vol > 0 else 1
    pe_vol_ratio = current_pe_vol / avg_pe_vol if avg_pe_vol > 0 else 1
    
    # Bollinger Bands
    upper_bb, middle_bb, lower_bb = calculate_bollinger_bands(spot_history_list)
    
    # Market regime detection
    volatility = spot_atr / spot_history_list[-1] if spot_history_list[-1] > 0 else 0
    regime, regime_confidence, volatility_regime = detect_market_regime(spot_rsi, adx, macd_hist, volatility, pcr)
    
    # Update market state
    market_state.update({
        "rsi": round(spot_rsi, 1),
        "macd_hist": round(macd_hist, 4),
        "atr": round(spot_atr, 2),
        "adx": round(adx, 1),
        "pcr": round(pcr, 2),
        "trend": "UPTREND" if price_above_fast and price_above_slow else "DOWNTREND" if not price_above_fast and not price_above_slow else "MIXED",
        "volatility": volatility_regime,
        "regime": regime
    })
    
    market_signal.update({
        "regime": regime,
        "regime_confidence": regime_confidence,
        "volatility_regime": volatility_regime
    })
    
    current_time = time.time()
    
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
        
        # Trailing stop logic
        if current_premium > signal_state["highest_premium_seen"]:
            signal_state["highest_premium_seen"] = current_premium
            new_sl = current_premium - (spot_atr * CONFIG["TRAILING_ATR_MULT"])
            if new_sl > signal_state["stop_loss"]:
                old_sl = signal_state["stop_loss"]
                signal_state["stop_loss"] = new_sl
                if new_sl > signal_state["entry_price"] and old_sl <= signal_state["entry_price"]:
                    send_telegram_alert(f"🔒 <b>SL MOVED TO BREAKEVEN</b>\n{active_side} @ {current_premium:.2f}")
        
        # Stop loss hit
        if current_premium <= signal_state["stop_loss"]:
            pnl_points = current_premium - signal_state["entry_price"]
            pnl_amount = pnl_points * 50
            portfolio_state["equity"] += pnl_amount
            portfolio_state["total_pnl"] += pnl_amount
            daily_trade_count += 1
            portfolio_state["total_trades"] += 1
            
            if pnl_amount > 0:
                portfolio_state["winning_trades"] += 1
            
            exit_msg = generate_alert_message("EXIT", "STOP_LOSS", latest_ticks["spot_price"], 
                                            current_premium, 0, 0, ["Trailing SL Hit"], 0)
            send_telegram_alert(exit_msg + f"\n💵 P&L: {pnl_points:.2f} pts (₹{pnl_amount:,.2f})")
            
            save_trade_to_db(signal_state["trade_id"], signal_state["entry_price"], 
                           current_premium, pnl_amount, "STOP_LOSS")
            reset_signal_state(current_time)
            return
        
        # Target achieved
        if current_premium >= signal_state["target"]:
            pnl_points = current_premium - signal_state["entry_price"]
            pnl_amount = pnl_points * 50
            portfolio_state["equity"] += pnl_amount
            portfolio_state["total_pnl"] += pnl_amount
            daily_trade_count += 1
            portfolio_state["total_trades"] += 1
            portfolio_state["winning_trades"] += 1
            
            exit_msg = generate_alert_message("EXIT", "TARGET", latest_ticks["spot_price"], 
                                            current_premium, 0, 0, ["Target Achieved"], 100)
            send_telegram_alert(exit_msg + f"\n💵 P&L: {pnl_points:.2f} pts (₹{pnl_amount:,.2f})")
            
            save_trade_to_db(signal_state["trade_id"], signal_state["entry_price"], 
                           current_premium, pnl_amount, "TARGET")
            reset_signal_state(current_time)
            return
        
        # Profit lock at 50% drawdown from peak
        if signal_state["max_profit_seen"] > spot_atr * 0.5:
            drawdown_from_peak = signal_state["max_profit_seen"] - unrealized_pnl
            if drawdown_from_peak > signal_state["max_profit_seen"] * 0.5:
                pnl_points = current_premium - signal_state["entry_price"]
                pnl_amount = pnl_points * 50
                portfolio_state["equity"] += pnl_amount
                portfolio_state["total_pnl"] += pnl_amount
                daily_trade_count += 1
                portfolio_state["total_trades"] += 1
                
                if pnl_amount > 0:
                    portfolio_state["winning_trades"] += 1
                
                exit_msg = generate_alert_message("EXIT", "PROFIT_LOCK", latest_ticks["spot_price"], 
                                                current_premium, 0, 0, ["Profit Lock - 50% Drawdown"], 0)
                send_telegram_alert(exit_msg + f"\n💵 P&L: {pnl_points:.2f} pts (₹{pnl_amount:,.2f})")
                
                save_trade_to_db(signal_state["trade_id"], signal_state["entry_price"], 
                               current_premium, pnl_amount, "PROFIT_LOCK")
                reset_signal_state(current_time)
                return
        
        # Time-based exit
        hold_time_min = (current_time - signal_state["entry_time"]) / 60
        if hold_time_min >= CONFIG["MAX_HOLD_TIME_MIN"]:
            pnl_points = current_premium - signal_state["entry_price"]
            pnl_amount = pnl_points * 50
            portfolio_state["equity"] += pnl_amount
            portfolio_state["total_pnl"] += pnl_amount
            daily_trade_count += 1
            portfolio_state["total_trades"] += 1
            
            if pnl_amount > 0:
                portfolio_state["winning_trades"] += 1
            
            exit_msg = generate_alert_message("EXIT", "TIME", latest_ticks["spot_price"], 
                                            current_premium, 0, 0, [f"Max Hold Time ({CONFIG['MAX_HOLD_TIME_MIN']}min)"], 0)
            send_telegram_alert(exit_msg + f"\n💵 P&L: {pnl_points:.2f} pts (₹{pnl_amount:,.2f})")
            
            save_trade_to_db(signal_state["trade_id"], signal_state["entry_price"], 
                           current_premium, pnl_amount, "MAX_HOLD_TIME")
            reset_signal_state(current_time)
            return
        
        # Momentum reversal detection
        if active_side == "BUY_CE":
            if spot_rsi < 45 or macd_hist < -0.5 or not price_above_fast:
                pnl_points = current_premium - signal_state["entry_price"]
                pnl_amount = pnl_points * 50
                portfolio_state["equity"] += pnl_amount
                portfolio_state["total_pnl"] += pnl_amount
                daily_trade_count += 1
                portfolio_state["total_trades"] += 1
                
                if pnl_amount > 0:
                    portfolio_state["winning_trades"] += 1
                
                reasons = []
                if spot_rsi < 45:
                    reasons.append("RSI<45")
                if macd_hist < -0.5:
                    reasons.append("MACD reversal")
                if not price_above_fast:
                    reasons.append("Price<EMA20")
                
                exit_msg = generate_alert_message("EXIT", "MOMENTUM", latest_ticks["spot_price"], 
                                                current_premium, 0, 0, reasons, 0)
                send_telegram_alert(exit_msg + f"\n💵 P&L: {pnl_points:.2f} pts (₹{pnl_amount:,.2f})")
                
                save_trade_to_db(signal_state["trade_id"], signal_state["entry_price"], 
                               current_premium, pnl_amount, "MOMENTUM_REVERSAL")
                reset_signal_state(current_time)
                return
        
        elif active_side == "BUY_PE":
            if spot_rsi > 55 or macd_hist > 0.5 or price_above_fast:
                pnl_points = current_premium - signal_state["entry_price"]
                pnl_amount = pnl_points * 50
                portfolio_state["equity"] += pnl_amount
                portfolio_state["total_pnl"] += pnl_amount
                daily_trade_count += 1
                portfolio_state["total_trades"] += 1
                
                if pnl_amount > 0:
                    portfolio_state["winning_trades"] += 1
                
                reasons = []
                if spot_rsi > 55:
                    reasons.append("RSI>55")
                if macd_hist > 0.5:
                    reasons.append("MACD reversal")
                if price_above_fast:
                    reasons.append("Price>EMA20")
                
                exit_msg = generate_alert_message("EXIT", "MOMENTUM", latest_ticks["spot_price"], 
                                                current_premium, 0, 0, reasons, 0)
                send_telegram_alert(exit_msg + f"\n💵 P&L: {pnl_points:.2f} pts (₹{pnl_amount:,.2f})")
                
                save_trade_to_db(signal_state["trade_id"], signal_state["entry_price"], 
                               current_premium, pnl_amount, "MOMENTUM_REVERSAL")
                reset_signal_state(current_time)
                return
        
        # Update status message
        hold_mins = int((current_time - signal_state["entry_time"]) / 60)
        pnl_pct = ((current_premium - signal_state["entry_price"]) / signal_state["entry_price"] * 100) if signal_state["entry_price"] > 0 else 0
        market_signal["alert_message"] = (f"📊 {active_side} ACTIVE | Hold: {hold_mins}m | "
                                         f"P&L: {pnl_pct:.1f}% | SL: {signal_state['stop_loss']:.2f}")
        market_signal["signal_strength"] = "ACTIVE"
    
    # ========== STATE B: POSITION DISCOVERY (SCANNER) ==========
    else:
        if current_time < signal_state["cooldown_until"]:
            remaining = int(signal_state["cooldown_until"] - current_time)
            market_signal["alert_message"] = f"⏳ Cooldown: {remaining}s remaining"
            market_signal["signal_strength"] = "COOLDOWN"
            return
        
        # Check for bullish signals
        if (spot_rsi >= CONFIG["CONSIDER_CE_RSI"] and macd_hist > 0 and 
            price_above_fast and regime in ["TRENDING_UP", "BREAKOUT"]):
            
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
                spot_rsi, macd_hist, adx, pcr, ce_vol_ratio, "BULLISH", volatility_regime
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
            
            # Calculate position size
            position_size = risk_manager.calculate_position_size(score * 10, spot_atr, portfolio_state["equity"])
            
            sl = ce_premium - (spot_atr * CONFIG["ENTRY_ATR_MULT"])
            target = ce_premium + (spot_atr * CONFIG["TARGET_ATR_MULT"])
            confidence = min(95, score * 10 + spot_rsi * 0.3)
            
            if score >= 8:
                grade = "A+"
            elif score >= 7:
                grade = "A"
            elif score >= 5:
                grade = "B+"
            else:
                grade = "B"
            
            trade_id = str(uuid.uuid4())
            signal_state.update({
                "current_action": "BUY_CE",
                "entry_price": ce_premium,
                "highest_premium_seen": ce_premium,
                "stop_loss": sl,
                "target": target,
                "signal_grade": grade,
                "confidence": confidence,
                "entry_time": current_time,
                "max_profit_seen": 0.0,
                "position_size_pct": position_size,
                "trade_id": trade_id
            })
            
            portfolio_state["open_positions"] = 1
            signal_buffer["consecutive_ce"] += 1
            signal_buffer["consecutive_pe"] = 0
            
            # Save entry to database
            save_signal_to_db(trade_id, "BUY_CE", grade, confidence, spot_rsi, pcr, regime)
            
            alert = generate_alert_message("BUY_CE", strength, latest_ticks["spot_price"], 
                                         ce_premium, sl, target, factors, confidence)
            send_telegram_alert(alert + f"\n📐 Position Size: {position_size} lots")
            logger.info(f"SIGNAL: {strength} CE BUY | Score: {score} | Grade: {grade} | Size: {position_size}")
        
        # Check for bearish signals
        elif (spot_rsi <= CONFIG["CONSIDER_PE_RSI"] and macd_hist < 0 and 
              not price_above_fast and regime in ["TRENDING_DOWN", "BREAKOUT"]):
            
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
                spot_rsi, macd_hist, adx, pcr, pe_vol_ratio, "BEARISH", volatility_regime
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
            
            # Calculate position size
            position_size = risk_manager.calculate_position_size(score * 10, spot_atr, portfolio_state["equity"])
            
            sl = pe_premium - (spot_atr * CONFIG["ENTRY_ATR_MULT"])
            target = pe_premium + (spot_atr * CONFIG["TARGET_ATR_MULT"])
            confidence = min(95, score * 10 + (100 - spot_rsi) * 0.3)
            
            if score >= 8:
                grade = "A+"
            elif score >= 7:
                grade = "A"
            elif score >= 5:
                grade = "B+"
            else:
                grade = "B"
            
            trade_id = str(uuid.uuid4())
            signal_state.update({
                "current_action": "BUY_PE",
                "entry_price": pe_premium,
                "highest_premium_seen": pe_premium,
                "stop_loss": sl,
                "target": target,
                "signal_grade": grade,
                "confidence": confidence,
                "entry_time": current_time,
                "max_profit_seen": 0.0,
                "position_size_pct": position_size,
                "trade_id": trade_id
            })
            
            portfolio_state["open_positions"] = 1
            signal_buffer["consecutive_pe"] += 1
            signal_buffer["consecutive_ce"] = 0
            
            # Save entry to database
            save_signal_to_db(trade_id, "BUY_PE", grade, confidence, spot_rsi, pcr, regime)
            
            alert = generate_alert_message("BUY_PE", strength, latest_ticks["spot_price"], 
                                         pe_premium, sl, target, factors, confidence)
            send_telegram_alert(alert + f"\n📐 Position Size: {position_size} lots")
            logger.info(f"SIGNAL: {strength} PE BUY | Score: {score} | Grade: {grade} | Size: {position_size}")
        
        else:
            signal_buffer["ce_count"] = 0
            signal_buffer["pe_count"] = 0
            
            # Determine market status message
            if regime == "TRENDING_UP":
                status = f"Bullish trend (ADX: {adx:.0f}) - waiting for confirmation"
            elif regime == "TRENDING_DOWN":
                status = f"Bearish trend (ADX: {adx:.0f}) - waiting for confirmation"
            elif regime == "VOLATILE":
                status = "High volatility - reducing position sizes"
            else:
                status = "Market ranging - no clear direction"
            
            market_signal["alert_message"] = (f"⏸️ HOLD | {status}\n"
                                            f"RSI: {spot_rsi:.1f} | MACD: {macd_hist:.2f} | ADX: {adx:.1f} | "
                                            f"Regime: {regime}")
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
        "daily_trades": daily_trade_count,
        "upper_bb": round(upper_bb, 2) if upper_bb else 0,
        "middle_bb": round(middle_bb, 2) if middle_bb else 0,
        "lower_bb": round(lower_bb, 2) if lower_bb else 0
    })
    
    # Update daily performance metrics
    update_daily_performance()

def save_signal_to_db(trade_id, action, grade, confidence, rsi, pcr, regime):
    """Save signal to database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""INSERT INTO signals (timestamp, action, signal_type, grade, confidence, 
                     ce_price, pe_price, rsi, pcr, regime, id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                 (time.time(), action, "AUTO", grade, confidence,
                  latest_ticks["ce_price"], latest_ticks["pe_price"],
                  rsi, pcr, regime, trade_id))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to save signal: {e}")

def save_trade_to_db(trade_id, entry_price, exit_price, pnl, exit_reason):
    """Save trade record to database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""INSERT INTO trades (timestamp, action, entry_price, exit_price, pnl, 
                     size_pct, status, grade, exit_reason, id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                 (time.time(), signal_state["current_action"], entry_price, exit_price, pnl,
                  signal_state.get("position_size_pct", 0), "CLOSED", 
                  signal_state["signal_grade"], exit_reason, trade_id))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to save trade: {e}")

def update_daily_performance():
    """Update daily performance metrics in database"""
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Calculate drawdown
        current_dd = ((portfolio_state["daily_peak"] - portfolio_state["equity"]) / 
                     portfolio_state["daily_peak"] * 100) if portfolio_state["daily_peak"] > 0 else 0
        
        if current_dd > portfolio_state["max_drawdown_today"]:
            portfolio_state["max_drawdown_today"] = current_dd
        
        c.execute("""INSERT OR REPLACE INTO daily_performance 
                     (date, equity, daily_pnl, drawdown_pct, sharpe, var, trades_count)
                     VALUES (?, ?, ?, ?, ?, ?, ?)""",
                 (today, portfolio_state["equity"], portfolio_state["daily_pnl"],
                  portfolio_state["max_drawdown_today"], portfolio_state["sharpe_ratio"],
                  portfolio_state["var_95"], daily_trade_count))
        
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to update daily performance: {e}")

def reset_signal_state(current_time):
    """Reset signal state after trade completion"""
    global signal_state, portfolio_state, signal_buffer, daily_trade_count
    
    signal_state.update({
        "current_action": "HOLD", 
        "entry_price": 0.0, 
        "stop_loss": 0.0,
        "target": 0.0, 
        "highest_premium_seen": 0.0, 
        "confidence": 0.0,
        "cooldown_until": current_time + CONFIG["COOLDOWN_SEC"],
        "entry_time": 0, 
        "max_profit_seen": 0.0,
        "trade_id": None
    })
    
    portfolio_state["open_positions"] = 0
    signal_buffer["ce_count"] = 0
    signal_buffer["pe_count"] = 0

# ============================================================
# TIMING AND SESSION STRUCTURING UTILITIES
# ============================================================
def get_ist_now():
    """Get current IST time"""
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def is_market_open():
    """Check if market is open for trading"""
    now_ist = get_ist_now()
    if now_ist.weekday() >= 5:  # Weekend
        return False
    return dt_time(9, 15) <= now_ist.time() <= dt_time(15, 30)

# ============================================================
# WEBSOCKET CONNECTION WITH AUTO-RECONNECTION
# ============================================================
def connect_websocket_with_retry():
    """Establish WebSocket connection with retry logic"""
    global sws, ws_running, reconnection_attempts, last_reconnection_time
    
    delay = CONFIG["WS_RECONNECT_DELAY_BASE"]
    
    while True:
        try:
            if not is_market_open():
                logger.info("Market closed, waiting for market hours...")
                time.sleep(60)
                continue
            
            logger.info(f"Attempting WebSocket connection (attempt {reconnection_attempts + 1})...")
            
            auth_token, feed_token, obj = get_auth_token()
            if not feed_token:
                logger.error("Failed to get feed token")
                time.sleep(delay)
                reconnection_attempts += 1
                delay = min(delay * 2, CONFIG["WS_RECONNECT_DELAY_MAX"])
                continue
            
            if not CE_TOKEN or not PE_TOKEN:
                logger.info("Resolving option tokens...")
                get_current_atm_tokens()
                if not CE_TOKEN or not PE_TOKEN:
                    logger.error("Could not resolve option tokens")
                    time.sleep(delay)
                    continue
            
            sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_ID, feed_token)
            
            sws.on_open = on_ws_open
            sws.on_data = on_ws_data
            sws.on_error = on_ws_error
            sws.on_close = on_ws_close
            
            ws_running = True
            reconnection_attempts = 0
            last_reconnection_time = time.time()
            
            sws.connect()
            
            # If we get here, connection was lost
            logger.warning("WebSocket connection lost, reconnecting...")
            ws_running = False
            time.sleep(delay)
            reconnection_attempts += 1
            delay = min(delay * 2, CONFIG["WS_RECONNECT_DELAY_MAX"])
            
        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")
            ws_running = False
            time.sleep(delay)
            reconnection_attempts += 1
            delay = min(delay * 2, CONFIG["WS_RECONNECT_DELAY_MAX"])

# ============================================================
# WEBSOCKET SUBSCRIPTION STREAM DATA INTERFACES
# ============================================================
def on_ws_open(wsapp, open_message):
    """Handle WebSocket open event"""
    global sws
    logger.info(f"WebSocket opened: {open_message}")
    
    if sws and CE_TOKEN and PE_TOKEN:
        try:
            subscription_payload = [
                {"exchangeType": 1, "tokens": [SPOT_TOKEN]},
                {"exchangeType": 2, "tokens": [CE_TOKEN, PE_TOKEN]}
            ]
            sws.subscribe("tradeguru_001", 1, subscription_payload)
            logger.info("Successfully subscribed to market data feeds")
            send_telegram_alert("✅ WebSocket connected and subscribed to market data")
        except Exception as e:
            logger.error(f"Subscription failed: {e}")

def on_ws_error(wsapp, error):
    """Handle WebSocket errors"""
    logger.error(f"WebSocket Error: {error}")
    send_telegram_alert(f"⚠️ WebSocket Error: {str(error)[:100]}")

def on_ws_close(wsapp, code, msg):
    """Handle WebSocket close event"""
    global ws_running
    ws_running = False
    logger.warning(f"WebSocket closed: code={code}, msg={msg}")

def process_tick_queue():
    """Background thread to process ticks asynchronously"""
    while True:
        try:
            tick = tick_queue.get(timeout=CONFIG["TICK_PROCESSING_TIMEOUT"])
            process_single_tick(tick)
            tick_queue.task_done()
        except queue.Empty:
            continue
        except Exception as e:
            logger.error(f"Tick processing error: {e}")

# ============================================================
# WEBSOCKET DATA HANDLER WITH TIMEFRAME AGGREGATION
# ============================================================
def process_single_tick(tick):
    """Process a single tick update"""
    global tick_counter, last_tick_time, latest_ticks
    global spot_price_history, ce_price_history, pe_price_history
    global last_timeframe_update, timeframe_candles, timeframe_history
    
    try:
        token = str(tick.get("token") or tick.get("tk", ""))
        ltp = tick.get("ltp") or tick.get("last_traded_price", 0)
        
        # Fix for price scaling
        if isinstance(ltp, (int, float)) and ltp > 50000 and token != SPOT_TOKEN:
            ltp = ltp / 100
        
        vol = tick.get("v") or tick.get("volume_trade_for_the_day", 0)
        oi = tick.get("oi") or tick.get("open_interest", 0)
        
        if token == SPOT_TOKEN:
            latest_ticks["spot_price"] = ltp
            spot_price_history.append(ltp)
            tick_counter += 1
            
            current_time = time.time()
            
            # Update timeframe candles
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
        
        # Run signal engine periodically
        if tick_counter % 3 == 0:
            run_signal_engine()
            
    except Exception as e:
        logger.error(f"Tick processing error: {e}")

def on_ws_data(wsapp, message):
    """Handle incoming WebSocket data"""
    try:
        if isinstance(message, bytes):
            if sws is None:
                return
            
            # Parse binary data
            try:
                tick = sws._parse_binary_data(message)
                if tick:
                    # Add to processing queue
                    tick_queue.put(tick)
            except Exception as e:
                logger.error(f"Binary parse error: {e}")
        else:
            # Parse JSON data
            data = json.loads(message) if isinstance(message, str) else message
            ticks = data if isinstance(data, list) else [data]
            
            for tick in ticks:
                if tick:
                    tick_queue.put(tick)
                    
    except Exception as e:
        logger.error(f"WebSocket data handler error: {e}")

# ============================================================
# DATA SOURCING PIPELINES
# ============================================================
auth_cache = {"token": None, "feed_token": None, "timestamp": 0, "obj": None}

def get_auth_token():
    """Get authentication token with caching"""
    now = time.time()
    if auth_cache["token"] and (now - auth_cache["timestamp"] < 3600):
        return auth_cache["token"], auth_cache["feed_token"], auth_cache["obj"]
    
    try:
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        obj = SmartConnect(api_key=ANGEL_API_KEY)
        session = obj.generateSession(ANGEL_CLIENT_ID, ANGEL_PASSWORD, totp)
        
        if not session.get("status"):
            logger.error(f"Authentication failed: {session}")
            return None, None, None
        
        auth_token = session["data"]["jwtToken"]
        feed_token = obj.getfeedToken()
        
        auth_cache.update({
            "token": auth_token, 
            "feed_token": feed_token, 
            "timestamp": now, 
            "obj": obj
        })
        
        logger.info("Authentication successful")
        return auth_token, feed_token, obj
        
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        return None, None, None

def get_nifty_spot():
    """Get current NIFTY spot price"""
    _, _, obj = get_auth_token()
    if not obj:
        return None
    
    try:
        response = obj.ltpData("NSE", "NIFTY", SPOT_TOKEN)
        if response.get("status") and response.get("data"):
            return float(response["data"].get("ltp", 0))
    except Exception as e:
        logger.error(f"Error fetching spot NIFTY: {e}")
    
    return None

def get_nifty_pcr():
    """Calculate Put-Call Ratio"""
    global ce_oi_history, pe_oi_history
    
    if ce_oi_history and pe_oi_history:
        ce_sum = sum(ce_oi_history)
        pe_sum = sum(pe_oi_history)
        if ce_sum > 0:
            pcr = round(pe_sum / ce_sum, 2)
            pcr_cache.update({"value": pcr, "time": time.time()})
            return pcr
    
    return pcr_cache["value"]

def get_current_atm_tokens():
    """Get ATM strike option tokens"""
    global CE_TOKEN, PE_TOKEN, ATM_STRIKE, EXPIRY_DATE
    
    spot = get_nifty_spot()
    if not spot:
        return None, None
    
    atm_strike = round(spot / 50) * 50
    
    try:
        url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
        resp = requests.get(url, timeout=15)
        df = pd.DataFrame(resp.json())
        
        nifty_opts = df[(df["name"] == "NIFTY") & 
                       (df["instrumenttype"] == "OPTIDX") & 
                       (df["exch_seg"] == "NFO")].copy()
        
        if nifty_opts.empty:
            return None, None
        
        nifty_opts["expiry_date"] = pd.to_datetime(nifty_opts["expiry"], format="%d%b%Y", errors="coerce")
        nifty_opts = nifty_opts.dropna(subset=["expiry_date"])
        nifty_opts["strike"] = pd.to_numeric(nifty_opts["strike"], errors="coerce") / 100
        
        today = datetime.now()
        future = nifty_opts[nifty_opts["expiry_date"] >= today]
        nearest_expiry = future["expiry_date"].min()
        
        atm_opts = future[(future["strike"] == atm_strike) & (future["expiry_date"] == nearest_expiry)]
        ce = atm_opts[atm_opts["symbol"].str.contains("CE")]
        pe = atm_opts[atm_opts["symbol"].str.contains("PE")]
        
        if ce.empty or pe.empty:
            return None, None
        
        CE_TOKEN = str(ce.iloc[0]["token"])
        PE_TOKEN = str(pe.iloc[0]["token"])
        ATM_STRIKE = atm_strike
        EXPIRY_DATE = nearest_expiry.strftime("%d%b%Y").upper()
        
        logger.info(f"Tokens resolved: CE={CE_TOKEN}, PE={PE_TOKEN}, Strike={ATM_STRIKE}")
        return CE_TOKEN, PE_TOKEN
        
    except Exception as e:
        logger.error(f"Error resolving option tokens: {e}")
        return None, None

# ============================================================
# BACKGROUND THREAD INITIALIZATION
# ============================================================
def init_background_threads():
    """Initialize all background worker threads"""
    logger.info("Starting background threads...")
    
    # WebSocket connection thread
    ws_thread = threading.Thread(target=connect_websocket_with_retry, daemon=True)
    ws_thread.start()
    
    # Tick processing thread
    tick_processor = threading.Thread(target=process_tick_queue, daemon=True)
    tick_processor.start()
    
    # Metrics collector thread
    metrics_thread = threading.Thread(target=run_metrics_collector, daemon=True)
    metrics_thread.start()
    
    # Performance reporting thread (daily)
    def performance_reporter():
        last_report_date = ""
        while True:
            today = datetime.now().strftime("%Y-%m-%d")
            if today != last_report_date and datetime.now().hour == 15 and datetime.now().minute > 30:
                send_performance_update()
                last_report_date = today
            time.sleep(60)
    
    reporting_thread = threading.Thread(target=performance_reporter, daemon=True)
    reporting_thread.start()
    
    logger.info("All background threads started successfully")

_init_completed = False

@app.before_request
def ensure_threads_are_breathing():
    """Ensure background threads are running before handling requests"""
    global _init_completed
    if not _init_completed:
        init_background_threads()
        _init_completed = True

# ============================================================
# FLASK API ENDPOINTS
# ============================================================
@app.route("/", methods=["GET", "HEAD"])
def home():
    """Root endpoint with system information"""
    return jsonify({
        "status": "healthy",
        "engine": "Grade 1 Pro Signal Bot v2.0",
        "market_open": is_market_open(),
        "timestamp": time.time(),
        "uptime": time.time() - last_reconnection_time if last_reconnection_time > 0 else 0,
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
            "Multi-timeframe trend analysis",
            "Market regime detection",
            "Dynamic position sizing",
            "VaR and Sharpe ratio tracking",
            "Auto-reconnection with backoff",
            "Performance metrics collection",
            "Enhanced risk management"
        ]
    }), 200

@app.route("/api/live-signals", methods=["GET"])
@app.route("/api/signals", methods=["GET"])
def live_signals():
    """Get current live signals and market state"""
    return jsonify({
        "timestamp": get_ist_now().isoformat(),
        "market_signal": market_signal,
        "market_state": market_state,
        "signal_state": {
            k: v for k, v in signal_state.items() 
            if k not in ["trade_id"]  # Exclude sensitive/internal IDs
        },
        "portfolio_state": {
            k: v for k, v in portfolio_state.items()
            if k not in ["returns_history"]  # Exclude large arrays
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
            "risk_per_trade_pct": CONFIG["RISK_PER_TRADE_PCT"]
        },
        "performance": {
            "win_rate": (portfolio_state["winning_trades"] / portfolio_state["total_trades"] * 100) 
                       if portfolio_state["total_trades"] > 0 else 0,
            "total_trades": portfolio_state["total_trades"],
            "total_pnl": portfolio_state["total_pnl"],
            "sharpe_ratio": portfolio_state["sharpe_ratio"],
            "max_drawdown": portfolio_state["max_drawdown_today"]
        }
    }), 200

@app.route("/api/health", methods=["GET"])
def health_check():
    """Health check endpoint for monitoring"""
    return jsonify({
        "status": "OK",
        "alive": True,
        "ws_running": ws_running,
        "last_tick": last_tick_time,
        "ticks_received": tick_counter,
        "queue_size": tick_queue.qsize(),
        "reconnection_attempts": reconnection_attempts,
        "market_open": is_market_open(),
        "active_position": signal_state["current_action"] != "HOLD"
    }), 200

@app.route("/api/metrics", methods=["GET"])
def get_metrics():
    """Get historical metrics data"""
    try:
        hours = request.args.get("hours", default=24, type=int)
        cutoff_time = time.time() - (hours * 3600)
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""SELECT timestamp, metric_name, metric_value, tags 
                     FROM metrics 
                     WHERE timestamp > ? 
                     ORDER BY timestamp DESC 
                     LIMIT 1000""", (cutoff_time,))
        
        metrics = [{
            "timestamp": row[0],
            "metric_name": row[1],
            "metric_value": row[2],
            "tags": row[3]
        } for row in c.fetchall()]
        
        conn.close()
        return jsonify({"metrics": metrics}), 200
    except Exception as e:
        logger.error(f"Error fetching metrics: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/trades", methods=["GET"])
def get_trades():
    """Get recent trades"""
    try:
        limit = request.args.get("limit", default=50, type=int)
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""SELECT timestamp, action, entry_price, exit_price, pnl, 
                            exit_reason, grade
                     FROM trades 
                     ORDER BY timestamp DESC 
                     LIMIT ?""", (limit,))
        
        trades = [{
            "timestamp": row[0],
            "action": row[1],
            "entry_price": row[2],
            "exit_price": row[3],
            "pnl": row[4],
            "exit_reason": row[5],
            "grade": row[6]
        } for row in c.fetchall()]
        
        conn.close()
        return jsonify({"trades": trades}), 200
    except Exception as e:
        logger.error(f"Error fetching trades: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/config", methods=["GET", "POST"])
def manage_config():
    """Get or update configuration"""
    global CONFIG
    
    if request.method == "POST":
        if not request.is_json:
            return jsonify({"error": "JSON required"}), 400
        
        updates = request.get_json()
        
        # Validate updates
        allowed_keys = set(CONFIG.keys())
        for key in updates:
            if key not in allowed_keys:
                return jsonify({"error": f"Invalid config key: {key}"}), 400
        
        # Apply updates
        CONFIG.update(updates)
        
        send_telegram_alert(f"⚙️ Configuration updated: {', '.join(updates.keys())}")
        
        return jsonify({
            "message": "Configuration updated",
            "updated_keys": list(updates.keys())
        }), 200
    
    # GET request - return config (hide sensitive values)
    safe_config = {k: v for k, v in CONFIG.items() if not any(sensitive in k.lower() 
                   for sensitive in ["password", "secret", "token", "key"])}
    return jsonify(safe_config), 200

# ============================================================
# MAIN APPLICATION ENTRY POINT
# ============================================================
if __name__ == "__main__":
    if not _init_completed:
        init_background_threads()
        _init_completed = True
    
    port = int(os.environ.get("PORT", 10000))
    
    # Log startup information
    logger.info("=" * 60)
    logger.info("GRADE 1 PRO SIGNAL BOT v2.0")
    logger.info(f"Market Status: {'OPEN' if is_market_open() else 'CLOSED'}")
    logger.info(f"WebSocket: {'ENABLED' if ws_running else 'DISCONNECTED'}")
    logger.info(f"Port: {port}")
    logger.info("=" * 60)
    
    # Start Flask app
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
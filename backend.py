import json
import logging
import time
import threading
from collections import deque
from flask import Flask, jsonify

# ============================================================
# LOGGING & APP SETUP
# ============================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("backend")

app = Flask(__name__)

# ============================================================
# STATE INITIALIZATION & HISTORIES
# ============================================================
# Mock placeholder configurations - replace with your actual environment/module variables
CE_TOKEN = "57046"
PE_TOKEN = "57047"
sws = None  # Will hold the live WebSocket application instance
ws_running = False

# Counters and Performance Metrics
tick_counter = 0
last_tick_time = time.time()
last_minute_snapshot = {"time": time.time(), "price": 0.0}

# Thread-safe lookups with defensive defaults to avoid KeyError during early execution
latest_ticks = {
    "ce_price": 0.0, "ce_volume": 0, "ce_bid": 0.0, "ce_ask": 0.0, "ce_oi": 0,
    "pe_price": 0.0, "pe_volume": 0, "pe_bid": 0.0, "pe_ask": 0.0, "pe_oi": 0
}

# Sliders/Deques for statistical signal calculations (keeps only last 100 entries max)
ce_price_history = deque(maxlen=100)
pe_price_history = deque(maxlen=100)
ce_volume_history = deque(maxlen=100)
pe_volume_history = deque(maxlen=100)
ce_oi_history = deque(maxlen=100)
pe_oi_history = deque(maxlen=100)

timeframe_history = {
    "1m": deque(maxlen=60),
    "5m": deque(maxlen=60),
    "15m": deque(maxlen=60)
}

# ============================================================
# HOOKS & MOCK FUNCTIONS
# ============================================================
def save_tick(token, ltp, vol, bid, ask, oi):
    """Saves incoming tick data to temporary memory or DB log matrix."""
    pass

def push_tick_callback(tick_data):
    """Dispatches real-time structured updates down WebSocket pipelines to active browser clients."""
    pass

def run_signal_engine(ce_price, pe_price, ce_p_hist, pe_p_hist, ce_v_hist, pe_v_hist):
    """Calculates alpha trading indicator criteria for Nifty call/put options."""
    logger.info(f"Signal Engine evaluated at CE: {ce_price} | PE: {pe_price}")

# ============================================================
# WEBSOCKET CALLBACKS
# ============================================================
def on_ws_open(wsapp):
    global sws
    logger.info("WebSocket opened")
    if sws and CE_TOKEN and PE_TOKEN:
        try:
            # ExchangeType 2 refers to NFO (National Stock Exchange Futures & Options)
            sws.subscribe("tradeguru_001", 1, [{"exchangeType": 2, "tokens": [CE_TOKEN, PE_TOKEN]}])
            logger.info(f"Subscribed to CE={CE_TOKEN}, PE={PE_TOKEN}")
        except Exception as e:
            logger.error(f"Subscribe error: {e}")

def on_ws_error(ws, error):
    logger.error(f"WebSocket Error occurred: {error}")

def on_ws_close(wsapp, close_status_code, close_msg):
    global ws_running
    ws_running = False
    logger.warning(f"WebSocket Disconnected: Code {close_status_code} | Msg: {close_msg}")

def on_ws_data(wsapp, message, *args):
    global tick_counter, last_tick_time, latest_ticks, ce_price_history, pe_price_history
    global ce_volume_history, pe_volume_history, ce_oi_history, pe_oi_history, last_minute_snapshot

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
            token = str(tick.get("token") or tick.get("tk"))
            
            # Skip evaluation loop if packet elements belong to untracked instruments
            if token != CE_TOKEN and token != PE_TOKEN:
                continue

            ltp = tick.get("ltp") or tick.get("last_traded_price", 0)
            if isinstance(ltp, (int, float)) and ltp > 1000:
                ltp = ltp / 100  # Normalizes structural anomalies matching AngelOne formatting points
                
            vol = tick.get("v") or tick.get("volume_trade_for_the_day", 0)
            bid = tick.get("bp") or tick.get("best_bid_price", 0)
            ask = tick.get("sp") or tick.get("best_ask_price", 0)
            oi = tick.get("oi") or tick.get("open_interest", 0)

            tick_data = {
                "token": token, "ltp": ltp, "volume": vol,
                "bid": bid, "ask": ask, "oi": oi
            }

            if token == CE_TOKEN:
                latest_ticks["ce_price"] = ltp
                latest_ticks["ce_volume"] = vol
                latest_ticks["ce_bid"] = bid
                latest_ticks["ce_ask"] = ask
                latest_ticks["ce_oi"] = oi
                ce_price_history.append(ltp)
                ce_volume_history.append(vol)
                ce_oi_history.append(oi)
            elif token == PE_TOKEN:
                latest_ticks["pe_price"] = ltp
                latest_ticks["pe_volume"] = vol
                latest_ticks["pe_bid"] = bid
                latest_ticks["pe_ask"] = ask
                latest_ticks["pe_oi"] = oi
                pe_price_history.append(ltp)
                pe_volume_history.append(vol)
                pe_oi_history.append(oi)

            tick_counter += 1
            save_tick(token, ltp, vol, bid, ask, oi)
            
            if 'push_tick_callback' in globals() and push_tick_callback:
                push_tick_callback(tick_data)

            # Prevent zero divisions or initial edge cases if one option lag-loads ahead of the other
            if latest_ticks["ce_price"] == 0.0 or latest_ticks["pe_price"] == 0.0:
                continue

            now = time.time()
            if now - last_minute_snapshot["time"] >= 60:
                avg_price = (latest_ticks["ce_price"] + latest_ticks["pe_price"]) / 2
                avg_vol = (latest_ticks["ce_volume"] + latest_ticks["pe_volume"]) / 2
                snap = {"time": now, "price": avg_price, "volume": avg_vol}
                for tf in timeframe_history:
                    timeframe_history[tf].append(snap)
                last_minute_snapshot["time"] = now
                last_minute_snapshot["price"] = avg_price

            if tick_counter % 5 == 0 and len(ce_price_history) >= 30 and len(pe_price_history) >= 30:
                run_signal_engine(
                    latest_ticks["ce_price"], latest_ticks["pe_price"],
                    list(ce_price_history), list(pe_price_history),
                    list(ce_volume_history), list(pe_volume_history)
                )
    except Exception as e:
        logger.error(f"WebSocket data error: {e}", exc_info=True)

# ============================================================
# HTTP FLASK ENDPOINTS
# ============================================================
@app.route("/")
def index():
    return jsonify({"status": "healthy", "service": "nifty-signals"}), 200

@app.route("/api/live-signals")
def live_signals():
    return jsonify({
        "timestamp": time.time(),
        "latest_ticks": latest_ticks,
        "tick_count": tick_counter
    }), 200

# ============================================================
# APPLICATION INITIALIZATION ENGINE
# ============================================================
def start_websocket():
    """Initializes and runs the custom streaming service pipeline binding handlers securely."""
    global sws, ws_running
    
    # IMPORT RISK WARNING: Make sure your underlying library import matches this exact handle instantiation
    # from SmartConnect import SmartWebSocketV2 
    try:
        # Example Initialization (Change variables to match your configuration parameters)
        # sws = SmartWebSocketV2(AUTH_TOKEN, API_KEY, CLIENT_CODE, FEED_TOKEN)
        
        # This acts as a mock object configuration interface reflecting structural assignments safely
        class MockWebSocket:
            def subscribe(self, client, mode, tokens): pass
            def connect(self): pass
        
        if sws is None:
            sws = MockWebSocket()
            
        # Hook callback routes to global function references now defined explicitly above
        sws.on_open = on_ws_open
        sws.on_error = on_ws_error
        sws.on_close = on_ws_close
        sws.on_data = on_ws_data
        
        logger.info("Initializing WebSocket background event loop registration...")
        ws_running = True
        # sws.connect() # Uncomment when utilizing live underlying driver connection models
        
    except Exception as e:
        logger.error(f"Failed to cleanly spin up WebSocket infrastructure components: {e}")

# Spin up WebSocket connection handling dynamically across distinct threads without choking Gunicorn worker flows
ws_thread = threading.Thread(target=start_websocket, daemon=True)
ws_thread.start()

if __name__ == "__main__":
    # Fallback configuration for running locally
    app.run(host="0.0.0.0", port=10000)
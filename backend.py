# backend.py - Nifty Options Signal Engine (Fixed contract selection)

import os
import threading
import time
import logging
import requests
import csv
import asyncio
from collections import deque
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS
from dhanhq import marketfeed

# --------------------------------------------------
# Logging & Flask
# --------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app

# --------------------------------------------------
# Proxy debug endpoint
# --------------------------------------------------
@app.route('/debug/static-ip')
def debug_my_ip():
    import requests
    proxy_url = os.getenv("QUOTAGUARDSTATIC_URL")
    proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
    r = requests.get('https://ip.quotaguard.com', proxies=proxies, timeout=10)
    if r.status_code == 200:
        return r.text
    return "Failed", 500

# --------------------------------------------------
# Environment variables
# --------------------------------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

# Static fallback IDs (will be replaced if dynamic works)
LAST_KNOWN_CE = "35000"
LAST_KNOWN_PE = "35001"
SELECTED_CE_ID = LAST_KNOWN_CE
SELECTED_PE_ID = LAST_KNOWN_PE

# --------------------------------------------------
# Dynamic contract selection (flexible filtering)
# --------------------------------------------------
def update_contracts():
    global SELECTED_CE_ID, SELECTED_PE_ID, LAST_KNOWN_CE, LAST_KNOWN_PE
    try:
        url = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        lines = response.text.splitlines()
        reader = csv.DictReader(lines)
        
        opts = []
        for row in reader:
            # 1. Use 'D' for Derivatives (FNO)
            if row.get("SEGMENT") != "D":
                continue
            
            # 2. Match OPTIDX
            if row.get("INSTRUMENT") != "OPTIDX":
                continue
            
            # 3. Ensure it is NIFTY but NOT BANKNIFTY or FINNIFTY
            symbol = row.get("SYMBOL_NAME") or row.get("SYMBOL") or ""
            if "NIFTY" not in symbol or "BANK" in symbol or "FIN" in symbol:
                continue

            # 4. Extract Expiry
            expiry_str = row.get("SM_EXPIRY_DATE") or row.get("EXPIRY_DATE")
            if not expiry_str:
                continue
            try:
                # Dhan date format is usually '2026-06-30' or '30-Jun-2026'
                # Let's try both common formats
                try:
                    expiry = datetime.strptime(expiry_str, "%Y-%m-%d")
                except:
                    expiry = datetime.strptime(expiry_str, "%d-%b-%Y")
            except:
                continue

            # 5. Extract Strike
            try:
                strike = float(row.get("STRIKE_PRICE") or row.get("STRIKE", 0))
            except:
                continue

            opts.append({
                "expiry": expiry,
                "strike": strike,
                "option_type": row.get("OPTION_TYPE", ""),
                "security_id": row.get("SECURITY_ID", "")
            })

        if not opts:
            raise ValueError("No NIFTY rows matched filters (Segment 'D', Inst 'OPTIDX')")

        # --- Rest of logic remains the same ---
        # Find nearest expiry
        min_expiry = min(opts, key=lambda x: x["expiry"])["expiry"]
        near_opts = [o for o in opts if o["expiry"] == min_expiry]

        # Fetch Spot (Using fallback 24000 if NSE blocks Render)
        spot = 24000.0 
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            s = requests.Session()
            s.get("https://www.nseindia.com", headers=headers, timeout=5)
            r = s.get("https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050", headers=headers, timeout=5)
            spot = float(r.json()["data"][0]["lastPrice"])
        except:
            logger.warning("NSE Spot fetch blocked (common on Render), using 24000.0")

        # Find ATM strike
        strikes = sorted(set(o["strike"] for o in near_opts))
        atm_strike = min(strikes, key=lambda x: abs(x - spot))

        ce_id = next((o["security_id"] for o in near_opts if o["strike"] == atm_strike and o["option_type"] == "CE"), None)
        pe_id = next((o["security_id"] for o in near_opts if o["strike"] == atm_strike and o["option_type"] == "PE"), None)

        if ce_id and pe_id:
            SELECTED_CE_ID = str(int(float(ce_id)))
            SELECTED_PE_ID = str(int(float(pe_id)))
            LAST_KNOWN_CE, LAST_KNOWN_PE = SELECTED_CE_ID, SELECTED_PE_ID
            logger.info(f"✅ Success! CE={SELECTED_CE_ID}, PE={SELECTED_PE_ID} at Strike {atm_strike}")
            return True
        
    except Exception as e:
        logger.error(f"Contract update failed: {e}")
        SELECTED_CE_ID, SELECTED_PE_ID = LAST_KNOWN_CE, LAST_KNOWN_PE
        return False

# Run initial contract update
update_contracts()

# --------------------------------------------------
# Global state (same as before)
# --------------------------------------------------
latest_data = {
    "signal": "WAITING",
    "ce_price": 0.0,
    "pe_price": 0.0,
    "spread": 0.0,
    "rsi": 50,
    "macd": 0.0,
    "pcr": 1.0,
    "timestamp": ""
}
market_state = {
    "rsi": 50,
    "momentum": "NEUTRAL",
    "strength": "LOW",
    "trend": "SIDEWAYS",
    "action": "HOLD",
    "confidence": 0,
    "volatility": "NORMAL",
    "alert": "NONE"
}
institutional_state = {
    "vwap": 0,
    "ema_fast": 0,
    "ema_slow": 0,
    "ema_signal": "NEUTRAL",
    "atr": 0,
    "oi_buildup": "NEUTRAL",
    "iv_state": "NORMAL",
    "candle_structure": "SIDEWAYS",
    "market_breadth": "BALANCED",
    "volume_profile": "NORMAL",
    "smart_money_flow": "NEUTRAL",
    "delta": 0,
    "gamma": 0,
    "theta": 0,
    "vega": 0,
    "institutional_signal": "HOLD",
    "institutional_confidence": 0
}
price_history = deque(maxlen=200)
volume_history = deque(maxlen=200)
tick_counter = 0
UPDATE_INTERVAL = 10
SPREAD_THRESHOLD = 5.0

# --------------------------------------------------
# Technical indicators (pure Python)
# --------------------------------------------------
def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calculate_macd(prices, fast=12, slow=26):
    if len(prices) < slow:
        return 0.0
    def ema(data, period):
        alpha = 2 / (period + 1)
        value = data[0]
        for p in data[1:]:
            value = alpha * p + (1 - alpha) * value
        return value
    return ema(prices, fast) - ema(prices, slow)

def calculate_ema(prices, period):
    if len(prices) < period:
        return prices[-1] if prices else 0
    alpha = 2 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = alpha * p + (1 - alpha) * ema
    return round(ema, 2)

def calculate_atr(prices, period=14):
    if len(prices) < period + 1:
        return 0
    trs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    return round(sum(trs[-period:]) / period, 2)

def calculate_vwap(prices, volumes):
    if not prices or not volumes or len(prices) != len(volumes):
        return 0
    pv = sum(p * v for p, v in zip(prices, volumes))
    tv = sum(volumes)
    return round(pv / tv, 2) if tv else 0

def estimate_greeks(ce, pe):
    delta = round((ce - pe) / 100, 2)
    gamma = round(abs(delta) / 10, 2)
    theta = round(-(ce + pe) / 1000, 2)
    vega = round((ce + pe) / 500, 2)
    return delta, gamma, theta, vega

def analyze_oi_buildup(pcr):
    if pcr < 0.8: return "LONG BUILDUP"
    if pcr > 1.2: return "SHORT BUILDUP"
    return "NEUTRAL"

def detect_iv_crush(spread):
    if abs(spread) < 5: return "IV CRUSH"
    if abs(spread) > 20: return "HIGH IV"
    return "NORMAL"

def detect_candle_structure(prices):
    if len(prices) < 5: return "SIDEWAYS"
    if prices[-1] > prices[-5]: return "BULLISH"
    if prices[-1] < prices[-5]: return "BEARISH"
    return "SIDEWAYS"

def market_breadth_analysis(pcr):
    if pcr < 0.9: return "BULLISH"
    if pcr > 1.1: return "BEARISH"
    return "BALANCED"

def volume_profile_analysis(prices):
    if len(prices) < 10: return "NORMAL"
    volatility = max(prices[-10:]) - min(prices[-10:])
    return "HIGH" if volatility > 20 else "LOW"

def smart_money_analysis(spread, pcr):
    if spread > 10 and pcr < 0.8: return "SMART MONEY BUYING"
    if spread < -10 and pcr > 1.2: return "SMART MONEY SELLING"
    return "NEUTRAL"

def multi_tf_confirmation(rsi):
    if rsi > 60: return "BULLISH CONFIRMATION"
    if rsi < 40: return "BEARISH CONFIRMATION"
    return "NO CONFIRMATION"

# --------------------------------------------------
# PCR caching
# --------------------------------------------------
pcr_cache = {"value": 1.0, "time": 0}
PCR_TTL = 60

def get_nifty_pcr():
    now = time.time()
    if now - pcr_cache["time"] < PCR_TTL:
        return pcr_cache["value"]
    url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        s = requests.Session()
        s.get("https://www.nseindia.com", headers=headers, timeout=5)
        time.sleep(0.5)
        r = s.get(url, headers=headers, timeout=5)
        data = r.json()
        ce_oi = sum(x.get("CE", {}).get("openInterest", 0) for x in data["records"]["data"] if "CE" in x)
        pe_oi = sum(x.get("PE", {}).get("openInterest", 0) for x in data["records"]["data"] if "PE" in x)
        value = pe_oi / ce_oi if ce_oi else 1.0
        pcr_cache["value"] = value
        pcr_cache["time"] = now
        return value
    except Exception as e:
        logger.error(f"PCR fetch failed: {e}")
        return pcr_cache["value"]

# --------------------------------------------------
# Advanced analysis (called periodically)
# --------------------------------------------------
def run_advanced_analysis(ce, pe, spread, pcr, price_list, volume_list):
    global market_state, institutional_state
    if len(price_list) < 14:
        return

    rsi = calculate_rsi(price_list)
    macd = calculate_macd(price_list)
    vwap = calculate_vwap(price_list, volume_list)
    ema_fast = calculate_ema(price_list, 9)
    ema_slow = calculate_ema(price_list, 21)
    ema_signal = "BULLISH" if ema_fast > ema_slow else "BEARISH"
    atr = calculate_atr(price_list)
    delta, gamma, theta, vega = estimate_greeks(ce, pe)

    oi = analyze_oi_buildup(pcr)
    iv = detect_iv_crush(spread)
    candle = detect_candle_structure(price_list)
    breadth = market_breadth_analysis(pcr)
    vol_profile = volume_profile_analysis(price_list)
    smart_money = smart_money_analysis(spread, pcr)
    multi_tf = multi_tf_confirmation(rsi)

    confidence = 0
    if ema_signal == "BULLISH": confidence += 15
    if rsi > 60: confidence += 15
    if smart_money == "SMART MONEY BUYING": confidence += 20
    if breadth == "BULLISH": confidence += 15
    if candle == "BULLISH": confidence += 10
    if vol_profile == "HIGH": confidence += 10
    if multi_tf == "BULLISH CONFIRMATION": confidence += 15

    if confidence >= 70:
        action = "STRONG BUY CE"
    elif confidence >= 50:
        action = "BUY CE"
    elif confidence <= 25:
        action = "EXIT"
    else:
        action = "HOLD"

    market_state.update({
        "rsi": round(rsi, 2),
        "momentum": "UPTREND" if spread > 0 else "DOWNTREND",
        "strength": "HIGH" if confidence > 60 else "LOW",
        "trend": ema_signal,
        "action": action,
        "confidence": confidence,
        "volatility": "HIGH" if abs(spread) > 20 else "NORMAL",
        "alert": "BUY" if action in ("BUY CE", "STRONG BUY CE") else "EXIT" if action == "EXIT" else "HOLD"
    })

    institutional_state.update({
        "vwap": vwap,
        "ema_fast": ema_fast,
        "ema_slow": ema_slow,
        "ema_signal": ema_signal,
        "atr": atr,
        "oi_buildup": oi,
        "iv_state": iv,
        "candle_structure": candle,
        "market_breadth": breadth,
        "volume_profile": vol_profile,
        "smart_money_flow": smart_money,
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "institutional_signal": action,
        "institutional_confidence": confidence
    })

# --------------------------------------------------
# WebSocket callback
# --------------------------------------------------
def on_message(instance, tick):
    global latest_data, price_history, volume_history, tick_counter
    try:
        sec_id = str(tick.get("security_id"))
        price = float(tick.get("ltp", 0))
        volume = int(tick.get("volume", 0))

        if sec_id == SELECTED_CE_ID:
            latest_data["ce_price"] = price
            if price > 0:
                price_history.append(price)
                volume_history.append(volume)
        elif sec_id == SELECTED_PE_ID:
            latest_data["pe_price"] = price

        if latest_data["ce_price"] > 0 and latest_data["pe_price"] > 0:
            ce = latest_data["ce_price"]
            pe = latest_data["pe_price"]
            spread = ce - pe
            latest_data["spread"] = round(spread, 2)

            if spread > SPREAD_THRESHOLD:
                latest_data["signal"] = "BULLISH"
            elif spread < -SPREAD_THRESHOLD:
                latest_data["signal"] = "BEARISH"
            else:
                latest_data["signal"] = "NEUTRAL"

            tick_counter += 1
            if tick_counter >= UPDATE_INTERVAL and len(price_history) >= 20:
                tick_counter = 0
                rsi_val = calculate_rsi(list(price_history))
                macd_val = calculate_macd(list(price_history))
                pcr_val = get_nifty_pcr()
                latest_data["rsi"] = round(rsi_val, 2)
                latest_data["macd"] = round(macd_val, 2)
                latest_data["pcr"] = round(pcr_val, 2)

                run_advanced_analysis(ce, pe, spread, pcr_val,
                                      list(price_history), list(volume_history))

            latest_data["timestamp"] = datetime.now().isoformat()
    except Exception as e:
        print(f"on_message error: {e}")

def on_connect(instance):
    print("✅ WebSocket connected and authorized")

def on_error(instance, error):
    print(f"❌ WebSocket error: {error}")

def on_close(instance):
    print("🔌 WebSocket closed, reconnecting...")

# --------------------------------------------------
# Feed runner with explicit event loop
# --------------------------------------------------
def run_feed():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    while True:
        try:
            update_contracts()
            print(f"Subscribing to CE={SELECTED_CE_ID}, PE={SELECTED_PE_ID} (Quote for volume)")
            feed = marketfeed.DhanFeed(
                client_id=CLIENT_ID,
                access_token=ACCESS_TOKEN,
                instruments=[
                    (marketfeed.NSE_FNO, str(SELECTED_CE_ID), marketfeed.Quote),
                    (marketfeed.NSE_FNO, str(SELECTED_PE_ID), marketfeed.Quote)
                ]
            )
            feed.on_connect = on_connect
            feed.on_error = on_error
            feed.on_close = on_close
            feed.on_message = on_message
            feed.run_forever()
        except Exception as e:
            print(f"Feed crashed: {e}, reconnecting in 10s")
            time.sleep(10)

# --------------------------------------------------
# Start background thread
# --------------------------------------------------
thread = threading.Thread(target=run_feed, daemon=True)
thread.start()
print("Background signal engine started (dynamic rollover + real volume)")

# --------------------------------------------------
# Flask routes
# --------------------------------------------------
@app.route("/")
def home():
    return jsonify({
        "status": "active",
        "data": latest_data,
        "market": market_state,
        "institutional": institutional_state
    })

@app.route("/api/health")
def health():
    return "OK", 200

@app.route("/debug/version")
def debug_version():
    return f"Stable version (no pandas) | CE={SELECTED_CE_ID} PE={SELECTED_PE_ID}"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
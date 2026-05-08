# backend.py - Advanced Nifty 50 Futures Sentiment & Signal Engine

import os
import asyncio
import pandas as pd
import time
from typing import List, Tuple, Optional
from datetime import datetime
import logging

# Dhan SDK imports
from dhanhq import DhanContext, dhanhq, marketfeed

# Third-party technical analysis libraries
try:
    from technopulse import RSI, MACD, SMA  # For robust technical indicators
    TECH_AVAILABLE = True
    logging.info("Technopulse library loaded successfully.")
except ImportError:
    TECH_AVAILABLE = False
    logging.warning("Technopulse library not found. Install with: pip install technopulse")

try:
    from option_chain_analyzer import OptionChainAnalyzer  # For sentiment analysis
    PCR_AVAILABLE = True
    logging.info("Option Chain Analyzer loaded successfully.")
except ImportError:
    PCR_AVAILABLE = False
    logging.warning("Option Chain Analyzer not found. Install from GitHub: https://github.com/Yashghodinde/option_chain_analyser")


# ===================================================
# 1. CONFIGURATION (From Environment Variables)
# ===================================================
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
CONTRACT_TYPE = os.getenv("CONTRACT_TYPE", "FUTURES")
CURRENT_NIFTY = int(os.getenv("CURRENT_NIFTY", "0"))

# Validate essential secrets are set
if not CLIENT_ID:
    raise ValueError("Environment variable DHAN_CLIENT_ID not set.")
if not ACCESS_TOKEN:
    raise ValueError("Environment variable DHAN_ACCESS_TOKEN not set.")

# Trading Signal Parameters (can be tuned)
SPREAD_THRESHOLD = 5.0          # Minimum spread to consider for signal
UPPER_PCR = 1.2                 # PCR above this suggests overbought/bearish warning
LOWER_PCR = 0.8                 # PCR below this suggests oversold/bullish warning

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s – %(levelname)s – %(message)s')
logger = logging.getLogger(__name__)


# ===================================================
# 2. DYNAMIC INSTRUMENT SELECTION (Same as before)
# ===================================================
def get_extreme_contract_ids(contract_type: str = "FUTURES", current_price: int = None) -> Tuple[List[str], List[dict]]:
    """Fetch instrument master and select two nearest expiry Futures contracts."""

    dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
    dhan = dhanhq(dhan_context)

    logger.info("Downloading instrument master...")
    instrument_df = dhan.fetch_security_list("detailed")
    logger.info(f"Loaded {len(instrument_df)} instruments.")

    fno_df = instrument_df[instrument_df["SEGMENT"] == "NSE_FNO"]
    if fno_df.empty:
        raise ValueError("No NSE_FNO segment data found.")

    if contract_type == "FUTURES":
        futures = fno_df[fno_df["INSTRUMENT"] == "FUTIDX"]
        if futures.empty:
            raise ValueError("No FUTIDX (Index Futures) found.")
        futures = futures.copy()
        futures.loc[:, "EXPIRY_DT"] = pd.to_datetime(futures["SEM_EXPIRY_DATE"], format="%d-%b-%y", errors="coerce")
        futures = futures.dropna(subset=["EXPIRY_DT"])
        futures_sorted = futures.sort_values("EXPIRY_DT")
        nearest_two = futures_sorted.head(2)
        selected_ids = [str(row["SECURITY_ID"]) for _, row in nearest_two.iterrows()]
        selected_details = [{"security_id": sid} for sid in selected_ids]
        logger.info(f"Selected Futures Security IDs: {selected_ids}")
        return selected_ids, selected_details
    else:
        # For options, we would need a more complex selection logic.
        # Returning empty for now.
        logger.error("Options contract type not implemented in this version.")
        return [], []



# ===================================================
# 3. CUSTOM WEBSOCKET HANDLER (With Market Sentiment)
# ===================================================
class SignalFeedHandler(marketfeed.MarketFeed):
    """
    Advanced Signal Generation Engine with Multi-Factor Sentiment Analysis.
    """

    def __init__(self, dhan_context, instruments, version):
        super().__init__(dhan_context, instruments, version)
        self.ticker_data = {}
        self.price_history = []  # For storing incoming ticks
        self.RSI_period = 14
        self.MACD_fast = 12
        self.MACD_slow = 26
        self.MACD_signal = 9
        self.SMA_short = 20
        self.SMA_long = 50
        self.last_signal = None
        # Initialize PCR Analyzer if available
        if PCR_AVAILABLE:
            self.pcr_analyzer = OptionChainAnalyzer(underlying="NIFTY")
        logger.info("SignalFeedHandler initialized with sentiment logic.")

    def on_ticker(self, ticker_data):
        """Process incoming ticks and generate sentiment signals"""
        # Store ticker data
        security_id = ticker_data.get('security_id')
        if 'ltp' in ticker_data and security_id is not None:
            ltp = ticker_data['ltp']
            self.ticker_data[security_id] = {"ltp": float(ltp), "ts": time.time()}
            # Add to price history
            self.price_history.append({"price": float(ltp), "ts": time.time()})
        # Trim history to last 200 ticks
        if len(self.price_history) > 200:
            self.price_history = self.price_history[-200:]

        # Generate signal if we have both contracts
        if len(self.ticker_data) >= 2:
            self.generate_signal()

    def calculate_rsi(self, prices, period=14):
        """Calculate RSI for a series of prices"""
        if len(prices) < period + 1 or not TECH_AVAILABLE:
            # Fallback: simple implementation
            deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
            gains = [d if d > 0 else 0 for d in deltas]
            losses = [-d if d < 0 else 0 for d in deltas]
            avg_gain = sum(gains[:period]) / period
            avg_loss = sum(losses[:period]) / period
            if avg_loss == 0:
                return 100.0
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            return rsi
        else:
            # Use technopulse library
            close_prices = pd.Series(prices)
            rsi_indicator = RSI(close_prices, period)
            return rsi_indicator.values.iloc[-1] if len(rsi_indicator) > 0 else 50

    def calculate_macd(self, prices):
        """Calculate MACD line, signal line, and histogram for a series of prices"""
        if len(prices) < self.MACD_slow + self.MACD_signal or not TECH_AVAILABLE:
            # Fallback: simplistic calculation
            return {"macd_line": 0, "signal_line": 0, "histogram": 0}
        else:
            close_prices = pd.Series(prices)
            macd_indicator = MACD(close_prices, self.MACD_fast, self.MACD_slow, self.MACD_signal)
            macd_line = macd_indicator.macd.values.iloc[-1] if len(macd_indicator.macd) > 0 else 0
            signal_line = macd_indicator.signal.values.iloc[-1] if len(macd_indicator.signal) > 0 else 0
            histogram = macd_indicator.histogram.values.iloc[-1] if len(macd_indicator.histogram) > 0 else 0
            return {"macd_line": macd_line, "signal_line": signal_line, "histogram": histogram}

    def get_put_call_ratio(self):
        """Fetch live Put/Call Ratio from option chain"""
        if PCR_AVAILABLE and self.pcr_analyzer is not None:
            try:
                # Use the analyzer to fetch PCR
                pcr = self.pcr_analyzer.calculate_pcr()
                return pcr
            except Exception as e:
                logger.warning(f"Could not fetch PCR: {e}")
        return None

    def generate_signal(self):
        """Consolidate indicators and decide on LONG/SHORT/NEUTRAL"""
        if len(self.ticker_data) < 2:
            return

        try:
            # --- 1. Spread Component ---
            ids = list(self.ticker_data.keys())
            ltp1 = self.ticker_data[ids[0]]["ltp"]
            ltp2 = self.ticker_data[ids[1]]["ltp"]
            spread = ltp1 - ltp2

            # --- 2. Technical Indicators (from price history) ---
            prices = [p["price"] for p in self.price_history]
            rsi = self.calculate_rsi(prices, self.RSI_period) if prices else 50
            macd = self.calculate_macd(prices) if prices else {"histogram": 0}
            macd_bullish = macd["histogram"] > 0
            macd_bearish = macd["histogram"] <= 0

            # --- 3. Sentiment Analysis (PCR) ---
            pcr = self.get_put_call_ratio()
            if pcr is None:
                pcr = 1.0  # neutral fallback

            # Determine market sentiment
            bullish_sentiment = rsi < 70 and macd_bullish and pcr < LOWER_PCR
            bearish_sentiment = rsi > 30 and macd_bearish and pcr > UPPER_PCR

            # --- 4. Final Signal Logic ---
            signal = None
            strength = 0
            if spread > SPREAD_THRESHOLD and bullish_sentiment:
                signal = "LONG SPREAD"
                strength = min(100, int(abs(spread / SPREAD_THRESHOLD) * 30))
            elif spread < -SPREAD_THRESHOLD and bearish_sentiment:
                signal = "SHORT SPREAD"
                strength = min(100, int(abs(spread / SPREAD_THRESHOLD) * 30))
            else:
                signal = "NEUTRAL"
                strength = 0

            # Avoid repetitive logging if signal unchanged
            if signal != self.last_signal:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"[SIGNAL] {timestamp} | IDs: {ids} | Spread: {spread:.2f} | Action: {signal} (Strength {strength}) | RSI: {rsi:.2f} | PCR: {pcr:.2f} | MACD: {macd['histogram']:.2f}")
                self.last_signal = signal
            else:
                logger.debug(f"Spread: {spread:.2f} | RSI: {rsi:.2f} | PCR: {pcr:.2f}")

        except Exception as e:
            logger.error(f"Error in generate_signal: {e}")


# ===================================================
# 4. MAIN EXECUTION ENTRY POINT
# ===================================================
async def main():
    """Initializes the WebSocket feed and starts signal generation"""
    try:
        logger.info("Starting Nifty 50 Futures Sentiment Engine...")
        security_ids, _ = get_extreme_contract_ids(CONTRACT_TYPE, CURRENT_NIFTY)

        if len(security_ids) < 2:
            logger.error("Failed to obtain two valid contract IDs. Exiting.")
            return

        # Build instruments list for MarketFeed
        instruments = [(marketfeed.NSE_FNO, sid, marketfeed.Ticker) for sid in security_ids]

        dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
        logger.info(f"Subscribing to instruments {security_ids}")
        feed = SignalFeedHandler(dhan_context, instruments, version="v1")

        await feed.connect()
        await feed.subscribe_instruments()
        logger.info("✅ WebSocket connected, market feed active! Waiting for signals...")

        # Keep loop alive
        while True:
            await asyncio.sleep(2)

    except KeyboardInterrupt:
        logger.info("Shutdown signal received. Disconnecting...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        try:
            await feed.disconnect()
        except:
            pass
        logger.info("Application closed.")


if __name__ == "__main__":
    asyncio.run(main())
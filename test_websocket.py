import logging
from dhanhq import DhanContext, MarketFeed

# Set your credentials here
CLIENT_ID = "1103060314"         # <---- Replace with your actual client ID
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJwX2lwIjoiMjQwMTo0OTAwOjhmYzI6MTA0MjoyZDNkOjhkMGY6NTVhYTpjYjFmIiwic19pcCI6IjMuNy4yNDIuMTg0IiwiaXNzIjoiZGhhbiIsInBhcnRuZXJJZCI6IiIsImV4cCI6MTc3OTA5OTI0NywiaWF0IjoxNzc5MDEyODQ3LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiaHR0cHM6Ly9uaWZ0eS5saXZlYmxvZzM2NS5jb20vbmlmdHktc2lnbmFsLyIsImRoYW5DbGllbnRJZCI6IjExMDMwNjAzMTQifQ.J4YKb1nmeVIMNgxFncdml0E-zh8ltaNIILSj95_64cZxz6TP_LeOwJHV-SRDGIaKLG3-ZpgOQsh_lccRBQfi9w"   # <---- Replace with your actual access token

# Set up logging to see what's happening
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def on_connect(instance):
    logger.info("✅ WebSocket Connected Successfully!")

def on_message(instance, tick):
    logger.info(f"📊 TICK RECEIVED: {tick}")

def on_error(instance, error):
    logger.error(f"❌ WebSocket Error: {error}")

def on_close(instance):
    logger.warning("🔌 WebSocket Connection Closed")

# Create instruments list
# Use your contract's actual security ID (CE_ID or PE_ID) here
CE_ID = "23521"  # <---- Replace with your actual CE security ID
PE_ID = "23521"  # <---- Replace with your actual PE security ID

instruments = [
    (MarketFeed.NSE_FNO, CE_ID, MarketFeed.Ticker),
    (MarketFeed.NSE_FNO, PE_ID, MarketFeed.Ticker)
]

# Initialize Dhan context and feed
ctx = DhanContext(CLIENT_ID, ACCESS_TOKEN)
feed = MarketFeed(ctx, instruments, version="v2")

# Set callbacks
feed.on_connect = on_connect
feed.on_message = on_message
feed.on_error = on_error
feed.on_close = on_close

# Run the feed
logger.info("Starting WebSocket feed...")
feed.run_forever()
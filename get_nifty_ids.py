from dhanhq import marketfeed

CLIENT_ID = "your_client_id_here"
ACCESS_TOKEN = "your_fresh_token_here"
CE_ID = "51348"
PE_ID = "51349"

def on_connect(instance):
    print("✅ WebSocket connected")

def on_error(instance, err):
    print(f"❌ Error: {err}")

def on_close(instance):
    print("🔌 Closed")

def on_message(instance, tick):
    print(f"Tick: {tick}")

feed = marketfeed.DhanFeed(
    client_id=CLIENT_ID,
    access_token=ACCESS_TOKEN,
    instruments=[
        (marketfeed.NSE_FNO, CE_ID, marketfeed.Ticker),
        (marketfeed.NSE_FNO, PE_ID, marketfeed.Ticker)
    ]
)
feed.on_connect = on_connect
feed.on_error = on_error
feed.on_close = on_close
feed.on_message = on_message
print("Starting feed...")
feed.run_forever()
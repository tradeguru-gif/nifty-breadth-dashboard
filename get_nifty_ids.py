from dhanhq import marketfeed

CLIENT_ID = "1103060314"
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc4NzkzMTU0LCJpYXQiOjE3Nzg3MDY3NTQsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAzMDYwMzE0In0.h6f0FsjRIvmWYZ3f-npARoIxsYj8xjudx6EOUqMrF7BeDYhT90tDEjJwUWQ8EOmkDIb7yYugPTgYF0EoRED_PA"
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
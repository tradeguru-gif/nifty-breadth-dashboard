import datetime
import time
import pyotp
from smartapi import SmartConnect

# Your fixed credentials
API_KEY = os.environ.get("ANGEL_API_KEY")
CLIENT_CODE = os.environ.get("ANGEL_CLIENT_CODE")
PASSWORD_PIN = os.environ.get("ANGEL_MPIN")
TOTP_SECRET = os.environ.get("ANGEL_TOTP_SECRET")

def login_to_angel_one():
    obj = SmartConnect(api_key=API_KEY)
    totp = pyotp.TOTP(TOTP_SECRET)
    current_totp_pin = totp.now()
    
    print(f"Generating fresh morning session... Live TOTP pin is: {current_totp_pin}")
    session_data = obj.generateSession(CLIENT_CODE, PASSWORD_PIN, current_totp_pin)
    
    if session_data.get('status') is True:
        print("Successfully authenticated with Angel One!")
        return obj, session_data['data']['feedToken']
    else:
        print(f"Login failed: {session_data.get('message')}")
        return None, None

# --- Core Loop running on your Paid Render Server ---
authenticated = False
smart_conn = None
live_feed_token = None

while True:
    now = datetime.datetime.now().time()
    
    # 1. Wait until it's close to your target time to authenticate
    if datetime.time(9, 25) <= now < datetime.time(9, 30) and not authenticated:
        smart_conn, live_feed_token = login_to_angel_one()
        if smart_conn:
            authenticated = True
            
    # 2. Your signal processing kicks off at 9:35 AM
    if now >= datetime.time(9, 35) and datetime.time(9, 35) <= now < datetime.time(15, 30):
        if authenticated:
            print("Waking up at 9:35 AM! Fetching live signals...")
            # --- YOUR CORE TRADING / SIGNAL LOGIC GOES HERE ---
            # (e.g., fetch spot_price, calculate ATM strikes)
            
            time.sleep(1) # Check your live strategy every second
        else:
            print("It's 9:35 AM but authentication failed. Retrying login...")
            smart_conn, live_feed_token = login_to_angel_one()
            if smart_conn: authenticated = True
            time.sleep(5)
            
    # 3. Market is closed, reset authentication flag for the next day
    else:
        if now >= datetime.time(15, 31):
            authenticated = False # Ready to clear out for tomorrow morning
        print("Outside target strategy hours. Sleeping 1 minute...")
        time.sleep(60)
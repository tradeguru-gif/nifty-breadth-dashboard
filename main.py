import datetime
import time
import os  # FIXED: Added missing import to prevent application crash
import pyotp
from smartapi import SmartConnect

# Your fixed credentials loaded securely via Render environment variables
API_KEY = os.environ.get("ANGEL_API_KEY")
CLIENT_CODE = os.environ.get("ANGEL_CLIENT_CODE")
PASSWORD_PIN = os.environ.get("ANGEL_MPIN")
TOTP_SECRET = os.environ.get("ANGEL_TOTP_SECRET")

def login_to_angel_one():
    obj = SmartConnect(api_key=API_KEY)
    totp = pyotp.TOTP(TOTP_SECRET)
    current_totp_pin = totp.now()
    
    print(f"Generating fresh morning session... Live TOTP pin is: {current_totp_pin}")
    try:
        session_data = obj.generateSession(CLIENT_CODE, PASSWORD_PIN, current_totp_pin)
        if session_data.get('status') is True:
            print("Successfully authenticated with Angel One!")
            return obj, session_data['data']['feedToken']
        else:
            print(f"Login failed: {session_data.get('message')}")
            return None, None
    except Exception as e:
        print(f"Exception encountered during Angel One API auth sequence: {str(e)}")
        return None, None

# --- Core Loop running on your Paid Render Server ---
authenticated = False
smart_conn = None
live_feed_token = None

print("Trading Engine Daemon Initialized. Scanning loop active...")

while True:
    now = datetime.datetime.now().time()
    
    # 1. Pre-Market Authentication Windows (09:17 AM to 09:20 AM)
    if datetime.time(9, 17) <= now < datetime.time(9, 20):
        if not authenticated:
            smart_conn, live_feed_token = login_to_angel_one()
            if smart_conn:
                authenticated = True
        time.sleep(10) # Prevent checking every millisecond during the login window
            
    # 2. Live Core Strategy Execution Windows (09:20 AM to 03:30 PM)
    elif datetime.time(9, 20) <= now < datetime.time(15, 30):
        if authenticated:
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Processing live signals...")
            # --- YOUR CORE TRADING / SIGNAL LOGIC GOES HERE ---
            # (e.g., fetch spot_price, calculate ATM strikes)
            
            time.sleep(1) # FIXED: Controls iteration intervals inside trading window
        else:
            print("Execution hour active but session unauthenticated. Executing hot-reconnect...")
            smart_conn, live_feed_token = login_to_angel_one()
            if smart_conn: 
                authenticated = True
            time.sleep(5)
            
    # 3. Off-Market Downtime Windows (03:30 PM to 09:17 AM next day)
    else:
        if now >= datetime.time(15, 31) and authenticated:
            print("Market closed. Dropping active credential tokens for security resetting...")
            authenticated = False # Flush token state for tomorrow morning
            
        print("System outside standard tracking hours. Entering standby sleep state...")
        time.sleep(60) # FIXED: Safely caps execution speeds outside tracking hours
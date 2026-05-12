#Run on powershell command: python get_nifty_ids.py
#ACCESS_TOKEN = #"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc4NTc3NzczLCJpYXQiOjE3Nzg0#OTEzNzMsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAzMDYwMzE0In0.Ebf7WCfb3C###6uGBU1dDYRr_-SGMhuLfytE5iP0YGUYSJUq52VBMApFZrVTF4RK7zPNQy0rg57lDZjanWXzaxVog"   # <-- REPLACE

import requests
import csv
from io import StringIO

ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc4NTc3NzczLCJpYXQiOjE3Nzg0OTEzNzMsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAzMDYwMzE0In0.Ebf7WCfb3C6uGBU1dDYRr_-SGMhuLfytE5iP0YGUYSJUq52VBMApFZrVTF4RK7zPNQy0rg57lDZjanWXzaxVog"   # <-- REPLACE
url = "https://api.dhan.co/v2/instrument/NSE_FNO"
headers = {"access-token": ACCESS_TOKEN}

response = requests.get(url, headers=headers)
if response.status_code != 200:
    print(f"❌ API Error: {response.status_code}")
    exit()

# Read CSV
csv_data = StringIO(response.text)
reader = csv.DictReader(csv_data)

# 1. Show column headers
print("=== COLUMN HEADERS ===")
print(reader.fieldnames)
print()

# 2. Find all NIFTY OPTIDX contracts
print("=== NIFTY OPTIDX CONTRACTS (first 20) ===")
count = 0
for row in reader:
    # Adjust column names based on actual headers
    # Common names: 'SYMBOL_NAME', 'INSTRUMENT', 'OPTION_TYPE', 'SECURITY_ID', 'STRIKE_PRICE', 'SM_EXPIRY_DATE'
    symbol = row.get('SYMBOL_NAME', '')
    instr = row.get('INSTRUMENT', '')
    if 'NIFTY' in symbol and instr == 'OPTIDX':
        print(f"ID: {row['SECURITY_ID']:<10} | Type: {row.get('OPTION_TYPE','N/A')} | Strike: {row.get('STRIKE_PRICE','N/A')} | Expiry: {row.get('SM_EXPIRY_DATE','N/A')}")
        count += 1
        if count >= 20:
            break

if count == 0:
    print("No NIFTY OPTIDX found. Column names might be different.")
    print("Please copy the first column headers line and share it.")
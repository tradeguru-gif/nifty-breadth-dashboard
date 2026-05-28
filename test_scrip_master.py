import requests
import pandas as pd

url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
print(f"Attempting to fetch data from:\n{url}\n")

try:
    print("Downloading scrip master file... (This may take a moment)")
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()
    df = pd.DataFrame(data)
    
    nifty_options = df[df["name"] == "NIFTY"]
    
    if nifty_options.empty:
        print("Error: 'NIFTY' symbols not found in the downloaded file.")
    else:
        print(f"Successfully found {len(nifty_options)} NIFTY option symbols.")
        print("\nFirst 5 NIFTY option symbols (token, symbol, expiry, strike):")
        print(nifty_options[["token", "symbol", "expiry", "strike"]].head())
        
except requests.exceptions.Timeout:
    print("Error: The request to Angel One timed out.")
except requests.exceptions.RequestException as e:
    print(f"Error: An HTTP request error occurred: {e}")
except ValueError:
    print("Error: The response from Angel One was not valid JSON.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
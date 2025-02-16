import requests
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO


# Global varibles
nse_top_10 = ["RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "INFY", "HINDUNILVR", "SBIN", "BHARTIARTL", "ITC", "LICI"]
year = 2024
option_type = ["C", "P"]
headers = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "en-US,en;q=0.7",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "cookie": "PHPSESSID=ajeeq8spmpa5h4tspmq8ald2rm",
    "origin": "https://www.icharts.in",
    "referer": "https://www.icharts.in/opt/OpenHighLowScanOptions.php",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest"
}


# Read the dates from expiry_dates.json
with open('C:\levitas\script\icharts-options-OHLC\expiry_dates.json', 'r') as f:
    expiry_dates = json.load(f)

# Method to get dd from API
def get_dd(sym, expiry_date):
    url = f"https://www.icharts.in/opt/hcharts/stx8req/php/getExpiryTradingDate_Curr.php"
    payload = {
        "ed": expiry_date,
        "sym": sym
    }
    response = requests.post(url, headers=headers, data=payload)
    return response

# Method to get the payload for symbol and expiry date
def get_strikes_payload(symbol, expiry_date, option_type):
    dd = get_dd(symbol, expiry_date)
    payload = {
        "sym": symbol,
        "ed": expiry_date,
        "ot": option_type,
        "dd": dd
    }
    return payload

def get_strikes(symbol, expiry_date, option_type):
    url = "https://www.icharts.in/opt/hcharts/stx8req/php/getStrikesForDateSymExpOT_Currency.php"
    response = requests.post(url, headers=headers, data = get_strikes_payload(symbol, expiry_date, option_type))
    data = response.json()
    strikes =  [item["id"] for item in data]
    return strikes


def get_ohlc_payload(symbol, expiry_date, option_type, strike):
    option_string = f"{symbol}-{strike}{option_type}-{expiry_date}"
    payload = {
        "mode": "INTRA",
        "symbol": option_string,
        "timeframe": "1min",
        "u": "Levitas",
        "sid": "ajeeq8spmpa5h4tspmq8ald2rm"
    }
    return payload, option_string


# Method to get OHLC data
def get_ohlc(symbol, expiry_date, option_type, strike):
    url = f"https://www.icharts.in/opt/hcharts/stx8req/php/getdataForOptions_curr_atp_tj.php?mode=INTRA&symbol={symbol}-{strike}{option_type}-{expiry_date}&timeframe=1min&u=Levitas&sid=ajeeq8spmpa5h4tspmq8ald2rm"
    payload, option_string = get_ohlc_payload(symbol, expiry_date, option_type, strike)
    response = requests.post(url, headers=headers, data=payload)
    
    data_bytes = response.content
    # Step 1: Decode bytes to string
    data_str = data_bytes.decode("utf-8")

    # Step 2: Convert string to list of rows
    rows = [line.split(",") for line in data_str.split("\n")]

    # Step 3: Create DataFrame
    df = pd.DataFrame(rows, columns=[
        "DateTime", "Open", "High", "Low", "Close", "Volume", "OI Change", "VWAP", "Day Low", "Day High"
    ])

    # Step 4: Convert numeric columns
    df[["Open", "High", "Low", "Close", "Volume", "OI Change", "VWAP", "Day Low", "Day High"]] = df[
        ["Open", "High", "Low", "Close", "Volume", "OI Change", "VWAP", "Day Low", "Day High"]
    ].astype(float)

    # Step 5: Convert DateTime column to proper datetime format
    df["DateTime"] = pd.to_datetime(df["DateTime"], format="%d.%m.%y %H:%M:%S")

    # Step 6: Append the option_string column to each row in the DataFrame
    df["Option String"] = option_string
    
    # Display the DataFrame
    print(df)
    return df

# ----------------------------------------------------------------------------------------------------------------
sym = nse_top_10[0]
ed = expiry_dates[-1]
ot = option_type[-1]

strikes = get_strikes(sym, ed, ot) 

strike = strikes[0]

print(strikes)  # Output: [1480, 1520, 1560]
try_sample = get_ohlc(sym, ed, ot, strike)
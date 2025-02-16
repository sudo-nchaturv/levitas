import requests
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed


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
url = "https://www.icharts.in/opt/hcharts/stx8req/php/getStrikesForDateSymExpOT_Currency.php"

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
def get_payload(symbol, expiry_date, option_type):
    dd = get_dd(symbol, expiry_date)
    payload = {
        "sym": symbol,
        "ed": expiry_date,
        "ot": option_type,
        "dd": dd
    }
    return payload

def get_strikes(symbol, expiry_date, option_type):
    response = requests.post(url, headers=headers, data = get_payload(symbol, expiry_date, option_type))
    data = response.json()
    return data

sym = nse_top_10[0]
ed = expiry_dates[0]
ot = option_type[0]

strikes = get_strikes(sym, ed, ot)  # Extract all 'id' values

print(strikes)  # Output: [1480, 1520, 1560]
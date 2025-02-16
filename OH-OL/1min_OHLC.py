import requests
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# Top 10 NSE symbols by market cap
nse_top_10 = ["RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "INFY", "HINDUNILVR", "SBIN", "BHARTIARTL", "ITC", "LICI"]
symbol = "RELIANCE"
year = 2024
optType = 'C'
strike = 2500
exp_date = '25JAN24'
# Method to get the last Thursday of each month
def get_last_thursday(year):
    last_thursdays = []
    for month in range(1, 13):
        for day in range(31, 0, -1):
            try:
                date = f"{year}-{month:02d}-{day:02d}"
                if pd.to_datetime(date).weekday() == 3:
                    last_thursdays.append(date)
                    break
            except ValueError:
                pass
    # Adding first month's last Thursday of next year
    month = 1
    for day in range(31, 0, -1):
        try:
            date = f"{year+1}-{month:02d}-{day:02d}"
            if pd.to_datetime(date).weekday() == 3:
                last_thursdays.append(date)
                break
        except ValueError:
            pass
    return last_thursdays


url = f'https://www.icharts.in/opt/hcharts/stx8req/php/getdataForOptions_curr_atp_tj.php?mode=INTRA&symbol={symbol}-{strike}{optType}-{exp_date}&timeframe=1min&u=Levitas&sid=ajeeq8spmpa5h4tspmq8ald2rm'


headers = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "en-US,en;q=0.7",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "cookie": "PHPSESSID=ajeeq8spmpa5h4tspmq8ald2rm",
    "origin": "https://www.icharts.in",
    "referer": "https://www.icharts.in/opt/OptionsChart.php",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest"
}

# Function to create payload

payload = {
    "mode" : "INTRA",
    "symbol" : f"{symbol}-{strike}{optType}-{exp_date}",
    "timeframe" : "1min",
    "u" : "Levitas",
    "sid" : "ajeeq8spmpa5h4tspmq8ald2rm"
}


response = requests.post(url, headers = headers, data = payload)
data = response.content
print(data)

# Function to fetch data

def fetch_data(symbol, date, expdate, optType):
    response = requests.post(url, headers=headers, data=get_payload(date, expdate, symbol, optType))
    if response.status_code == 200:
        try:
            if not response.text.strip():
                print(f"Empty response for {symbol}, optType {optType} on {date}")
                return symbol, date, optType, []
            data = response.json()
            options_data = data.get("aaData", [])
            print(f"Fetched {len(options_data)} records for {symbol}, optType {optType} on {date}")
            return symbol, date, optType, options_data
        except json.JSONDecodeError:
            print(f"JSON decode error for {symbol}, optType {optType} on {date}. Status: {response.status_code}, Response text: {response.text}")
    else:
        print(f"Error fetching data for {symbol}, optType {optType} on {date}: {response.status_code}, Response: {response.text}")
    return symbol, date, optType, []

# Parallel execution

data_store = {symbol: {opt_types[opt]: {} for opt in opt_types} for symbol in nse_top_10}
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(fetch_data, symbol, date, expdate, optType) for symbol in nse_top_10 for date, expdate in dates_expiry_map.items() for optType in opt_types]
    for future in as_completed(futures):
        symbol, date, optType, options_data = future.result()
        data_store[symbol][opt_types[optType]][date] = options_data

# Save the fetched data to a JSON file
with open("nse_top_10_options_data.json", "w") as f:
    json.dump(data_store, f, indent=4)

print("Data saved to 'nse_top_10_options_data.json'.")

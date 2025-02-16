import requests
import json
import pandas as pd

# Top 10 NSE symbols by market cap
nse_top_10 = ["RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "INFY", "HINDUNILVR", "SBIN", "BHARTIARTL", "ITC", "LICI"]

year = 2024
optType = 1  # Assuming this is fixed

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

# Method to map each date to the next expiration Thursday

def get_expiry_dates(year):
    all_dates = pd.date_range(start=f"{year}-01-01", end=f"{year}-12-31", freq="B").strftime("%Y-%m-%d").tolist()
    last_thursdays = get_last_thursday(year)
    
    dates_expiry_map = {}
    j = 0
    for i in range(len(all_dates) - 1):
        if all_dates[i] <= last_thursdays[j]:
            dates_expiry_map[all_dates[i]] = last_thursdays[j]
        else:
            j += 1
            dates_expiry_map[all_dates[i]] = last_thursdays[j]
    return dates_expiry_map

# Fetch all expiry dates

dates_expiry_map = get_expiry_dates(year)

# API details
url = "https://www.icharts.in/opt/hcharts/stx8req/php/getDataForOpenHighLowScanOptions_v2.php"
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

# Function to create payload

def get_payload(date, expdate, optSymbol):
    return {
        "value": optType,
        "expdate": expdate,
        "date": date,
        "rdDataType": "hist",
        "optSymbol": optSymbol,
        "userName": "Levitas",
        "atmstrikenumber": 15,
        "striketypeval": "allstrikes",
        "sID": "ajeeq8spmpa5h4tspmq8ald2rm",
        "interval": "1min",
        "timeframe_enabled_val": 1
    }

# Fetch data for all dates and symbols

data_store = {}

for symbol in nse_top_10:
    data_store[symbol] = {}
    for date, expdate in dates_expiry_map.items():
        response = requests.post(url, headers=headers, data=get_payload(date, expdate, symbol))
        if response.status_code == 200:
            try:
                if not response.text.strip():  # Check if response is empty
                    print(f"Empty response for {symbol} on {date}")
                    continue
                data = response.json()
                options_data = data.get("aaData", [])
                data_store[symbol][date] = options_data
                print(f"Fetched {len(options_data)} records for {symbol} on {date}")
            except json.JSONDecodeError:
                print(f"JSON decode error for {symbol} on {date}. Status: {response.status_code}, Response text: {response.text}")
        else:
            print(f"Error fetching data for {symbol} on {date}: {response.status_code}, Response: {response.text}")

# Save the fetched data to a JSON file
with open("nse_top_10_options_data.json", "w") as f:
    json.dump(data_store, f, indent=4)

print("Data saved to 'nse_top_10_options_data.json'.")

import requests
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Top 10 NSE symbols by market cap
nse_top_10 = ["RELIANCE"]
# nse_top_10 = ["RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "INFY", "HINDUNILVR", "SBIN", "BHARTIARTL", "ITC", "LICI"]

year = 2024

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
url = "https://www.icharts.in/opt/hcharts/stx8req/php/getHistoricalSpotPrice_v10.php"
headers = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
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
        "OptExpDate": expdate,
        "txtDate": date,
        "optSymbol": optSymbol,
        "HistDate": "undefined"    
    }

# Function to fetch data with retry logic
def fetch_data(symbol, date, expdate, retries=3, backoff_factor=0.3):
    for attempt in range(retries):
        try:
            response = requests.post(url, headers=headers, data=get_payload(date, expdate, symbol))
            if response.status_code == 200:
                try:
                    if not response.text.strip():
                        print(f"Empty response for {symbol} on {date}")
                        return symbol, date, expdate, []
                    data = response.json()
                    options_data = data
                    print(f"Fetched {len(options_data)} records for {symbol} on {date}")
                    return symbol, date, expdate, options_data
                except json.JSONDecodeError:
                    print(f"JSON decode error for {symbol} on {date}. Status: {response.status_code}, Response text: {response.text}")
            else:
                print(f"Error fetching data for {symbol} on {date}: {response.status_code}, Response: {response.text}")
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error for {symbol} on {date}: {e}")
            time.sleep(backoff_factor * (2 ** attempt))
    return symbol, date, expdate, []

# Parallel execution
data_store = []
with ThreadPoolExecutor(max_workers=50) as executor:  # Increase max_workers for more parallelism
    futures = [executor.submit(fetch_data, symbol, date, expdate) for symbol in nse_top_10 for date, expdate in dates_expiry_map.items()]
    for future in as_completed(futures):
        symbol, date, expdate, options_data = future.result()
        for record in options_data:
            if record:  # Ensure record is not None
                data_store.append([symbol, *record, expdate])

# Inspect the first few rows to understand the structure
print(data_store[:5])

# Convert to DataFrame
columns = ["Symbol", "Spot price", "Date", "Change", "Change%", "Type", "Random", "Lot size", "Expiry Date"]
df = pd.DataFrame(data_store)

# Ensure the DataFrame has the correct columns
df = df.iloc[:, :len(columns)]
df.columns = columns

# Save the DataFrame to a JSON file
# append time to file name

time = pd.Timestamp.now()

filename = (f"nse_top_10_spot_data_{time}.json")
df.to_json(filename, orient="records", lines=True)

print("Data saved to 'nse_top_10_spot_data.json'.")
import requests
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Top 10 NSE symbols by market cap
# nse_top_10 = ["RELIANCE"]
# Uncomment below for full list
nse_top_10 = ["RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "INFY", "HINDUNILVR", "SBIN", "BHARTIARTL", "ITC", "LICI"]

year = 2024

def get_last_thursday(year):
    """Get last Thursday of each month for a given year."""
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

def get_expiry_dates(year):
    """Map business dates to their corresponding expiry dates."""
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
url = "https://www.icharts.in/opt/hcharts/stx8req/php/getHistPriceInfo.php"
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

def get_payload(date, expdate, optSymbol):
    """Create payload for API request."""
    return {
        "optExpDate": expdate,
        "defaultDate": date,
        "optSymbol": optSymbol,
    }

def fetch_option_data(date, expdate, symbol):
    """
    Fetch option data for a specific date, expiration date, and symbol.
    
    Args:
        date (str): Trading date
        expdate (str): Option expiration date
        symbol (str): Stock symbol
    
    Returns:
        dict: Fetched option data or None if fetch fails
    """
    try:
        payload = get_payload(date, expdate, symbol)
        response = requests.post(url, headers=headers, data=payload, timeout=10)
        
        if response.status_code == 200:
            json_data = response.json()
            # Additional check to ensure meaningful data
            if json_data and not isinstance(json_data, str):
                return {
                    "date": date,
                    "expdate": expdate,
                    "symbol": symbol,
                    "data": json_data
                }
        print(f"No valid data for {symbol} on {date}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Network error for {symbol} on {date}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error for {symbol} on {date}: {e}")
        return None

def fetch_parallel_option_data(symbols, year, max_workers=5):
    """
    Fetch option data in parallel for given symbols and year.
    
    Args:
        symbols (list): List of stock symbols
        year (int): Year for data fetching
        max_workers (int): Maximum number of concurrent threads
    
    Returns:
        list: List of fetched option data
    """
    data_store = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Collect all futures
        futures = []
        for symbol in symbols:
            for date, expdate in dates_expiry_map.items():
                futures.append(
                    executor.submit(fetch_option_data, date, expdate, symbol)
                )
        
        # Collect results with progress tracking
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            if result:
                data_store.append(result)
            print(f"Processed {i}/{len(futures)} tasks")
    
    return data_store

# Main execution
if __name__ == "__main__":
    # Fetch option data
    data_store = fetch_parallel_option_data(nse_top_10, year)

    # Get time in unix timestamp format
    current_time = int(pd.Timestamp.now().timestamp())

    # Save the fetched data to a JSON file
    with open(f"nse_top_10_options_data_{current_time}.json", "w") as f:
        json.dump(data_store, f, indent=4)

    print(f"Data saved to 'nse_top_10_options_data_{current_time}.json'.")
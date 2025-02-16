import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import time

def get_month_date_ranges(year):
    """Generate start and end dates for each month of the given year."""
    date_ranges = []
    for month in range(1, 2):
        # Get the last day of the month
        if month == 12:
            start_date = f"{year}-{month:02d}-01"
            end_date = f"{year}-{month:02d}-31"
        else:
            start_date = f"{year}-{month:02d}-01"
            next_month = datetime(year, month + 1, 1) - timedelta(days=1)
            end_date = next_month.strftime("%Y-%m-%d")
        date_ranges.append((start_date, end_date))
    return date_ranges

def fetch_stock_data(symbol, start_date, end_date, headers):
    """Fetch data for a single symbol and date range."""
    url = f"https://api.upstox.com/v2/historical-candle/{symbol}/1minute/{end_date}/{start_date}"
    
    try:
        response = requests.request("GET", url, headers=headers, data={})
        data_json = json.loads(response.text)
        data = data_json['data']['candles']
        
        # Handle varying number of columns by selecting only the first 6 columns
        processed_data = [row[:6] for row in data]
        
        # Convert to DataFrame
        df = pd.DataFrame(processed_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['symbol'] = symbol
        return df
    
    except Exception as e:
        print(f"Error fetching data for {symbol} from {start_date} to {end_date}: {str(e)}")
        return pd.DataFrame()

def main():
    
    # # Load file symboltocode.csv
    # symboltocode_df = pd.read_csv('symboltocode.csv')

    # symbols = symboltocode_df

    # List of symbols

    symbols = [
        'NSE_EQ|INE002A01018', 'NSE_EQ|INE467B01029', 'NSE_EQ|INE040A01034',
        'NSE_EQ|INE397D01024', 'NSE_EQ|INE090A01021', 'NSE_EQ|INE009A01021',
        'NSE_EQ|INE062A01020', 'NSE_EQ|INE030A01027', 'NSE_EQ|INE154A01025',
        'NSE_EQ|INE115A01026'
    ]
    
    headers = {
        'Accept': 'application/json'
    }
    
    # Get date ranges for each month of 2024
    date_ranges = get_month_date_ranges(2024)
    
    # Initialize empty list to store all DataFrames
    all_data = []
    
    # Fetch data for each symbol and date range
    for symbol in symbols:
        print(f"Fetching data for {symbol}")
        for start_date, end_date in date_ranges:
            print(f"Processing period: {start_date} to {end_date}")
            
            df = fetch_stock_data(symbol, start_date, end_date, headers)
            if not df.empty:
                all_data.append(df)
            
            # Add delay to avoid hitting rate limits
            time.sleep(1)
    
    # Combine all DataFrames
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Convert timestamp to datetime
        final_df['timestamp'] = pd.to_datetime(final_df['timestamp'])
        
        # Sort by timestamp and symbol
        final_df = final_df.sort_values(['symbol', 'timestamp'])
        
        # Save to CSV file stock_data_2024.csv in the current directory
        import os
        path = os.path.join(os.getcwd(), 'stock_data_2024_jan.csv')
        final_df.to_csv(path, index=False)
        print(f"Data saved to {path}")
        
        return final_df
    else:
        print("No data was collected")
        return pd.DataFrame()

if __name__ == "__main__":
    df = main()
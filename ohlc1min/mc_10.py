import requests
import pandas as pd
import json
from datetime import datetime

def unix_to_date(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

symbols = ['RELIANCE', 'TCS', 'HDFCBANK', 'BHARTIARTL', 'ICICIBANK', 'INFY', 'SBIN', 'HINDUNILVR', 'ITC', 'LICHSGFIN']
base_url = 'https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history'  # Replace with the actual URL
all_data = []

for symbol in symbols:
    try:
        response = requests.get(f"{base_url}?symbol={symbol}&resolution=1&countback=805500&currencyCode=INR")
        response.raise_for_status()
        result = response.json()
        data = pd.DataFrame(result)
        data['date'] = data['t'].apply(unix_to_date)
        data = data[['date', 'o', 'h', 'l', 'c', 'v']].rename(columns={'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'})
        
        # Convert to datetime and set as index
        data['date'] = pd.to_datetime(data['date'])
        data.set_index('date', inplace=True)
        
        # Resample to weekly OHLC
        weekly_data = data.resample('W').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        weekly_data['symbol'] = symbol
        all_data.append(weekly_data)
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {symbol}: {e}")
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON for {symbol}: {e}")

# Concatenate all data into a single DataFrame
final_data = pd.concat(all_data)

# Print or save the final data
print(final_data)
# final_data.to_csv('weekly_ohlc_2024.csv')
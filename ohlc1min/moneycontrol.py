import urllib.request
import urllib.parse
import time
import pandas as pd
from datetime import datetime

def date_to_unix(date):
    return int(time.mktime(date.timetuple()))

def unix_to_date(timestamp):
    return datetime.fromtimestamp(timestamp)

symbol = 'RELIANCE'
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)

start_date_unix = date_to_unix(start_date)
end_date_unix = date_to_unix(end_date)

url = f'https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history?symbol={symbol}&resolution=1&from={start_date_unix}&to={end_date_unix}&countback=805600&currencyCode=INR'
#url = 'https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history?symbol=RELIANCE&resolution=1&from=1737698169&to=1737901541&countback=918&currencyCode=INR'
print(url)

req = urllib.request.Request(url)
req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3')

response = urllib.request.urlopen(req)

if response.getcode() == 200:
    try:
        result = response.read().decode('utf-8')
        import json
        result = json.loads(result)
        data = pd.DataFrame(result)
        data['date'] = data['t'].apply(unix_to_date)
        data = data[['date', 'o', 'h', 'l', 'c', 'v']].rename(columns={'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'})
        print(data)

        # Save date and close price to a CSV file
        data.to_csv('RELIANCE_1min_March-Dec-2024.csv', index=False)

    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
else:
    print(f"Error: {response.getcode()}")


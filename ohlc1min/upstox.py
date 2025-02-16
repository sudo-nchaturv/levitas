import requests
import json
import pandas as pd

# company symbols
symbol = 'BSE_EQ|INE002A01018'

# Define the URL
url = f"https://api.upstox.com/v2/historical-candle/{symbol}/1minute/2024-11-30/2024-11-01"

payload = {}
headers = {
  'Accept': 'application/json'
}

response = requests.request("GET", url, headers=headers, data=payload)

data_json = json.loads(response.text)
data_json = data_json['data']['candles']

# convert data_json to pandas dataframe
df = pd.DataFrame(data_json)

# Inspect the first few rows to understand the structure
print(df.head())

# Rename columns to timestamp, open, high, low, and close
df = df.rename(columns={0: 'timestamp', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'})

print(df)
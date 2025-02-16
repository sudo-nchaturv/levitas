import requests
import json
import pandas as pd

year = 2024
date = "2024-01-01"
expdate = "25JAN24"
optSymbol = "RELIANCE"
optType = 1

# Method to get the last thursday date of each month of the year and save it to a list
# Also add the last thursday of the first month of the next year to the list
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

# Method to give a df of all dates mapped to the last thursday of the month that comes right after that date
def get_expiry_dates(year):

    # List of all weekday dates of the year
    all_dates = pd.date_range(start=f"{year}-01-01", end=f"{year}-12-31", freq="B").strftime("%Y-%m-%d").tolist()

    last_thursdays_2024 = get_last_thursday(year)

    # Map all dates of the year to the last thursday of the month that comes right after that date
    dates_expiry_map = pd.Series()
    j = 0
    for i in range(len(all_dates) - 1):
        if all_dates[i] <= last_thursdays_2024[j]:
            dates_expiry_map[all_dates[i]] = last_thursdays_2024[j]
        else:
            j += 1
            dates_expiry_map[all_dates[i]] = last_thursdays_2024[j]

    return dates_expiry_map

# Method to get payload for each date
def get_payload(date, expdate):
    payload = {
    "value": optType,
    "expdate": expdate,
    "date": date,
    "rdDataType": "hist",
    "optSymbol": optSymbol,
    "userName": "Levitas",
    "atmstrikenumber": 50,
    "striketypeval": "allstrikes",
    "sID": "ajeeq8spmpa5h4tspmq8ald2rm",
    "interval": "1min",
    "timeframe_enabled_val": 1
    }
    return payload


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


response = requests.post(url, headers=headers, data=get_payload(date, expdate))

if response.status_code == 200:
    data = response.json()
    options_data = data.get("aaData", [])

    print(f"Fetched {len(options_data)} records:\n")
    print("Strike Price | Open  | High  | Latest High   | LTP | Change % | Volume | OI Change")
    print("-" * 90)

    for option in options_data:
        strike = option[0]
        open_price = option[1]
        high_price = option[2]
        lates_high_price = option[3]
        ltp_price = option[4]
        percentage_change = option[5]
        volume = option[6]
        oi_change = option[7]

        print(f"{strike:25} {open_price:6} {high_price:6} {lates_high_price:6} {ltp_price:6} {percentage_change:10} {volume:6} {oi_change:6}")

    # Save to a JSON file
    with open("reliance_options_data_Jan1_2024.json", "w") as f:
        json.dump(data, f, indent=4)

    print("\nData saved to 'reliance_options_data.json'.")

else:
    print(f"Error: {response.status_code}, Response: {response.text}")

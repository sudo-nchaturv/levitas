import requests
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import os
from tqdm import tqdm

# Global variables
nse_top_10 = ["RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "INFY", "HINDUNILVR", "SBIN", "BHARTIARTL", "ITC", "LICI"]
# nse_top_10 = ["RELIANCE"]
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

# Read the dates from expiry_dates.json
with open('C:\levitas\script\icharts-options-OHLC\expiry_dates.json', 'r') as f:
    expiry_dates = json.load(f)

def get_dd(sym, expiry_date):
    url = "https://www.icharts.in/opt/hcharts/stx8req/php/getExpiryTradingDate_Curr.php"
    payload = {
        "ed": expiry_date,
        "sym": sym
    }
    try:
        response = requests.post(url, headers=headers, data=payload)
        return response.text
    except Exception as e:
        print(f"Error getting dd for {sym} {expiry_date}: {str(e)}")
        return None

def get_strikes(symbol, expiry_date, option_type):
    url = "https://www.icharts.in/opt/hcharts/stx8req/php/getStrikesForDateSymExpOT_Currency.php"
    payload = {
        "sym": symbol,
        "ed": expiry_date,
        "ot": option_type,
        "dd": get_dd(symbol, expiry_date)
    }
    try:
        response = requests.post(url, headers=headers, data=payload)
        data = response.json()
        return [item["id"] for item in data]
    except Exception as e:
        print(f"Error getting strikes for {symbol} {expiry_date} {option_type}: {str(e)}")
        return []

def get_ohlc(params):
    symbol, expiry_date, opt_type, strike = params
    option_string = f"{symbol}-{strike}{opt_type}-{expiry_date}"
    url = f"https://www.icharts.in/opt/hcharts/stx8req/php/getdataForOptions_curr_atp_tj.php?mode=INTRA&symbol={option_string}&timeframe=1min&u=Levitas&sid=ajeeq8spmpa5h4tspmq8ald2rm"
    payload = {
        "mode": "INTRA",
        "symbol": option_string,
        "timeframe": "1min",
        "u": "Levitas",
        "sid": "ajeeq8spmpa5h4tspmq8ald2rm"
    }
    
    try:
        response = requests.post(url, headers=headers, data=payload)
        data_str = response.content.decode("utf-8")
        
        # Check for empty or invalid response
        if not data_str.strip() or "error" in data_str.lower():
            print(f"No valid data for {option_string}")
            return None
            
        # Split the data into rows and filter out empty rows
        rows = [line.split(",") for line in data_str.strip().split("\n") if line.strip()]
        
        # Validate row format
        valid_rows = [row for row in rows if len(row) == 10]  # We expect 10 columns
        
        if not valid_rows:
            print(f"No valid rows found for {option_string}")
            return None
            
        df = pd.DataFrame(valid_rows, columns=[
            "DateTime", "Open", "High", "Low", "Close", "Volume", 
            "OI Change", "VWAP", "Day Low", "Day High"
        ])
        
        # Convert numeric columns, handling errors
        numeric_columns = ["Open", "High", "Low", "Close", "Volume", 
                         "OI Change", "VWAP", "Day Low", "Day High"]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert DateTime and handle invalid dates
        try:
            df["DateTime"] = pd.to_datetime(df["DateTime"], format="%d.%m.%y %H:%M:%S")
            df["Date"] = df["DateTime"].dt.date
            df["Time"] = df["DateTime"].dt.time
        except Exception as e:
            print(f"DateTime conversion error for {option_string}: {str(e)}")
            return None
        
        # Add metadata columns
        df["Symbol"] = symbol
        df["Strike"] = strike
        df["Option_Type"] = opt_type
        df["Expiry_Date"] = expiry_date
        df["Option_String"] = option_string
        
        # Final validation
        if df.empty:
            print(f"Empty DataFrame after processing for {option_string}")
            return None
            
        return df
        
    except Exception as e:
        print(f"Error getting OHLC data for {option_string}: {str(e)}")
        return None

def main():
    # Create output directory if it doesn't exist
    output_dir = "options_data"
    os.makedirs(output_dir, exist_ok=True)
    
    # Create a log file for errors
    log_file = os.path.join(output_dir, f"error_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
    
    # Prepare all parameter combinations
    all_params = []
    for symbol in nse_top_10:
        for expiry in expiry_dates:
        # for expiry in ["25JAN24"]:
            for opt_type in option_type:
            # for opt_type in ["P"]:
                strikes = get_strikes(symbol, expiry, opt_type)
                for strike in strikes:
                # for strike in ["2860"]:
                    all_params.append((symbol, expiry, opt_type, strike))
    
    print(f"Total combinations to process: {len(all_params)}")
    
    # Initialize an empty list to store all DataFrames
    all_data = []
    
    # Process in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=10) as executor:  # Reduced max_workers to 5 for stability
        # Create a list of futures
        futures = [executor.submit(get_ohlc, params) for params in all_params]
        
        # Process results as they complete with progress bar
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing options data"):
            try:
                df = future.result()
                if df is not None and not df.empty:
                    all_data.append(df)
            except Exception as e:
                with open(log_file, 'a') as f:
                    f.write(f"{datetime.now()}: Error processing future: {str(e)}\n")
    
    # Combine all DataFrames
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Reorder columns for better organization
        column_order = [
            'Date', 'Time', 'Symbol', 'Strike', 'Option_Type', 'Expiry_Date',
            'Open', 'High', 'Low', 'Close', 'Volume', 'OI Change', 'VWAP',
            'Day Low', 'Day High', 'Option_String'
        ]
        final_df = final_df[column_order]
        
        # Save to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"options_data_{timestamp}.csv")
        final_df.to_csv(output_file, index=False)
        print(f"\nData saved to {output_file}")
        
        # Print summary statistics
        print("\nSummary:")
        print(f"Total records: {len(final_df)}")
        print(f"Unique symbols: {final_df['Symbol'].nunique()}")
        print(f"Date range: {final_df['Date'].min()} to {final_df['Date'].max()}")
    else:
        print("No data was collected. Please check the error messages above.")

if __name__ == "__main__":
    main()
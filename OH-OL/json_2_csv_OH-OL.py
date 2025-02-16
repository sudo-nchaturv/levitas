import json
import csv
from pathlib import Path

def json_to_csv(json_file, csv_output):
    # Read the JSON file
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    # Open CSV file for writing
    with open(csv_output, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        
        # Write header
        header = [
            'Stock', 'Date', 'Option_Symbol', 'Open', 'High/Low', 
            'Latest High/Low', 'LTP', 'Change %', 'Volume', 'OI Change'
        ]
        csvwriter.writerow(header)
        
        # Iterate through the JSON data
        for stock, stock_data in data.items():
            if 'open_high_calls' in stock_data:
                for date, options in stock_data['open_high_calls'].items():
                    for option in options:
                        # Create a row with stock, date, and option details
                        row = [
                            stock, 
                            date, 
                            option[0],   # Option Symbol
                            option[1],   # Open
                            option[2],   # High
                            option[3],   # Low
                            option[4],   # Close
                            option[5],   # Volume/OI Ratio
                            option[6],   # Volume
                            option[7]    # Change
                        ]
                        csvwriter.writerow(row)

# Example usage
input_json = 'nse_top_10_options_data.json'
output_csv = 'output.csv'
json_to_csv(input_json, output_csv)

print(f"Conversion complete. CSV saved to {output_csv}")
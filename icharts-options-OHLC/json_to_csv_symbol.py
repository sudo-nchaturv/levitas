import json
import csv
import pandas as pd
from datetime import datetime

def convert_json_to_csv(input_file, output_file):
    """
    Convert a JSON file containing options data to CSV format
    
    Parameters:
    input_file (str): Path to input JSON file
    output_file (str): Path to output CSV file
    """
    
    # Read JSON data
    with open(input_file, 'r') as f:
        # Read the file line by line as each line is a separate JSON object
        json_data = [json.loads(line) for line in f]
    
    # Convert timestamp to readable date format
    for row in json_data:
        # Convert milliseconds timestamp to datetime
        row['Date'] = datetime.fromtimestamp(row['Date']/1000).strftime('%Y-%m-%d')
    
    # Convert to DataFrame
    df = pd.DataFrame(json_data)
    
    # Save to CSV
    df.to_csv(output_file, index=False)
    print(f"Successfully converted {input_file} to {output_file}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 3:
        print("Usage: python script.py input.json output.csv")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    try:
        convert_json_to_csv(input_file, output_file)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
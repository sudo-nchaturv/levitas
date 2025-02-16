import json
import csv
import sys

def json_to_csv(input_json_file, output_csv_file):
    """
    Convert JSON file to CSV file
    
    Args:
    input_json_file (str): Path to input JSON file
    output_csv_file (str): Path to output CSV file
    """
    try:
        # Read JSON file
        with open(input_json_file, 'r') as json_file:
            data = json.load(json_file)
        
        # Open CSV file for writing
        with open(output_csv_file, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            
            # Write headers
            headers = ['date', 'expdate', 'symbol'] + [f'data_{i}' for i in range(len(data[0]['data']))]
            csv_writer.writerow(headers)
            
            # Write data rows
            for item in data:
                row = [
                    item['date'], 
                    item['expdate'], 
                    item['symbol']
                ] + item['data']
                csv_writer.writerow(row)
        
        print(f"Conversion successful. CSV file saved as {output_csv_file}")
    
    except FileNotFoundError:
        print(f"Error: File {input_json_file} not found.")
    except json.JSONDecodeError:
        print("Error: Invalid JSON format.")
    except Exception as e:
        print(f"An error occurred: {e}")

def main():

    input_file = "nse_top_10_options_data_spot.json"
    output_file = "nse_top_10_options_data_spot_final.csv"
    
    json_to_csv(input_file, output_file)

if __name__ == "__main__":
    main()
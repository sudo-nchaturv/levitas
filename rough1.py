from concurrent.futures.process import _process_chunk
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import timeit

dates = []
dates = [datetime(2019, 12, 31),datetime(2019, 12, 30),datetime(2019, 12, 29),
         datetime(2019, 11, 30),datetime(2019, 11, 29),datetime(2019, 11, 28), 
         datetime(2019, 6, 30),datetime(2019, 6, 29),datetime(2019, 6, 28),
         datetime(2018, 12, 31),datetime(2018, 12, 30),datetime(2018, 12, 29)]

# Convert dates to strings
date_strings = [date.strftime('%Y-%m-%d') for date in dates]

# Create SQLAlchemy engine
engine = create_engine('mssql+pyodbc://@DELL-123456789\\MSSQLSERVER01/ACCORD_DATA?driver=ODBC+Driver+17+for+SQL+Server')

# Query to get a sample of stock data (use TOP for SQL Server to limit rows)
query = """
SELECT A_Date, MCAP_Crs, NSE_Symbol 
FROM ACCORD_DATA.dbo.ACCORD_DATA
WHERE A_Date LIKE '2019-12-31'
"""
#.format(','.join("'{}'".format(date) for date in date_strings))
#.format(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

# Load the sample data into a pandas DataFrame
start_time = timeit.default_timer()

# Chunk approach
# chunksize = 100
# chunks = []
# n_chunk = 0
# try:
#     for chunk in pd.read_sql(query, engine, params=(start_date, end_date), chunksize=chunksize):
#         #chunks.append(chunk)
#         print("Chunk no.:", ++n_chunk," ongoing.\n")
#         _process_chunk(chunk)
# except Exception as e:
#     print("Error occurred:", e)
# df = pd.concat(chunks, axis=0)

#Chunk free approach
df = pd.read_sql(query, engine)
print("Loading data took {:.2f} seconds".format(timeit.default_timer() - start_time))

# Ensure A_Date and MCAP_Crs columns exist and contain valid data
if 'A_Date' in df.columns and 'MCAP_Crs' in df.columns:
    # Find the maximum A_Date value
    max_date = df['A_Date'].max()

    # Filter rows with the maximum A_Date value
    start_time = timeit.default_timer()
    max_date_rows = df[df['A_Date'] == max_date]
    print("Filtering data took {:.2f} seconds".format(timeit.default_timer() - start_time))


    # Ensure there are enough rows to calculate the 500th maximum value
    if not max_date_rows.empty:
        # Find the 500th maximum value of the MCAP_Crs column
        mcap_sorted = max_date_rows['MCAP_Crs'].sort_values(ascending=False)
        mcap_500th_max = mcap_sorted.iloc[499] if len(mcap_sorted) >= 500 else mcap_sorted.iloc[-1]
        print("500th maximum value of MCAP_Crs:",mcap_500th_max,"\n")
        # Filter NSE_Symbols with MCAP_Crs >= 500th maximum value
        filtered_symbols = max_date_rows[max_date_rows['MCAP_Crs'] >= mcap_500th_max]['NSE_Symbol']

        # Create a new DataFrame with these NSE_Symbols
        filtered_df = pd.DataFrame({'NSE_Symbol': filtered_symbols})

        # Display the resulting DataFrame
        print("Size:",filtered_df.shape)
    else:
        print("No rows found with the maximum A_Date value.")
else:
    print("Required columns A_Date or MCAP_Crs are missing in the data.")
engine.dispose()
from concurrent.futures.process import _process_chunk
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import timeit

last_date = datetime(2019, 12, 31)

# convert last_date to string
last_date_string = last_date.strftime('%Y-%m-%d')

# Create SQLAlchemy engine
engine = create_engine('mssql+pyodbc://@DELL-123456789\\MSSQLSERVER01/ACCORD_DATA?driver=ODBC+Driver+17+for+SQL+Server')

# Query to get top 500 NSE_Symbols
query = f"""
SELECT TOP 500 NSE_Symbol, MAX(MCAP_Crs) AS Last_MCAP
FROM ACCORD_DATA.dbo.ACCORD_DATA
WHERE A_Date = '{last_date_string}'
GROUP BY NSE_Symbol
ORDER BY Last_MCAP DESC;
"""
#.format(','.join("'{}'".format(date) for date in date_strings))
#.format(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

# Load the sample data into a pandas DataFrame
start_time = timeit.default_timer()
df = pd.read_sql(query, engine)
print("Loading data took {:.2f} seconds".format(timeit.default_timer() - start_time))

# Ensure A_Date and MCAP_Crs columns exist and contain valid data
if 'Last_MCAP' in df.columns and 'NSE_Symbol' in df.columns:
   print(df)
engine.dispose()
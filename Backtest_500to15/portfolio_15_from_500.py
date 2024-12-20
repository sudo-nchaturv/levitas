from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import timeit

last_date = datetime(2019, 12, 31)

# Convert last_date to string
last_date_string = last_date.strftime('%Y-%m-%d')

# Create SQLAlchemy engine
engine = create_engine('mssql+pyodbc://@DELL-123456789\\MSSQLSERVER01/ACCORD_DATA?driver=ODBC+Driver+17+for+SQL+Server')

# Step 1: Query to get top 500 companies by MCAP_Crs
query_top_500 = f"""
SELECT TOP 500 
    NSE_Symbol, 
    MCAP_Crs AS Last_MCAP, 
    A_Date, 
    A_Close, 
    Sharpe_30, 
    Sharpe_90, 
    Sharpe_180, 
    Sharpe_365
FROM ACCORD_DATA.dbo.ACCORD_DATA
WHERE A_Date = '{last_date_string}'
ORDER BY MCAP_Crs DESC;
"""

# Load the top 500 companies into a pandas DataFrame
start_time = timeit.default_timer()
df_top_500 = pd.read_sql(query_top_500, engine)
print("Loading top 500 data took {:.2f} seconds".format(timeit.default_timer() - start_time))

# Step 2: Select the top 15 companies based on Sharpe_365
df_top_15 = df_top_500.nlargest(15, 'Sharpe_365')

# Display the result
print(df_top_15)

# Dispose of the engine connection
engine.dispose()

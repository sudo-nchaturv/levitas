from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import timeit

# Define the last_date
last_date = datetime(2019, 12, 31)

# Convert last_date to string
last_date_string = last_date.strftime('%Y-%m-%d')

# List of dates to filter data
selected_dates = [
    datetime(2019, 12, 31), datetime(2019, 12, 30), datetime(2019, 12, 29),
    datetime(2019, 11, 30), datetime(2019, 11, 29), datetime(2019, 11, 28),
    datetime(2019, 6, 30), datetime(2019, 6, 29), datetime(2019, 6, 28),
    datetime(2018, 12, 31), datetime(2018, 12, 30), datetime(2018, 12, 29)
]

# Convert dates to SQL-friendly format
selected_dates_strings = ", ".join([f"'{date.strftime('%Y-%m-%d')}'" for date in selected_dates])

# Create SQLAlchemy engine
engine = create_engine('mssql+pyodbc://@DELL-123456789\\MSSQLSERVER01/ACCORD_DATA?driver=ODBC+Driver+17+for+SQL+Server')

# Query to get top 500 NSE_Symbols based on MCAP_Crs on the last date
top_500_query = f"""
SELECT TOP 500 NSE_Symbol, MAX(MCAP_Crs) AS Last_MCAP
FROM ACCORD_DATA.dbo.ACCORD_DATA
WHERE A_Date = '{last_date_string}'
GROUP BY NSE_Symbol
ORDER BY Last_MCAP DESC;
"""

# Load the top 500 symbols into a pandas DataFrame
start_time = timeit.default_timer()
top_500_df = pd.read_sql(top_500_query, engine)
print("Loading top 500 symbols took {:.2f} seconds".format(timeit.default_timer() - start_time))

if not top_500_df.empty:
    # Extract symbols from the first query
    top_500_symbols = ", ".join([f"'{symbol}'" for symbol in top_500_df['NSE_Symbol']])

    # Query to fetch rows for selected symbols and dates
    final_query = f"""
    SELECT NSE_Symbol, A_Date, A_Close
    FROM ACCORD_DATA.dbo.ACCORD_DATA
    WHERE NSE_Symbol IN ({top_500_symbols})
      AND A_Date IN ({selected_dates_strings})
    ORDER BY NSE_Symbol, A_Date;
    """

    # Load the filtered data into a pandas DataFrame
    start_time = timeit.default_timer()
    final_df = pd.read_sql(final_query, engine)
    print("Loading filtered data took {:.2f} seconds".format(timeit.default_timer() - start_time))

    # Print the resulting DataFrame
    if not final_df.empty:
        print(final_df)
    else:
        print("No data found for the specified symbols and dates.")
else:
    print("No data found for the top 500 symbols on the specified date.")

# Dispose of the engine
engine.dispose()

from sqlalchemy import create_engine
import pandas as pd

# Create SQLAlchemy engine
engine = create_engine('mssql+pyodbc://@DELL-123456789\\MSSQLSERVER01/ACCORD_DATA?driver=ODBC+Driver+17+for+SQL+Server')

# Query to get stock data
query = "SELECT * FROM ACCORD_DATA.dbo.ACCORD_DATA"

# Load the data into a pandas DataFrame
df = pd.read_sql(query, engine)

# Find the maximum A_Date value
max_date = df['A_Date'].max()

# Filter rows with the maximum A_Date value
max_date_rows = df[df['A_Date'] == max_date]

# Find the 500th maximum value of the MCAP_Crs column
mcap_sorted = max_date_rows['MCAP_Crs'].sort_values(ascending=False)
mcap_500th_max = mcap_sorted.iloc[499] if len(mcap_sorted) >= 500 else mcap_sorted.iloc[-1]

# Filter NSE_Symbols with MCAP_Crs >= 500th maximum value
filtered_symbols = max_date_rows[max_date_rows['MCAP_Crs'] >= mcap_500th_max]['NSE_Symbols']

# Create a new DataFrame with these NSE_Symbols
filtered_df = pd.DataFrame({'NSE_Symbols': filtered_symbols})

# Display the resulting DataFrame
print(filtered_df.head())

# Optional: Save the new table to the database (uncomment if needed)
# filtered_df.to_sql('Filtered_NSE_Symbols', engine, if_exists='replace', index=False)
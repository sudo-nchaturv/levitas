from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import timeit

years = list(range(2011,2012))

# Create SQLAlchemy engine
engine = create_engine('mssql+pyodbc://@DELL-123456789\\MSSQLSERVER01/ACCORD_DATA?driver=ODBC+Driver+17+for+SQL+Server')

# Function to fetch the last date of each month in 2019
def get_month_end_dates(year):
    # Query to get the last date of every month in 2019
    query = f"""
    SELECT DISTINCT MAX(A_Date) AS Last_Date_Of_Month
    FROM ACCORD_DATA.dbo.ACCORD_DATA
    WHERE A_Date BETWEEN '{year}-01-01' AND '{year}-12-31'
    GROUP BY YEAR(A_Date), MONTH(A_Date)
    ORDER BY Last_Date_Of_Month;
    """
    return pd.read_sql(query, engine)

# Get last dates of every month
for year in years:
    month_end_dates = get_month_end_dates(year)

# Initialize total percentage change accumulator
# total_percentage_change = 0
    months_in_year = len(month_end_dates) - 1  
    
# List to store monthly returns
    monthly_returns = []
    monthly_portfolio = []

# Loop through each month's last date
    for i in range(months_in_year):
        print(f"Processing month:",i,"\n")
        current_date = month_end_dates.iloc[i]['Last_Date_Of_Month']
        next_date = month_end_dates.iloc[i + 1]['Last_Date_Of_Month']
        
        # Convert dates to strings for SQL query
        current_date_str = current_date.strftime('%Y-%m-%d')
        next_date_str = next_date.strftime('%Y-%m-%d')
        
        # Step 1: Query to get top 500 companies by MCAP_Crs for the current month end date
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
        WHERE A_Date = '{current_date_str}'
        ORDER BY MCAP_Crs DESC;
        """
        
        # Load the top 500 companies for current month end date
        df_top_500 = pd.read_sql(query_top_500, engine)
        
        # Step 2: Select the top 15 companies based on Sharpe_365
        df_top_15 = df_top_500.nlargest(15, 'Sharpe_365')
        monthly_portfolio.append(df_top_15['NSE_Symbol'].tolist())


        # Step 3: For each of the top 15 companies, get the A_Close for both current and next month end dates
        monthly_percentage_changes = []
        
        for _, row in df_top_15.iterrows():
            symbol = row['NSE_Symbol']
            
            # Query for current month's A_Close and next month's A_Close
            query_close = f"""
            SELECT A_Date, A_Close
            FROM ACCORD_DATA.dbo.ACCORD_DATA
            WHERE NSE_Symbol = '{symbol}' AND A_Date IN ('{current_date_str}', '{next_date_str}')
            ORDER BY A_Date;
            """
            df_closes = pd.read_sql(query_close, engine)
            
            # Ensure we have exactly two dates (current and next month)
            if len(df_closes) == 2:
                # print(current_date_str)
                # print(df_closes['A_Date'])
                # print(current_date in df_closes['A_Date'].values)
                current_close = df_closes.loc[df_closes['A_Date'] == current_date, 'A_Close'].iloc[0]
                next_close = df_closes.loc[df_closes['A_Date'] == next_date, 'A_Close'].iloc[0]

    #            current_close = df_closes.loc[df_closes['A_Date'] == current_date_str, 'A_Close'].iloc[0]
    #            next_close = df_closes.loc[df_closes['A_Date'] == next_date_str, 'A_Close'].iloc[0]

                # Calculate percentage change
                percentage_change = ((next_close - current_close) / current_close) * 100
                monthly_percentage_changes.append(percentage_change)
        
        # Calculate the average percentage change for this month
        if monthly_percentage_changes:
            avg_monthly_change = sum(monthly_percentage_changes) / len(monthly_percentage_changes)
            monthly_returns.append((current_date_str, avg_monthly_change))
#            total_percentage_change += avg_monthly_change

    # Calculate the overall average percentage change for the year
#    average_percentage_change = total_percentage_change / months_in_year

    # Output monthly returns and the overall average percentage change
        print("Year: {year}\n")
        
        print("Monthly Portfolio:")
        month_idx = 0
        for month in monthly_portfolio:
            month_idx += 1
            print(f"Month {month_idx}: {month}\n")

        print("\nMonthly Returns:")
        for month, return_pct in monthly_returns:
            print(f"{month}: {return_pct:.2f}%")

        with open("output.txt", "a") as f:
            f.write("Year: {year}\n")
        
            f.write("Monthly Portfolio:")
            month_idx = 0
            for month in monthly_portfolio:
                month_idx += 1
                f.write(f"Month {month_idx}: {month}\n")

            f.write("\nMonthly Returns:")
            for month, return_pct in monthly_returns:
                f.write(f"{month}: {return_pct:.2f}%")
            f.close()

        
        #clear the variables
        monthly_returns = []
        monthly_portfolio = []

#    print(f"\nThe overall average percentage change in A_Close for the top 15 companies in 2019 is: {average_percentage_change:.2f}%")

    # Dispose of the engine connection
    engine.dispose()

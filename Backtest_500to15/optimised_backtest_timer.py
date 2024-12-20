import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
import logging
from typing import List, Tuple
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class StockAnalyzer:
    def __init__(self, connection_string: str):
        """
        Initialize the stock analyzer with a database connection.
        
        :param connection_string: SQLAlchemy database connection string
        """
        try:
            # Create engine with connection pooling
            self.engine = create_engine(
                connection_string, 
                poolclass=QueuePool,
                pool_size=10,  # Adjust based on your system
                max_overflow=20
            )
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise

    def get_month_end_dates(self, year: int) -> pd.DataFrame:
        """
        Fetch the last date of each month for a given year.
        
        :param year: Year to analyze
        :return: DataFrame with last dates of each month
        """
        query = f"""
        SELECT DISTINCT MAX(A_Date) AS Last_Date_Of_Month
        FROM ACCORD_DATA.dbo.ACCORD_DATA
        WHERE A_Date BETWEEN '{year}-01-01' AND '{year}-12-31'
        GROUP BY YEAR(A_Date), MONTH(A_Date)
        ORDER BY Last_Date_Of_Month;
        """
        return pd.read_sql_query(query, self.engine)

    def analyze_top_stocks(self, year: int) -> Tuple[List[List[str]], List[Tuple[str, float]]]:
        """
        Analyze top stocks for a given year.
        
        :param year: Year to analyze
        :return: Tuple of monthly portfolios and monthly returns
        """
        try:
            # Get month end dates
            month_end_dates = self.get_month_end_dates(year)
            months_in_year = len(month_end_dates) - 1

            monthly_returns = []
            monthly_portfolio = []

            # Process each month
            for i in range(months_in_year):
                # Start timing the month's processing
                month_start_time = time.time()

                current_date = month_end_dates.iloc[i]['Last_Date_Of_Month']
                next_date = month_end_dates.iloc[i + 1]['Last_Date_Of_Month']
                
                current_date_str = current_date.strftime('%Y-%m-%d')
                next_date_str = next_date.strftime('%Y-%m-%d')
                
                # Fetch top 500 companies by market cap
                top_500_query = f"""
                SELECT TOP 500 
                    NSE_Symbol, 
                    MCAP_Crs AS Last_MCAP, 
                    A_Date, 
                    A_Close, 
                    Sharpe_365
                FROM ACCORD_DATA.dbo.ACCORD_DATA
                WHERE A_Date = '{current_date_str}'
                ORDER BY MCAP_Crs DESC;
                """
                df_top_500 = pd.read_sql_query(top_500_query, self.engine)
                
                # Select top 15 companies by Sharpe_365
                df_top_15 = df_top_500.nlargest(15, 'Sharpe_365')
                monthly_portfolio.append(df_top_15['NSE_Symbol'].tolist())

                # Calculate percentage changes for top 15 companies
                monthly_percentage_changes = []
                
                for _, row in df_top_15.iterrows():
                    symbol = row['NSE_Symbol']
                    
                    # Fetch closing prices for current and next month
                    close_query = f"""
                    SELECT A_Date, A_Close
                    FROM ACCORD_DATA.dbo.ACCORD_DATA
                    WHERE NSE_Symbol = '{symbol}' AND A_Date IN ('{current_date_str}', '{next_date_str}')
                    ORDER BY A_Date;
                    """
                    df_closes = pd.read_sql_query(close_query, self.engine)
                    
                    if len(df_closes) == 2:
                        current_close = df_closes.loc[df_closes['A_Date'] == current_date, 'A_Close'].iloc[0]
                        next_close = df_closes.loc[df_closes['A_Date'] == next_date, 'A_Close'].iloc[0]

                        percentage_change = ((next_close - current_close) / current_close) * 100
                        monthly_percentage_changes.append(percentage_change)
                
                # Calculate average monthly return
                if monthly_percentage_changes:
                    avg_monthly_change = sum(monthly_percentage_changes) / len(monthly_percentage_changes)
                    monthly_returns.append((current_date_str, avg_monthly_change))

                # Calculate and log processing time
                month_processing_time = time.time() - month_start_time
                logger.info(f"Processed month {i+1}/{months_in_year} | Processing Time: {month_processing_time:.2f} seconds")

            return monthly_portfolio, monthly_returns

        except Exception as e:
            logger.error(f"Error in stock analysis: {e}")
            raise

    def write_results(self, year: int, monthly_portfolio: List[List[str]], monthly_returns: List[Tuple[str, float]]):
        """
        Write analysis results to a file.
        
        :param year: Year of analysis
        :param monthly_portfolio: List of monthly stock portfolios
        :param monthly_returns: List of monthly returns
        """
        filename = f"results_{year}.txt"
        try:
            with open(filename, "a") as f:
                f.write(f"Year: {year}\n\n")
                
                f.write("Monthly Portfolio:\n")
                for month_idx, month_stocks in enumerate(monthly_portfolio, 1):
                    f.write(f"Month {month_idx}: {', '.join(month_stocks)}\n")

                f.write("\nMonthly Returns:\n")
                for month, return_pct in monthly_returns:
                    f.write(f"{month}: {return_pct:.2f}%\n")
            
            logger.info(f"Results written to {filename} for year {year}")
        except IOError as e:
            logger.error(f"Error writing results to file: {e}")

def main():
    # Database connection string
    connection_string = 'mssql+pyodbc://@DELL-123456789\\MSSQLSERVER01/ACCORD_DATA?driver=ODBC+Driver+17+for+SQL+Server'
    
    # Years to analyze
    years = list(range(2011, 2024))
    
    # Create analyzer
    analyzer = StockAnalyzer(connection_string)
    
    # Process each year
    for year in years:
        try:
            # Start timing the year's processing
            year_start_time = time.time()
            
            # Analyze stocks
            monthly_portfolio, monthly_returns = analyzer.analyze_top_stocks(year)
            
            # Write results
            analyzer.write_results(year, monthly_portfolio, monthly_returns)
            
            # Calculate and log total year processing time
            year_processing_time = time.time() - year_start_time
            logger.info(f"Total processing time for {year}: {year_processing_time:.2f} seconds")
        
        except Exception as e:
            logger.error(f"Failed to process year {year}: {e}")

if __name__ == "__main__":
    main()
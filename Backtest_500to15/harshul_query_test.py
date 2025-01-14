import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
import logging
from typing import List, Tuple
import time
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class StockAnalyzer:
    def __init__(self, connection_string: str):
        try:
            self.engine = create_engine(
                connection_string, 
                poolclass=QueuePool,
                pool_size=10,
                max_overflow=20
            )
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise

    def get_month_end_dates(self, year: int) -> pd.DataFrame:
        y1 = year-1
        y2 = year+1
        query = f"""
        SELECT DISTINCT MAX(A_Date) AS Last_Date_Of_Month
        FROM ACCORD_DATA_SUBSET.dbo.ACCORD_DATA
        WHERE A_Date BETWEEN '{y1}-12-01' AND '{y2}-01-31'
        GROUP BY YEAR(A_Date), MONTH(A_Date)
        ORDER BY Last_Date_Of_Month;
        """
        return pd.read_sql_query(query, self.engine)

    def get_daily_portfolio_value(self, symbols: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """
        Calculate daily portfolio value for given symbols between dates.
        """
        symbols_str = "','".join(symbols)
        query = f"""
        SELECT 
            NSE_Symbol,
            --MAX(CASE WHEN A_Date = DATEADD(day, 1, '{start_date}') THEN A_Close END) AS A_Open,
            --MAX(CASE WHEN A_Date = '{end_date}' THEN A_Close END) AS A_Close,
            --(MAX(CASE WHEN A_Date = '{end_date}' THEN A_Close END) / MAX(CASE WHEN A_Date = DATEADD(day, 1, '{start_date}') THEN A_Close END) - 1) * 100 AS A_Return
            A_Date,
            A_Close
        FROM ACCORD_DATA_SUBSET.dbo.ACCORD_DATA
        WHERE NSE_Symbol IN ('{symbols_str}')
        AND A_Date BETWEEN DATEADD(day, 1, '{start_date}') AND '{end_date}'
        --GROUP BY NSE_Symbol
        --ORDER BY NSE_Symbol;
        """
        df = pd.read_sql_query(query, self.engine)
        
       # Pivot to get daily closing prices
        portfolio_value = df.pivot(index='A_Date', columns='NSE_Symbol', values='A_Close')
        
        # Calculate the first closing price for each symbol in the monthly portfolio
        first_closes = portfolio_value.iloc[0]
        
        # Calculate the adjusted daily portfolio value
        adjusted_portfolio_value = portfolio_value.div(first_closes)
        
        # Sum the adjusted values to get the total value
        adjusted_portfolio_value['Total_Value'] = adjusted_portfolio_value.sum(axis=1)

        return adjusted_portfolio_value

    def calculate_max_drawdown(self, portfolio_values: pd.Series) -> float:
        """
        Calculate maximum drawdown from a series of portfolio values.
        """
        rolling_max = portfolio_values.expanding(min_periods=1).max()
        drawdowns = (portfolio_values - rolling_max) / rolling_max
        return float(drawdowns.min() * 100)  # Convert to percentage

    def analyze_top_stocks(self, year: int) -> Tuple[List[List[str]], List[Tuple[str, float]], float, float]:
        try:
            month_end_dates = self.get_month_end_dates(year)
            months_in_year = len(month_end_dates)-2

            monthly_returns = []
            monthly_portfolio = []
            all_portfolio_values = pd.DataFrame()

            # total_return = 0.0

            # Process each month
            for i in range(months_in_year):
                month_start_time = time.time()

                current_date = month_end_dates.iloc[i]['Last_Date_Of_Month']
                next_date = month_end_dates.iloc[i + 1]['Last_Date_Of_Month']
                
                current_date_str = current_date.strftime('%Y-%m-%d')
                next_date_str = next_date.strftime('%Y-%m-%d')

                # trying Harshul's query add one day to current_date_str
                #current_date_str = (current_date + pd.DateOffset(days=1)).strftime('%Y-%m-%d')
                # current_date_str = '2011-02-01'
                # next_date_str = '2011-03-03'

                # Your existing top 500 query remains the same
                top_500_query = f"""
                WITH RankedCompanies AS (
                    SELECT DISTINCT TOP 500 
                        NSE_Symbol,
                        MCAP_Crs
                    FROM ACCORD_DATA_SUBSET.dbo.ACCORD_DATA
                    WHERE A_Date = '{current_date_str}'
                    AND Sharpe_365 IS NOT NULL
                    ORDER BY MCAP_Crs DESC
                ),
                LastFiveDays AS (
                    SELECT 
                        a.NSE_Symbol,
                        a.A_Date,
                        a.A_Close,
                        a.Sharpe_365,
                        a.MCAP_Crs,
                        ROW_NUMBER() OVER (PARTITION BY a.NSE_Symbol ORDER BY a.A_Date DESC) as rn
                    FROM ACCORD_DATA_SUBSET.dbo.ACCORD_DATA a
                    INNER JOIN RankedCompanies r ON a.NSE_Symbol = r.NSE_Symbol
                    --WHERE a.A_Date <= '{current_date_str}'
                    --AND a.A_Date > DATEADD(day, -5, '{current_date_str}')
                    --Harshul's query
                    WHERE a.A_Date = '{current_date_str}'
                )
                SELECT TOP 10 --Harshul's query 
                    NSE_Symbol,
                    -- MAX(MCAP_Crs) as Last_MCAP,
                    -- MAX(CASE WHEN rn = 1 THEN A_Close END) as A_Close,
                    AVG(Sharpe_365) as Avg_Sharpe_365
                FROM LastFiveDays
                GROUP BY NSE_Symbol
                -- HAVING COUNT(*) = 5
                ORDER BY AVG(Sharpe_365) DESC;
                """
                
                # df_top_500 = pd.read_sql_query(top_500_query, self.engine)
                # df_top_15 = df_top_500.head(15)
                df_top_15 = pd.read_sql_query(top_500_query, self.engine)

                if df_top_15.empty:
                    logger.info("No data found for the top 15 symbols on {current_date_str}")
                    logger.info("Current date: {current_date_str}")
                    continue

                current_portfolio = df_top_15['NSE_Symbol'].tolist()
                monthly_portfolio.append(current_portfolio)

                # Get daily portfolio values for current month
                portfolio_values = self.get_daily_portfolio_value(
                    current_portfolio,
                    current_date_str,
                    next_date_str
                )
                
                all_portfolio_values = pd.concat([all_portfolio_values, portfolio_values])
                
                # Calculate monthly return
                if not portfolio_values.empty:
                    start_value = portfolio_values['Total_Value'].iloc[0]
                    end_value = portfolio_values['Total_Value'].iloc[-1]
                    # monthly_return = 0.0
                    # for symbol in current_portfolio:
                    #     # monthly_return += (portfolio_values[symbol].last() / portfolio_values[symbol].first() - 1) * 100 
                    #     monthly_return += (portfolio_values[symbol].iloc[-1] / portfolio_values[symbol].iloc[0] - 1) * 100
                    # monthly_return = portfolio_values['A_Return'].sum()
                    #monthly_return = ((end_value - start_value) / start_value) * 100
                    #Assuming all stocks have equal weightage and unit quantity
                    monthly_return = ((end_value - start_value) / start_value) *current_portfolio.__len__()* 100
                    monthly_returns.append((current_date_str, monthly_return))
                    # total_return += monthly_return
                month_processing_time = time.time() - month_start_time
                logger.info(f"Processed month {i+1}/{months_in_year} | Processing Time: {month_processing_time:.2f} seconds")
            # Calculate total return and maximum drawdown
            if not all_portfolio_values.empty:
                # total_return = ((all_portfolio_values['Total_Value'].iloc[-1] - 
                #                all_portfolio_values['Total_Value'].iloc[0]) /
                #               all_portfolio_values['Total_Value'].iloc[0] )* 100
                max_drawdown = self.calculate_max_drawdown(all_portfolio_values['Total_Value'])
            else:
                total_return = max_drawdown = 0.0
            total_return = sum([mr[1] for mr in monthly_returns])
            logger.info(f"Total Return: {total_return:.2f}%")
            return monthly_portfolio, monthly_returns, total_return, max_drawdown

        except Exception as e:
            logger.error(f"Error in stock analysis: {e}")
            raise

    def write_results(self, year: int, monthly_portfolio: List[List[str]], 
                     monthly_returns: List[Tuple[str, float]], 
                     total_return: float, max_drawdown: float):
        filename = f"../results/long_15_from_500/results_{year}.txt"
        try:
            with open(filename, "a") as f:
                f.write(f"Year: {year}\n\n")
                
                f.write("Monthly Portfolio:\n")
                for month_idx, month_stocks in enumerate(monthly_portfolio, 1):
                    f.write(f"Month {month_idx}: {', '.join(month_stocks)}\n")

                f.write("\nMonthly Returns:\n")
                for month, return_pct in monthly_returns:
                    f.write(f"{month}: {return_pct:.2f}%\n")
                
                f.write(f"\nTotal Return: {total_return:.2f}%\n")
                f.write(f"Maximum Drawdown: {max_drawdown:.2f}%\n")
            
            logger.info(f"Results written to {filename} for year {year}")
            logger.info(f"Total Return: {total_return:.2f}%")
            logger.info(f"Maximum Drawdown: {max_drawdown:.2f}%")
        except IOError as e:
            logger.error(f"Error writing results to file: {e}")

def main():
    connection_string = 'mssql+pyodbc://@DELL-123456789\\MSSQLSERVER01/ACCORD_DATA_SUBSET?driver=ODBC+Driver+17+for+SQL+Server'
    years = list(range(2011,2012))
    analyzer = StockAnalyzer(connection_string)
    portfolio_returns = []
    portfolio_drawdown = []
    
    for year in years:
        try:
            year_start_time = time.time()
            monthly_portfolio, monthly_returns, total_return, max_drawdown = analyzer.analyze_top_stocks(year)
            analyzer.write_results(year, monthly_portfolio, monthly_returns, total_return, max_drawdown)
            year_processing_time = time.time() - year_start_time
            logger.info(f"Total processing time for {year}: {year_processing_time:.2f} seconds\n\n")
            portfolio_returns.append(total_return)
            portfolio_drawdown.append(max_drawdown)

        except Exception as e:
            logger.error(f"Failed to process year {year}: {e}")
        logger
    logger.info(f"For the period {years[0]}-{years[-1]}:")
    logger.info(f"Average Total Return: {np.sum(portfolio_returns):.2f}%")
    logger.info(f"Average Maximum Drawdown: {np.min(portfolio_drawdown):.2f}%")
if __name__ == "__main__":
    main()
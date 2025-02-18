import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
import logging
from typing import List, Tuple
import time
import numpy as np
import matplotlib.pyplot as plt


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
            A_Date,
            NSE_Symbol,
            A_Close
        FROM ACCORD_DATA_SUBSET.dbo.ACCORD_DATA
        WHERE NSE_Symbol IN ('{symbols_str}')
        --AND A_Date BETWEEN DATEADD(day,1,'{start_date}') AND '{end_date}'
        AND A_Date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY A_Date, NSE_Symbol;
        """
        df = pd.read_sql_query(query, self.engine)
        
        # Pivot to get daily closing prices
        portfolio_value = df.pivot(index='A_Date', columns='NSE_Symbol', values='A_Close')
        
        # Calculate the first closing price for each symbol in the monthly portfolio
        # first_open_value = self.get_open_value(start_date, symbols)
        # scaled_values = scaled_values / first_day_open_value
        first_closes = portfolio_value.iloc[0]
        
        # Calculate the adjusted daily portfolio value
        adjusted_portfolio_value = portfolio_value.div(first_closes)
        # adjusted_portfolio_value = portfolio_value.div(first_open_value)

        # Remove the first date as it is not needed
        adjusted_portfolio_value = adjusted_portfolio_value.iloc[1:]

        # Sum the adjusted values to get the total value
        adjusted_portfolio_value['Total_Value'] = adjusted_portfolio_value.sum(axis=1)
        # Calculate individual stock returns
        last_closes = portfolio_value.iloc[-1]
        individual_stock_returns = [(symbol, (last_closes[symbol] - first_closes[symbol]) / first_closes[symbol] * 100) for symbol in symbols]
    
        return adjusted_portfolio_value, individual_stock_returns

    def calculate_max_drawdown(self, portfolio_values: pd.Series) -> float:
        """
        Calculate maximum drawdown from a series of portfolio values.
        """
        rolling_max = portfolio_values.expanding(min_periods=1).max()
        drawdowns = (portfolio_values - rolling_max) / rolling_max
        return float(drawdowns.min() * 100)  # Convert to percentage
   
    def get_full_portfolio_series(self, all_monthly_portfolio_values: List[pd.DataFrame], portfolio_size: int) -> pd.Series:
        """
        Concatenate all monthly portfolio values into a single series.
        """
        
        # Initialize portfolio value at 1
        current_portfolio_value = 1
        all_daily_values = []
        symbols_by_month_list = []
        
        # Process each month's data across all years
        for portfolio_df in all_monthly_portfolio_values:
            if portfolio_df.empty:
                continue
                
            # Get portfolio NSE_symbols for each month (remove column 'Total_Value')
            symbols = list(portfolio_df.columns)
            symbols.remove('Total_Value')

            # List of symbols by month
            symbols_by_month_list.append(pd.Series(symbols, name=portfolio_df.index[0]))

            # Get normalized daily values for current month
            daily_values = portfolio_df['Total_Value'].copy()
            
            # Scale the daily values by the current portfolio value
            scaled_values = daily_values * (current_portfolio_value / portfolio_size)
            
            # Scale the values to account for the difference in A_Close and A_Open of first day
            # first_day_open_value = self.get_open_value(portfolio_df.index[0], symbols)
            # scaled_values = scaled_values / first_day_open_value

            # Store the values
            # all_daily_values is a list of pandas Series, 
            # each representing the daily portfolio values for a specific month.
            all_daily_values.append(pd.Series(scaled_values, index=portfolio_df.index))
            
            # Update portfolio value for next month
            current_portfolio_value = scaled_values.iloc[-1]
        
        # Combine all daily values
        full_portfolio_series = pd.concat(all_daily_values)

        # Combine all symbols by month
        symbols_by_month = pd.concat(symbols_by_month_list, axis=1)
    
        # Write daily portfolio returns of full_portfolio_series in file: daily_portfolio_returns.csv
        full_portfolio_series.to_csv('../results/daily_portfolio_returns.csv', index=True)

        # Write symbols by month in file: symbols_by_month.csv
        symbols_by_month.to_csv('../results/symbols_by_month.csv', index=True)
        

        return full_portfolio_series

    def get_open_value(self, date: str, symbols: List[str]) -> float:
        """
        Fetch the opening value for the given date and symbols.
        """
        # Join all but last symbol with comma
        # symbols_str = ','.join(symbols[:-1])
        symbols_str = "','".join(symbols)
        query = f"""
        SELECT 
            A_Date,
            NSE_Symbol,
            A_Open
        FROM ACCORD_DATA_SUBSET.dbo.ACCORD_DATA
        WHERE NSE_Symbol IN ('{symbols_str}')
        AND A_Date = '{date}'
        """
        df = pd.read_sql_query(query, self.engine)
        
        return df['A_Open'].sum()

    def plot_portfolio_performance(self, all_monthly_portfolio_values: List[pd.DataFrame], start_year: int, end_year: int, portfolio_size: int, max_drawdown: float) -> None:
        """
        Plot daily portfolio values for the entire period across multiple years.
        
        Args:
            all_monthly_portfolio_values: List of DataFrames containing daily portfolio values for all months
            all_monthly_returns: List of (date, return) tuples for all months
            start_year: First year in the analysis
            end_year: Last year in the analysis
        """
        plt.figure(figsize=(15, 8))
        
        # Get the full portfolio series normalised and scaled monthly
        full_portfolio_series = self.get_full_portfolio_series(all_monthly_portfolio_values, portfolio_size)
        # for portfolio_df in all_monthly_portfolio_values:
        #     if portfolio_df.empty:
        #         continue
                
        #     # Get normalized daily values for current month
        #     daily_values = portfolio_df['Total_Value'].copy()
            
        #     # Scale the daily values by the current portfolio value
        #     scaled_values = daily_values * (current_portfolio_value / portfolio_size)
            
        #     # Store the values
        #     all_daily_values.append(pd.Series(scaled_values, index=portfolio_df.index))
            
        #     # Update portfolio value for next month
        #     current_portfolio_value = scaled_values.iloc[-1]
        
        # # Combine all daily values
        # full_portfolio_series = pd.concat(all_daily_values)
        
       
        # Plot the portfolio value
        plt.plot(full_portfolio_series.index, full_portfolio_series.values, 'b-', linewidth=2)
        
        # Add labels and title
        plt.title(f'Portfolio Performance ({start_year}-{end_year})', fontsize=14)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Portfolio Value', fontsize=12)
        
        # Format x-axis dates
        plt.gcf().autofmt_xdate()
        
        # Add grid
        plt.grid(True, linestyle='--', alpha=0.7)
        
        # Add annotations for key metrics
        total_return = ((full_portfolio_series.iloc[-1] - full_portfolio_series.iloc[0]) / 
                       full_portfolio_series.iloc[0] * 100)
        #max_drawdown = self.calculate_max_drawdown(full_portfolio_series)
        
        # Add CAGR calculation
        days = (full_portfolio_series.index[-1] - full_portfolio_series.index[0]).days
        years = days / 365.25
        cagr = (((full_portfolio_series.iloc[-1] / full_portfolio_series.iloc[0]) ** (1/years)) - 1) * 100
        
        plt.annotate(f'Total Return: {total_return:.1f}%\nCAGR: {cagr:.1f}%\nMax Drawdown: {max_drawdown:.1f}%',
                    xy=(0.02, 0.95), xycoords='axes fraction',
                    bbox=dict(facecolor='white', alpha=0.8, edgecolor='none'))
        
        # Save the plot
        plt.savefig(f'../results/portfolio_performance_{start_year}_{end_year}.png', dpi=300, bbox_inches='tight')
        plt.show()
        plt.close()    
        return total_return    

    def get_top_stocks(self, current_date_str: str, portfolio_size: int) -> List[str]:
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
                    WHERE a.A_Date = '{current_date_str}'
                    --WHERE a.A_Date <= '{current_date_str}'
                    --AND a.A_Date > DATEADD(day, -5, '{current_date_str}')
                )
                SELECT TOP {portfolio_size} 
                    NSE_Symbol
                    -- MAX(MCAP_Crs) as Last_MCAP,
                    -- MAX(CASE WHEN rn = 1 THEN A_Close END) as A_Close,
                    --,AVG(Sharpe_365) as Avg_Sharpe_365
                FROM LastFiveDays
                GROUP BY NSE_Symbol
                -- HAVING COUNT(*) = 5
                ORDER BY AVG(Sharpe_365) DESC;
                """
                
        # df_top_500 = pd.read_sql_query(top_500_query, self.engine)
        # df_top_15 = df_top_500.head(15)
        df_top_15 = pd.read_sql_query(top_500_query, self.engine)
        return df_top_15
    
    def analyze_top_stocks(self, year: int, portfolio_size: int) -> Tuple[List[List[str]], List[Tuple[str, float]], float, float]:
        monthly_returns = []
        monthly_portfolio = []
        monthly_portfolio_values = []  # New list to store portfolio values
        all_portfolio_values = pd.DataFrame()
        monthly_individual_stock_returns = []
        
        try:
            # Step 1: Get month end dates for the year
            month_end_dates = self.get_month_end_dates(year)
            months_in_year = len(month_end_dates)-2
            
            # Process each month
            for i in range(months_in_year):
                month_start_time = time.time()

                portfolio_selection_date = month_end_dates.iloc[i]['Last_Date_Of_Month']
                next_portfolio_selection_date = month_end_dates.iloc[i + 1]['Last_Date_Of_Month']
                
                portfolio_selection_date_str = portfolio_selection_date.strftime('%Y-%m-%d')
                next_portfolio_selection_date_str = next_portfolio_selection_date.strftime('%Y-%m-%d')

                # Step 2: Get top 15 stocks on portfolio_selection_date
                df_top_15 = self.get_top_stocks(portfolio_selection_date_str, portfolio_size)

                if df_top_15.empty:
                    logger.info("No data found for the top 15 symbols on {current_date_str}")
                    logger.info("Current date: {current_date_str}")
                    continue


                current_portfolio = df_top_15['NSE_Symbol'].tolist()
                monthly_portfolio.append(current_portfolio)

                # Step 3: Get daily portfolio values for current month
                portfolio_values, individual_stock_returns = self.get_daily_portfolio_value(
                    current_portfolio,
                    portfolio_selection_date_str,
                    next_portfolio_selection_date_str
                )
                
                # Store the monthly portfolio values
                monthly_portfolio_values.append(portfolio_values)

                all_portfolio_values = pd.concat([all_portfolio_values, portfolio_values])
                monthly_individual_stock_returns.append(individual_stock_returns)
                
                # Step 4: Calculate monthly return
                if not portfolio_values.empty:
                    start_value = portfolio_values['Total_Value'].iloc[0]
                    end_value = portfolio_values['Total_Value'].iloc[-1]

                    monthly_portfolio_return = ((end_value - start_value) / start_value) * 100

                    monthly_returns.append((portfolio_selection_date_str, monthly_portfolio_return))

                month_processing_time = time.time() - month_start_time
                logger.info(f"Processed month {i+1}/{months_in_year} | Processing Time: {month_processing_time:.2f} seconds")
            
            # Step 5: Calculate maximum drawdown
            if not all_portfolio_values.empty:
                max_drawdown = self.calculate_max_drawdown(all_portfolio_values['Total_Value'])
            else:
                max_drawdown = 0.0

            # Calculate total return
            annual_return = sum([mr[1] for mr in monthly_returns])
            logger.info(f"Annual Return for year {year}: {annual_return:.2f}%")
            return monthly_portfolio, monthly_returns, annual_return, max_drawdown, monthly_individual_stock_returns, monthly_portfolio_values


        except Exception as e:
            logger.error(f"Error in stock analysis: {e}")
            raise

    def write_results(self, year: int, monthly_portfolio: List[List[str]], 
                     monthly_returns: List[Tuple[str, float]], 
                     annual_return: float, max_drawdown: float,
                     monthly_individual_returns: List[List[Tuple[str, float]]]):

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
                
                f.write(f"\nTotal Return: {annual_return:.2f}%\n")
                f.write(f"Maximum Drawdown: {max_drawdown:.2f}%\n")

                f.write("\nIndividual Stock Returns:\n")
                for month_idx, individual_returns in enumerate(monthly_individual_returns, 1):
                    f.write(f"\nMonth {month_idx}:\n")
                    for symbol, return_pct in individual_returns:
                        f.write(f"{symbol}: {return_pct:.2f}%\n")
            
            logger.info(f"Results written to {filename} for year {year}")
            logger.info(f"Total Return: {annual_return:.2f}%")
            logger.info(f"Maximum Drawdown: {max_drawdown:.2f}%")
        except IOError as e:
            logger.error(f"Error writing results to file: {e}")

def main():
    connection_string = 'mssql+pyodbc://@DELL-123456789\\MSSQLSERVER01/ACCORD_DATA_SUBSET?driver=ODBC+Driver+17+for+SQL+Server'
    
    #enter the period and portfolio size
    years = list(range(2011,2014))
    portfolio_size = 10
    
    analyzer = StockAnalyzer(connection_string)
    portfolio_drawdown = []

    # Lists to store all monthly data
    all_monthly_portfolio_values = []
    all_monthly_returns = []
    
    for year in years:
        
        try:
            year_start_time = time.time()
            monthly_portfolio, monthly_returns, annual_return, max_drawdown, monthly_individual_stock_returns, monthly_portfolio_values = (
                analyzer.analyze_top_stocks(year, portfolio_size)
            )

            # Accumulate monthly data
            all_monthly_portfolio_values.extend(monthly_portfolio_values)
            all_monthly_returns.extend(monthly_returns)

            portfolio_drawdown.append(max_drawdown)

            # Write results to file and log time
            analyzer.write_results(year, monthly_portfolio, monthly_returns, annual_return, max_drawdown, monthly_individual_stock_returns)
            year_processing_time = time.time() - year_start_time
            logger.info(f"Total processing time for {year}: {year_processing_time:.2f} seconds\n\n")

        except Exception as e:
            logger.error(f"Failed to process year {year}: {e}")
        logger
      
    # Create single plot for all years
    total_portfolio_return = analyzer.plot_portfolio_performance(all_monthly_portfolio_values, years[0], years[-1], portfolio_size, min(portfolio_drawdown))

    logger.info(f"For the period {years[0]}-{years[-1]}:")
    logger.info(f"Total Return: {total_portfolio_return:.2f}%")
    logger.info(f"Maximum Drawdown: {np.min(portfolio_drawdown):.2f}%")

    
if __name__ == "__main__":
    main()
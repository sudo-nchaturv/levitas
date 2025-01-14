import pandas as pd
import matplotlib.pyplot as plt

def plot_portfolio_value(portfolio, returns):
    """
    Plots the overall portfolio value over time.

    Parameters:
    portfolio (pd.DataFrame): A dataframe with dates as index and stocks with their quantities as columns.
                              Example:
                              Date         StockA  StockB
                              2024-01-01   10      5
                              2024-01-02   15      10

    returns (pd.DataFrame): A dataframe with dates as index and stocks with their daily returns as columns.
                            Example:
                            Date         StockA  StockB
                            2024-01-01   0.02    0.01
                            2024-01-02   -0.01   0.03

    Output:
    Displays a plot showing portfolio value over time.
    """
    # Ensure both dataframes have the same date index
    portfolio.index = pd.to_datetime(portfolio.index)
    returns.index = pd.to_datetime(returns.index)

    # Calculate daily stock values based on returns
    stock_values = (1 + returns).cumprod() * portfolio

    # Calculate portfolio value by summing across all stocks
    portfolio_value = stock_values.sum(axis=1)

    # Plot the portfolio value
    plt.figure(figsize=(10, 6))
    plt.plot(portfolio_value, label='Portfolio Value', marker='o')
    plt.title('Portfolio Value Over Time', fontsize=16)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Portfolio Value', fontsize=12)
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.show()

# Example Usage
portfolio = pd.DataFrame({
    'StockA': [10, 15, 15],
    'StockB': [5, 10, 10]
}, index=['2024-01-01', '2024-01-02', '2024-01-03'])

returns = pd.DataFrame({
    'StockA': [0.02, -0.01, 0.03],
    'StockB': [0.01, 0.03, -0.02]
}, index=['2024-01-01', '2024-01-02', '2024-01-03'])

plot_portfolio_value(portfolio, returns)

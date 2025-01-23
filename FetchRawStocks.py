import time
import sqlite3
import pandas as pd
from utils import fetch_yfinance_data, fetch_us_symbols, fetch_sp500_symbols
import os, sys

# Constants
db_name = "stocks.db"
default_batch_size = 100  # Optimize for large runs

FINNHUB_API_KEY = "ctpgeohr01qqsrsaov10ctpgeohr01qqsrsaov1g"


def add_performance_and_cdpp(stock_data):
    # Calculate performance
    stock_data['Performance'] = ((stock_data['Close'] - stock_data['Open']) / stock_data['Open']) * 100
    
    # Initialize the cdpp column
    stock_data['cdpp'] = 0
    
    # Sort by date to ensure consecutive days are in order
    stock_data = stock_data.sort_values(by='Date')
    
    # Group by Ticker to calculate cdpp for each stock separately
    for ticker, group in stock_data.groupby('Ticker'):
        count = 0
        for i in range(len(group)):
            if group.iloc[i]['Performance'] > 1:
                count += 1
            else:
                count = 0
            stock_data.loc[group.index[i], 'cdpp'] = count
    
    return stock_data


import numpy as np

def add_technical_indicators(stock_data):
    # Simple Moving Average (SMA)
    stock_data['SMA5'] = stock_data['Close'].rolling(window=5).mean()
    stock_data['SMA10'] = stock_data['Close'].rolling(window=10).mean()
    stock_data['SMA20'] = stock_data['Close'].rolling(window=20).mean()
    
    # Exponential Moving Average (EMA)
    stock_data['EMA5'] = stock_data['Close'].ewm(span=5, adjust=False).mean()
    stock_data['EMA10'] = stock_data['Close'].ewm(span=10, adjust=False).mean()
    stock_data['EMA20'] = stock_data['Close'].ewm(span=20, adjust=False).mean()
    
    # Relative Strength Index (RSI)
    delta = stock_data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    stock_data['RSI'] = 100 - (100 / (1 + rs))
    
    # Moving Average Convergence Divergence (MACD)
    stock_data['MACD'] = stock_data['Close'].ewm(span=12, adjust=False).mean() - stock_data['Close'].ewm(span=26, adjust=False).mean()
    stock_data['MACD Signal'] = stock_data['MACD'].ewm(span=9, adjust=False).mean()
    
    # Bollinger Bands
    stock_data['BB Middle'] = stock_data['Close'].rolling(window=20).mean()
    stock_data['BB Upper'] = stock_data['BB Middle'] + 2 * stock_data['Close'].rolling(window=20).std()
    stock_data['BB Lower'] = stock_data['BB Middle'] - 2 * stock_data['Close'].rolling(window=20).std()
    
    # Calculate daily return
    stock_data['Daily Return'] = stock_data['Close'].pct_change(fill_method=None)
    
    # Beta, Alpha, and Sharpe Ratio
    # Assuming 'SPY' is the market benchmark
    market_data = stock_data[stock_data['Ticker'] == 'SPY']
    if not market_data.empty:
        for ticker, group in stock_data.groupby('Ticker'):
            if ticker == 'SPY':
                continue
            if len(group) > 1 and len(market_data) > 1:
                cov_matrix = np.cov(group['Daily Return'][1:], market_data['Daily Return'][1:])
                beta = cov_matrix[0, 1] / cov_matrix[1, 1]
                alpha = group['Daily Return'].mean() - beta * market_data['Daily Return'].mean()
                sharpe_ratio = group['Daily Return'].mean() / group['Daily Return'].std()
                
                stock_data.loc[group.index, 'Beta'] = beta
                stock_data.loc[group.index, 'Alpha'] = alpha
                stock_data.loc[group.index, 'Sharpe Ratio'] = sharpe_ratio
            else:
                stock_data.loc[group.index, 'Beta'] = np.nan
                stock_data.loc[group.index, 'Alpha'] = np.nan
                stock_data.loc[group.index, 'Sharpe Ratio'] = np.nan
    
    return stock_data





if __name__ == "__main__":

    print("> Starting the application...")
    start_time = time.time()

    # Fetch symbols from Finnhub
    symbols_df = fetch_us_symbols(FINNHUB_API_KEY)
    
    # Fetch S&P 500 symbols
    sp500_df = fetch_sp500_symbols()
    
    # Convert the DataFrame to a list of 1000 tickers
    symbols_list = sp500_df['Symbol'].tolist()
    # symbols_list = symbols_df['Symbol'].head(100).tolist()

    print(symbols_list)
    print(type(symbols_list))

    # Fetch stock data
    #stock_data = fetch_yfinance_data(symbols_list, default_batch_size)
    
    #rint("> Saving the data as pickle file...")
    #stock_data.to_pickle("stock_data.pkl")

    print("> Loading the data from pickle file...")
    stock_data = pd.read_pickle("stock_data.pkl")

    print(stock_data.info())
    print(stock_data.head())

    # Add performance and cdpp columns
    stock_data = add_performance_and_cdpp(stock_data)

    print(stock_data.info())
    print(stock_data.head())

    # Add technical indicators
    stock_data = add_technical_indicators(stock_data)
    print(stock_data.info())
    print(stock_data.head())




    # Create a connection to the SQLite database
    conn = sqlite3.connect('stocks.db')

    # Save the DataFrame to the SQLite database
    stock_data.to_sql('stock_data', conn, if_exists='replace', index=False)

    # Close the connection
    conn.close()

    print("DataFrame saved to SQLite database successfully.")
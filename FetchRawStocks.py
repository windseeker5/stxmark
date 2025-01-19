import time
import sqlite3
import pandas as pd
from utils import fetch_yfinance_data, fetch_us_symbols, fetch_sp500_symbols
import os, sys

# Constants
db_name = "stocks.db"
default_batch_size = 100  # Optimize for large runs

FINNHUB_API_KEY = "ctpgeohr01qqsrsaov10ctpgeohr01qqsrsaov1g"

if __name__ == "__main__":

    print("> Starting the application...")
    start_time = time.time()

    # Fetch symbols from Finnhub
    # symbols_df = fetch_us_symbols(FINNHUB_API_KEY)
    
    # Convert the DataFrame to a list of 1000 tickers
    # symbols_list = symbols_df['Symbol'].head(1000).tolist()
    # print(symbols_list)

    # Fetch S&P 500 symbols
    sp500_df = fetch_sp500_symbols()
    
    # Convert the DataFrame to a list of 1000 tickers
    symbols_list = sp500_df['Symbol'].tolist()

    # Fetch stock data
    # stock_data = fetch_yfinance_data(symbols_list, default_batch_size)
    
    # print("> Saving the data as pickle file...")
    # stock_data.to_pickle("stock_data.pkl")

    print("> Loading the data from pickle file...")
    stock_data = pd.read_pickle("stock_data.pkl")

    # Check if required columns are present
    required_columns = ['High', 'Low', 'Close', 'Ticker']
    for col in required_columns:
        if col not in stock_data.columns:
            raise KeyError(f"Column '{col}' not found in the data")

    # Convert the Date column to datetime format
    stock_data['Date'] = pd.to_datetime(stock_data['Date'])

    # Calculate daily performance based on Open and Close prices
    stock_data['Performance'] = ((stock_data['Close'] - stock_data['Open']) / stock_data['Open']) * 100

    # Ensure data is sorted by Date within each Ticker group
    stock_data = stock_data.sort_values(by=['Ticker', 'Date'])

    # Calculate 20-day Simple Moving Average (SMA)
    stock_data['SMA_20'] = stock_data.groupby('Ticker')['Close'].transform(lambda x: x.rolling(window=20, min_periods=1).mean())

    # Calculate Bollinger Bands
    def calculate_bollinger_bands(series, window=20, num_std_dev=2):
        sma = series.rolling(window=window, min_periods=1).mean()
        std_dev = series.rolling(window=window, min_periods=1).std()
        upper_band = sma + (std_dev * num_std_dev)
        lower_band = sma - (std_dev * num_std_dev)
        return pd.DataFrame({'BB_upper': upper_band, 'BB_lower': lower_band})

    # Apply Bollinger Bands calculation and ensure the lengths match
    bb_values = stock_data.groupby('Ticker')['Close'].apply(lambda x: calculate_bollinger_bands(x, window=20)).reset_index(level=0, drop=True)
    stock_data = pd.concat([stock_data, bb_values], axis=1)

    # Debugging: Check for NaN values in the calculated columns
    print("SMA_20 NaN count:", stock_data['SMA_20'].isna().sum())
    print("BB_upper NaN count:", stock_data['BB_upper'].isna().sum())
    print("BB_lower NaN count:", stock_data['BB_lower'].isna().sum())

    # Calculate Relative Strength Index (RSI) with a 14-day window
    def calculate_rsi(series, window=14):
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window, min_periods=1).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    stock_data['RSI_14'] = stock_data.groupby('Ticker')['Close'].transform(lambda x: calculate_rsi(x, window=14))

    def calculate_atr_vectorized(high, low, close, window=14):
        high_low = high - low
        high_close = (high - close.shift()).abs()
        low_close = (low - close.shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        return tr.rolling(window=window, min_periods=1).mean()

    # Ensure the columns 'High', 'Low', and 'Close' exist
    if all(col in stock_data.columns for col in ['High', 'Low', 'Close']):
        stock_data['ATR_14'] = stock_data.groupby('Ticker', group_keys=False).transform(
            lambda group: calculate_atr_vectorized(group['High'], group['Low'], group['Close'])
        )
    else:
        print("Columns 'High', 'Low', or 'Close' are missing from the DataFrame.")

    def calculate_obv_vectorized(close, volume):
        direction = close.diff().fillna(0).apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        return (volume * direction).cumsum()

    stock_data['OBV'] = stock_data.groupby('Ticker', group_keys=False).transform(
        lambda group: calculate_obv_vectorized(group['Close'], group['Volume'])
    )

    end_time = time.time()
    duration_minutes = (end_time - start_time) / 60
    
    print(f"Time taken to process {len(symbols_list)} symbols: {duration_minutes:.2f} minutes")
    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    print(stock_data.head())
    print(stock_data.shape)

    # Save the DataFrame to a CSV file
    # stock_data.to_csv("stock_data.csv", index=False)

    # Save the DataFrame to a SQLite database
    conn = sqlite3.connect(db_name)
    stock_data.to_sql('stock_data', conn, if_exists='replace', index=False)
    conn.close()
    print(f"Data saved to {db_name} database.")

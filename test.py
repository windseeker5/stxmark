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

    # Fetch S&P 500 symbols
    sp500_df = fetch_sp500_symbols()
    symbols_list = sp500_df['Symbol'].tolist()

    # Load stock data
    print("> Loading the data from pickle file...")
    stock_data = pd.read_pickle("stock_data.pkl")

    # Convert the Date column to datetime format
    stock_data['Date'] = pd.to_datetime(stock_data['Date'])

    # Calculate daily performance based on Open and Close prices
    stock_data['Performance'] = ((stock_data['Close'] - stock_data['Open']) / stock_data['Open']) * 100


    # Calculate consecutive days with performance above 0.2%
    stock_data['CDPP'] = stock_data.groupby('Ticker')['Performance'].apply(
        lambda x: (x > 0.2).astype(int).groupby((x <= 0.2).cumsum()).cumsum()
    ).reset_index(level=0, drop=True)



    # Ensure data is sorted by Date within each Ticker group
    stock_data = stock_data.sort_values(by=['Ticker', 'Date'])

    # Calculate 20-day Simple Moving Average (SMA)
    stock_data['SMA_20'] = stock_data.groupby('Ticker')['Close'].transform(
        lambda x: x.rolling(window=20, min_periods=1).mean()
    )

    # Calculate Bollinger Bands
    def calculate_bollinger_bands(series, window=20, num_std_dev=2):
        sma = series.rolling(window=window, min_periods=1).mean()
        std_dev = series.rolling(window=window, min_periods=1).std()
        upper_band = sma + (std_dev * num_std_dev)
        lower_band = sma - (std_dev * num_std_dev)
        return pd.DataFrame({'BB_upper': upper_band, 'BB_lower': lower_band})

    bb_values = stock_data.groupby('Ticker')['Close'].apply(
        lambda x: calculate_bollinger_bands(x, window=20)
    ).reset_index(level=0, drop=True)
    stock_data = pd.concat([stock_data, bb_values], axis=1)

    # Calculate Relative Strength Index (RSI) with a 14-day window
    def calculate_rsi(series, window=14):
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window, min_periods=1).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    stock_data['RSI_14'] = stock_data.groupby('Ticker')['Close'].transform(
        lambda x: calculate_rsi(x, window=14)
    )

    # Calculate Average True Range (ATR) with a 14-day window
    def calculate_atr(df, window=14):
        high_low = df['High'] - df['Low']
        high_close = (df['High'] - df['Close'].shift()).abs()
        low_close = (df['Low'] - df['Close'].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = tr.rolling(window=window, min_periods=1).mean()
        return atr

    stock_data['ATR_14'] = stock_data.groupby('Ticker', group_keys=False).apply(
        lambda x: calculate_atr(x[['High', 'Low', 'Close']])
    )

    # Calculate On-Balance Volume (OBV)
    def calculate_obv(df):
        obv = (df['Volume'] * ((df['Close'] - df['Close'].shift()) > 0).astype(int) -
               df['Volume'] * ((df['Close'] - df['Close'].shift()) < 0).astype(int)).cumsum()
        return obv

    stock_data['OBV'] = stock_data.groupby('Ticker', group_keys=False).apply(
        lambda x: calculate_obv(x[['Close', 'Volume']])
    )

    end_time = time.time()
    duration_minutes = (end_time - start_time) / 60

    print(f"Time taken to process {len(symbols_list)} symbols: {duration_minutes:.2f} minutes")
    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    print(stock_data.head())
    print(stock_data.shape)

    # Save the DataFrame to a SQLite database
    conn = sqlite3.connect(db_name)
    stock_data.to_sql('stock_data', conn, if_exists='replace', index=False)
    conn.close()
    print(f"Data saved to {db_name} database.")

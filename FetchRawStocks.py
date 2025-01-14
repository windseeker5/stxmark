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
    #stock_data = fetch_yfinance_data(symbols_list, default_batch_size)
    
    #print("> Saving the data as pickle file...")
    #stock_data.to_pickle("stock_data.pkl")

    print("> Loading the data from pickle file...")
    stock_data = pd.read_pickle("stock_data.pkl")

    # Calculate daily performance based on Open and Close prices
    stock_data['Performance'] = ((stock_data['Close'] - stock_data['Open']) / stock_data['Open']) * 100


    # Ensure data is sorted by Date within each Ticker group
    stock_data = stock_data.sort_values(by=['Ticker', 'Date'])

    # Calculate consecutive days with performance above 0.2%
    stock_data['CDPP'] = stock_data.groupby('Ticker')['Performance'].apply(
        lambda x: (x > 0.2).astype(int).groupby((x <= 0.2).cumsum()).cumsum()
    ).reset_index(level=0, drop=True)



    end_time = time.time()
    duration_minutes = (end_time - start_time) / 60
    
    print(f"Time taken to process {len(symbols_list)} symbols: {duration_minutes:.2f} minutes")
    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    print(stock_data.head())
    print(stock_data.shape)

    # Save the DataFrame to a CSV file
    #stock_data.to_csv("stock_data.csv", index=False)

    # Save the DataFrame to a SQLite database
    conn = sqlite3.connect(db_name)
    stock_data.to_sql('stock_data', conn, if_exists='replace', index=False)
    conn.close()
    print(f"Data saved to {db_name} database.")

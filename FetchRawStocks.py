import time
import sqlite3
import pandas as pd
import os
from utils import fetch_yfinance_data, fetch_us_symbols, fetch_sp500_symbols, add_performance_and_cdpp, add_technical_indicators, save_to_sqlite  
import numpy as np



DB_NAME = "stocks2.db"
FINNHUB_API_KEY = "ctpgeohr01qqsrsaov10ctpgeohr01qqsrsaov1g"



if __name__ == "__main__":
    start_time = time.time()

    # Fetch S&P 500 symbols
    print("> Fetching S&P 500 symbols...")
    sp500_df = fetch_sp500_symbols()
    print(sp500_df.shape)

    # Fetch US symbols
    print("> Fetching US symbols...")
    us_df = fetch_us_symbols(FINNHUB_API_KEY)
    print(us_df.shape)  

    print("> Fetching stock data...")   
    symbols_list = us_df['Symbol'].tolist()[:1000]  # Fetching first 1000 for testing

    # Fetch stock data with optimized batch fetching
    stock_data = fetch_yfinance_data(symbols_list, batch_size=500, max_workers=10)

    # Add technical indicators & performance calculations

    print("> Adding technical indicators and performance calculations...")
    stock_data = add_performance_and_cdpp(stock_data)
    stock_data = add_technical_indicators(stock_data)

    # Save raw data as Parquet (faster than pickle)
    stock_data.to_parquet("stock_data.parquet", compression='snappy')

    # Save to SQLite using incremental updates
    save_to_sqlite(stock_data, "stock_data")

    print("Data saved successfully.")
    print(f"Total execution time: {time.time() - start_time:.2f} seconds")

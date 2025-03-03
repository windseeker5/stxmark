import time
import sqlite3
import pandas as pd
import os
import numpy as np
# FetchRawStocks.py
from utils import (
    fetch_yfinance_data_no_retry,
    fetch_us_symbols,
    fetch_sp500_symbols,
    add_performance_and_cdpp,
    add_technical_indicators,
    save_to_sqlite
)



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


    # Inside FetchRawStocks.py
    symbols_list = us_df['Symbol'].tolist()[:10]

    fetch_yfinance_data_no_retry(
        symbols_list=symbols_list,
        batch_size=10,      # or even 1
        sleep_per_batch=60, # 60 seconds (or more) after each batch
        table_name="stock_data"
    )



    print(f"Total execution time: {time.time() - start_time:.2f} seconds")

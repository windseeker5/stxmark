import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
import time
import logging
import numpy as np
import logging
import concurrent.futures
import sqlite3


DB_NAME = "stocks2.db"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_yfinance_data222(symbols_list, batch_size=500, max_retries=3):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=50)
    
    data = {}
    failed_symbols = []
    
    for i in range(0, len(symbols_list), batch_size):
        batch_symbols = symbols_list[i:i + batch_size]
        retries = 0
        success = False
        
        while retries < max_retries and not success:
            try:
                tickers = yf.download(batch_symbols, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
                for symbol in batch_symbols:
                    if symbol in tickers.columns.levels[1]:
                        data[symbol] = tickers.xs(symbol, level=1, axis=1)
                    else:
                        failed_symbols.append(symbol)
                success = True
            except Exception as e:
                logging.error(f"Error fetching data for batch {batch_symbols}: {e}")
                retries += 1
                time.sleep(5)  # Sleep before retrying
        
        # Sleep for 10 seconds to avoid hitting API rate limits
        time.sleep(10)
    
    if not data:
        logging.warning("No data fetched.")
        return pd.DataFrame()
    
    # Concatenate all dataframes
    df = pd.concat(data.values(), keys=data.keys(), axis=1)
    
    # Flatten the multi-index DataFrame
    df = df.stack(level=0).reset_index()
    df.columns = ['Date', 'Ticker'] + list(df.columns[2:])
    
    if failed_symbols:
        logging.warning(f"Failed to fetch data for the following symbols: {failed_symbols}")
    
    return df










# Fetch all US symbols from Finnhub and save them to a DataFrame
def fetch_us_symbols(api_key):
    import requests
    url = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        symbols_data = response.json()

        if not symbols_data:
            print("No symbols retrieved from Finnhub.")
            return pd.DataFrame()

        # Prepare data for DataFrame
        symbols = [item['symbol'] for item in symbols_data]
        symbols_df = pd.DataFrame(symbols, columns=["Symbol"])

        return symbols_df

    except requests.RequestException as e:
        print(f"Error fetching US symbols from Finnhub: {e}")
        return pd.DataFrame()






# Fetch S&P 500 symbols and save them to a DataFrame
def fetch_sp500_symbols():
    """Fetch all S&P 500 symbols and save them to a DataFrame."""
    try:
        sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        sp500_table = pd.read_html(sp500_url, header=0)
        sp500_df = sp500_table[0]
        sp500_symbols = sp500_df["Symbol"].tolist()

        # Save symbols to a DataFrame
        sp500_df = pd.DataFrame(sp500_symbols, columns=["Symbol"])

        return sp500_df
    except Exception as e:
        print(f"Error fetching S&P 500 symbols: {e}")
        return pd.DataFrame()
    



def fetch_yfinance_batch(batch_symbols, start_date, end_date):
    """
    Fetches a batch of symbols from Yahoo Finance.
    """
    try:
        tickers = yf.download(batch_symbols, start=start_date, end=end_date, threads=True)
        return tickers
    except Exception as e:
        logging.error(f"Error fetching batch {batch_symbols}: {e}")
        return None




def fetch_yfinance_data(symbols_list, batch_size=500, max_workers=10, max_retries=3):
    """
    Fetch stock data from Yahoo Finance efficiently with parallel processing.
    """
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    data = {}
    failed_symbols = set(symbols_list)  # Keep track of failed symbols

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbols = {}

        for i in range(0, len(symbols_list), batch_size):
            batch_symbols = symbols_list[i:i + batch_size]
            future = executor.submit(fetch_yfinance_batch, batch_symbols, start_date, end_date)
            future_to_symbols[future] = batch_symbols

        for future in concurrent.futures.as_completed(future_to_symbols):
            batch_symbols = future_to_symbols[future]
            try:
                tickers = future.result()
                if tickers is not None:
                    for symbol in batch_symbols:
                        if symbol in tickers.columns.levels[1]:
                            data[symbol] = tickers.xs(symbol, level=1, axis=1)
                            failed_symbols.discard(symbol)  # Remove successful fetches
            except Exception as e:
                logging.error(f"Error processing batch {batch_symbols}: {e}")

    if not data:
        logging.warning("No data fetched.")
        return pd.DataFrame()

    df = pd.concat(data.values(), keys=data.keys(), axis=1)

    # Flatten the multi-index DataFrame
    # df = df.stack(level=0).reset_index()

    df = df.stack(level=0, future_stack=True).reset_index()


    df.columns = ['Date', 'Ticker'] + list(df.columns[2:])

    if failed_symbols:
        logging.warning(f"Failed symbols: {list(failed_symbols)}")

    return df






def add_performance_and_cdpp(stock_data):
    """
    Adds Performance and Consecutive Days of Positive Performance (cdpp) indicators efficiently.
    """
    stock_data = stock_data.copy()  # Ensure it's a copy to prevent modifying the original DataFrame

    stock_data['Performance'] = ((stock_data['Close'] - stock_data['Open']) / stock_data['Open']) * 100

    # Sort data properly
    stock_data = stock_data.sort_values(by=['Ticker', 'Date']).reset_index(drop=True)

    # Compute cdpp using vectorized approach
    cdpp_series = (
        stock_data.groupby('Ticker')['Performance']
        .apply(lambda x: (x > 1).astype(int).groupby((x <= 1).cumsum()).cumsum())
        .reset_index(drop=True)
    )

    # Ensure indexes match before assignment
    stock_data['cdpp'] = cdpp_series

    return stock_data







def add_technical_indicators(stock_data):
    """
    Adds key technical indicators: SMA, EMA, RSI, MACD, Bollinger Bands, Alpha, Beta, and Sharpe Ratio.
    """
    stock_data = stock_data.copy()

    # Ensure data is sorted correctly
    stock_data = stock_data.sort_values(by=['Ticker', 'Date']).reset_index(drop=True)

    # Moving Averages
    stock_data['SMA5'] = stock_data.groupby('Ticker')['Close'].transform(lambda x: x.rolling(5).mean())
    stock_data['SMA10'] = stock_data.groupby('Ticker')['Close'].transform(lambda x: x.rolling(10).mean())
    stock_data['SMA20'] = stock_data.groupby('Ticker')['Close'].transform(lambda x: x.rolling(20).mean())

    stock_data['EMA5'] = stock_data.groupby('Ticker')['Close'].transform(lambda x: x.ewm(span=5, adjust=False).mean())
    stock_data['EMA10'] = stock_data.groupby('Ticker')['Close'].transform(lambda x: x.ewm(span=10, adjust=False).mean())
    stock_data['EMA20'] = stock_data.groupby('Ticker')['Close'].transform(lambda x: x.ewm(span=20, adjust=False).mean())

    # RSI Calculation
    delta = stock_data.groupby('Ticker')['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    stock_data['RSI'] = 100 - (100 / (1 + rs))

    # MACD Calculation
    stock_data['MACD'] = stock_data.groupby('Ticker')['Close'].transform(lambda x: x.ewm(span=12, adjust=False).mean() - x.ewm(span=26, adjust=False).mean())
    stock_data['MACD Signal'] = stock_data.groupby('Ticker')['MACD'].transform(lambda x: x.ewm(span=9, adjust=False).mean())

    # Bollinger Bands
    stock_data['BB Middle'] = stock_data.groupby('Ticker')['Close'].transform(lambda x: x.rolling(20).mean())
    stock_data['BB Upper'] = stock_data['BB Middle'] + 2 * stock_data.groupby('Ticker')['Close'].transform(lambda x: x.rolling(20).std())
    stock_data['BB Lower'] = stock_data['BB Middle'] - 2 * stock_data.groupby('Ticker')['Close'].transform(lambda x: x.rolling(20).std())

    # Fix FutureWarning: Replace `fillna(method="bfill")` with `.bfill()`
    stock_data.bfill(inplace=True)

    return stock_data







def save_to_sqlite(df, table_name, db_name="stocks2.db", chunksize=500):
    """
    Efficiently saves a DataFrame to SQLite, ensuring schema consistency.
    """
    import sqlite3  # Ensure sqlite3 is available

    conn = sqlite3.connect(db_name)

    # Drop table if schema mismatch happens (optional)
    df.to_sql(table_name, conn, if_exists='replace', index=False, chunksize=chunksize)

    conn.close()

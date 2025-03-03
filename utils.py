import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
import numpy as np
import sqlite3
import requests
import concurrent.futures

DB_NAME = "stocks_no_retry.db"  # rename DB if you want
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_us_symbols(api_key):
    """
    Fetch all US symbols from Finnhub and return as a DataFrame with one column 'Symbol'.
    """
    url = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={api_key}"
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        symbols_data = resp.json()
        if not symbols_data:
            logging.warning("No symbols retrieved from Finnhub.")
            return pd.DataFrame()

        symbols = [item['symbol'] for item in symbols_data]
        return pd.DataFrame(symbols, columns=["Symbol"])

    except requests.RequestException as e:
        logging.error(f"Error fetching US symbols from Finnhub: {e}")
        return pd.DataFrame()

def fetch_sp500_symbols():
    """
    Fetch S&P 500 symbols from Wikipedia and return as a DataFrame with one column 'Symbol'.
    """
    try:
        sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        sp500_table = pd.read_html(sp500_url, header=0)
        df = sp500_table[0]
        symbols = df["Symbol"].tolist()
        return pd.DataFrame(symbols, columns=["Symbol"])
    except Exception as e:
        logging.error(f"Error fetching S&P 500 symbols: {e}")
        return pd.DataFrame()

def save_to_sqlite(df, table_name, db_name=DB_NAME, chunksize=500):
    """Append DataFrame rows to a SQLite table, creating the table if needed."""
    if df.empty:
        logging.warning(f"No data to save to table {table_name}. Skipping.")
        return

    conn = sqlite3.connect(db_name)
    try:
        df.to_sql(table_name, conn, if_exists='append', index=False, chunksize=chunksize)
        logging.info(f"Saved {len(df)} rows to table '{table_name}'.")
    except Exception as e:
        logging.error(f"Error saving to table {table_name}: {e}")
    finally:
        conn.close()

def add_performance_and_cdpp(df):
    """
    Adds Performance (% change from Open to Close) and
    consecutive days of >1% performance (cdpp).
    """
    if df.empty:
        return df

    df = df.copy()
    df['Performance'] = ((df['Close'] - df['Open']) / df['Open']) * 100
    df.sort_values(by=['Ticker', 'Date'], inplace=True)

    # Consecutive days with Performance > 1%
    cdpp_series = (
        df.groupby('Ticker')['Performance']
          .apply(lambda x: (x > 1).astype(int).groupby((x <= 1).cumsum()).cumsum())
          .reset_index(drop=True)
    )
    df['cdpp'] = cdpp_series
    return df

def add_technical_indicators(df):
    """
    Adds technical indicators: SMA, EMA, RSI, MACD, Bollinger.
    """
    if df.empty:
        return df

    df = df.copy()
    df.sort_values(by=['Ticker', 'Date'], inplace=True)

    # 1) Moving Averages
    df['SMA5'] = df.groupby('Ticker')['Close'].transform(lambda x: x.rolling(5).mean())
    df['SMA10'] = df.groupby('Ticker')['Close'].transform(lambda x: x.rolling(10).mean())
    df['SMA20'] = df.groupby('Ticker')['Close'].transform(lambda x: x.rolling(20).mean())

    df['EMA5'] = df.groupby('Ticker')['Close'].transform(lambda x: x.ewm(span=5, adjust=False).mean())
    df['EMA10'] = df.groupby('Ticker')['Close'].transform(lambda x: x.ewm(span=10, adjust=False).mean())
    df['EMA20'] = df.groupby('Ticker')['Close'].transform(lambda x: x.ewm(span=20, adjust=False).mean())

    # 2) RSI
    delta = df.groupby('Ticker')['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))

    # 3) MACD
    df['MACD'] = df.groupby('Ticker')['Close'].transform(
        lambda x: x.ewm(span=12, adjust=False).mean() - x.ewm(span=26, adjust=False).mean()
    )
    df['MACD Signal'] = df.groupby('Ticker')['MACD'].transform(
        lambda x: x.ewm(span=9, adjust=False).mean()
    )

    # 4) Bollinger Bands (20 day)
    df['BB Middle'] = df.groupby('Ticker')['Close'].transform(lambda x: x.rolling(20).mean())
    df['BB Upper'] = df['BB Middle'] + 2 * df.groupby('Ticker')['Close'].transform(lambda x: x.rolling(20).std())
    df['BB Lower'] = df['BB Middle'] - 2 * df.groupby('Ticker')['Close'].transform(lambda x: x.rolling(20).std())

    # Fill leading NaNs
    df.bfill(inplace=True)
    return df

def fetch_yfinance_batch(batch_symbols, start_date, end_date):
    """
    Download a batch of symbols via yfinance. If rate-limited or error, return None.
    threads=False -> reduce concurrency so fewer connections in parallel.
    """
    try:
        # We'll set threads=False to avoid the "Connection pool is full" warnings as much as possible.
        tickers = yf.download(
            tickers=batch_symbols,
            start=start_date,
            end=end_date,
            threads=False
        )
        if tickers is None or tickers.empty:
            logging.warning(f"Empty data returned for batch: {batch_symbols}")
            return None
        return tickers
    except Exception as e:
        logging.error(f"Error fetching batch {batch_symbols}: {e}")
        return None

def fetch_yfinance_data_no_retry(symbols_list,
                                 batch_size=50,
                                 sleep_per_batch=15,
                                 table_name="stock_data"):
    """
    Single-pass (NO RETRY) fetching of data from yfinance.  
    - Splits symbols into batches of 'batch_size'.
    - For each batch, if fetch fails, we skip it; no retries.
    - Sleep 'sleep_per_batch' seconds after each batch to avoid rate-limiting.
    - Saves partial data to SQLite as soon as it's fetched.

    :param symbols_list: list of ticker symbols
    :param batch_size: number of symbols per batch
    :param sleep_per_batch: time to sleep after each batch
    :param table_name: name of the table to save
    """
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    logging.info(f"Beginning single-pass fetch for {len(symbols_list)} symbols.")
    # We'll do a single-thread approach to minimize parallel connections.
    # If you want to fetch multiple batches in parallel, you could use max_workers=2 or so.
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        for i in range(0, len(symbols_list), batch_size):
            batch_syms = symbols_list[i:i + batch_size]
            future = executor.submit(fetch_yfinance_batch, batch_syms, start_date, end_date)
            tickers_df = future.result()  # run in single thread

            if tickers_df is None:
                # If the batch fails or is empty, skip it
                logging.warning(f"Skipping failed batch: {batch_syms}")
            else:
                # Convert multi-index columns -> normal columns
                df = tickers_df.stack(level=1).reset_index()
                df.rename(columns={'level_1': 'Ticker', 'level_0': 'Date'}, inplace=True)

                # Let's rename columns to ensure consistency
                df.rename(columns={
                    'Open': 'Open',
                    'High': 'High',
                    'Low': 'Low',
                    'Close': 'Close',
                    'Adj Close': 'AdjClose',
                    'Volume': 'Volume'
                }, inplace=True)

                # Add performance and technical indicators
                df = add_performance_and_cdpp(df)
                df = add_technical_indicators(df)

                # Save partial success for this batch
                save_to_sqlite(df, table_name=table_name, db_name=DB_NAME)

            # Sleep to reduce rate-limit hits
            time.sleep(sleep_per_batch)

    logging.info("Single-pass fetch completed (no retries).")

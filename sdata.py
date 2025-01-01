import sqlite3
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time

DB_NAME = "stocks.db"
TABLE_NAME = "stock_data"
BATCH_SIZE = 100
MAX_WORKERS = 10
db_lock = Lock()
failed_symbols = set()

def init_database():
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_data (
                Date TEXT,
                Open REAL,
                High REAL,
                Low REAL,
                Close REAL,
                Adj_Close REAL,
                Volume INTEGER,
                Symbol TEXT
            )
        ''')
        conn.commit()

def fetch_with_retry(symbol, start_date, end_date, retries=3):
    for attempt in range(retries):
        try:
            print(f"Attempt {attempt + 1} to fetch {symbol}")
            data = yf.download(symbol, start=start_date, end=end_date, group_by="ticker")
            if not data.empty:
                print(f"Fetched data for {symbol}:\n{data.head()}")
                return data
            print(f"No data for symbol: {symbol}. Skipping further retries.")
            failed_symbols.add(symbol)
            return pd.DataFrame()
        except Exception as e:
            log_error(f"Error fetching data for symbol {symbol} on attempt {attempt + 1}: {e}")
            if attempt == retries - 1:
                failed_symbols.add(symbol)
        time.sleep(2)
    log_error(f"Failed to fetch data for {symbol} after {retries} attempts.")
    return pd.DataFrame()

def fetch_stocks_batch(symbols, start_date, end_date):
    results = []
    for symbol in symbols:
        try:
            data = fetch_with_retry(symbol, start_date, end_date)
            if data.empty:
                log_error(f"No data for symbol: {symbol}")
                continue
            data = data.reset_index()
            data["Symbol"] = symbol
            results.append(data)
        except Exception as e:
            log_error(f"Error fetching data for symbol {symbol}: {e}")
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

def save_to_sqlite(dataframe):
    if dataframe.empty:
        print("No data to save.")
        return
    
    dataframe.rename(columns={"Adj Close": "Adj_Close"}, inplace=True)
    print(f"Saving the following data to the database:\n{dataframe.head()}")
    with db_lock:
        conn = sqlite3.connect(DB_NAME)
        try:
            dataframe.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
            print(f"Saved {len(dataframe)} rows to the database.")
        except Exception as e:
            log_error(f"Error saving data: {e}")
        finally:
            conn.close()

def log_error(message):
    with open("error_log.txt", "a") as log_file:
        log_file.write(message + "\n")

def main():
    init_database()
    end_date = datetime.today()
    start_date = end_date - timedelta(days=90)
    symbols = ["AAPL", "GOOGL", "MSFT", "NVDA"]  # Example for testing
    data = fetch_stocks_batch(symbols, start_date, end_date)
    save_to_sqlite(data)

if __name__ == "__main__":
    main()

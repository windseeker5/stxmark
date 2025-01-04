import sqlite3
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import logging
import urllib3
import time  # Import time for delays



# Configure connection pool
urllib3.util.connection.HAS_IPV6 = False
pool_manager = urllib3.PoolManager(maxsize=50)  # Increase pool size



DB_NAME = "stocks.db"
TABLE_NAME = "stock_data"
SYMBOLS_TABLE = "symbols"
SP500_TABLE = "sp500"
MAX_WORKERS = 10
BATCH_SIZE = 100
FINNHUB_API_KEY = "ctpgeohr01qqsrsaov10ctpgeohr01qqsrsaov1g"

# Historic days to keep
hdays = 20



db_lock = Lock()


logging.getLogger("yfinance").setLevel(logging.WARNING)



logging.basicConfig(
    filename="fetch_and_save_batch.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w"
)



# Initialize the database
def init_database():
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        # Create stock_data table with additional columns
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_data (
                Date TEXT,
                Open REAL,
                High REAL,
                Low REAL,
                Close REAL,
                Adj_Close REAL,  -- Ensure this column exists
                Volume INTEGER,
                Symbol TEXT,
                Performance REAL,
                cdpp INTEGER,
                Average_Daily_Return REAL,
                Volatility REAL,
                Recent_Performance REAL,
                MA_5 REAL,
                MA_10 REAL,
                MA_15 REAL,
                RSI REAL,
                Bollinger_Upper REAL,
                Bollinger_Lower REAL
            )
        ''')
        # Create symbols table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS symbols (
                symbol TEXT PRIMARY KEY,
                yFinanceInvalid_ind INTEGER DEFAULT 0
            )
        ''')
        # Create sp500 table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sp500 (
                symbol TEXT PRIMARY KEY,
                yFinanceInvalid_ind INTEGER DEFAULT 0
            )
        ''')
        conn.commit()






# Fetch S&P 500 symbols and save them to the database
def fetch_sp500_symbols():
    """Fetch all S&P 500 symbols and save them to the database."""
    try:
        print("Fetching S&P 500 symbols...")
        sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        sp500_table = pd.read_html(sp500_url, header=0)
        sp500_df = sp500_table[0]
        sp500_symbols = sp500_df["Symbol"].tolist()

        # Save symbols to the database
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.executemany('''
                INSERT OR IGNORE INTO sp500 (symbol)
                VALUES (?)
            ''', [(symbol,) for symbol in sp500_symbols])
            conn.commit()

        print(f"Saved {len(sp500_symbols)} S&P 500 symbols to the database.")
    except Exception as e:
        print(f"Error fetching S&P 500 symbols: {e}")





# Fetch all US symbols from Finnhub and save them to the database
def fetch_us_symbols():
    url = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={FINNHUB_API_KEY}"
    try:
        print("Fetching US symbols from Finnhub...")
        response = requests.get(url)
        response.raise_for_status()
        symbols_data = response.json()

        if not symbols_data:
            print("No symbols retrieved from Finnhub.")
            return

        # Prepare data for insertion
        symbols = [(item['symbol'], 0) for item in symbols_data]

        # Save symbols to the database
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.executemany('''
                INSERT OR IGNORE INTO symbols (symbol, yFinanceInvalid_ind)
                VALUES (?, ?)
            ''', symbols)
            conn.commit()
            print(f"Saved {len(symbols)} symbols to the database.")

    except requests.RequestException as e:
        print(f"Error fetching US symbols from Finnhub: {e}")







# Fetch symbols from the database
def get_symbols_from_db():
    with sqlite3.connect(DB_NAME) as conn:
        query = """
            SELECT DISTINCT symbol
            FROM (
                   SELECT symbol FROM sp500 WHERE yFinanceInvalid_ind = 0
                     UNION
                   SELECT symbol FROM symbols WHERE yFinanceInvalid_ind = 0
            ) AS combined
        """
        return pd.read_sql_query(query, conn)["symbol"].tolist()






def drop_tables():
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        try:
            # Drop stock_data table if it exists
            cursor.execute("DROP TABLE IF EXISTS stock_data")
            cursor.execute("DROP TABLE IF EXISTS symbols")
            cursor.execute("DROP TABLE IF EXISTS sp500")
            cursor.execute("DROP TABLE IF EXISTS stock_model")
            conn.commit()
            print("Tables dropped successfully.")
        except Exception as e:
            print(f"Error dropping tables: {e}")







# Main function to fetch and save stock data
def main():
    drop_tables()

    # Initialize the database
    init_database()

    # Fetch S&P 500 symbols and save to database
    fetch_sp500_symbols()

    # Fetch US symbols and save to database
    fetch_us_symbols()




    # Define start and end dates
    end_date = datetime.today()
    start_date = end_date - timedelta(days=hdays)

    # Fetch symbols from the database and limit for testing
    symbols = get_symbols_from_db()
    symbols = symbols[:1000]

    print(symbols)
    print(type(symbols))

    print(f"Total symbols to process: {len(symbols)}")



if __name__ == "__main__":
    main()

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




def save_to_sqlite(data):
    with sqlite3.connect(DB_NAME) as conn:
        try:
            data.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
            logging.info(f"Saved {len(data)} rows to the database.")
        except Exception as e:
            logging.error(f"Failed to save data to database. Error: {e}\nData:\n{data.head()}")




def mark_symbol_as_invalid(symbol):
    """Mark a symbol as invalid in the database."""
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE symbols SET yFinanceInvalid_ind = 1 WHERE symbol = ?", (symbol,))
        cursor.execute("UPDATE sp500 SET yFinanceInvalid_ind = 1 WHERE symbol = ?", (symbol,))
        conn.commit()
    logging.info(f"Marked symbol as invalid: {symbol}")






def calculate_technical_indicators(df):
    if len(df) < 5:
        logging.warning(f"Insufficient data for technical indicators: {len(df)} rows available")
        return df

    df = df.copy()
    df = df.dropna(subset=['Close'])

    df['MA_5'] = df['Close'].rolling(window=5).mean()
    df['MA_10'] = df['Close'].rolling(window=10).mean()
    df['MA_15'] = df['Close'].rolling(window=15).mean()

    delta = df['Close'].diff()
    gain = delta.where(delta > 0, 0).rolling(window=14).mean()
    loss = -delta.where(delta < 0, 0).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))

    max_window = min(len(df), 14)
    rolling_mean = df['Close'].rolling(window=max_window).mean()
    rolling_std = df['Close'].rolling(window=max_window).std()
    df['Bollinger_Upper'] = rolling_mean + (2 * rolling_std)
    df['Bollinger_Lower'] = rolling_mean - (2 * rolling_std)

    return df










"""
# Setup logging to only log warnings and errors
logging.basicConfig(
    filename="fetch_and_save_batch.log",
    level=logging.WARNING,  # Change level to WARNING to filter out DEBUG and INFO logs
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w"
)
"""

logging.basicConfig(
    filename="fetch_and_save_batch.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w"
)









def fetch_and_save_batch(symbols, start_date, end_date, max_retries=3):
    logging.info(f"Processing batch of {len(symbols)} symbols...")
    rows_saved = 0

    for symbol in symbols:
        attempt = 0
        success = False

        while attempt < max_retries and not success:
            try:
                logging.info(f"Fetching data for symbol: {symbol} (Attempt {attempt + 1})")
                symbol_data = yf.download(
                    symbol,
                    start=start_date,
                    end=end_date,
                    group_by="ticker",
                    threads=False
                )

                # Log the raw fetched data
                logging.info(f"Raw data fetched for {symbol}:\n{symbol_data.head()}")

                if symbol_data.empty or "Close" not in symbol_data.columns:
                    logging.warning(f"No valid data for symbol: {symbol}")
                    break

                # Process and save data
                symbol_data.reset_index(inplace=True)
                symbol_data["Symbol"] = symbol
                symbol_data.rename(columns={"Adj Close": "Adj_Close"}, inplace=True, errors='ignore')
                symbol_data["Adj_Close"] = symbol_data.get("Adj_Close", symbol_data["Close"])

                # Calculate technical indicators
                symbol_data = calculate_technical_indicators(symbol_data)
                if symbol_data.empty:
                    logging.warning(f"No valid processed data for symbol: {symbol}. Skipping.")
                    break

                # Save to database
                save_to_sqlite(symbol_data)
                rows_saved += len(symbol_data)
                logging.info(f"Saved {len(symbol_data)} rows for symbol: {symbol}")
                success = True

            except Exception as e:
                attempt += 1
                logging.error(f"Error fetching data for symbol: {symbol}. Error: {e}")

        if not success:
            logging.warning(f"Failed to fetch data for symbol: {symbol} after {max_retries} attempts.")
            mark_symbol_as_invalid(symbol)

    logging.info(f"Batch processed: {rows_saved} rows saved.")
    return rows_saved






def calculate_and_store_performance():
    with sqlite3.connect(DB_NAME) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", conn)
        if df.empty:
            logging.warning("No data found in the database. Skipping performance calculation.")
            return

    # Ensure proper date parsing and sorting
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df.sort_values(by=['Symbol', 'Date'], inplace=True)

    # Validate critical columns
    if 'Close' not in df.columns:
        logging.error("Column 'Close' is missing from the data. Skipping performance calculation.")
        return

    # Calculate daily percentage change
    try:
        df['Performance'] = (
            df.groupby('Symbol')['Close']
            .transform(lambda x: x.pct_change() * 100)
            .fillna(0)
        )
        df['Performance'] = pd.to_numeric(df['Performance'], errors='coerce').fillna(0)
    except Exception as e:
        logging.error(f"Error calculating Performance: {e}")
        return

    # Calculate Consecutive Days Performance > 2% (cdpp)
    try:
        def calculate_cdpp(group):
            consecutive_days = 0
            cdpp_list = []
            for performance in group:
                if performance > 2:
                    consecutive_days += 1
                else:
                    consecutive_days = 0
                cdpp_list.append(consecutive_days)
            return cdpp_list

        df['cdpp'] = df.groupby('Symbol')['Performance'].transform(calculate_cdpp)
    except Exception as e:
        logging.error(f"Error calculating cdpp: {e}")
        return

    # Calculate rolling metrics
    try:
        window = 5
        df['Average_Daily_Return'] = (
            df.groupby('Symbol')['Performance']
            .rolling(window=window)
            .mean()
            .reset_index(0, drop=True)
        )
        df['Volatility'] = (
            df.groupby('Symbol')['Performance']
            .rolling(window=window)
            .std()
            .reset_index(0, drop=True)
        )
    except Exception as e:
        logging.error(f"Error calculating rolling metrics: {e}")
        return

    # Add technical indicators
    try:
        df = calculate_technical_indicators(df)
    except Exception as e:
        logging.error(f"Error adding technical indicators: {e}")
        return

    # Save updated data back to the database
    try:
        with sqlite3.connect(DB_NAME) as conn:
            df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)
            logging.info(
                "Performance, cdpp, rolling metrics, and technical indicators calculated and stored in stock_data."
            )
    except Exception as e:
        logging.error(f"Error saving updated data to the database: {e}")
        return








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
            # Drop symbols table if it exists
            cursor.execute("DROP TABLE IF EXISTS symbols")
            # Drop sp500 table if it exists
            cursor.execute("DROP TABLE IF EXISTS sp500")
            # Drop stock_model table if it exists
            cursor.execute("DROP TABLE IF EXISTS stock_model")
            conn.commit()
            print("Tables dropped successfully.")
        except Exception as e:
            print(f"Error dropping tables: {e}")




# Main function to fetch and save stock data
def main():
    # Drop existing tables
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
    print(f"Total symbols to process: {len(symbols)}")

    # Batch symbols
    batches = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]

    # Process each batch sequentially with retries
    total_rows_saved = 0
    for batch in batches:
        rows_saved = fetch_and_save_batch(batch, start_date, end_date)
        total_rows_saved += rows_saved
        logging.info(f"Batch completed: {rows_saved} rows saved.")

    # Log final results
    logging.info(f"Total rows saved to the database: {total_rows_saved}")

    # Calculate performance and cdpp
    calculate_and_store_performance()


if __name__ == "__main__":
    main()

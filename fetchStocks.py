import sqlite3
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import logging
import urllib3

import urllib3
urllib3.util.connection.HAS_IPV6 = False
pool_manager = urllib3.PoolManager(maxsize=50)  # Increase pool size to 50



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
                Adj_Close REAL,
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





# Save data to SQLite
def save_to_sqlite(dataframe):
    if dataframe.empty:
        print("No data to save.")
        return

    with db_lock:
        with sqlite3.connect(DB_NAME) as conn:
            try:
                dataframe.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
                print(f"Saved {len(dataframe)} rows to the database.")
            except Exception as e:
                print(f"Error saving data: {e}")





def mark_symbol_as_invalid(symbol):
    """Mark a symbol as invalid in the database."""
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE symbols SET yFinanceInvalid_ind = 1 WHERE symbol = ?", (symbol,))
        cursor.execute("UPDATE sp500 SET yFinanceInvalid_ind = 1 WHERE symbol = ?", (symbol,))
        conn.commit()
    logging.info(f"Marked symbol as invalid: {symbol}")






def calculate_technical_indicators(df):
    """Calculate shorter Moving Averages, RSI, and Bollinger Bands."""
    if len(df) < 5:  # Ensure enough data for the smallest moving average
        logging.warning(f"Insufficient data for technical indicators: {len(df)} rows available")
        return df

    df = df.copy()  # Avoid modifying the original DataFrame

    # Shorter Moving Averages
    df.loc[:, 'MA_5'] = df['Close'].rolling(window=5).mean()
    df.loc[:, 'MA_10'] = df['Close'].rolling(window=10).mean()
    df.loc[:, 'MA_15'] = df['Close'].rolling(window=15).mean()

    # RSI Calculation
    delta = df['Close'].diff()
    gain = delta.where(delta > 0, 0).rolling(window=14).mean()
    loss = -delta.where(delta < 0, 0).rolling(window=14).mean()
    rs = gain / loss
    df.loc[:, 'RSI'] = 100 - (100 / (1 + rs))

    # Bollinger Bands with dynamic window size
    max_window = min(len(df), 14)  # Use 14 if enough data, otherwise adapt to available rows
    rolling_mean = df['Close'].rolling(window=max_window).mean()
    rolling_std = df['Close'].rolling(window=max_window).std()
    df.loc[:, 'Bollinger_Upper'] = rolling_mean + (2 * rolling_std)
    df.loc[:, 'Bollinger_Lower'] = rolling_mean - (2 * rolling_std)

    return df










# Setup logging to only log warnings and errors
logging.basicConfig(
    filename="fetch_and_save_batch.log",
    level=logging.WARNING,  # Change level to WARNING to filter out DEBUG and INFO logs
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w"
)




def fetch_and_save_batch(symbols, start_date, end_date, threads=True, max_retries=3):
    """Fetch and process stock data with retry logic and enhanced saving."""
    logging.info(f"Processing batch with {len(symbols)} symbols: {symbols}")

    try:
        # Retry logic for yfinance download
        raw_data = None
        for attempt in range(max_retries):
            try:
                raw_data = yf.download(
                    symbols,
                    start=start_date,
                    end=end_date,
                    group_by="ticker",
                    threads=threads
                )
                if not raw_data.empty:
                    break  # Exit retry loop if successful
            except Exception as e:
                if attempt < max_retries - 1:
                    sleep_time = 2**attempt  # Exponential backoff
                    logging.warning(f"Retrying batch due to error: {e}. Retry in {sleep_time} seconds.")
                    time.sleep(sleep_time)
                else:
                    logging.error(f"Failed to fetch batch after {max_retries} retries: {symbols}")
                    return

        # Check if raw_data is valid
        if raw_data is None or raw_data.empty:
            logging.warning(f"No data fetched for batch: {symbols}")
            return

        # Process each symbol in the batch
        for symbol in symbols:
            try:
                # Extract symbol-specific data
                symbol_data = (
                    raw_data[symbol].reset_index() if symbol in raw_data.columns.get_level_values(0) else pd.DataFrame()
                )

                # Check if data for the symbol exists
                if symbol_data.empty:
                    logging.warning(f"No data found for symbol: {symbol}")
                    mark_symbol_as_invalid(symbol)  # Mark the symbol as invalid
                    continue

                # Add symbol column
                symbol_data['Symbol'] = symbol

                # Rename columns to match database schema
                if "Adj Close" in symbol_data.columns:
                    symbol_data.rename(columns={"Adj Close": "Adj_Close"}, inplace=True)
                if "Adj_Close" not in symbol_data.columns:
                    symbol_data["Adj_Close"] = symbol_data["Close"]

                # Replace the creation of processed_data with:
                processed_data = symbol_data[
                    ["Date", "Open", "High", "Low", "Close", "Adj_Close", "Volume", "Symbol"]
                ].copy()

                # Safely modify processed_data
                processed_data.loc[:, 'Performance'] = processed_data['Close'].pct_change(fill_method=None) * 100
                processed_data.loc[:, 'Performance'] = processed_data['Performance'].fillna(0)

                # Add technical indicators
                processed_data = calculate_technical_indicators(processed_data)

                # Debug log processed data
                if not processed_data.empty:
                    logging.debug(f"Processed data for {symbol}: {processed_data}")

                # Save to the database
                if not processed_data.empty:
                    save_to_sqlite(processed_data)
                    logging.info(f"Successfully saved data for {symbol}")
                else:
                    logging.warning(f"No valid data to save for symbol: {symbol}")

            except Exception as e:
                logging.error(f"Error processing symbol {symbol}: {e}")
                mark_symbol_as_invalid(symbol)  # Mark the symbol as invalid on error

        # Add delay between batch requests
        time.sleep(1)  # Add a delay of 1 second between batches

    except Exception as e:
        logging.error(f"Error fetching batch {symbols}: {e}")






def calculate_and_store_performance():
    with sqlite3.connect(DB_NAME) as conn:
        # Load all data from the stock_data table
        query = f"""
            SELECT *
            FROM {TABLE_NAME}
            ORDER BY Symbol, Date
        """
        df = pd.read_sql_query(query, conn)

    # Ensure data is sorted by Symbol and Date
    df['Date'] = pd.to_datetime(df['Date'])
    df.sort_values(by=['Symbol', 'Date'], inplace=True)

    # Calculate daily percentage change (Performance)
    df['Performance'] = df.groupby('Symbol', group_keys=False)['Close'].pct_change(fill_method=None) * 100

    # Fill NaN values in Performance with 0
    df['Performance'] = df['Performance'].fillna(0)

    # Calculate cdpp (Consecutive Days Performance > 2%)
    def calculate_cdpp(group):
        consecutive_days = 0
        cdpp_list = []

        for performance in group:
            if performance > 2:  # Increment counter for Performance > 2%
                consecutive_days += 1
            else:
                consecutive_days = 0  # Reset counter if Performance <= 2%
            cdpp_list.append(consecutive_days)

        return cdpp_list

    # Apply cdpp calculation
    df['cdpp'] = df.groupby('Symbol', group_keys=False)['Performance'].apply(calculate_cdpp).explode().astype(int).values

    # Calculate Average Daily Return, Volatility, and Recent Performance
    window = 5  # Rolling window size for these metrics
    df['Average_Daily_Return'] = df.groupby('Symbol')['Performance'].rolling(window=window).mean().reset_index(0, drop=True)
    df['Volatility'] = df.groupby('Symbol')['Performance'].rolling(window=window).std().reset_index(0, drop=True)
    df['Recent_Performance'] = df.groupby('Symbol')['Performance'].rolling(window=window).mean().reset_index(0, drop=True)

    # Add technical indicators (Moving Averages, RSI, Bollinger Bands)
    df = calculate_technical_indicators(df)

    # Save updated data back to the database
    with sqlite3.connect(DB_NAME) as conn:
        df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)
        print("Performance, cdpp, rolling metrics, and technical indicators calculated and stored in stock_data.")







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

            LIMIT 100
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

    # Fetch stock data
    end_date = datetime.today()
    start_date = end_date - timedelta(days=hdays)
    symbols = get_symbols_from_db()
    print(f"Total symbols to process: {len(symbols)}")
    batches = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_and_save_batch, batch, start_date, end_date) for batch in batches]
        for future in futures:
            future.result()

    # Calculate performance and cdpp
    calculate_and_store_performance()



if __name__ == "__main__":
    main()

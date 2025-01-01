import sqlite3
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from threading import Lock


DB_NAME = "stocks.db"
TABLE_NAME = "stock_data"
SYMBOLS_TABLE = "symbols"
SP500_TABLE = "sp500"
MAX_WORKERS = 10
BATCH_SIZE = 100
FINNHUB_API_KEY = "ctpgeohr01qqsrsaov10ctpgeohr01qqsrsaov1g"

# Historic days to keep
hdays = 5




db_lock = Lock()



# Initialize the database
def init_database():
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        # Create stock_data table
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





# Fetch and save data for a batch of symbols
def fetch_and_save_batch(symbols, start_date, end_date):
    try:
        raw_data = yf.download(symbols, start=start_date, end=end_date, group_by="ticker", threads=True)

        if raw_data.empty:
            print(f"No data for batch: {symbols}")
            return

        # Process each symbol's data independently
        for symbol in symbols:
            try:
                symbol_data = raw_data[symbol].reset_index() if symbol in raw_data else pd.DataFrame()

                if symbol_data.empty:
                    print(f"No data for symbol: {symbol}")
                    # Mark as invalid in the database
                    with sqlite3.connect(DB_NAME) as conn:
                        cursor = conn.cursor()
                        cursor.execute("UPDATE symbols SET yFinanceInvalid_ind = 1 WHERE symbol = ?", (symbol,))
                        conn.commit()
                    continue

                # Add Symbol column
                symbol_data['Symbol'] = symbol

                # Ensure Adj Close column is present
                if "Adj Close" in symbol_data.columns:
                    symbol_data.rename(columns={"Adj Close": "Adj_Close"}, inplace=True)
                if "Adj_Close" not in symbol_data.columns:
                    symbol_data["Adj_Close"] = symbol_data["Close"]

                # Rename and filter columns
                processed_data = symbol_data.rename(columns={"Date": "Date"})[
                    ["Date", "Open", "High", "Low", "Close", "Adj_Close", "Volume", "Symbol"]
                ]

                # Save valid data
                save_to_sqlite(processed_data)

            except Exception as e:
                print(f"Error processing data for symbol {symbol}: {e}")

    except Exception as e:
        print(f"Error fetching batch {symbols}: {e}")





# Fetch symbols from the database
#def get_symbols_from_db():
#    with sqlite3.connect(DB_NAME) as conn:
#        query = "SELECT symbol FROM symbols WHERE yFinanceInvalid_ind = 0 LIMIT 1000"
#        return pd.read_sql_query(query, conn)["symbol"].tolist()



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
            LIMIT 1000
        """
        return pd.read_sql_query(query, conn)["symbol"].tolist()




# Main function to fetch and save stock data
def main():
    init_database()

    # Fetch S&P 500 symbols and save to database
    fetch_sp500_symbols()

    # Fetch US symbols and save to database
    fetch_us_symbols()

    # Uncomment the following for stock data fetching
    end_date = datetime.today()
    start_date = end_date - timedelta(days=hdays)
    
    symbols = get_symbols_from_db()
    print(f"Total symbols to process: {len(symbols)}")
    batches = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_and_save_batch, batch, start_date, end_date) for batch in batches]
        for future in futures:
             future.result()

if __name__ == "__main__":
    main()
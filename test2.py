import sqlite3
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from threading import Lock


DB_NAME = "stocks.db"
TABLE_NAME = "stock_data"
MAX_WORKERS = 10
BATCH_SIZE = 100

# Historic days to keep
hdays = 5

db_lock = Lock()




# Initialize the database
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
def get_symbols_from_db():
    with sqlite3.connect(DB_NAME) as conn:
        query = "SELECT symbol FROM symbols LIMIT 100"
        return pd.read_sql_query(query, conn)["symbol"].tolist()






# Main function to fetch and save stock data
def main():
    init_database()

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

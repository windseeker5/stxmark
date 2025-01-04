import sqlite3
import yfinance as yf
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


db_lock = threading.Lock()  # Thread-safe lock for database writes





def drop_yfinance_table(db_name="stocks.db", table_name="yfinance"):
    """
    Drop the yfinance table if it exists.

    Parameters:
    db_name (str): SQLite database name. Default is 'stocks.db'.
    table_name (str): Table name to drop. Default is 'yfinance'.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()
    conn.close()
    print(f"Table {table_name} dropped successfully in {db_name}.")





def create_yfinance_table(db_name="stocks.db", table_name="yfinance"):
    """
    Create the yfinance table in the SQLite database.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            Date TEXT,
            Open REAL,
            High REAL,
            Low REAL,
            Close REAL,
            Adj_Close REAL,
            Volume INTEGER,
            Symbol TEXT,
            Status TEXT,
            Currency TEXT,
            Exchange TEXT,
            Close_Unadjusted REAL
        )
    ''')
    conn.commit()
    conn.close()
    print(f"Table {table_name} created successfully in {db_name}.")






def fetch_and_save_historical(symbol, db_name="stocks.db", table_name="yfinance"):
    """
    Fetch and save historical data for a single symbol, with robust handling for multi-level columns.
    """
    conn = sqlite3.connect(db_name)
    try:
        print(f"Fetching historical data for {symbol}...")
        # Fetch historical data
        data = yf.download(symbol, period="1mo", interval="1d")

        # Debugging: Print the data structure
        print(f"[DEBUG] Raw data for {symbol}:\n{data}\n")

        if not data.empty:
            # Flatten multi-level columns if present
            if isinstance(data.columns, pd.MultiIndex):
                print(f"[DEBUG] Multi-level columns detected for {symbol}. Flattening...")
                data.columns = data.columns.get_level_values(0)

            # Debugging: Print flattened columns
            print(f"[DEBUG] Flattened columns for {symbol}: {data.columns}\n")

            # Check if 'Close' column exists
            if 'Close' not in data.columns:
                raise KeyError(f"'Close' column not found for {symbol}")

            # Reset index for clean manipulation
            data = data.reset_index()

            # Add required columns
            data["Symbol"] = symbol
            data["Status"] = "success"
            data["Currency"] = "N/A"  # Placeholder, added later
            data["Exchange"] = "N/A"  # Placeholder, added later
            data["Close_Unadjusted"] = data["Close"]

            # Save data to the database
            with db_lock:
                data.to_sql(table_name, conn, if_exists="append", index=False)
            print(f"Historical data for {symbol} saved successfully.")
        else:
            # Insert placeholder for symbols with no data
            with db_lock:
                cursor = conn.cursor()
                cursor.execute(f'''
                    INSERT INTO {table_name} (Symbol, Status)
                    VALUES (?, ?)
                ''', (symbol, "failed"))
                conn.commit()
            print(f"No historical data found for {symbol}. Placeholder saved.")
    except Exception as e:
        print(f"Error fetching historical data for {symbol}: {e}")
    finally:
        conn.close()







def add_real_time_prices(symbols, db_name="stocks.db", table_name="yfinance"):
    """
    Fetch and update real-time prices for symbols.
    """
    conn = sqlite3.connect(db_name)
    try:
        for symbol in symbols:
            print(f"Fetching real-time price for {symbol}...")
            ticker = yf.Ticker(symbol)
            real_time_price = ticker.info.get("regularMarketPrice", None)
            currency = ticker.info.get("currency", "N/A")
            exchange = ticker.info.get("exchange", "N/A")

            # Update the database with real-time price
            if real_time_price is not None:
                cursor = conn.cursor()
                cursor.execute(f'''
                    UPDATE {table_name}
                    SET Close = ?, Close_Unadjusted = ?, Currency = ?, Exchange = ?, Status = ?
                    WHERE Symbol = ? AND Date = (
                        SELECT MAX(Date) FROM {table_name} WHERE Symbol = ?
                    )
                ''', (real_time_price, real_time_price, currency, exchange, "success", symbol, symbol))
                conn.commit()
                print(f"Real-time price for {symbol} updated successfully: {real_time_price}")
            else:
                print(f"No real-time price available for {symbol}.")
    except Exception as e:
        print(f"Error updating real-time price for {symbol}: {e}")
    finally:
        conn.close()









def process_symbols(symbols, batch_size=10, max_workers=2, db_name="stocks.db", table_name="yfinance"):
    """
    Process symbols in two steps: historical data first, then real-time prices.
    """
    # Step 1: Fetch and save historical data in batches
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        print(f"Processing batch {i // batch_size + 1} with {len(batch)} symbols (historical data)...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_and_save_historical, symbol, db_name, table_name) for symbol in batch]
            for future in as_completed(futures):
                try:
                    future.result()  # Ensure any exceptions are raised
                except Exception as e:
                    print(f"Error in thread: {e}")

        # Add a small delay between batches to prevent API rate-limiting
        time.sleep(2)

    # Step 2: Add real-time prices sequentially
    print("\nFetching real-time prices sequentially...")
    add_real_time_prices(symbols, db_name, table_name)





def process_symbols(symbols, batch_size=10, max_workers=2, db_name="stocks.db", table_name="yfinance"):
    """
    Process symbols in two steps: historical data first, then real-time prices.
    """
    # Step 1: Fetch and save historical data in batches
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        print(f"Processing batch {i // batch_size + 1} with {len(batch)} symbols (historical data)...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_and_save_historical, symbol, db_name, table_name) for symbol in batch]
            for future in as_completed(futures):
                try:
                    future.result()  # Ensure any exceptions are raised
                except Exception as e:
                    print(f"Error in thread: {e}")

    # Step 2: Add real-time prices sequentially
    print("\nFetching real-time prices sequentially...")
    add_real_time_prices(symbols, db_name, table_name)
















if __name__ == "__main__":
    # Step 1: Drop the table
    drop_yfinance_table()

    # Step 2: Create the table
    create_yfinance_table()

    # Define a list of symbols (Replace with your actual symbols list)
    symbols_list = ["AAPL", "MSFT", "GOOGL"]

    # Step 3: Process symbols in batches
    process_symbols(symbols_list, batch_size=2, max_workers=1)
    #process_symbols(symbols_list, batch_size=2, max_workers=2)

    #process_symbols_in_batches(symbols_list, db_name="stocks.db", table_name="yfinance")
    #process_symbols_in_batches(symbols_list, batch_size=2, max_workers=1)
    #process_symbols_in_batches(symbols_list, batch_size=2, max_workers=2)
    #process_symbols_in_batches(symbols_list, batch_size=10, max_workers=2)
    #process_symbols_in_batches(symbols_list, batch_size=50, max_workers=5)
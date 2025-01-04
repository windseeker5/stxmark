import sqlite3
import yfinance as yf
import pandas as pd
import time

# Constants
db_name = "stocks.db"
default_batch_size = 100  # Optimize for large runs


def drop_yfinance_table(db_name="stocks.db", table_name="yfinance"):
    """Drops the yfinance table if it exists."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()
    conn.close()
    print(f"Table {table_name} dropped successfully in {db_name}.")


def create_yfinance_table(db_name="stocks.db", table_name="yfinance"):
    """Creates the yfinance table if it does not exist."""
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
            Status TEXT
        )
    ''')
    conn.commit()
    conn.close()
    print(f"Table {table_name} created successfully in {db_name}.")


def fetch_and_save_symbols(symbols, db_name="stocks.db", table_name="yfinance"):
    """Fetches data for symbols and saves to the database."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    print(f"Fetching historical data for {len(symbols)} symbols...")
    data = yf.download(symbols, period="1mo", interval="1d", group_by="ticker", threads=True)

    for symbol in symbols:
        try:
            if symbol in data.columns.get_level_values(0):
                # Data exists for this symbol
                symbol_data = data[symbol].reset_index()
                symbol_data.rename(columns={"Adj Close": "Adj_Close"}, inplace=True)
                symbol_data["Symbol"] = symbol
                symbol_data["Status"] = "success"

                # Insert into database
                symbol_data.to_sql(table_name, conn, if_exists="append", index=False)
            else:
                # No data for this symbol
                cursor.execute(f'''
                    INSERT INTO {table_name} (Date, Symbol, Status)
                    VALUES (NULL, ?, 'no_data')
                ''', (symbol,))
                conn.commit()
        except Exception as e:
            print(f"Error processing symbol {symbol}: {e}")
            cursor.execute(f'''
                INSERT INTO {table_name} (Date, Symbol, Status)
                VALUES (NULL, ?, 'error')
            ''', (symbol,))
            conn.commit()

    conn.close()
    print(f"Data fetching and saving complete for {len(symbols)} symbols.")


def process_symbols(symbols, batch_size=default_batch_size, db_name="stocks.db", table_name="yfinance"):
    """Processes symbols in batches."""
    print(f"Processing {len(symbols)} symbols in batches of {batch_size}...")
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        fetch_and_save_symbols(batch, db_name, table_name)


def get_symbols_from_db():
    """Fetches symbol list from the database."""
    with sqlite3.connect(db_name) as conn:
        query = """
            SELECT DISTINCT symbol
            FROM (
                   SELECT symbol FROM sp500 WHERE yFinanceInvalid_ind = 0
                     UNION
                   SELECT symbol FROM symbols WHERE yFinanceInvalid_ind = 0
            ) AS combined
        """
        return pd.read_sql_query(query, conn)["symbol"].tolist()


if __name__ == "__main__":
    start_time = time.time()

    drop_yfinance_table()
    create_yfinance_table()

    # Fetch symbols from the database and limit for testing
    symbols_list = get_symbols_from_db()
    symbols_list = symbols_list[:100]  # Replace with [:27000] for full list

    # Process symbols
    process_symbols(symbols_list)

    end_time = time.time()
    duration_minutes = (end_time - start_time) / 60
    print(f"Time taken to process {len(symbols_list)} symbols: {duration_minutes:.2f} minutes")

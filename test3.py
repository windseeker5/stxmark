import sqlite3
import yfinance as yf
import threading
from concurrent.futures import ThreadPoolExecutor
import pandas as pd


db_lock = threading.Lock()


db_name = "stocks.db"
DB_NAME = "stocks.db"


def drop_yfinance_table(db_name="stocks.db", table_name="yfinance"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()
    conn.close()
    print(f"Table {table_name} dropped successfully in {db_name}.")





def create_yfinance_table(db_name="stocks.db", table_name="yfinance"):
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





def fetch_and_save_historical_batch(symbols, db_name="stocks.db", table_name="yfinance"):
    conn = sqlite3.connect(db_name)
    try:
        print(f"Fetching historical data for batch: {symbols}...")
        data = yf.download(symbols, period="1mo", interval="1d", group_by="ticker", threads=False)

        for symbol in symbols:
            if symbol in data.columns.get_level_values(0):
                symbol_data = data[symbol].copy()
                if not symbol_data.empty:
                    symbol_data.reset_index(inplace=True)
                    symbol_data.rename(columns={"Adj Close": "Adj_Close"}, inplace=True)
                    symbol_data["Symbol"] = symbol
                    symbol_data["Status"] = "success"
                    symbol_data["Currency"] = "N/A"  # Placeholder for now
                    symbol_data["Exchange"] = "N/A"  # Placeholder for now
                    symbol_data["Close_Unadjusted"] = symbol_data["Close"]

                    with db_lock:
                        symbol_data.to_sql(table_name, conn, if_exists="append", index=False)
                    print(f"Historical data for {symbol} saved successfully.")
                else:
                    insert_failed_symbol(conn, table_name, symbol)
            else:
                insert_failed_symbol(conn, table_name, symbol)
    except Exception as e:
        print(f"Error fetching historical data for batch: {symbols}: {e}")
    finally:
        conn.close()




def insert_failed_symbol(conn, table_name, symbol):
    with db_lock:
        cursor = conn.cursor()
        cursor.execute(f'''
            INSERT INTO {table_name} (Symbol, Status, Currency, Exchange)
            VALUES (?, ?, ?, ?)
        ''', (symbol, "failed", "N/A", "N/A"))
        conn.commit()
    print(f"Placeholder saved for failed symbol: {symbol}")




def add_real_time_prices_multithread(symbols, db_name="stocks.db", table_name="yfinance", max_workers=10):
    def update_price(symbol):
        conn = sqlite3.connect(db_name)
        try:
            cursor = conn.cursor()

            print(f"Fetching real-time price for {symbol}...")
            ticker = yf.Ticker(symbol)
            info = ticker.info  # Fetch all available info
            real_time_price = info.get("regularMarketPrice", None)
            currency = info.get("currency", "N/A")  # Get currency
            exchange = info.get("exchange", "N/A")  # Get exchange

            if real_time_price is not None:
                cursor.execute(f'''
                    UPDATE {table_name}
                    SET Close = ?, Close_Unadjusted = ?, Currency = ?, Exchange = ?, Status = ?
                    WHERE Symbol = ? AND Date = (
                        SELECT MAX(Date) FROM {table_name} WHERE Symbol = ?
                    )
                ''', (real_time_price, real_time_price, currency, exchange, "success", symbol, symbol))
                conn.commit()
                print(f"Real-time price for {symbol} updated successfully: {real_time_price}, Currency: {currency}, Exchange: {exchange}")
            else:
                # Update only if no data exists
                cursor.execute(f'''
                    UPDATE {table_name}
                    SET Status = ?, Currency = ?, Exchange = ?
                    WHERE Symbol = ? AND Status != "success"
                ''', ("failed", "N/A", "N/A", symbol))
                conn.commit()
                print(f"No real-time price available for {symbol}. Placeholder updated.")
        except Exception as e:
            print(f"Error updating real-time price for {symbol}: {e}")
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(update_price, symbols)









def process_symbols(symbols, batch_size=100, db_name="stocks.db", table_name="yfinance"):
    print("\nProcessing historical data in batches...\n")
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        fetch_and_save_historical_batch(batch, db_name, table_name)

    print("\nFetching real-time prices with multithreading...\n")
    add_real_time_prices_multithread(symbols, db_name, table_name, max_workers=10)






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









if __name__ == "__main__":
    drop_yfinance_table()
    create_yfinance_table()

    #symbols_list = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN", "WTFABADASS"]


    # Fetch symbols from the database and limit for testing
    symbols_list = get_symbols_from_db()
    symbols_list = symbols_list[:100]


    #print(symbols_list)
    print(type(symbols_list))
    print(len(symbols_list))


    process_symbols(symbols_list)

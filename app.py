import sqlite3
import yfinance as yf
import time
from queue import Queue
from threading import Thread, Lock
from tqdm import tqdm

DB_NAME = "stocks.db"
db_queue = Queue()
log_lock = Lock()
problematic_symbols = set()

# Thread-safe logging
def thread_safe_log(message):
    with log_lock:
        print(message)

# Function to fetch historical data for a batch of symbols
def fetch_historical_data(symbols, start_date, end_date):
    try:
        thread_safe_log(f"Fetching data for symbols: {', '.join(symbols)}")
        data = yf.download(symbols, start=start_date, end=end_date, group_by="ticker", threads=True)
        result = []

        if isinstance(data, dict):  # Multi-ticker data comes as a dict
            for symbol, df in data.items():
                if df.empty:
                    thread_safe_log(f"No data for symbol: {symbol}")
                    problematic_symbols.add(symbol)
                    continue
                for date, row in df.iterrows():
                    if "Open" in row and not pd.isna(row["Open"]):
                        result.append({
                            "symbol": symbol,
                            "date": date.strftime("%Y-%m-%d"),
                            "open": row["Open"],
                            "close": row["Close"],
                            "high": row["High"],
                            "low": row["Low"],
                            "volume": row["Volume"],
                        })
        else:  # Single ticker data
            for date, row in data.iterrows():
                if "Open" in row and not pd.isna(row["Open"]):
                    result.append({
                        "symbol": symbols[0],  # Single symbol
                        "date": date.strftime("%Y-%m-%d"),
                        "open": row["Open"],
                        "close": row["Close"],
                        "high": row["High"],
                        "low": row["Low"],
                        "volume": row["Volume"],
                    })
        return result
    except yf.YFTzMissingError as e:
        thread_safe_log(f"Timezone error for symbols {symbols}: {e}")
        problematic_symbols.update(symbols)
        with open("error_log.txt", "a") as log_file:
            log_file.write(f"Timezone error for symbols {symbols}: {e}\n")
        return []
    except yf.YFPricesMissingError as e:
        thread_safe_log(f"No price data for symbols {symbols}: {e}")
        problematic_symbols.update(symbols)
        with open("error_log.txt", "a") as log_file:
            log_file.write(f"No price data for symbols {symbols}: {e}\n")
        return []
    except Exception as e:
        thread_safe_log(f"General error for symbols {symbols}: {e}")
        problematic_symbols.update(symbols)
        with open("error_log.txt", "a") as log_file:
            log_file.write(f"General error for symbols {symbols}: {e}\n")
        return []

# Function to save data to SQLite
def save_historical_data_to_db(data):
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.executemany(
                """
                INSERT INTO symbol_data (symbol, date, open, close, high, low, volume)
                VALUES (:symbol, :date, :open, :close, :high, :low, :volume)
                """,
                data
            )
            conn.commit()
            thread_safe_log(f"Saved {len(data)} rows to the database")
    except sqlite3.Error as e:
        thread_safe_log(f"Database error: {e}")
        with open("error_log.txt", "a") as log_file:
            log_file.write(f"Database error: {e}\n")

# Database writer thread
def db_writer():
    while True:
        data = db_queue.get()
        if data is None:  # Sentinel to stop the thread
            break
        save_historical_data_to_db(data)
        db_queue.task_done()

# Worker function for processing a batch of symbols
def process_symbols_batch(batch, start_date, end_date):
    # Skip symbols already marked as problematic
    batch = [symbol for symbol in batch if symbol not in problematic_symbols]
    if not batch:
        return  # Skip processing if the batch is empty
    try:
        data = fetch_historical_data(batch, start_date, end_date)
        if data:
            db_queue.put(data)
    except Exception as e:
        thread_safe_log(f"Error processing batch {batch}: {e}")
        with open("error_log.txt", "a") as log_file:
            log_file.write(f"Error processing batch {batch}: {e}\n")

# Batch processing with multithreading and progress bar
def update_symbol_data():
    start_date = "2023-01-01"
    end_date = "2023-12-31"
    batch_size = 10  # Number of symbols per batch

    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT symbol FROM symbols")
        symbols = [row[0] for row in cursor.fetchall()]

    # Split symbols into batches
    batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]

    # Start the writer thread
    writer_thread = Thread(target=db_writer, daemon=True)
    writer_thread.start()

    # Use threads for parallel data fetching with a progress bar
    with tqdm(total=len(batches)) as pbar:
        threads = []
        for batch in batches:
            thread = Thread(target=process_symbols_batch, args=(batch, start_date, end_date))
            threads.append(thread)
            thread.start()

            # Update the progress bar after starting each batch
            pbar.update(1)

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

    # Stop the writer thread
    db_queue.put(None)
    writer_thread.join()

# Initialize database for historical data
def init_historical_db():
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS symbol_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                close REAL,
                high REAL,
                low REAL,
                volume INTEGER
            )
        ''')
        conn.commit()

if __name__ == "__main__":
    init_historical_db()
    update_symbol_data()

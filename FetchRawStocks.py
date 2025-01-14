import time
from utils import fetch_yfinance_data,fetch_us_symbols
#, get_symbols_from_db



# Constants
db_name = "stocks.db"
default_batch_size = 100  # Optimize for large runs

FINNHUB_API_KEY = "ctpgeohr01qqsrsaov10ctpgeohr01qqsrsaov1g"



if __name__ == "__main__":

    print("> Starting the application...")
    start_time = time.time()

    # Fetch symbols from Finnhub
    symbols_df = fetch_us_symbols(FINNHUB_API_KEY)
    
    # Convert the DataFrame to a list of 10 tickers
    symbols_list = symbols_df['Symbol'].head(100).tolist()
    print(symbols_list)

    # Example symbols list of 5 tickers
    # symbols_list = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

    # Fetch stock data
    stock_data = fetch_yfinance_data(symbols_list, default_batch_size)
    
    end_time = time.time()
    duration_minutes = (end_time - start_time) / 60
    
    
    print(f"Time taken to process {len(symbols_list)} symbols: {duration_minutes:.2f} minutes")
    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    print(stock_data.head())
    print(stock_data.shape)


    # Save the DataFrame to a CSV file
    stock_data.to_csv("stock_data.csv", index=False)
    





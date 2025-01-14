import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
import time




# Fetch stock data from Yahoo Finance
def fetch_yfinance_data(symbols_list, batch_size=100):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    data = {}
    for i in range(0, len(symbols_list), batch_size):
        batch_symbols = symbols_list[i:i + batch_size]
        tickers = yf.download(batch_symbols, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
        for symbol in batch_symbols:
            data[symbol] = tickers.xs(symbol, level=1, axis=1)
        
        # Sleep for 1 second to avoid hitting API rate limits
        time.sleep(2)
    
    df = pd.concat(data, axis=1)
    
    # Flatten the multi-index DataFrame
    df = df.stack(level=0, future_stack=True).reset_index()
    df.columns = ['Date', 'Ticker'] + list(df.columns[2:])
    
    return df






# Fetch all US symbols from Finnhub and save them to a DataFrame
def fetch_us_symbols(api_key):
    import requests
    url = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={api_key}"
    try:
        print("Fetching US symbols from Finnhub...")
        response = requests.get(url)
        response.raise_for_status()
        symbols_data = response.json()

        if not symbols_data:
            print("No symbols retrieved from Finnhub.")
            return pd.DataFrame()

        # Prepare data for DataFrame
        symbols = [item['symbol'] for item in symbols_data]
        symbols_df = pd.DataFrame(symbols, columns=["Symbol"])

        print(f"Fetched {len(symbols)} US symbols.")
        return symbols_df

    except requests.RequestException as e:
        print(f"Error fetching US symbols from Finnhub: {e}")
        return pd.DataFrame()





# Fetch S&P 500 symbols and save them to a DataFrame
def fetch_sp500_symbols():
    """Fetch all S&P 500 symbols and save them to a DataFrame."""
    try:
        print("Fetching S&P 500 symbols...")
        sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        sp500_table = pd.read_html(sp500_url, header=0)
        sp500_df = sp500_table[0]
        sp500_symbols = sp500_df["Symbol"].tolist()

        # Save symbols to a DataFrame
        sp500_df = pd.DataFrame(sp500_symbols, columns=["Symbol"])

        print(f"Fetched {len(sp500_symbols)} S&P 500 symbols.")
        return sp500_df
    except Exception as e:
        print(f"Error fetching S&P 500 symbols: {e}")
        return pd.DataFrame()
import yfinance as yf
import pandas as pd

def fetch_sp500_symbols():
    """Fetch all S&P 500 symbols and save them to a DataFrame."""
    try:
        # Download the S&P 500 constituent data from Wikipedia
        sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        sp500_table = pd.read_html(sp500_url, header=0)

        # Extract the relevant table
        sp500_df = sp500_table[0]

        # Keep only the Ticker column
        sp500_symbols = sp500_df["Symbol"].tolist()

        print(f"Fetched {len(sp500_symbols)} symbols from the S&P 500.")
        return sp500_symbols
    except Exception as e:
        print(f"Error fetching S&P 500 symbols: {e}")
        return []

# Example usage
if __name__ == "__main__":
    symbols = fetch_sp500_symbols()
    print("S&P 500 Symbols:", symbols)

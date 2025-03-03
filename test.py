import yfinance as yf

df = yf.download("AAPL", period="1mo", interval="1d", threads=False)
print(df.head())

import yfinance as yf
from datetime import datetime, timedelta

start_date = datetime.today() - timedelta(days=20)
end_date = datetime.today()

data = yf.download("GOOGL", start=start_date, end=end_date)
print(data)

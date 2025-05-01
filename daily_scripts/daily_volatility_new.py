import yfinance as yf
import sqlite3
from datetime import datetime, timedelta

TICKERS = ["AAPL", "MSFT", "SPY", "TSLA", "TLT"]
start_date = "2025-04-12"
end_date = "2025-04-16"

for ticker in TICKERS:
    try:
        print(f"📈 Updating {ticker}: {start_date} to {end_date}")
        data = yf.download(ticker, start=start_date, end=end_date, auto_adjust=False, progress=False)

        if data.empty:
            print(f"⚠️ No data found for {ticker} in that date range.")
            continue

        close_prices = data['Close']
        print(close_prices)

        # Your volatility calculations here ...

    except Exception as e:
        print(f"❌ Error fetching {ticker}: {e}")


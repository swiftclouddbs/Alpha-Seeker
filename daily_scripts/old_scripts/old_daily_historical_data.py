import yfinance as yf
import sqlite3

tickers = ['AAPL', 'MSFT', 'SPY', 'TLT', 'NVDA']  # Add your tickers here
conn = sqlite3.connect("greeks_data.db")
cursor = conn.cursor()

for ticker in tickers:
    stock = yf.Ticker(ticker)
    hist = stock.history(period="1y")

    for date, row in hist.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO historical_data (
                ticker, date, open, high, low, close, volume
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            ticker,
            date.strftime("%Y-%m-%d"),
            float(row['Open']),
            float(row['High']),
            float(row['Low']),
            float(row['Close']),
            int(row['Volume']),
#            float(row['Adj Close'])  # Optional, but great to have
        ))

conn.commit()
conn.close()

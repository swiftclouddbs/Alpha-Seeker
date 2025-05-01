import yfinance as yf
import sqlite3

# Define tickers
tickers = ["SPY", "QQQ", "IWM", "TLT", "XLF", "GLD", "SLV", "HYG",
    "TQQQ", "SOXL", "EEM", "GDX", "KWEB", "EWZ", "TSLL", "KRE", "FXI",
    "SQQQ", "XLE", "ARKK", "NVDA", "TSLA", "AAPL", "MSTR", "PLTR", "AMZN",
    "HTZ", "IBIT", "AMD", "META", "INTC", "GOOGL", "NFLX", "GME", "UNH",
    "SOFI", "MARA", "SMCI", "BABA", "COIN"]  # Add your tickers here

# Connect to SQLite database
conn = sqlite3.connect("../data/greeks_data.db")
cursor = conn.cursor()

# Create the historical_data table if it doesn't exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS historical_data (
        ticker TEXT,
        date TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        PRIMARY KEY (ticker, date)
    )
""")
conn.commit()

# Fetch and insert historical data for each ticker
for ticker in tickers:
    print(f"📈 Fetching data for: {ticker}")
    stock = yf.Ticker(ticker)
    hist = stock.history(period="1y")  # Adjust the period if necessary

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
        ))

# Commit changes and close connection
conn.commit()
conn.close()

print("✅ Historical data has been stored successfully.")


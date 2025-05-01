import sqlite3
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

def log_fetch(cursor, symbol, start_date, end_date, rows_added, status):
    cursor.execute("""
        INSERT INTO fetch_log (symbol, start_date, end_date, rows_added, status, timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (symbol, start_date, end_date, rows_added, status, datetime.now().isoformat()))

def update_volatility_db(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create a simple log table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fetch_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            start_date TEXT,
            end_date TEXT,
            rows_added INTEGER,
            status TEXT,
            timestamp TEXT
        )
    """)
    conn.commit()

    # Get the most recent date for each symbol
    cursor.execute("SELECT symbol, MAX(date) FROM historical_prices GROUP BY symbol")
    last_dates = cursor.fetchall()

    print(f"💡 Found {len(last_dates)} symbols to update.\n")

    for symbol, last_date in last_dates:
        if last_date is None:
            print(f"⚠️ No data for {symbol} — skipping.\n")
            log_fetch(cursor, symbol, "N/A", "N/A", 0, "No existing data")
            continue

        start_date = (pd.to_datetime(last_date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = datetime.today().strftime('%Y-%m-%d')

        if start_date > end_date:
            print(f"✅ {symbol} is already up to date.\n")
            log_fetch(cursor, symbol, start_date, end_date, 0, "Already up to date")
            continue

        print(f"📈 Updating {symbol}: {start_date} to {end_date}")

        try:
            downloaded = yf.download(
                symbol,
                start=start_date,
                end=end_date,
                progress=False,
                auto_adjust=False
            )

            if 'Close' not in downloaded.columns:
                print(f"⚠️ No 'Close' data for {symbol} — skipping.\n")
                log_fetch(cursor, symbol, start_date, end_date, 0, "No Close Column")
                continue

            close_prices = downloaded['Close']

            # Defensive: squeeze if multi-column or multi-index
            if isinstance(close_prices, pd.DataFrame):
                close_prices = close_prices.squeeze()

            if close_prices.empty:
                print(f"ℹ️ No new data for {symbol}\n")
                log_fetch(cursor, symbol, start_date, end_date, 0, "No Data")
                continue

            df = pd.DataFrame({
                'symbol': symbol,
                'date': close_prices.index.date,
                'close': close_prices.values
            })

            df.to_sql('historical_prices', conn, if_exists='append', index=False)
            log_fetch(cursor, symbol, start_date, end_date, len(df), "Success")
            print(f"✅ Added {len(df)} new rows for {symbol}\n")

        except Exception as e:
            print(f"❌ Error fetching {symbol}: {e}\n")
            log_fetch(cursor, symbol, start_date, end_date, 0, f"Error: {e}")

    conn.commit()
    conn.close()
    print("🎉 Update complete.")

# Example usage:
if __name__ == '__main__':
    update_volatility_db("greeks_data.db")

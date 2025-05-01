import yfinance as yf
import sqlite3
import time
from datetime import date, datetime

# --- Config ---
DATABASE_PATH = "../data/greeks_data.db"
TICKERS = ["AAPL", "NVDA", "TSLA", "TLT", "SPY"]  # Extend as needed
BATCH_PAUSE_SECONDS = 5

# --- Database Connection ---
conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()

# --- Date Handling ---
FETCH_DATE = date.today().isoformat()  # Default to today

# --- Core Function ---
def fetch_options_for_ticker(ticker_symbol, fetch_date):
    try:
        ticker = yf.Ticker(ticker_symbol)
        history = ticker.history(period="1d")
        
        if history.empty:
            print(f"⚠️ No price data for {ticker_symbol}. Skipping.")
            return 0

        underlying_price = history["Close"].iloc[-1]
        options_dates = ticker.options

        inserted_count = 0

        for exp_date in options_dates:
            options = ticker.option_chain(exp_date)
            for option_type, contracts in zip(["call", "put"], [options.calls, options.puts]):

                if "contractSymbol" not in contracts.columns:
                    print(f"⚠️ No contractSymbol for {ticker_symbol} {option_type}s expiring {exp_date}. Skipping.")
                    continue

                for _, row in contracts.iterrows():
                    contract_name = row["contractSymbol"]
                    strike = row["strike"]
                    bid = row["bid"]
                    ask = row["ask"]
                    last_price = underlying_price
                    option_price = (bid + ask) / 2 if bid > 0 and ask > 0 else None
                    implied_volatility = row["impliedVolatility"]

                    cursor.execute("""
                        INSERT INTO options_data (
                            ticker, expiration_date, strike, option_type,
                            bid, ask, last_price, option_price,
                            implied_volatility, fetch_date, contract_name
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        ticker_symbol, exp_date, strike, option_type,
                        bid, ask, last_price, option_price,
                        implied_volatility, fetch_date, contract_name
                    ))
                    inserted_count += 1

        conn.commit()
        print(f"✅ {ticker_symbol}: {inserted_count} contracts stored for {fetch_date}")
        return inserted_count

    except Exception as e:
        print(f"❌ Error processing {ticker_symbol}: {e}")
        return 0


# --- Main Fetch Loop ---
def fetch_options_batch(tickers, fetch_date):
    total_inserted = 0

    for i in range(0, len(tickers), 1):  # One ticker at a time (feel free to increase chunk size)
        batch = tickers[i:i + 1]
        print(f"🚀 Processing batch {i + 1}/{len(tickers)}: {batch}")

        for ticker_symbol in batch:
            inserted = fetch_options_for_ticker(ticker_symbol, fetch_date)
            total_inserted += inserted

        print(f"⏳ Batch {i + 1} complete. Pausing {BATCH_PAUSE_SECONDS} seconds to respect rate limits.")
        time.sleep(BATCH_PAUSE_SECONDS)

    print(f"\n🎯 Done! Total contracts stored: {total_inserted}")


# --- Entry Point ---
if __name__ == "__main__":
    fetch_options_batch(TICKERS, FETCH_DATE)
    conn.close()


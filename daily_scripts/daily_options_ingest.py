# daily_options_ingest.py

import yfinance as yf
import sqlite3
import time
from datetime import date

# --- Config ---
DATABASE_PATH = "../data/greeks_data.db"
TICKERS = [
    "SPY", "QQQ", "IWM", "TLT", "XLF", "GLD", "SLV", "HYG",
    "TQQQ", "SOXL", "EEM", "GDX", "KWEB", "EWZ", "TSLL", "KRE", "FXI",
    "SQQQ", "XLE", "ARKK", "NVDA", "TSLA", "AAPL", "MSTR", "PLTR", "AMZN",
    "HTZ", "IBIT", "AMD", "META", "INTC", "GOOGL", "NFLX", "GME", "UNH",
    "SOFI", "MARA", "SMCI", "BABA", "COIN"
]

BATCH_SIZE = 5
BATCH_PAUSE_SECONDS = 5
MAX_RETRIES = 2

# --- Database Connection ---
conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()

# --- Core Fetch Function ---
def fetch_options_for_ticker(ticker_symbol):
    attempt = 0
    inserted_count = 0

    while attempt <= MAX_RETRIES:
        try:
            ticker = yf.Ticker(ticker_symbol)
            history = ticker.history(period="1d")

            if history.empty:
                print(f"⚠️ {ticker_symbol}: No price data. Skipping.")
                return 0

            data_date = history.index[-1].strftime("%Y-%m-%d")
            underlying_price = history["Close"].iloc[-1]
            options_dates = ticker.options

            for exp_date in options_dates:
                options = ticker.option_chain(exp_date)

                for option_type, contracts in zip(["call", "put"], [options.calls, options.puts]):
                    if "contractSymbol" not in contracts.columns:
                        print(f"⚠️ {ticker_symbol} {option_type}s expiring {exp_date}: Missing contractSymbol.")
                        continue

                    for _, row in contracts.iterrows():
                        contract_name = row["contractSymbol"]
                        strike = row["strike"]
                        bid = row["bid"]
                        ask = row["ask"]
                        last_price = underlying_price
                        option_price = (bid + ask) / 2 if bid > 0 and ask > 0 else None
                        implied_volatility = row["impliedVolatility"]
                        open_interest = row.get("openInterest")
                        volume = row.get("volume")

                        try:
                            cursor.execute("""
                                INSERT OR IGNORE INTO options_data (
                                    ticker, expiration_date, strike, option_type,
                                    bid, ask, last_price, option_price,
                                    implied_volatility, open_interest, volume,
                                    contract_name, data_date
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                ticker_symbol, exp_date, strike, option_type,
                                bid, ask, last_price, option_price,
                                implied_volatility, open_interest, volume,
                                contract_name, data_date
                            ))
                            inserted_count += 1
                        except sqlite3.DatabaseError as e:
                            print(f"⚠️ DB Insert Error [{contract_name}]: {e}")

            conn.commit()
            print(f"✅ {ticker_symbol}: {inserted_count} contracts stored (data_date={data_date})")
            return inserted_count

        except Exception as e:
            attempt += 1
            print(f"❌ Error on {ticker_symbol} (Attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt <= MAX_RETRIES:
                print("🔁 Retrying after short pause...")
                time.sleep(3 * attempt)
            else:
                print(f"💥 Failed after {MAX_RETRIES} attempts.")
                return 0

# --- Batch Processing Loop ---
def fetch_options_batch(tickers):
    total_inserted = 0

    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        print(f"\n🚀 Processing batch {i // BATCH_SIZE + 1} / {len(tickers) // BATCH_SIZE + 1}: {batch}")

        for ticker_symbol in batch:
            inserted = fetch_options_for_ticker(ticker_symbol)
            total_inserted += inserted

        print(f"⏳ Batch {i // BATCH_SIZE + 1} complete. Pausing {BATCH_PAUSE_SECONDS} seconds.")
        time.sleep(BATCH_PAUSE_SECONDS)

    print(f"\n🎯 Done! Total contracts stored: {total_inserted}")

# --- Entry Point ---
if __name__ == "__main__":
    fetch_options_batch(TICKERS)
    conn.close()

import sqlite3
from utils.logger import log_event

log_event("daily_prescreen_contracts", "START", "Prescreening contracts started.")

try:

    def prescreen_options(db_path="../data/greeks_data.db"):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Step 1: Create valid_options_data table if not exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS valid_options_data (
            option_id INTEGER PRIMARY KEY,
            ticker TEXT NOT NULL,
            expiration_date TEXT NOT NULL,
            strike REAL NOT NULL,
            option_type TEXT NOT NULL,
            last_price REAL,
            option_price REAL NOT NULL,
            implied_volatility REAL NOT NULL,
            data_date TEXT NOT NULL,
            underlying_price REAL NOT NULL,
            open_interest INTEGER NOT NULL,
            volume INTEGER NOT NULL
        );
        """)
        conn.commit()
        print("✅ Created valid_options_data table if not exists.")

        # Step 2: Check and log all rows that would be filtered out
        cursor.execute("""
        SELECT COUNT(*), COUNT(CASE WHEN option_price IS NULL THEN 1 END),
               COUNT(CASE WHEN implied_volatility IS NULL THEN 1 END),
               COUNT(CASE WHEN open_interest <= 10 THEN 1 END),
               COUNT(CASE WHEN volume <= 0 THEN 1 END),
               COUNT(CASE WHEN option_price <= 0.05 THEN 1 END),
               COUNT(CASE WHEN last_price <= 1 THEN 1 END)
        FROM options_data;
        """)
        row = cursor.fetchone()
        print(f"📝 Filters check: Total rows = {row[0]}, "
              f"Option price NULL = {row[1]}, Implied volatility NULL = {row[2]}, "
              f"Open interest <= 10 = {row[3]}, Volume <= 0 = {row[4]}, "
              f"Option price <= 0.05 = {row[5]}, Underlying price <= 1 (last_price) = {row[6]}")

        # Step 3: Fetch and print a sample of the options_data to debug values
        cursor.execute("""
        SELECT option_id, ticker, expiration_date, strike, option_type,
               option_price, implied_volatility, last_price, open_interest, volume
        FROM options_data
        LIMIT 5;
        """)
        rows = cursor.fetchall()
        print("📝 Sample of data before filtering:")
        for row in rows:
            print(row)

        # Step 4: Insert valid options into valid_options_data without the option_price > 0.05 filter (for debugging)

        cursor.execute("""
        INSERT INTO valid_options_data (
            option_id, ticker, expiration_date, strike, option_type,
            last_price, option_price, implied_volatility, data_date,
            underlying_price, open_interest, volume
        )
        SELECT option_id, ticker, expiration_date, strike, option_type,
               last_price, option_price, implied_volatility, data_date,
               last_price AS underlying_price,
               COALESCE(open_interest, 0) AS open_interest,
               COALESCE(volume, 0) AS volume
        FROM options_data
        WHERE option_price IS NOT NULL
          AND implied_volatility IS NOT NULL
          AND COALESCE(open_interest, 0) > 10
          AND COALESCE(volume, 0) > 0
          AND option_price > 0.05
          AND last_price > 1

        """)

        conn.commit()

        # Check how many valid options were inserted
        cursor.execute("SELECT COUNT(*) FROM valid_options_data")
        count = cursor.fetchone()[0]
        print(f"✅ Inserted {count} valid options into valid_options_data.")

        # Log a few of the valid options inserted for verification
        cursor.execute("""
        SELECT option_id, ticker, expiration_date, strike, option_type, 
               option_price, implied_volatility, last_price
        FROM valid_options_data
        LIMIT 5;
        """)
        rows = cursor.fetchall()
        print(f"📝 Sample of valid options inserted:")
        for row in rows:
            print(row)

        conn.close()

    # --- Entry Point ---
    if __name__ == "__main__":
        prescreen_options()

    log_event("daily_prescreen_contracts", "SUCCESS", "Prescreening contracts completed.")
except Exception as e:
    log_event("daily_prescreen_contracts", "ERROR", str(e))
    raise

#daily_archive_expired_greeks.py

import sqlite3
from datetime import datetime

def archive_and_remove_expired_contracts(db_path):
    today_str = datetime.today().strftime("%Y-%m-%d")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Ensure greeks_archive table exists with correct schema
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS greeks_archive (
            option_id TEXT,
            ticker TEXT,
            expiry TEXT,
            call_put TEXT,
            strike REAL,
            delta REAL,
            gamma REAL,
            vega REAL,
            theta REAL,
            rho REAL,
            underlying_price REAL,
            days_to_expiry REAL,
            risk_free_rate REAL,
            implied_volatility REAL,
            data_date TEXT
        )
    """)

    # Count expired contracts
    cursor.execute("""
        SELECT COUNT(*) FROM greeks
        WHERE DATE(expiry) < DATE(?)
    """, (today_str,))
    count = cursor.fetchone()[0]

    if count == 0:
        print("✅ No expired contracts to archive.")
        conn.close()
        return

    # Archive expired rows
    cursor.execute(f"""
        INSERT INTO greeks_archive (
            option_id, ticker, expiry, call_put,
            strike, delta, gamma, vega, theta, rho,
            underlying_price, days_to_expiry, risk_free_rate,
            implied_volatility, data_date
        )
        SELECT 
            option_id, ticker, expiry, call_put,
            strike, delta, gamma, vega, theta, rho,
            underlying_price, days_to_expiry, risk_free_rate,
            implied_volatility, ?
        FROM greeks
        WHERE DATE(expiry) < DATE(?)
    """, (today_str, today_str))

    # Remove expired contracts from active table
    cursor.execute("""
        DELETE FROM greeks
        WHERE DATE(expiry) < DATE(?)
    """, (today_str,))

    conn.commit()
    conn.close()

    print(f"📦 Archived and removed {count} expired contracts.")

if __name__ == "__main__":
    db_path = "../data/greeks_data.db"
    archive_and_remove_expired_contracts(db_path)

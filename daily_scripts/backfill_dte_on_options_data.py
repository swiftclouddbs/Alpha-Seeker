import sqlite3
from datetime import datetime

def backfill_days_to_expiry(db_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Fetch rows missing days_to_expiry
    rows = cur.execute("""
        SELECT option_id, data_date, expiration_date
        FROM options_data
        WHERE days_to_expiry IS NULL
          AND data_date IS NOT NULL
          AND expiration_date IS NOT NULL;
    """).fetchall()

    print(f"🛠 Found {len(rows)} rows needing DTE calculation...")

    update_count = 0
    for option_id, data_date, exp_date in rows:
        try:
            dte = (datetime.strptime(exp_date, "%Y-%m-%d") - datetime.strptime(data_date, "%Y-%m-%d")).days
            cur.execute("""
                UPDATE options_data
                SET days_to_expiry = ?
                WHERE option_id = ?;
            """, (dte, option_id))
            update_count += 1
        except Exception as e:
            print(f"⚠️ Skipped option_id={option_id} due to error: {e}")

    conn.commit()
    conn.close()
    print(f"✅ Updated {update_count} rows with valid days_to_expiry.")

if __name__ == "__main__":
    backfill_days_to_expiry("../data/greeks_data.db")

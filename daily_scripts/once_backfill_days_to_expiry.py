import sqlite3

def backfill_days_to_expiry(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Count how many greeks rows have NULL or 0 days_to_expiry
    cursor.execute("""
        SELECT COUNT(*) FROM greeks
        WHERE days_to_expiry IS NULL OR days_to_expiry = 0
    """)
    count = cursor.fetchone()[0]
    print(f"🔍 Found {count} greeks rows needing backfill.")

    if count == 0:
        print("✅ No backfilling needed.")
        conn.close()
        return

    # Perform the backfill from options_data
    cursor.execute("""
        UPDATE greeks
        SET days_to_expiry = (
            SELECT options_data.days_to_expiry
            FROM options_data
            WHERE options_data.option_id = greeks.option_id
            LIMIT 1
        )
        WHERE days_to_expiry IS NULL OR days_to_expiry = 0
    """)

    conn.commit()
    conn.close()
    print(f"✅ Backfilled {count} rows in greeks.days_to_expiry.")

if __name__ == "__main__":
    db_path = "../data/greeks_data.db"
    backfill_days_to_expiry(db_path)

import sqlite3
from datetime import datetime

def inspect_days_to_expiry(db_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Check if the necessary columns exist
    cur.execute("PRAGMA table_info(options_data);")
    columns = [row[1] for row in cur.fetchall()]
    if not {"data_date", "expiration_date", "days_to_expiry"}.issubset(set(columns)):
        print("❌ Required columns missing in options_data.")
        return

    # Query a sample of rows to inspect
    sample = cur.execute("""
        SELECT data_date, expiration_date, days_to_expiry
        FROM options_data
        WHERE data_date IS NOT NULL AND expiration_date IS NOT NULL AND days_to_expiry IS NOT NULL
        LIMIT 100;
    """).fetchall()

    if not sample:
        print("⚠️ No rows found with non-null data_date, expiration_date, and days_to_expiry.")
        return

    print("🧪 Checking days_to_expiry calculations:")
    for data_date_str, expiry_str, days in sample[:10]:  # Only print a few rows
        try:
            data_date = datetime.strptime(data_date_str, "%Y-%m-%d")
            expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d")
            expected_days = (expiry_date - data_date).days
            status = "✅ OK" if expected_days == days else f"❌ MISMATCH (Expected: {expected_days})"
            print(f"{data_date_str} → {expiry_str} | Reported: {days}, Calculated: {expected_days} → {status}")
        except Exception as e:
            print(f"⚠️ Error parsing dates: {data_date_str}, {expiry_str}: {e}")

    conn.close()

if __name__ == "__main__":
    inspect_days_to_expiry("../data/greeks_data.db")

import sqlite3

# --- Config ---
DATABASE_PATH = "../data/greeks_data.db"

# --- Connect to Database ---
conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()

# --- Remove Junk Contracts from options_data ---
print("🧹 Deleting junk contracts from options_data...")
cursor.execute("DELETE FROM options_data WHERE is_junk = 1;")
print(f"✅ {cursor.rowcount} junk contracts deleted.")

# --- Commit Changes and Close Connection ---
conn.commit()
conn.close()
print("🎯 Reset complete! Database is clean and ready for fresh processing.")


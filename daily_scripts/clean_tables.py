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

# --- Clear greeks Table ---
print("🧹 Clearing greeks table...")
cursor.execute("DELETE FROM greeks;")
print(f"✅ {cursor.rowcount} rows deleted from greeks.")

# --- Clear historical_volatility Table ---
print("🧹 Clearing historical_volatility table...")
cursor.execute("DELETE FROM historical_volatility;")
print(f"✅ {cursor.rowcount} rows deleted from historical_volatility.")

# --- Clear feature_store Table ---
print("🧹 Clearing feature_store table...")
cursor.execute("DELETE FROM feature_store;")
print(f"✅ {cursor.rowcount} rows deleted from feature_store.")

# --- Clear pipeline_log Table (optional) ---
print("🧹 Clearing pipeline_log table...")
cursor.execute("DELETE FROM pipeline_log;")
print(f"✅ {cursor.rowcount} rows deleted from pipeline_log.")

# --- Commit Changes and Close Connection ---
conn.commit()
conn.close()
print("🎯 Reset complete! Database is clean and ready for fresh processing.")


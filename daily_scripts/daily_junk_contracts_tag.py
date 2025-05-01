import sqlite3

# --- Database Connection ---
DATABASE_PATH = "../data/greeks_data.db"
conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()

# --- Add 'is_junk' Column If Needed ---
try:
    cursor.execute("ALTER TABLE options_data ADD COLUMN is_junk INTEGER DEFAULT 0")
    conn.commit()
    print("✅ 'is_junk' column added to options_data.")
except sqlite3.OperationalError as e:
    if "duplicate column name" in str(e):
        print("ℹ️ 'is_junk' column already exists, continuing...")
    else:
        raise  # re-raise if it's another error

# --- Flag Junk Contracts ---
cursor.execute("""
    UPDATE options_data
    SET is_junk = 1
    WHERE 
        option_price IS NULL 
        OR option_price <= 0.05
        OR implied_volatility IS NULL
        OR open_interest <= 10
        OR volume <= 0
        OR last_price <= 1
""")
conn.commit()

# --- Summary ---
cursor.execute("SELECT COUNT(*) FROM options_data WHERE is_junk = 1")
junk_count = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM options_data")
total_count = cursor.fetchone()[0]

print(f"🧹 Junk contracts flagged: {junk_count} / {total_count}")

conn.close()

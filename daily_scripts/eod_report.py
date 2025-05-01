import sqlite3
from datetime import datetime

# --- Connect to your database ---
DATABASE_PATH = "../data/greeks_data.db"
conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()

print(f"📊 AlphaSeeker Report — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# --- Basic Statistics ---
cursor.execute("SELECT COUNT(*) FROM options_data")
total_options = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM options_data WHERE is_junk = 1")
junk_options = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM feature_store")
valid_feature_records = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM candidate_trades")
candidate_trades = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM strategy_suggestions")
strategy_suggestions = cursor.fetchone()[0]

# --- Display Results ---
print(f"Total contracts in options_data: {total_options}")
print(f"Junk contracts flagged: {junk_options}")
print(f"Valid contracts in feature_store: {valid_feature_records}")
print(f"Candidate trades identified: {candidate_trades}")
print(f"Strategy suggestions generated: {strategy_suggestions}")

# --- Preview Top Opportunities ---
print("\n💎 Top 10 Strategy Suggestions (expected return):")
cursor.execute("""
    SELECT ticker, suggested_strategy, strike, expiration_date, expected_return
    FROM strategy_suggestions
    ORDER BY expected_return DESC
    LIMIT 100
""")
for row in cursor.fetchall():
    print(f"{row}")

# --- Clean Up ---
conn.close()

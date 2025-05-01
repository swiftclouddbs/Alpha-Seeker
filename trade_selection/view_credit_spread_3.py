import sqlite3
import pandas as pd
from datetime import date

# --- Config ---
DATABASE_PATH = "../data/greeks_data.db"
CREDIT_SPREADS_TABLE = "credit_spread_candidates"
OUTPUT_CSV = f"../reports/credit_spread_report_{date.today().isoformat()}.csv"

# --- Connect to Database ---
conn = sqlite3.connect(DATABASE_PATH)

# --- Query Spread Candidates ---
query = f"""
    SELECT 
        short_leg_id, long_leg_id,
        ticker, expiration_date, spread_type,
        short_strike, long_strike,
        short_premium, long_premium,
        net_credit, max_loss, risk_reward_ratio,
        decision_date
    FROM {CREDIT_SPREADS_TABLE}
    ORDER BY risk_reward_ratio DESC
"""

df = pd.read_sql_query(query, conn)

# --- Save to CSV ---
df.to_csv(OUTPUT_CSV, index=False)

print(f"✅ Report written to {OUTPUT_CSV}")
print(df.head(10))  # Show a preview of top candidates

conn.close()

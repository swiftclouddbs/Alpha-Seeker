import sqlite3
import csv
from datetime import date

# --- Config ---
DATABASE_PATH = "../data/greeks_data.db"
OUTPUT_CSV = "../reports/credit_spread_candidates_con_ids.csv"

# --- Database Connection ---
conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()

# --- Query Credit Spread Candidates ---
cursor.execute("""
    SELECT 
        s.ticker,
        s.expiration_date,
        s.spread_type,
        s.short_leg_id,
        o1.strike AS short_strike,
        o1.last_price AS short_premium,
        s.long_leg_id,
        o2.strike AS long_strike,
        o2.last_price AS long_premium,
        ROUND((o1.last_price - o2.last_price), 2) AS net_credit,
        ROUND((o2.strike - o1.strike) - (o1.last_price - o2.last_price), 2) AS max_loss,
        CASE WHEN (o2.strike - o1.strike) - (o1.last_price - o2.last_price) > 0 THEN
            ROUND((o1.last_price - o2.last_price) / ((o2.strike - o1.strike) - (o1.last_price - o2.last_price)), 2)
        ELSE NULL END AS risk_reward
    FROM credit_spread_candidates s
    JOIN options_data o1 ON s.short_leg_id = o1.rowid
    JOIN options_data o2 ON s.long_leg_id = o2.rowid
    ORDER BY s.ticker, s.expiration_date, s.spread_type
""")

results = cursor.fetchall()

# --- Write to CSV ---
with open(OUTPUT_CSV, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([
        "Ticker", "Expiration", "Spread Type",
        "Short Leg ID", "Short Strike", "Short Premium",
        "Long Leg ID", "Long Strike", "Long Premium",
        "Net Credit", "Max Loss", "Risk:Reward"
    ])
    for row in results:
        writer.writerow(row)

print(f"✅ Exported {len(results)} credit spread candidates to {OUTPUT_CSV}")

conn.close()

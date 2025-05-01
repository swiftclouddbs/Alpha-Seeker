import sqlite3
import pandas as pd

conn = sqlite3.connect("../data/greeks_data.db")
df = pd.read_sql_query("SELECT * FROM credit_spread_candidates ORDER BY risk_reward_ratio DESC LIMIT 100", conn)
conn.close()

df.to_csv("../reports/credit_spread_candidates_sample.csv", index=False)
print("✅ Sample saved to credit_spread_candidates_sample.csv")

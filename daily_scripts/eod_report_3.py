import sqlite3
import pandas as pd

# --- Config ---
DATABASE_PATH = "../data/greeks_data.db"
MAX_SAMPLES_PER_GROUP = 5
OUTPUT_CSV = "../reports/strategy_suggestions_balanced.csv"

# --- Database Connection ---
conn = sqlite3.connect(DATABASE_PATH)

# --- Query: Get a balanced mix ---
query = f"""
WITH RankedStrategies AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY ticker, suggested_strategy ORDER BY implied_volatility DESC) as rank
    FROM strategy_suggestions
    WHERE dte >= 7  -- Exclude short DTE contracts
)
SELECT 
    option_id, ticker, expiration_date, strike, option_type, delta,
    implied_volatility, last_price, dte, suggested_strategy, expected_return, decision_date
FROM RankedStrategies
WHERE rank <= {MAX_SAMPLES_PER_GROUP}
ORDER BY suggested_strategy, ticker, expiration_date;
"""

# --- Fetch and Export ---
df = pd.read_sql_query(query, conn)
df.to_csv(OUTPUT_CSV, index=False)

print(f"✅ Balanced strategy report written to: {OUTPUT_CSV}")
print(df.head(20))  # Show first few rows for confirmation

conn.close()

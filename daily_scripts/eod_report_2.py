import sqlite3
import csv
import json

# --- Config ---
DATABASE_PATH = "../data/greeks_data.db"
STRATEGY_TABLE = "strategy_suggestions"
CSV_OUTPUT = "../reports/strategy_report.csv"
JSON_OUTPUT = "../reports/strategy_report.json"
MIN_DTE = 7
SAMPLES_PER_STRATEGY = 10

# --- Database Connection ---
conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()

# --- Fetch Distinct Strategies ---
cursor.execute(f"""
    SELECT DISTINCT suggested_strategy 
    FROM {STRATEGY_TABLE}
    WHERE dte >= ?
""", (MIN_DTE,))
strategies = [row[0] for row in cursor.fetchall()]

final_results = []

# --- Sample N Contracts per Strategy ---
for strategy in strategies:
    cursor.execute(f"""
        SELECT 
            ticker, expiration_date, strike, option_type, delta, 
            implied_volatility, last_price, dte, suggested_strategy, expected_return, decision_date
        FROM {STRATEGY_TABLE}
        WHERE suggested_strategy = ?
        AND dte >= ?
        ORDER BY expected_return DESC
        LIMIT ?
    """, (strategy, MIN_DTE, SAMPLES_PER_STRATEGY))
    
    rows = cursor.fetchall()
    final_results.extend(rows)

conn.close()

# --- Write CSV ---
with open(CSV_OUTPUT, mode="w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Ticker", "Expiration", "Strike", "Type", "Delta", "IV", "LastPrice", "DTE", "Strategy", "ExpectedReturn", "DateSuggested"])
    writer.writerows(final_results)

print(f"✅ Report saved to {CSV_OUTPUT}")

# --- Write JSON ---
json_data = [
    {
        "ticker": row[0],
        "expiration_date": row[1],
        "strike": row[2],
        "option_type": row[3],
        "delta": row[4],
        "implied_volatility": row[5],
        "last_price": row[6],
        "dte": row[7],
        "suggested_strategy": row[8],
        "expected_return": row[9],
        "date_suggested": row[10],
    }
    for row in final_results
]

with open(JSON_OUTPUT, "w") as f:
    json.dump(json_data, f, indent=4)

print(f"✅ JSON saved to {JSON_OUTPUT}")

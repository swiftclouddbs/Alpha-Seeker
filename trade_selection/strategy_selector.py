import sqlite3
from datetime import date

# --- Config ---
DATABASE_PATH = "../data/greeks_data.db"
STRATEGY_TABLE = "strategy_suggestions"

# --- Database Connection ---
conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()

# --- Create Strategy Suggestion Table ---
cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {STRATEGY_TABLE} (
    option_id INTEGER PRIMARY KEY,
    ticker TEXT,
    expiration_date TEXT,
    strike REAL,
    option_type TEXT,
    delta REAL,
    implied_volatility REAL,
    last_price REAL,
    dte INTEGER,
    suggested_strategy TEXT,
    expected_return REAL,
    decision_date TEXT
);
""")
conn.commit()

# --- Fetch Valid Contracts from candidate_trades ---
cursor.execute("""
    SELECT 
        c.option_id, c.ticker, c.expiration_date, c.strike, c.option_type, 
        c.delta, c.implied_volatility, c.last_price,
        julianday(c.expiration_date) - julianday('now') AS dte
    FROM candidate_trades c
    WHERE c.data_date = (SELECT MAX(data_date) FROM candidate_trades)
""")
candidates = cursor.fetchall()

# --- Strategy Logic ---
def suggest_strategy(delta, iv, dte, option_type):
    if dte < 7:
        return "Avoid - Too Close to Expiry"
    
    if abs(delta) > 0.8:
        return "Deep ITM - Consider Synthetic" if option_type == "call" else "Deep ITM - Consider Protective Put"

    if abs(delta) < 0.1:
        return "High Gamma Play - Consider Long Straddle"

    if iv > 0.5 and dte >= 20:
        return "Sell Premium - Credit Spread"

    if iv < 0.3 and dte >= 20:
        return "Buy Premium - Long Call/Put"

    return "Neutral Setup - Condor or Butterfly"

# --- Process Candidates ---
inserted = 0
for option in candidates:
    option_id, ticker, exp_date, strike, opt_type, delta, iv, last_price, dte = option
    strategy = suggest_strategy(delta, iv, dte, opt_type)

    # Basic placeholder for expected return: delta / last_price
    if last_price > 0:
        expected_return = abs(delta) / last_price
    else:
        expected_return = 0.0

    cursor.execute(f"""
        INSERT OR REPLACE INTO {STRATEGY_TABLE} (
            option_id, ticker, expiration_date, strike, option_type, delta, 
            implied_volatility, last_price, dte, suggested_strategy, expected_return, decision_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        option_id, ticker, exp_date, strike, opt_type, delta, iv, last_price, dte,
        strategy, expected_return, date.today().isoformat()
    ))

    inserted += 1

conn.commit()

print(f"✅ Strategy suggestions written: {inserted}")
conn.close()

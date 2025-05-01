import sqlite3
from datetime import date
from collections import defaultdict

# --- Config ---
DATABASE_PATH = "../data/greeks_data.db"
CREDIT_SPREADS_TABLE = "credit_spread_candidates"

# --- Database Connection ---
conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()

# --- Create Table for Spread Candidates ---
cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {CREDIT_SPREADS_TABLE} (
    short_leg_id INTEGER,
    long_leg_id INTEGER,
    ticker TEXT,
    expiration_date TEXT,
    spread_type TEXT,
    short_strike REAL,
    long_strike REAL,
    short_premium REAL,
    long_premium REAL,
    net_credit REAL,
    max_loss REAL,
    risk_reward_ratio REAL,
    decision_date TEXT
);
""")
conn.commit()

# --- Fetch Potential Candidates with RowID ---
cursor.execute("""
    SELECT 
        rowid, ticker, expiration_date, strike, option_type, last_price
    FROM options_data
    WHERE is_junk = 0
    ORDER BY ticker, expiration_date, option_type, strike
""")
contracts = cursor.fetchall()

# --- Group Contracts by Ticker, Expiry, and Option Type ---
grouped = defaultdict(list)
for option_id, ticker, expiry, strike, opt_type, price in contracts:
    grouped[(ticker, expiry, opt_type)].append((option_id, strike, price))

# --- Strategy Detection ---
inserted = 0

for (ticker, expiry, opt_type), options_list in grouped.items():
    if opt_type not in ('call', 'put'):
        continue

    # sort by strike ascending
    options_list.sort(key=lambda x: x[1])

    for i in range(len(options_list) - 1):
        short_id, short_strike, short_price = options_list[i]
        long_id, long_strike, long_price = options_list[i + 1]

        if opt_type == 'put' and short_strike < long_strike:
            spread_type = "Bull Put Spread"
        elif opt_type == 'call' and short_strike < long_strike:
            spread_type = "Bear Call Spread"
        else:
            continue

        net_credit = short_price - long_price
        max_loss = (long_strike - short_strike) - net_credit if net_credit > 0 else None
        risk_reward_ratio = net_credit / max_loss if max_loss and max_loss > 0 else None

        if net_credit > 0 and risk_reward_ratio and risk_reward_ratio >= 0.25:
            cursor.execute(f"""
                INSERT INTO {CREDIT_SPREADS_TABLE} (
                    short_leg_id, long_leg_id, ticker, expiration_date, spread_type, 
                    short_strike, long_strike, short_premium, long_premium, net_credit, 
                    max_loss, risk_reward_ratio, decision_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                short_id, long_id, ticker, expiry, spread_type, short_strike, long_strike,
                short_price, long_price, net_credit, max_loss, risk_reward_ratio, date.today().isoformat()
            ))
            inserted += 1

conn.commit()
print(f"✅ Credit Spread candidates written: {inserted}")
conn.close()

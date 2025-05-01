#credit_spread_detector
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

# --- Fetch Latest Candidate Contracts ---
cursor.execute("""
    SELECT 
        option_id, ticker, expiration_date, strike, option_type, last_price
    FROM candidate_trades
    WHERE data_date = (SELECT MAX(data_date) FROM candidate_trades)
    AND last_price IS NOT NULL
    ORDER BY ticker, expiration_date, option_type, strike
""")
contracts = cursor.fetchall()
print(f"Fetched {len(contracts)} contracts from candidate_trades")

# --- Group Contracts by Ticker, Expiry, and Option Type ---
grouped = defaultdict(list)
for option_id, ticker, expiry, strike, opt_type, price in contracts:
    grouped[(ticker, expiry, opt_type)].append({
        "id": option_id,
        "strike": strike,
        "price": price
    })
print(f"Grouped into {len(grouped)} ticker-expiry-option_type sets")

# --- Strategy Detection ---
inserted = 0

for (ticker, expiry, opt_type), options_list in grouped.items():
    if opt_type not in ('call', 'put'):
        continue

    # sort by strike ascending
    options_list.sort(key=lambda x: x["strike"])

    for i in range(len(options_list) - 1):
        short = options_list[i]
        long = options_list[i + 1]

        if short["strike"] == long["strike"]:
            print(f"❌ Skipping: same strike {short['strike']} for {ticker} {opt_type}")
            continue

        # Check for proper strike order based on strategy
        if opt_type == 'put' and short["strike"] < long["strike"]:
            spread_type = "Bull Put Spread"
        elif opt_type == 'call' and short["strike"] < long["strike"]:
            spread_type = "Bear Call Spread"
##        else:
##            print(f"❌ Skipping: invalid strike relationship for {ticker} {opt_type} short={short['strike']} long={long['strike']}")
            continue

        net_credit = short["price"] - long["price"]
        max_loss = (long["strike"] - short["strike"]) - net_credit if net_credit > 0 else None
        risk_reward_ratio = net_credit / max_loss if max_loss and max_loss > 0 else None

        if net_credit > 0:
            rr_str = f"{risk_reward_ratio:.2f}" if risk_reward_ratio else "N/A"
            print(f"✅ Found {spread_type} for {ticker}: short={short['strike']} (${short['price']}), long={long['strike']} (${long['price']}), RR={rr_str}")

            cursor.execute(f"""
                INSERT INTO {CREDIT_SPREADS_TABLE} (
                    short_leg_id, long_leg_id, ticker, expiration_date, spread_type, 
                    short_strike, long_strike, short_premium, long_premium, net_credit, 
                    max_loss, risk_reward_ratio, decision_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                short["id"], long["id"], ticker, expiry, spread_type,
                short["strike"], long["strike"],
                short["price"], long["price"], net_credit,
                max_loss, risk_reward_ratio, date.today().isoformat()
            ))
            inserted += 1

conn.commit()
print(f"✅ Credit Spread candidates written: {inserted}")
conn.close()

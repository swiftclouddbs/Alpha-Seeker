# credit_spread_detector.py
import sqlite3
from datetime import date
from collections import defaultdict, Counter

# --- Config ---
DATABASE_PATH = "../data/greeks_data.db"
CREDIT_SPREADS_TABLE = "credit_spread_candidates"

# --- Filter Thresholds (adjust as needed) ---
MIN_NET_CREDIT = 0.01       # Min credit to justify entering a spread
MAX_SPREAD_WIDTH = 20       # Avoid overly wide spreads
MIN_RISK_REWARD = 0.01      # Avoid spreads with poor R/R
MAX_MAX_LOSS = 2000         # Avoid spreads risking more than $1000
PRINT_SKIPPED_DETAILS = True  # Show info about near-misses

# --- Connect to DB ---
conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()

# --- Create Table ---
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
    decision_date TEXT,
    spread_width REAL,
    break_even REAL
);
""")
conn.commit()

# --- Fetch Contracts from Candidate Table ---
cursor.execute("""
    SELECT option_id, ticker, expiration_date, strike, option_type, last_price
    FROM candidate_trades
    WHERE data_date = (SELECT MAX(data_date) FROM candidate_trades)
    AND last_price IS NOT NULL
    ORDER BY ticker, expiration_date, option_type, strike
""")
contracts = cursor.fetchall()

# --- Group by Ticker, Expiry, Type ---
grouped = defaultdict(list)
for option_id, ticker, expiry, strike, opt_type, price in contracts:
    grouped[(ticker, expiry, opt_type)].append({
        "id": option_id,
        "strike": strike,
        "price": price
    })

# --- Process Each Group ---
inserted = 0
skipped_reasons = Counter()

for (ticker, expiry, opt_type), options_list in grouped.items():
    if opt_type not in ('call', 'put'):
        continue

    # Sort by strike ascending
    options_list.sort(key=lambda x: x["strike"])

    for i in range(len(options_list)):
        short = options_list[i]

        for j in range(i + 1, len(options_list)):
            long = options_list[j]

            if short["strike"] == long["strike"]:
                skipped_reasons["same_strike"] += 1
                continue

            # Determine spread type
            if opt_type == 'put' and short["strike"] < long["strike"]:
                spread_type = "Bull Put Spread"
            elif opt_type == 'call' and short["strike"] < long["strike"]:
                spread_type = "Bear Call Spread"
            else:
                skipped_reasons["wrong_order"] += 1
                continue

            net_credit = short["price"] - long["price"]
            spread_width = long["strike"] - short["strike"]
            max_loss = spread_width - net_credit if net_credit > 0 else None
            risk_reward = net_credit / max_loss if max_loss and max_loss > 0 else None
            break_even = (
                short["strike"] + net_credit if spread_type == "Bear Call Spread"
                else short["strike"] - net_credit
            )

            # --- Filters ---
            if net_credit <= 0:
                skipped_reasons["no_credit"] += 1
                continue
            if net_credit < MIN_NET_CREDIT:
                skipped_reasons["low_credit"] += 1
                if PRINT_SKIPPED_DETAILS and inserted < 10:
                    print(f"⚠️ {ticker} {spread_type}: credit too low ({net_credit:.2f})")
                continue
            if spread_width > MAX_SPREAD_WIDTH:
                skipped_reasons["too_wide"] += 1
                continue
            if max_loss is None or max_loss > MAX_MAX_LOSS:
                skipped_reasons["max_loss_exceeded"] += 1
                continue
            if risk_reward is None or risk_reward < MIN_RISK_REWARD:
                skipped_reasons["low_rr"] += 1
                continue

            # --- Insert Valid Spread ---
            cursor.execute(f"""
                INSERT INTO {CREDIT_SPREADS_TABLE} (
                    short_leg_id, long_leg_id, ticker, expiration_date, spread_type,
                    short_strike, long_strike, short_premium, long_premium, net_credit,
                    max_loss, risk_reward_ratio, decision_date, spread_width, break_even
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                short["id"], long["id"], ticker, expiry, spread_type,
                short["strike"], long["strike"],
                short["price"], long["price"], net_credit,
                max_loss, risk_reward, date.today().isoformat(),
                spread_width, break_even
            ))
            inserted += 1

conn.commit()
conn.close()

# --- Report ---
print(f"✅ Credit Spread candidates written: {inserted}")
if skipped_reasons:
    print("🚫 Skipped spreads summary:")
    for reason, count in skipped_reasons.items():
        print(f"   • {reason}: {count}")

# trade_selection/feature_table_trade_simulator.py

import sqlite3
from config import DATABASE_PATH

# Pick which table to point at
TABLE = "feature_store"  # could be "candidate_trades" or "options_data"

def fetch_delta(option_id, data_date):
    data_date = str(data_date)[:10]
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
            SELECT delta FROM {TABLE}
            WHERE option_id = ? AND data_date = ?
        """, (option_id, data_date))
        row = cursor.fetchone()
    finally:
        conn.close()

    if row is None:
        print(f"[DEBUG] No delta for {option_id} on {data_date}")
        return None
    return row[0]

def fetch_tradable_contracts():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT option_id
            FROM {TABLE}
            GROUP BY option_id
            HAVING COUNT(DISTINCT data_date) >= 2;
        """)
        contracts = [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()

    return contracts

def get_entry_exit_dates(option_id):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT data_date FROM {TABLE}
            WHERE option_id = ?
            ORDER BY data_date ASC;
        """, (option_id,))
        dates = [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()

    return (dates[0], dates[-1]) if len(dates) >= 2 else (None, None)

def simulate_trade(option_id, entry_date, exit_date):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
            SELECT last_price FROM {TABLE}
            WHERE option_id = ? AND data_date = ?
        """, (option_id, entry_date))
        entry = cursor.fetchone()

        cursor.execute(f"""
            SELECT last_price FROM {TABLE}
            WHERE option_id = ? AND data_date = ?
        """, (option_id, exit_date))
        exit_ = cursor.fetchone()
    finally:
        conn.close()

    if entry and exit_ and entry[0] is not None and exit_[0] is not None:
        pnl = exit_[0] - entry[0]
        return {
            'option_id': option_id,
            'entry_date': entry_date,
            'exit_date': exit_date,
            'entry_price': entry[0],
            'exit_price': exit_[0],
            'pnl': pnl,
            'status': 'Completed'
        }
    else:
        return {
            'option_id': option_id,
            'entry_date': entry_date,
            'exit_date': exit_date,
            'entry_price': entry[0] if entry else None,
            'exit_price': exit_[0] if exit_ else None,
            'status': 'Data Missing'
        }

def run_batch_simulation(limit=100):
    tradable = fetch_tradable_contracts()
    results = []

    total_checked = 0
    successful_trades = 0

    for contract in tradable[:limit]:
        entry_date, exit_date = get_entry_exit_dates(contract)
        total_checked += 1
        if entry_date and exit_date:
            trade = simulate_trade(contract, entry_date, exit_date)
            if trade['status'] == 'Completed':
                successful_trades += 1
            results.append(trade)

    print("\n=== Trade Simulation Summary ===")
    print(f"Total Contracts Reviewed: {total_checked}")
    print(f"Trades with Valid Data: {successful_trades}")
    print(f"Missing or Incomplete Data: {total_checked - successful_trades}")
    print("================================\n")

    return results

def analyze_trades(trades, profit_threshold=0.5):
    print(f"\n=== Trades with > {profit_threshold * 100:.0f}% Profit ===\n")
    profitable = [
        t for t in trades
        if 'pnl' in t and t['entry_price'] and t['entry_price'] > 0 and (t['pnl'] / t['entry_price']) >= profit_threshold
    ]
    print(f"Total: {len(profitable)}\n")

    for t in sorted(profitable, key=lambda x: x['pnl'], reverse=True):
        delta = fetch_delta(t['option_id'], t['entry_date'])
        percent_profit = (t['pnl'] / t['entry_price']) * 100 if t['entry_price'] else 0

        print(f"{t['option_id']} | {t['entry_date']} -> {t['exit_date']} | "
              f"Profit: {percent_profit:.2f}% | Delta at Entry: {delta}")

# ==== Run it ====
if __name__ == "__main__":
    trades = run_batch_simulation(limit=100)
    analyze_trades(trades, profit_threshold=0.5)

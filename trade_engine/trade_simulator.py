import sqlite3
from config import DATABASE_PATH

def fetch_delta(contract_name, fetch_date):
    fetch_date = str(fetch_date)[:10]  # ensure YYYY-MM-DD
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT option_id FROM options_data 
        WHERE contract_name = ? AND fetch_date = ?
    """, (contract_name, fetch_date))
    row = cursor.fetchone()

    if row is None:
        print(f"[DEBUG] No option_id for {contract_name} on {fetch_date}")
        conn.close()
        return None

    option_id = row[0]

    cursor.execute("""
        SELECT delta FROM greeks
        WHERE option_id = ?
    """, (option_id,))
    delta_row = cursor.fetchone()
    conn.close()

    if delta_row is None:
        print(f"[DEBUG] No delta for option_id {option_id} ({contract_name}) on {fetch_date}")
    return delta_row[0] if delta_row else None

def fetch_tradable_contracts():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT contract_name
        FROM options_data
        GROUP BY contract_name
        HAVING COUNT(DISTINCT fetch_date) >= 2;
    """)
    contracts = [row[0] for row in cursor.fetchall()]
    conn.close()
    return contracts

def get_entry_exit_dates(contract_name):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT fetch_date FROM options_data
        WHERE contract_name = ?
        ORDER BY fetch_date ASC;
    """, (contract_name,))
    dates = [row[0] for row in cursor.fetchall()]
    conn.close()
    return (dates[0], dates[-1]) if len(dates) >= 2 else (None, None)

def simulate_trade(contract_name, entry_date, exit_date):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT option_price FROM options_data
        WHERE contract_name = ? AND fetch_date = ?
    """, (contract_name, entry_date))
    entry = cursor.fetchone()

    cursor.execute("""
        SELECT option_price FROM options_data
        WHERE contract_name = ? AND fetch_date = ?
    """, (contract_name, exit_date))
    exit_ = cursor.fetchone()

    conn.close()

    if entry and exit_ and entry[0] is not None and exit_[0] is not None:
        pnl = exit_[0] - entry[0]
        return {
            'contract_name': contract_name,
            'entry_date': entry_date,
            'exit_date': exit_date,
            'entry_price': entry[0],
            'exit_price': exit_[0],
            'pnl': pnl,
            'status': 'Completed'
        }
    else:
        return {
            'contract_name': contract_name,
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
        if 'pnl' in t and t['entry_price'] > 0 and (t['pnl'] / t['entry_price']) >= profit_threshold
    ]
    print(f"Total: {len(profitable)}\n")

    for t in sorted(profitable, key=lambda x: x['pnl'], reverse=True):
        delta = fetch_delta(t['contract_name'], t['entry_date'])
        percent_profit = (t['pnl'] / t['entry_price']) * 100 if t['entry_price'] else 0

        print(f"{t['contract_name']} | {t['entry_date']} -> {t['exit_date']} | "
              f"Profit: {percent_profit:.2f}% | Delta at Entry: {delta}")

# ==== Run it ====
if __name__ == "__main__":
    trades = run_batch_simulation(limit=100)
    analyze_trades(trades, profit_threshold=0.5)

import sqlite3
import pandas as pd

def simulate_trade_candidate(row):
    entry_price = row['last_price_entry']
    exit_price = row['last_price_exit']
    if pd.isna(entry_price) or pd.isna(exit_price) or entry_price == 0:
        print(f"Data Missing for Option: {row['option_id']} | Entry: {entry_price}, Exit: {exit_price}")
        return {
            'option_id': row['option_id'],
            'ticker': row['ticker'],
            'expiration_date': row['expiration_date'],
            'strike': row['strike'],
            'option_type': row['option_type'],
            'entry_date': row['data_date_entry'],
            'exit_date': row['data_date_exit'],
            'entry_price': entry_price,
            'exit_price': exit_price,
            'pnl': None,
            'profit_pct': None,
            'status': 'Data Missing'
        }
    pnl = exit_price - entry_price
    profit_pct = pnl / entry_price
    print(f"Option {row['option_id']} | PnL: {pnl}, Profit Percent: {profit_pct}")
    return {
        'option_id': row['option_id'],
        'ticker': row['ticker'],
        'expiration_date': row['expiration_date'],
        'strike': row['strike'],
        'option_type': row['option_type'],
        'entry_date': row['data_date_entry'],
        'exit_date': row['data_date_exit'],
        'entry_price': entry_price,
        'exit_price': exit_price,
        'pnl': pnl,
        'profit_pct': profit_pct,
        'status': 'Completed'
    }


def run_candidate_simulation(db_path, entry_date, exit_date):
    conn = sqlite3.connect(db_path)

    query = f"""
    SELECT
        ct.option_id,
        ct.ticker,
        ct.expiration_date,
        ct.strike,
        ct.option_type,
        ct.days_to_expiry,
        ct.last_price AS last_price_entry,
        ct.data_date AS data_date_entry,
        f_exit.last_price AS last_price_exit,
        f_exit.data_date AS data_date_exit
    FROM candidate_trades ct
    LEFT JOIN candidate_trades f_exit
        ON ct.option_id = f_exit.option_id
       AND f_exit.data_date = '{exit_date}'
    WHERE ct.data_date = '{entry_date}'
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    print(f"\nPulled {len(df)} rows from database.")

    if df.empty:
        print("No data found for given dates!")
        return pd.DataFrame()  # Early exit if no data

    results = []
    for _, row in df.iterrows():
        results.append(simulate_trade_candidate(row))

    results_df = pd.DataFrame(results)

    # === Trade Simulation Summary ===
    print("\n=== Trade Simulation Summary ===")
    print(f"Total Contracts Reviewed: {len(results_df)}")
    if 'status' in results_df.columns:
        completed = (results_df['status'] == 'Completed').sum()
        missing = (results_df['status'] == 'Data Missing').sum()
        print(f"Trades with Valid Data: {completed}")
        print(f"Missing or Incomplete Data: {missing}")
    else:
        print("Trades with Valid Data: 0")
        print("Missing or Incomplete Data: 0")
    print("================================\n")

    # === Profitable Trades Report ===
    if 'profit_pct' in results_df.columns:
        profitable_trades = results_df[
            (results_df['status'] == 'Completed') &
            (results_df['profit_pct'] > 0.5)
        ]

        print("\n=== Trades with > 50% Profit ===\n")
        print(f"Total: {len(profitable_trades)}\n")

        for _, row in profitable_trades.iterrows():
            print(f"{row['ticker']} | {row['expiration_date']} | {row['strike']} {row['option_type']} | {row['entry_date']} -> {row['exit_date']} | Profit: {row['profit_pct']*100:.2f}%")
    else:
        print("\nNo profitable trades to report.\n")

    return results_df

# ---- Runner ----
if __name__ == "__main__":
    db_path = "../data/greeks_data.db"  # Or whatever path you're using
    results_df = run_candidate_simulation(db_path, "2025-04-21", "2025-04-24")

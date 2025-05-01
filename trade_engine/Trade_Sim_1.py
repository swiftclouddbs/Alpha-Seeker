import sqlite3

def simulate_trade(db_path, contract_name, start_date, end_date):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Fetch option prices for the selected contract within the date range
    cursor.execute("""
        SELECT fetch_date, option_price
        FROM options_data
        WHERE contract_name = ? AND fetch_date BETWEEN ? AND ?
        ORDER BY fetch_date
    """, (contract_name, start_date, end_date))

    rows = cursor.fetchall()

    if not rows:
        print(f"❌ No data found for {contract_name} between {start_date} and {end_date}.")
        return

    # Print the option prices on each day
    print(f"📈 Simulating trade for {contract_name} from {start_date} to {end_date}")
    for row in rows:
        print(f"Date: {row[0]}, Option Price: {row[1]}")

    # Simulate a simple trade (buy on the first day, sell on the last day)
    entry_price = rows[0][1]  # Option price on the first date
    exit_price = rows[-1][1]  # Option price on the last date
    profit_loss = exit_price - entry_price

    print(f"\n🎯 Trade Summary:")
    print(f"Buy at {start_date} for {entry_price}")
    print(f"Sell at {end_date} for {exit_price}")
    print(f"Profit/Loss: {profit_loss}")

    conn.close()

# Example usage:
simulate_trade("greeks_data.db", "TSLA271217P00460000", "2025-04-14", "2025-04-16")

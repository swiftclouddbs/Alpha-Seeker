from trade_engine import trade_simulator

def main():
    trades = trade_simulator.run_batch_simulation(limit=500)

    total = len(trades)
    valid = len([t for t in trades if t['status'] == 'Completed'])
    missing = total - valid

##    print("\n=== Trade Simulation Summary ===")
##    print(f"Total Contracts Reviewed: {total}")
##    print(f"Trades with Valid Data: {valid}")
##    print(f"Missing or Incomplete Data: {missing}")
##    print("================================")

    # Now analyze the trades
    trade_simulator.analyze_trades(trades)

if __name__ == "__main__":
    main()

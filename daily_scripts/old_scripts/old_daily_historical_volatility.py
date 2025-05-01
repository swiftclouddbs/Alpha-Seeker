# historical_volatility.py

import sqlite3
import pandas as pd
import numpy as np

DB_PATH = "greeks_data.db"

def calculate_hv_20d(prices: pd.Series) -> pd.Series:
    log_returns = np.log(prices / prices.shift(1))
    rolling_std = log_returns.rolling(window=20).std()
    hv_20d = rolling_std * np.sqrt(252)  # Annualize the volatility
    return hv_20d

def update_historical_volatility():
    with sqlite3.connect(DB_PATH) as conn:
        prices_query = """
            SELECT symbol, date, close
            FROM historical_prices
            ORDER BY symbol, date
        """
        df = pd.read_sql_query(prices_query, conn, parse_dates=["date"])

        results = []

        for symbol, group in df.groupby("symbol"):
            group = group.sort_values("date")
            group["hv_20d"] = calculate_hv_20d(group["close"])
            valid = group.dropna(subset=["hv_20d"])[["symbol", "date", "hv_20d"]]
            results.append(valid)

        all_results = pd.concat(results)

        # Save back to database
        all_results.to_sql("historical_volatility", conn, if_exists="replace", index=False)

        print(f"✅ Historical volatility updated. {len(all_results)} records written.")

if __name__ == "__main__":
    update_historical_volatility()

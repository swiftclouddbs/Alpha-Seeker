import sqlite3
from datetime import datetime

def build_feature_store(db_path='../data/greeks_data.db'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Clear existing data to avoid duplicates (or use UPSERT logic)
    cursor.execute("DELETE FROM feature_store;")
    conn.commit()

    # Build the query to join relevant tables
    feature_query = """
    INSERT INTO feature_store
    SELECT 
        o.option_id,
        o.ticker,
        o.fetch_date,
        o.expiration_date AS expiry,
        o.option_type,
        o.strike,
        o.last_price,
        o.bid,
        o.ask,
        g.delta,
        g.gamma,
        g.vega,
        g.theta,
        g.rho,
        g.underlying_price,
        g.implied_volatility,
        g.days_to_expiry,
        hv.hv_20 AS historical_volatility_20d,
        r.rate AS risk_free_rate
    FROM options_data o
    JOIN greeks g ON o.option_id = g.option_id
    LEFT JOIN historical_volatility hv 
        ON o.ticker = hv.ticker AND o.fetch_date = hv.fetch_date
    LEFT JOIN risk_free_rates r 
        ON o.fetch_date = r.date
    WHERE o.fetch_date = (SELECT MAX(fetch_date) FROM options_data);
    """

    # Execute the population query
    cursor.execute(feature_query)
    conn.commit()

    print("✅ Feature Store built successfully.")

    # Clean up
    conn.close()


if __name__ == "__main__":
    build_feature_store()


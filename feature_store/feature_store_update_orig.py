import sqlite3
from utils.logger import log_event

log_event("feature_store_update", "START", "Feature store update started.")

try:

    def update_feature_store(db_path="../data/greeks_data.db"):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Step 1: Clear old data
        cursor.execute("DELETE FROM feature_store;")
        conn.commit()
        print("✅ Feature Store cleared.")

        # Step 2: Insert fresh snapshot including data_date
        cursor.execute("""
        INSERT INTO feature_store (
            option_id,
            ticker,
            fetch_date,
            data_date,
            expiry,
            option_type,
            strike,
            last_price,
            bid,
            ask,
            delta,
            gamma,
            vega,
            theta,
            rho,
            underlying_price,
            implied_volatility,
            days_to_expiry,
            historical_volatility_20d,
            risk_free_rate
        )
        SELECT 
            g.option_id,
            g.ticker,
            g.fetch_date,
            g.data_date,
            g.expiry,
            g.call_put AS option_type,
            g.strike,
            NULL as last_price,
            NULL as bid,
            NULL as ask,
            g.delta,
            g.gamma,
            g.vega,
            g.theta,
            g.rho,
            g.underlying_price,
            g.implied_volatility,
            g.days_to_expiry,
            h.hv_20,
            r.rate AS risk_free_rate
        FROM greeks g
        LEFT JOIN historical_volatility h
            ON g.ticker = h.ticker AND g.data_date = h.data_date
        LEFT JOIN risk_free_rates r
            ON g.data_date = r.date
        WHERE g.days_to_expiry > 0;
        """)
        conn.commit()
        print("✅ Feature Store updated with fresh records.")

        conn.close()

    if __name__ == "__main__":
        update_feature_store()

    log_event("feature_store_update", "SUCCESS", "Feature store update completed.")

except Exception as e:
    log_event("feature_store_update", "ERROR", str(e))
    raise

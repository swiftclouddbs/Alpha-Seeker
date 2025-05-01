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

        # Step 2: Insert fresh snapshot with full columns
        cursor.execute("""
        INSERT INTO feature_store (
            option_id,
            ticker,
            expiration_date,
            option_type,
            strike,
            last_price,
            bid,
            ask,
            change,
            percent_change,
            volume,
            open_interest,
            implied_volatility,
            in_the_money,
            contract_name,
            bs_price,
            bs_diff,
            option_price,
            days_to_expiry,
            data_date,
            data_quality_flag,
            is_junk,
            delta,
            gamma,
            vega,
            theta,
            rho,
            underlying_price,
            historical_volatility_20d,
            risk_free_rate
        )
        SELECT 
            g.option_id,
            g.ticker,
            g.expiry AS expiration_date,
            g.call_put AS option_type,
            g.strike,
            o.last_price,
            o.bid,
            o.ask,
            o.change,
            o.percent_change,
            o.volume,
            o.open_interest,
            o.implied_volatility,
            o.in_the_money,
            o.contract_name,
            o.bs_price,
            o.bs_diff,
            o.option_price,
            g.days_to_expiry,
            o.data_date,
            o.data_quality_flag,
            o.is_junk,
            g.delta,
            g.gamma,
            g.vega,
            g.theta,
            g.rho,
            g.underlying_price,
            h.hv_20,
            r.rate AS risk_free_rate
        FROM greeks g
        LEFT JOIN historical_volatility h
            ON g.ticker = h.ticker AND g.data_date = h.data_date
        LEFT JOIN risk_free_rates r
            ON g.data_date = r.date
        LEFT JOIN options_data o
            ON g.option_id = o.option_id
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

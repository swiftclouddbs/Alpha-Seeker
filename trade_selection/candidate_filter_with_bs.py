#candidate_filter_with_bs.py
import sqlite3
from utils.logger import log_event

log_event("candidate_filter", "START", "Candidate filter started.")

try:
    def filter_candidates(db_path: str):
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        # Drop & recreate candidate_trades
        cur.execute("DROP TABLE IF EXISTS candidate_trades;")

        cur.execute("""
            CREATE TABLE candidate_trades (
                option_id INT,
                ticker TEXT,
                expiration_date TEXT,
                strike REAL,
                option_type TEXT,
                days_to_expiry INT,
                implied_volatility REAL,
                delta REAL,
                gamma REAL,
                theta REAL,
                vega REAL,
                last_price REAL,
                bid REAL,
                ask REAL,
                underlying_price REAL,
                volume INT,
                open_interest INT,
                bs_price REAL,
                bs_diff REAL,
                data_date TEXT
            );
        """)

        # Insert filtered data into candidate_trades
        cur.execute("""
            INSERT INTO candidate_trades
            SELECT
                option_id,
                ticker,
                expiration_date,
                strike,
                option_type,
                days_to_expiry,
                implied_volatility,
                delta,
                gamma,
                theta,
                vega,
                last_price,
                bid,
                ask,
                underlying_price,
                volume,
                open_interest,
                bs_price,
                bs_diff,
                data_date
            FROM feature_store
            WHERE 
                days_to_expiry >= 20
                AND implied_volatility >= 0.35
                AND volume > 100
                AND open_interest > 100;
        """)

        # Create indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_candidate_option_id ON candidate_trades (option_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_candidate_ticker_date ON candidate_trades (ticker, data_date);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_candidate_expiration ON candidate_trades (expiration_date);")

        conn.commit()

        count = cur.execute("SELECT COUNT(*) FROM candidate_trades;").fetchone()[0]
        print(f"✅ {count} candidate trades selected with filters: days_to_expiry >= 20, IV >= 5%, volume > 1000, open_interest > 1000.")

        # --- Filter interesting trades ---
        cur.execute("DROP TABLE IF EXISTS interesting_candidate_trades;")

        cur.execute("""
            CREATE TABLE interesting_candidate_trades AS
            SELECT *
            FROM candidate_trades
            WHERE ABS(bs_diff) > 0.1;
        """)

        conn.commit()

        interesting_count = cur.execute("SELECT COUNT(*) FROM interesting_candidate_trades;").fetchone()[0]
        print(f"✨ {interesting_count} interesting candidate trades with |bs_diff| > 0.1.")

        conn.close()

    if __name__ == "__main__":
        filter_candidates("../data/greeks_data.db")
        log_event("candidate_filter", "SUCCESS", "Candidate filter completed.")

except Exception as e:
    log_event("candidate_filter", "ERROR", str(e))
    raise

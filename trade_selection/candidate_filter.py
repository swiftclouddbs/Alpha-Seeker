import sqlite3
from utils.logger import log_event

# --- Configurable Filter Thresholds ---
MIN_DTE = 20
MIN_IV = 0.15
MIN_VOLUME = 100
MIN_OI = 100
MIN_BS_DIFF = 0.05

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
        cur.execute(f"""
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
                days_to_expiry >= {MIN_DTE}
                AND implied_volatility >= {MIN_IV}
                AND volume > {MIN_VOLUME}
                AND open_interest > {MIN_OI};
        """)

        # Create indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_candidate_option_id ON candidate_trades (option_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_candidate_ticker_date ON candidate_trades (ticker, data_date);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_candidate_expiration ON candidate_trades (expiration_date);")

        conn.commit()

        count = cur.execute("SELECT COUNT(*) FROM candidate_trades;").fetchone()[0]
        print(f"✅ {count} candidate trades selected with filters: days_to_expiry >= {MIN_DTE}, IV >= {MIN_IV:.0%}, volume > {MIN_VOLUME}, open_interest > {MIN_OI}.")

        # --- Filter interesting trades ---
        cur.execute("DROP TABLE IF EXISTS interesting_candidate_trades;")

        cur.execute(f"""
            CREATE TABLE interesting_candidate_trades AS
            SELECT *
            FROM candidate_trades
            WHERE ABS(bs_diff) > {MIN_BS_DIFF};
        """)

        conn.commit()

        interesting_count = cur.execute("SELECT COUNT(*) FROM interesting_candidate_trades;").fetchone()[0]
        print(f"✨ {interesting_count} interesting candidate trades with |bs_diff| > {MIN_BS_DIFF}.")

        conn.close()

    if __name__ == "__main__":
        filter_candidates("../data/greeks_data.db")
        log_event("candidate_filter", "SUCCESS", "Candidate filter completed.")

except Exception as e:
    log_event("candidate_filter", "ERROR", str(e))
    raise

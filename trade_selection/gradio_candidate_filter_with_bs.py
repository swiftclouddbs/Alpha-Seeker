import gradio as gr
import sqlite3
import pandas as pd
from utils.logger import log_event

log_event("candidate_filter", "START", "Candidate filter started.")

def filter_and_fetch(db_path: str, min_days_to_expiry: int, min_iv: float, min_volume: int, min_open_interest: int):
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
            underlying_price REAL,
            volume INT,
            open_interest INT,
            data_date TEXT
        );
    """)

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
            underlying_price,
            volume,
            open_interest,
            data_date
        FROM feature_store
        WHERE 
            days_to_expiry >= ?
            AND implied_volatility >= ?
            AND volume > ?
            AND open_interest > ?;
    """, (min_days_to_expiry, min_iv, min_volume, min_open_interest))

    # Indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_candidate_option_id ON candidate_trades (option_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_candidate_ticker_date ON candidate_trades (ticker, data_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_candidate_expiration ON candidate_trades (expiration_date);")

    conn.commit()
    candidate_count = cur.execute("SELECT COUNT(*) FROM candidate_trades;").fetchone()[0]

    # Filter interesting trades
    cur.execute("DROP TABLE IF EXISTS interesting_candidate_trades;")
    cur.execute("""
        CREATE TABLE interesting_candidate_trades AS
        SELECT ct.*
        FROM candidate_trades ct
        JOIN feature_store fs ON ct.option_id = fs.option_id
        WHERE ABS(fs.bs_diff) > 0.25;
    """)
    conn.commit()
    interesting_count = cur.execute("SELECT COUNT(*) FROM interesting_candidate_trades;").fetchone()[0]

    # Fetch full interesting trades
    df = pd.read_sql_query("""
        SELECT * FROM interesting_candidate_trades
        ORDER BY ticker, expiration_date;
    """, conn)

    conn.close()

    summary = (
        f"✅ {candidate_count} candidate trades selected with filters: "
        f"days_to_expiry >= {min_days_to_expiry}, IV >= {min_iv*100:.1f}%, "
        f"volume > {min_volume}, open_interest > {min_open_interest}.\n"
        f"✨ {interesting_count} interesting candidate trades with |bs_diff| > 0.25."
    )

    return summary, df


# Gradio App with Blocks layout
with gr.Blocks(title="Candidate Filter with BS") as app:
    gr.Markdown("### 📊 Filter Options Contracts Using Black-Scholes Deviation")

    with gr.Row():
        min_days = gr.Slider(minimum=0, maximum=100, step=1, label="Min Days to Expiry", value=20)
        min_iv = gr.Slider(minimum=0.0, maximum=1.0, step=0.01, label="Min Implied Volatility", value=0.05)
        min_volume = gr.Slider(minimum=0, maximum=5000, step=100, label="Min Volume", value=1000)
        min_oi = gr.Slider(minimum=0, maximum=5000, step=100, label="Min Open Interest", value=1000)

    run_button = gr.Button("Run Filter")

    with gr.Column():
        summary_box = gr.Textbox(label="Summary", lines=4)
        ticker_df = gr.Dataframe(label="Interesting Candidate Trades", wrap=True)# max_rows=20)

    run_button.click(
        fn=lambda days, iv, vol, oi: filter_and_fetch("../data/greeks_data.db", days, iv, vol, oi),
        inputs=[min_days, min_iv, min_volume, min_oi],
        outputs=[summary_box, ticker_df]
    )

app.launch()

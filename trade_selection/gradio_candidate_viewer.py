# gradio_candidate_viewer.py

import gradio as gr
import sqlite3
import pandas as pd
from utils.logger import log_event

log_event("candidate_viewer", "START", "Candidate viewer started.")

def view_filtered_candidates(db_path: str, min_days_to_expiry: int, min_iv: float, min_volume: int, min_open_interest: int):
    conn = sqlite3.connect(db_path)

    query = """
        SELECT
            option_id as ID,
            ticker,
            expiration_date as Expiry,
            strike,
            option_type as type,
            days_to_expiry as DTE,
            ROUND(implied_volatility, 4) as IV,
            ROUND(last_price, 2) AS last,
            bid,
            ask,
            volume,
            open_interest as OI,
            ROUND(bs_diff, 4) as BS_DIFF,
            data_date
        FROM interesting_candidate_trades
        WHERE days_to_expiry >= ?
          AND implied_volatility >= ?
          AND volume > ?
          AND open_interest > ?
        ORDER BY ticker, expiration_date;
    """
#ROUND(last_price, 2) AS last
    df = pd.read_sql_query(query, conn, params=(min_days_to_expiry, min_iv, min_volume, min_open_interest))
    conn.close()

    summary = (
        f"📊 Showing **{len(df)}** matching trades\n\n"
        f"🔎 Filters:\n"
        f"- Days to Expiry ≥ {min_days_to_expiry}\n"
        f"- IV ≥ {min_iv*100:.1f}%\n"
        f"- Volume > {min_volume}\n"
        f"- Open Interest > {min_open_interest}"
    )
    return summary, df


# Gradio App
with gr.Blocks(title="Candidate Trade Viewer") as app:
    gr.Markdown("### 📈 View Filtered Options Trades (Read-Only)")

    with gr.Row():
        min_days = gr.Slider(minimum=0, maximum=100, step=1, label="Min Days to Expiry", value=20)
        min_iv = gr.Slider(minimum=0.0, maximum=1.0, step=0.01, label="Min Implied Volatility", value=0.05)
        min_volume = gr.Slider(minimum=0, maximum=5000, step=100, label="Min Volume", value=1000)
        min_oi = gr.Slider(minimum=0, maximum=5000, step=100, label="Min Open Interest", value=1000)

    run_button = gr.Button("View Filtered Trades")

    with gr.Column():
        summary_box = gr.Textbox(label="Summary", lines=4)
        ticker_df = gr.Dataframe(label="Matching Trades", wrap=True)

    run_button.click(
        fn=lambda days, iv, vol, oi: view_filtered_candidates("../data/greeks_data.db", days, iv, vol, oi),
        inputs=[min_days, min_iv, min_volume, min_oi],
        outputs=[summary_box, ticker_df]
    )

app.launch()

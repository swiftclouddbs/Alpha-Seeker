import gradio as gr
import sqlite3
import pandas as pd

DB_PATH = "../data/greeks_data.db"

# --- Core filtering logic ---
def suggest_plays(margin, max_dte, ticker_filter):
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT option_id, ticker, expiry, strike, option_type, days_to_expiry,
               implied_volatility, delta, theta, vega, last_price, underlying_price
        FROM feature_store
        WHERE days_to_expiry <= ?
              AND last_price <= ?
              AND implied_volatility IS NOT NULL
              AND delta IS NOT NULL
    """
    params = [max_dte, margin]

    if ticker_filter:
        query += " AND ticker = ?"
        params.append(ticker_filter.upper())

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    df = df.sort_values(by=["days_to_expiry", "implied_volatility"], ascending=[True, False])
    return df.head(20)

# --- Gradio UI ---
with gr.Blocks() as demo:
    gr.Markdown("## 🧠 Options Strategy Dashboard")

    with gr.Row():
        margin_input = gr.Number(label="Max Margin ($)", value=500)
        dte_input = gr.Number(label="Max Days to Expiry", value=14)
        ticker_input = gr.Textbox(label="Optional Ticker Filter (e.g. AAPL)")

    run_button = gr.Button("Suggest Trades")
    result_table = gr.Dataframe(label="Suggested Plays", interactive=True)

    run_button.click(
        fn=suggest_plays,
        inputs=[margin_input, dte_input, ticker_input],
        outputs=result_table
    )

if __name__ == "__main__":
    demo.launch()

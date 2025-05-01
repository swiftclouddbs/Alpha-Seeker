import sqlite3
import pandas as pd
import gradio as gr
from datetime import datetime

DB_PATH = "../data/greeks_data.db"

def get_unique_tickers():
    conn = sqlite3.connect(DB_PATH)
    tickers = pd.read_sql_query("SELECT DISTINCT ticker FROM options_data ORDER BY ticker ASC;", conn)
    conn.close()
    return ["All"] + tickers['ticker'].tolist()

def query_mispricings(threshold=0.1, option_type="both", ticker="All", max_days=90):
    conn = sqlite3.connect(DB_PATH)

    conditions = [
        "bs_price IS NOT NULL",
        "option_price IS NOT NULL",
        "ABS(option_price - bs_price) >= ?"
    ]
    params = [threshold]

    if option_type != "both":
        conditions.append("option_type = ?")
        params.append(option_type)

    if ticker != "All":
        conditions.append("ticker = ?")
        params.append(ticker)

    if max_days > 0:
        today = datetime.today().strftime("%Y-%m-%d")
        conditions.append("julianday(expiration_date) - julianday(?) <= ?")
        params.extend([today, max_days])

    where_clause = " AND ".join(conditions)

    query = f"""
        SELECT 
            option_id, ticker, contract_name, option_type, strike, expiration_date, fetch_date,
            option_price, bs_price, ROUND(option_price - bs_price, 4) AS price_diff
        FROM 
            options_data
        WHERE 
            {where_clause}
        ORDER BY 
            ABS(option_price - bs_price) DESC
        LIMIT 100;
    """

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

# Gradio UI setup
def build_interface():
    tickers = get_unique_tickers()

    with gr.Blocks(title="Mispricing Explorer") as demo:
        gr.Markdown("# 💡 Options Mispricing Explorer")

        with gr.Row():
            threshold = gr.Slider(0.01, 5.0, step=0.01, value=0.1, label="Mispricing Threshold ($)")
            option_type = gr.Dropdown(["both", "call", "put"], value="both", label="Option Type")
            ticker_dropdown = gr.Dropdown(tickers, value="All", label="Ticker")
            expiry_slider = gr.Slider(0, 365, step=1, value=90, label="Max Days to Expiry")

        result_table = gr.Dataframe(interactive=True, wrap=True)

        # Bind query to inputs
        inputs = [threshold, option_type, ticker_dropdown, expiry_slider]
        threshold.change(query_mispricings, inputs, result_table)
        option_type.change(query_mispricings, inputs, result_table)
        ticker_dropdown.change(query_mispricings, inputs, result_table)
        expiry_slider.change(query_mispricings, inputs, result_table)

        # Initial table load
        demo.load(query_mispricings, inputs, result_table)

    return demo

if __name__ == "__main__":
    app = build_interface()
    app.launch()

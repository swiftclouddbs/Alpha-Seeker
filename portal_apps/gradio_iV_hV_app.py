import sqlite3
import gradio as gr
import pandas as pd

# --- Database Query Function ---
def search_volatility(ticker, min_ratio, max_ratio):
    conn = sqlite3.connect('../data/greeks_data.db')
    
    query = f"""
        SELECT g.ticker, g.fetch_date, g.expiry, g.call_put, g.implied_volatility,
               h.hv_20, ROUND(g.implied_volatility / h.hv_20, 2) AS iv_hv_ratio,
               g.strike, g.underlying_price, g.delta, g.gamma, g.vega, g.theta, g.rho
        FROM greeks g
        LEFT JOIN historical_volatility h
        ON g.ticker = h.ticker AND g.fetch_date = h.fetch_date
        WHERE g.fetch_date = (SELECT MAX(fetch_date) FROM greeks)
          AND g.ticker = ?
          AND h.hv_20 IS NOT NULL
          AND (g.implied_volatility / h.hv_20) BETWEEN ? AND ?
        ORDER BY iv_hv_ratio DESC
        LIMIT 50;
    """
    
    df = pd.read_sql_query(query, conn, params=(ticker, min_ratio, max_ratio))
    conn.close()
    return df

# --- Gradio UI ---
def main():
    with gr.Blocks() as demo:
        gr.Markdown("# 📊 IV vs HV Explorer")

        with gr.Row():
            ticker_dropdown = gr.Dropdown(
                choices=["AAPL", "NVDA", "TSLA", "SPY", "TLT"],  # adjust as needed
                label="Select Ticker",
                value="AAPL"
            )

        iv_hv_min = gr.Slider(
            minimum=0.0, maximum=3.0, step=0.05, value=0.8,
            label="IV / HV Ratio Min"
        )
        iv_hv_max = gr.Slider(
            minimum=0.0, maximum=3.0, step=0.05, value=1.2,
            label="IV / HV Ratio Max"
        )

        output_table = gr.Dataframe(
            interactive=False,
            wrap=True,
            label="Filtered Options"
        )

        run_button = gr.Button("Search")

        run_button.click(
            fn=search_volatility,
            inputs=[ticker_dropdown, iv_hv_min, iv_hv_max],
            outputs=output_table
        )

    demo.launch()

if __name__ == "__main__":
    main()

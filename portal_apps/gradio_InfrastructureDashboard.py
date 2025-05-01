import gradio as gr
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

DB_PATH = "../data/greeks_data.db"

# Get table summary info: row count and last update date
def get_table_info():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        tables = ["options_data", "greeks", "risk_free_rates", "historical_volatility"]
        info = []

        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]

            try:
                cursor.execute(f"SELECT MAX(fetch_date) FROM {table}")
                last_date = cursor.fetchone()[0] or "N/A"
            except:
                last_date = "N/A"

            info.append({"table": table, "rows": count, "last_update": last_date})

        df = pd.DataFrame(info)
        return df

# Generate a bar chart showing row counts per table
def generate_bar_plot():
    df = get_table_info()
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(df["table"], df["rows"], color="skyblue")
    ax.set_ylabel("Row Count")
    ax.set_title("Number of Entries per Table")
    ax.set_xticks(range(len(df["table"])))
    ax.set_xticklabels(df["table"], rotation=45, ha="right")
    plt.tight_layout()
    return fig

# Summary: count records per ticker
def get_ticker_summary():
    with sqlite3.connect(DB_PATH) as conn:
        query = """
            SELECT ticker, COUNT(*) as num_records
            FROM options_data
            GROUP BY ticker
            ORDER BY num_records DESC;
        """
        df = pd.read_sql_query(query, conn)
    return df

# Generate a pie chart showing top 10 tickers by record share
def generate_pie_chart():
    df = get_ticker_summary()
    top10 = df.head(10)
    fig, ax = plt.subplots(figsize=(6,6))
    ax.pie(top10["num_records"], labels=top10["ticker"], autopct='%1.1f%%', startangle=140)
    ax.set_title("Top 10 Tickers by Record Share")
    plt.tight_layout()
    return fig

# Main dashboard function
def dashboard():
    table_info = get_table_info()
    fig = generate_bar_plot()
    ticker_summary = get_ticker_summary()
    pie_chart = generate_pie_chart()
    return table_info, fig, ticker_summary, pie_chart

# Gradio App
with gr.Blocks(title="Dashboard") as demo:
    gr.Markdown("## Data Infrastructure Overview")
    gr.Markdown("This dashboard shows a summary of your SQLite-backed options data infrastructure.")

    with gr.Row():
        table_output = gr.Dataframe(label="Table Stats")
        plot_output = gr.Plot(label="Table Entry Counts")

    with gr.Row():
        ticker_output = gr.Dataframe(label="Record Count by Ticker")
        pie_output = gr.Plot(label="Top 10 Ticker Distribution")

    refresh_button = gr.Button("Refresh")
    refresh_button.click(fn=dashboard, outputs=[
        table_output, plot_output, ticker_output, pie_output
    ])

    demo.load(fn=dashboard, outputs=[
        table_output, plot_output, ticker_output, pie_output
    ])

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7885)

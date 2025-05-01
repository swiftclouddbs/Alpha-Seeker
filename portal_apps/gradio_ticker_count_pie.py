import gradio as gr
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

DB_PATH = "../data/greeks_data.db"

def get_ticker_contract_counts():
    with sqlite3.connect(DB_PATH) as conn:
        query = """
            SELECT ticker, COUNT(*) as num_records
            FROM options_data
            GROUP BY ticker
            ORDER BY num_records DESC;
        """
        df = pd.read_sql_query(query, conn)
    return df

def generate_contracts_pie_chart():
    df = get_ticker_contract_counts()

    if df.empty:
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, "No Data Available", fontsize=14, ha='center')
        ax.axis("off")
        return fig, "Total contracts: 0"

    total_contracts = df["num_records"].sum()

    # Group all but top 15 into "Other"
    top_n = 15
    if len(df) > top_n:
        top_df = df.head(top_n).copy()
        other_sum = df["num_records"].iloc[top_n:].sum()
        # Append "Other" at the end
        other_row = pd.DataFrame({"ticker": ["Other"], "num_records": [other_sum]})
        top_df = pd.concat([top_df, other_row], ignore_index=True)
    else:
        top_df = df

    # Prepare labels
    labels = [f"{ticker}: {count}" for ticker, count in zip(top_df["ticker"], top_df["num_records"])]

    # Assign colors: SPY = bright red, Other = dark blue, others use colormap
    base_colors = plt.get_cmap('tab20').colors
    colors = []

    for ticker in top_df["ticker"]:
        if ticker == "SPY":
            colors.append("#e41a1c")  # Bright red for SPY
        elif ticker == "Other":
            colors.append("#08306b")  # Dark blue for Other
        else:
            colors.append(base_colors[len(colors) % len(base_colors)])

    fig, ax = plt.subplots(figsize=(7,7))
    ax.pie(
        top_df["num_records"],
        labels=labels,
        colors=colors,
        startangle=140
    )
    ax.set_title("Top 15 Tickers by Options Contract Count")
    plt.tight_layout()

    summary_text = f"Total contracts: {total_contracts:,}"

    return fig, summary_text

# Gradio app
with gr.Blocks(title="Options Contracts by Ticker") as demo:
    gr.Markdown("## Options Contracts Breakdown by Ticker")
    total_text = gr.Markdown("Loading total contract count...")

    pie_output = gr.Plot(label="Contract Counts by Ticker")
    refresh_button = gr.Button("Refresh Chart")

    refresh_button.click(
        fn=generate_contracts_pie_chart,
        outputs=[pie_output, total_text]
    )

    demo.load(
        fn=generate_contracts_pie_chart,
        outputs=[pie_output, total_text]
    )

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7887)

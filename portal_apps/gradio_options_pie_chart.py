import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import gradio as gr

def get_affordable_options(max_margin):
    conn = sqlite3.connect('greeks_data.db')

    query = f'''
    SELECT 
        ticker,
        option_type,
        strike,
        bid,
        ask,
        last_price AS underlying_price,
        ROUND(((bid + ask) / 2) * 100, 2) AS estimated_premium,
        ROUND(last_price * 100 * 0.20, 2) AS estimated_margin,
        ROUND( (( (bid + ask) / 2 ) * 100 ) / (last_price * 100 * 0.20) * 100, 2 ) AS return_percent
    FROM 
        options_data
    WHERE 
        last_price * 100 * 0.20 <= {max_margin}
        AND (( (bid + ask) / 2 ) * 100 ) / (last_price * 100 * 0.20) * 100 >= 20
    ORDER BY 
        return_percent DESC;
    '''

    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        return "No contracts meet the criteria.", None

    total_contracts = len(df)

    # Create the pie chart with fixed color mapping
    pie_data = df['ticker'].value_counts()
    tickers = sorted(pie_data.index)  # Sort alphabetically for stable color assignment

    # Use a matplotlib colormap and assign fixed colors
    cmap = plt.get_cmap('tab20')
    colors = {ticker: cmap(i % 20) for i, ticker in enumerate(tickers)}  # Map each ticker to a color

    plt.close('all')  # Reset any old figures
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.pie(
        pie_data,
        labels=pie_data.index,
        colors=[colors[t] for t in pie_data.index],  # Apply consistent colors
        autopct=lambda p: f'{int(p * sum(pie_data) / 100)}',
        startangle=140
    )
    ax.set_title(f'Affordable Contracts by Ticker\n(Margin ≤ ${max_margin:,}, Return ≥ 20%)')

    summary = f"Total contracts found: {total_contracts}"
    return summary, fig

# Gradio app
with gr.Blocks(title="Affordable Options Screening Tool") as demo:
    gr.Markdown("Enter your maximum available margin to see which tickers have affordable option contracts (Return ≥ 20%).")

    with gr.Row():
        max_margin_input = gr.Number(label="Maximum Margin Allowed ($)", value=30000)

    summary_text = gr.Textbox(label="Summary")
    output_plot = gr.Plot(label="Ticker Distribution: Contract Counts")

    run_button = gr.Button("Find Affordable Options")

    run_button.click(
        fn=get_affordable_options,
        inputs=[max_margin_input],
        outputs=[summary_text, output_plot]
    )

demo.launch(server_name="0.0.0.0", server_port=7889)

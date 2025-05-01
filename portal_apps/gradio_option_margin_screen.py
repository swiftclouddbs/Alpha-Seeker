import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import gradio as gr

def affordable_options(max_margin):
    # Connect to your local SQLite database
    conn = sqlite3.connect('greeks_data.db')

    # SQL query with dynamic margin filter
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
        return_percent DESC
    LIMIT 100;
    '''

    # Load query results into a DataFrame
    df = pd.read_sql_query(query, conn)
    conn.close()

    # If no qualifying contracts, return message
    if df.empty:
        return "No contracts found for the given margin and return criteria.", None

    # Pie chart: distribution by ticker
    pie_data = df['ticker'].value_counts()
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.pie(pie_data, labels=pie_data.index, autopct='%1.1f%%', startangle=140)
    ax.set_title(f'Affordable Contracts by Ticker\n(Margin ≤ ${max_margin:,}, Return ≥ 20%)')

    return df, fig

# Gradio Interface
demo = gr.Interface(
    fn=affordable_options,
    inputs=gr.Number(label="Maximum Margin Allowed ($)", value=30000),
    outputs=[
        gr.Dataframe(label="Qualifying Option Contracts"),
        gr.Plot(label="Ticker Distribution Pie Chart")
    ],
    title="Affordable Options Screening Tool",
    description="Enter your maximum available margin and see which option contracts fit your affordability and target return criteria."
)

demo.launch(server_name="0.0.0.0", server_port=7889)

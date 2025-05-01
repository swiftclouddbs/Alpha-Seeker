import sqlite3
import pandas as pd
import gradio as gr

def find_favorable_trades(limit=100, db_path='greeks_data.db'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT 
            g.ticker, g.fetch_date, g.expiry, g.call_put, g.delta, g.gamma, 
            g.vega, g.theta, g.rho, g.underlying_price, g.implied_volatility, 
            hv.hv_20d
        FROM greeks g
        LEFT JOIN historical_volatility hv
        ON g.ticker = hv.symbol 
        AND hv.date LIKE g.fetch_date || '%'
        WHERE hv.hv_20d IS NOT NULL
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        return pd.DataFrame(columns=["ticker", "fetch_date", "expiry", "call_put", 
                                     "underlying_price", "implied_volatility", "hv_20d", "IV_HV_Diff"])

    # Calculate IV - HV difference
    df['IV_HV_Diff'] = df['implied_volatility'] - df['hv_20d']

    # Filter for trades where IV > HV (relaxed condition for now)
    candidates = df[df['IV_HV_Diff'] > 0]

    # Sort by largest IV-HV difference
    candidates = candidates.sort_values(by='IV_HV_Diff', ascending=False)

    # Limit the number of results
    top_trades = candidates.head(limit)

    if top_trades.empty:
        return pd.DataFrame(columns=["ticker", "fetch_date", "expiry", "call_put", 
                                     "underlying_price", "implied_volatility", "hv_20d", "IV_HV_Diff"])
    else:
        return top_trades[['ticker', 'fetch_date', 'expiry', 'call_put', 
                           'underlying_price', 'implied_volatility', 'hv_20d', 'IV_HV_Diff']]

with gr.Blocks(title="Trade Suggestions") as demo:
    gr.Markdown("# Favorable Trade Scanner")
    gr.Markdown("Favorable Trades by comparing Implied Volatility and Historical Volatility.")

    with gr.Row():
        limit_slider = gr.Slider(
            minimum=10,
            maximum=500,
            step=10,
            value=100,
            label="Number of Trades"
        )

    output_dataframe = gr.DataFrame(label="Favorable Trades")

    greet_btn = gr.Button("Find Favorable Trades")

    greet_btn.click(
        fn=find_favorable_trades,
        inputs=limit_slider,
        outputs=output_dataframe
    )

demo.launch(server_name="0.0.0.0", server_port=7886)

### Gradio interface function
##def gradio_interface(limit=100):
##    return find_favorable_trades(limit=limit)
##
### Define Gradio components
##slider = gr.Slider(minimum=10, maximum=500, step=10, value=100, label="Number of Trades")
##outputs = gr.DataFrame(label="Favorable Trades")
##
### Launch Gradio interface
##gr.Interface(fn=gradio_interface, inputs=slider, outputs=outputs, 
##             title="Favorable Trade Scanner", 
##             description="Favorable Trades by comparing Implied Volatility and Historical Volatility.").launch(server_name="0.0.0.0", server_port=7886)

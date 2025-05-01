import gradio as gr
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

DB_PATH = "greeks_data.db"

# Configuration filters
MIN_OPTION_PRICE = 0.10  # Avoid illiquid or zero-premium contracts
MAX_OPTION_PRICE = 1000  # Avoid junk data
MIN_CREDIT = 0.10        # Minimum spread credit worth considering
MAX_STRIKE_DISTANCE = 20  # No spreads wider than $20
DTE_MIN = 7
DTE_MAX = 45

# Global variable for storing last scan results
last_result_df = pd.DataFrame()

def find_bull_put_spreads(min_credit_filter=MIN_CREDIT):
    with sqlite3.connect(DB_PATH) as conn:
        query = f"""
            SELECT ticker, expiration_date, strike, option_type, bid, ask, option_price, implied_volatility, fetch_date
            FROM options_data
            WHERE option_type = 'put'
            AND julianday(expiration_date) - julianday(fetch_date) BETWEEN {DTE_MIN} AND {DTE_MAX}
            AND option_price BETWEEN {MIN_OPTION_PRICE} AND {MAX_OPTION_PRICE}
            ORDER BY ticker, expiration_date, strike ASC
        """

        df = pd.read_sql_query(query, conn)

    candidates = []

    for (ticker, exp), group in df.groupby(['ticker', 'expiration_date']):
        group_sorted = group.sort_values('strike')
        for i, short_leg in group_sorted.iterrows():
            for j, long_leg in group_sorted.iterrows():
                if long_leg['strike'] > short_leg['strike'] and (long_leg['strike'] - short_leg['strike']) <= MAX_STRIKE_DISTANCE:
                    credit = short_leg['bid'] - long_leg['ask']
                    if credit >= min_credit_filter:
                        candidates.append({
                            'Ticker': ticker,
                            'Expiration': exp,
                            'Short Strike': short_leg['strike'],
                            'Long Strike': long_leg['strike'],
                            'Credit': round(credit, 2),
                            'Width': round(long_leg['strike'] - short_leg['strike'], 2),
                            'Max Return %': round((credit / (long_leg['strike'] - short_leg['strike'])) * 100, 2)
                        })

    result_df = pd.DataFrame(candidates)
    result_df = result_df.sort_values(['Max Return %'], ascending=False)
    global last_result_df
    last_result_df = result_df
    total = len(result_df)
    return total, result_df

def run_bull_put_scanner(min_credit):
    total, df = find_bull_put_spreads(min_credit_filter=min_credit)
    return total, df

def plot_return_histogram():
    global last_result_df
    if last_result_df.empty:
        return None
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.hist(last_result_df['Max Return %'], bins=30, color='skyblue', edgecolor='black')
    ax.set_title('Distribution of Max Return % for Bull Put Spreads')
    ax.set_xlabel('Max Return %')
    ax.set_ylabel('Frequency')
    plt.tight_layout()
    return fig

with gr.Blocks(title="Bull Put Spread") as app:
    gr.Markdown("## Bull Put Spread Finder")
    gr.Markdown("This tool identifies credit spreads from your options database within 7-45 days to expiration.")

    min_credit_slider = gr.Slider(0.1, 5.0, value=MIN_CREDIT, step=0.05, label="Minimum Credit Filter ($)")

    total_output = gr.Number(label="Total Candidates Found")
    spread_output = gr.Dataframe(label="Bull Put Spread Candidates")
    hist_output = gr.Plot(label="Reward Distribution Histogram")

    find_button = gr.Button("Find Bull Put Spreads")
    status_text = gr.Markdown("Waiting for input...")

    spread_event = find_button.click(
        fn=run_bull_put_scanner, 
        inputs=[min_credit_slider], 
        outputs=[total_output, spread_output]
    )
    spread_event.then(
        fn=plot_return_histogram, 
        outputs=[hist_output]
    )
    spread_event.then(
        fn=lambda: "Scan complete. Histogram updated!", 
        outputs=[status_text]
    )

if __name__ == "__main__":
    app.launch(server_name="0.0.0.0", server_port=7887)

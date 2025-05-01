import sqlite3
import pandas as pd
import gradio as gr

DB_PATH = "../data/greeks_data.db"

# Only keep key columns
DISPLAY_COLUMNS = [
    "ticker", "spread_type", "expiration_date", 
    "short_strike", "long_strike", 
    "short_premium", "long_premium", 
    "net_credit", "max_loss", "risk_reward_ratio"
]

def fetch_spreads():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT rowid, * FROM credit_spread_candidates
        WHERE risk_reward_ratio IS NOT NULL
          AND net_credit > 0
          AND max_loss > 0
          AND risk_reward_ratio > 0.5
        ORDER BY risk_reward_ratio DESC
        LIMIT 100
    """, conn)
    conn.close()
    return df

def get_display_df():
    df = fetch_spreads()
    return df[["rowid"] + DISPLAY_COLUMNS]

def save_selected(rows_str):
    try:
        rows = [int(i.strip()) for i in rows_str.split(",") if i.strip().isdigit()]
        all_data = fetch_spreads()
        selected = all_data.iloc[rows]
        selected.drop(columns=["rowid"], inplace=True)

        conn = sqlite3.connect(DB_PATH)
        selected.to_sql("spreads_to_execute", conn, if_exists="append", index=False)
        conn.close()

        return f"✅ Saved {len(selected)} spreads to spreads_to_execute."
    except Exception as e:
        return f"❌ Error: {str(e)}"

def app():
    with gr.Blocks() as demo:
        df = get_display_df()
        data_table = gr.Dataframe(value=df, headers=df.columns.tolist(), label="Candidate Spreads", 
                                  interactive=False, wrap=True)

        selected_rows = gr.Textbox(label="Selected Row Indexes (comma-separated)", placeholder="e.g. 0, 2, 5")

        with gr.Row():
            refresh_btn = gr.Button("🔄 Reload Spreads")
            save_btn = gr.Button("✅ Save Selected Spreads")
        output = gr.Textbox(label="Output")

        refresh_btn.click(lambda: get_display_df(), outputs=data_table)
        save_btn.click(save_selected, inputs=selected_rows, outputs=output)

    return demo

if __name__ == "__main__":
    app().launch()

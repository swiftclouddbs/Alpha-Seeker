# gradio_candidate_viewer.py

import gradio as gr
import sqlite3
import pandas as pd
from utils.logger import log_event
import json

log_event("candidate_viewer", "START", "Candidate viewer started.")

def view_filtered_candidates(db_path: str, min_days_to_expiry: int, min_iv: float, min_volume: int, min_open_interest: int):
    conn = sqlite3.connect(db_path)

    query = """
        SELECT
            option_id as ID,
            ticker,
            expiration_date as Expiry,
            strike,
            option_type as type,
            days_to_expiry as DTE,
            ROUND(implied_volatility, 4) as IV,
            ROUND(last_price, 2) AS last,
            bid,
            ask,
            volume,
            open_interest as OI,
            ROUND(bs_diff, 4) as BS_DIFF,
            data_date
        FROM interesting_candidate_trades
        WHERE days_to_expiry >= ?
          AND implied_volatility >= ?
          AND volume > ?
          AND open_interest > ?
        ORDER BY ticker, expiration_date;
    """
#ROUND(last_price, 2) AS last
    df = pd.read_sql_query(query, conn, params=(min_days_to_expiry, min_iv, min_volume, min_open_interest))
    conn.close()

    summary = (
        f"📊 Showing **{len(df)}** matching trades\n\n"
        f"🔎 Filters:\n"
        f"- Days to Expiry ≥ {min_days_to_expiry}\n"
        f"- IV ≥ {min_iv*100:.1f}%\n"
        f"- Volume > {min_volume}\n"
        f"- Open Interest > {min_open_interest}"
    )
    return summary, df

from langchain_ollama import OllamaLLM  # Llama3 wrapper
from langchain_core.prompts import ChatPromptTemplate
import json

model = OllamaLLM(model="llama3")

def build_llm_prompt_from_df(df: pd.DataFrame, data_date: str):
    contracts = []
    for _, row in df.iterrows():
        contracts.append(f"""
    Contract Name: {row['ticker']} {row['type']} Strike {row['strike']} Exp {row['Expiry']}
    IV: {row['IV']}, BS Diff: {row['BS_DIFF']}, Last Price: {row['last']}
    DTE: {row['DTE']}, Volume: {row['volume']}, OI: {row['OI']}
    """)
        joined = "\n---\n".join(contracts)
    return f"""
    📅 Report Date: {data_date}

    You are an options strategist assistant.

    Given the following option contracts, rank them in order of attractiveness for a trade. For each contract, briefly explain your reasoning and indicate whether you would recommend a **Buy** or **Sell**.

    Consider factors like Implied Volatility (IV), BS_DIFF, time to expiry, and Greeks where available.

    Respond in this format:

    1. Contract Name: ...
       Recommendation: Buy or Sell
       Reason: ...

    2. Contract Name: ...
       Recommendation: Buy or Sell
       Reason: ...

    Contracts:
    {joined}
    """

def rank_with_llm(df: pd.DataFrame):
    if df.empty:
        return "No trades to analyze."

    data_date = df["data_date"].iloc[0] if "data_date" in df.columns else "Unknown"
    prompt = build_llm_prompt_from_df(df, data_date)
    response = model(prompt)

    # Post-process to format for Markdown display
    try:
        lines = response.strip().splitlines()
        output = f"📅 **Report Date:** {data_date}\n\n"
        for line in lines:
            if line.strip().startswith(tuple(str(i) for i in range(1, 10))):
                # Format ranking numbers bold
                output += f"**{line.strip()}**\n"
            elif "Recommendation:" in line:
                # Emphasize recommendation
                output += f"🔹 *{line.strip()}*\n"
            else:
                output += f"{line.strip()}\n"
        return output
    except Exception as e:
        return f"⚠️ Error formatting response:\n{response}\n\nException: {e}"


## Old prompt that works
##    return f"""
##Report Date: {data_date}
##
##You are an options strategist assistant.
##
##Given the following contracts, rank them by attractiveness for a trade and briefly explain your ranking. Consider IV, BS_DIFF, and other metrics.
##
##Respond in this format:
##[
##  {{
##    "contract_name": "...",
##    "reason": "..."
##  }},
##  ...
##]
##
##Contracts:
##{joined}
##"""

## Old but functional
##def rank_with_llm(df: pd.DataFrame):
##    if df.empty:
##        return "No trades to analyze."
##
##    data_date = df["data_date"].iloc[0] if "data_date" in df.columns else "Unknown"
##    prompt = build_llm_prompt_from_df(df, data_date)
##    response = model(prompt)
##
##    output = f"📅 **Report Date:** {data_date}\n\n"
##    output += response.strip()
##    return output


# Gradio App
with gr.Blocks(title="Candidate Trade Viewer") as app:
    gr.Markdown("### 📈 View Filtered Options Trades (Read-Only)")

    with gr.Row():
        min_days = gr.Slider(minimum=0, maximum=100, step=1, label="Min Days to Expiry", value=20)
        min_iv = gr.Slider(minimum=0.0, maximum=1.0, step=0.01, label="Min Implied Volatility", value=0.05)
        min_volume = gr.Slider(minimum=0, maximum=5000, step=100, label="Min Volume", value=1000)
        min_oi = gr.Slider(minimum=0, maximum=5000, step=100, label="Min Open Interest", value=1000)

    run_button = gr.Button("View Filtered Trades")

    with gr.Column():
        summary_box = gr.Textbox(label="Summary", lines=4)
        ticker_df = gr.Dataframe(label="Matching Trades", wrap=True)

    run_button.click(
        fn=lambda days, iv, vol, oi: view_filtered_candidates("../data/greeks_data.db", days, iv, vol, oi),
        inputs=[min_days, min_iv, min_volume, min_oi],
        outputs=[summary_box, ticker_df]
    )

    rank_button = gr.Button("🤖 Rank with AI Assistant")
    llm_output = gr.Textbox(label="LLM Ranking Output", lines=20, interactive=False)

    def rank_displayed_trades(df):
        return rank_with_llm(df)

    rank_button.click(fn=rank_displayed_trades, inputs=[ticker_df], outputs=[llm_output])


app.launch()

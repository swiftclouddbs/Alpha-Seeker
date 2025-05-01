import gradio as gr
import sqlite3
import pandas as pd
from datetime import datetime

# Dummy LLM placeholder
def mock_llm_response(prompt):
    return """📈 Trade Recommendations for AAPL\n\n1. Strike 180 Call, DTE 5 – Good IV and favorable BS_DIFF. Strong open interest suggests liquidity.\n2. Strike 175 Put, DTE 3 – Bearish play with high IV, but less volume."""

# Simplified SQL builder from text
def build_sql_from_question(question: str):
    if "AAPL" in question.upper() and "THIS WEEK" in question.lower():
        return """
        SELECT option_id, ticker, expiration_date, strike, option_type,
               days_to_expiry, implied_volatility, volume, open_interest,
               bs_diff, data_date
        FROM interesting_candidate_trades
        WHERE ticker = 'AAPL'
          AND days_to_expiry BETWEEN 0 AND 7
        ORDER BY bs_diff DESC
        LIMIT 10;
        """
    return None

# Query and generate output
def answer_question(question):
    sql = build_sql_from_question(question)
    if not sql:
        return "Sorry, I couldn't understand your question. Try asking about a specific ticker or timeframe."

    try:
        conn = sqlite3.connect("options.db")
        df = pd.read_sql_query(sql, conn)
        conn.close()

        if df.empty:
            return "No trades matched your query."

        # Format prompt for LLM
        rows = []
        for _, row in df.iterrows():
            rows.append(f"{row['option_type'].capitalize()}, Strike {row['strike']}, DTE {row['days_to_expiry']}, IV {row['implied_volatility']}, BS_DIFF {row['bs_diff']:.2f}, OI {row['open_interest']}, Vol {row['volume']}")
        prompt = """You are an options strategist assistant.\n\nHere are some option trades. Rank them by attractiveness and explain your reasoning.\n\nTrades:\n""" + "\n".join(rows)

        return mock_llm_response(prompt)

    except Exception as e:
        return f"Error: {e}"

# Gradio UI
demo = gr.Interface(
    fn=answer_question,
    inputs=gr.Textbox(label="Ask your trade question", placeholder="e.g. What are some good AAPL trades this week?"),
    outputs=gr.Markdown(label="Strategy Output"),
    title="Trade Strategy Assistant",
    description="Ask questions about trade opportunities. We'll analyze candidate trades and give you a ranked summary."
)

demo.launch()

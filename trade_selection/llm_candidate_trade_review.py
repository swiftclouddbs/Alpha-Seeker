import sqlite3
import json
import time
from langchain_ollama import OllamaLLM  # Replace with your Llama3 call.
from langchain_core.prompts import ChatPromptTemplate

model = OllamaLLM(model="llama3")

# chain = prompt | model
db_path = "../data/greeks_data.db"

def fetch_candidate_trades(db_path, limit=10):
    conn = sqlite3.connect(db_path)
    query = """
    SELECT 
        option_id, ticker, strike, expiration_date, option_type, 
        delta, gamma, theta, vega, last_price, underlying_price, 
        days_to_expiry, implied_volatility, data_date
    FROM candidate_trades
    ORDER BY data_date DESC
    LIMIT ?
    """
    rows = conn.execute(query, (limit,)).fetchall()
    conn.close()
    return rows

def build_prompt(trades, data_date):
    contracts = []
    for row in trades:
        contracts.append(f"""
Contract Name: {row[1]} {row[4]} Strike {row[2]} Exp {row[3]}
Delta: {row[5]:.2f}, Gamma: {row[6]:.2f}, Theta: {row[7]:.2f}, Vega: {row[8]:.2f}
Underlying Price: {row[10]:.2f}, Current Option Price: {row[9]:.2f}
Days to Expiry: {row[11]}
Implied Volatility: {row[12]:.2f}
""")

    joined = "\n---\n".join(contracts)
    return f"""
Report Date: {data_date}

You are an options strategist assistant.

Given the following contracts, select trades you would consider interesting and briefly explain why.

Respond in this format:
[
  {{
    "contract_name": "...",
    "reason": "..."
  }},
  ...
]

Contracts:
{joined}
"""

def write_to_file(output, filename="trade_ideas.txt"):
    with open(filename, "a") as f:
        f.write(output + "\n\n")

def process_trades_in_batches(db_path):
    batch_size = 10
    while True:
        trades = fetch_candidate_trades(db_path, limit=batch_size)
        if not trades:
            break  # No more trades to process

        # Get the data_date from the most recent trade in the batch
        data_date = trades[0][13]  # The data_date is the last column in the result

        prompt = build_prompt(trades, data_date)
        response = model(prompt)  # Replace with your Llama3 infer function.

        try:
            trade_ideas = json.loads(response)
            print(f"💡 Trade Ideas Suggested by LLM for {data_date}:")
            output = f"Report Date: {data_date}\n\n"
            for idea in trade_ideas:
                idea_output = f"- {idea['contract_name']}: {idea['reason']}"
                print(idea_output)
                output += idea_output + "\n"

            # Write the trade ideas to a text file
            write_to_file(output)

        except json.JSONDecodeError:
            print("⚠️ Invalid JSON from LLM:\n", response)

        # Pause for 10 seconds before fetching the next batch
        time.sleep(10)

def main():
    process_trades_in_batches(db_path)

if __name__ == "__main__":
    main()

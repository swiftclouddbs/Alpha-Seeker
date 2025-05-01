import sqlite3
import json
import time
from langchain_ollama import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate

# Initialize LLM
model = OllamaLLM(model="llama3")
db_path = "../data/greeks_data.db"

# Output file
output_file = "strategy_suggestions_output.txt"

def fetch_strategy_suggestions(db_path, offset=0, limit=10):
    conn = sqlite3.connect(db_path)
    query = """
    SELECT 
        option_id, ticker, expiration_date, strike, option_type, 
        delta, implied_volatility, last_price, dte, suggested_strategy, expected_return, decision_date
    FROM strategy_suggestions
    ORDER BY expected_return DESC
    LIMIT ? OFFSET ?
    """
    rows = conn.execute(query, (limit, offset)).fetchall()
    conn.close()
    return rows

def build_prompt(trades):
    contracts = []
    for row in trades:
        contracts.append(f"""
Contract Name: {row[1]} {row[4]} Strike {row[3]} Exp {row[2]}
Delta: {row[5]:.2f}, Implied Volatility: {row[6]:.2f}, Last Price: {row[7]:.2f}, DTE: {row[8]}
Suggested Strategy: {row[9]}
Expected Return: {row[10]:.2f}
Decision Date: {row[11]}
""")
    joined = "\n---\n".join(contracts)
    return f"""
You are an options strategist assistant.

Given the following contracts, summarize what strategies might be worth considering. Identify unusual setups or edge cases.

Respond in this format:
[
  {{
    "contract_name": "...",
    "strategy_note": "..."
  }},
  ...
]

Contracts:
{joined}
"""

def write_to_file(batch_number, suggestions):
    with open(output_file, 'a') as file:
        file.write(f"Batch {batch_number} - Strategy Insights:\n")
        for idea in suggestions:
            file.write(f"- {idea['contract_name']}: {idea['strategy_note']}\n")
        file.write("\n---\n")

def main():
    offset = 0
    batch_size = 10
    batch_number = 1

    while True:
        trades = fetch_strategy_suggestions(db_path, offset, batch_size)

        if not trades:
            print("No more strategy suggestions found.")
            break

        prompt = build_prompt(trades)
        response = model(prompt)

        try:
            suggestions = json.loads(response)
            print(f"📈 Strategy Insights from LLM (Batch {batch_number}):")
            for idea in suggestions:
                print(f"- {idea['contract_name']}: {idea['strategy_note']}")

            # Write the results to a text file
            write_to_file(batch_number, suggestions)

            # Move to the next batch
            offset += batch_size
            batch_number += 1

            # Pause for 10 seconds between batches
            print(f"⏳ Pausing for 10 seconds before next batch...")
            time.sleep(10)

        except json.JSONDecodeError:
            print("⚠️ Invalid JSON from LLM:\n", response)

if __name__ == "__main__":
    main()

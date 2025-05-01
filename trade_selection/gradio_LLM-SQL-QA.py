import sqlite3
import pandas as pd
from langchain_ollama import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate

# Initialize model
model = OllamaLLM(model="llama3")

# Prompt template: system + user
prompt_template = ChatPromptTemplate.from_messages([
    ("system", 
     "You are a SQL expert. Your job is to convert user questions into SQLite queries. "
     "Only return the SQL query. Do not explain anything. Use the table 'interesting_candidate_trades'.\n"
     "Available columns: option_id, ticker, expiration_date, strike, option_type, days_to_expiry, "
     "implied_volatility, volume, open_interest, bs_diff, data_date.\n\n"
     "Example questions and SQL:\n"
     "Q: How many AAPL contracts are in the table?\n"
     "A: SELECT COUNT(*) FROM interesting_candidate_trades WHERE ticker = 'AAPL';\n\n"
     "Q: What are the contracts for TSLA expiring in the next 3 days?\n"
     "A: SELECT * FROM interesting_candidate_trades WHERE ticker = 'TSLA' AND days_to_expiry <= 3;"
    ),
    ("user", "{question}")
])

def generate_sql_from_question(question: str) -> str:
    """Use LLaMA3 to turn a natural language question into a SQL query."""
    prompt = prompt_template.format_messages(question=question)
    response = model.invoke(prompt)
    sql_candidate = response.strip()

    if not sql_candidate.lower().startswith("select"):
        raise ValueError(f"Model returned non-SQL response:\n{sql_candidate}")
    return sql_candidate

def run_query(sql: str) -> pd.DataFrame:
    """Run a SQL query against the local options.db SQLite database."""
    conn = sqlite3.connect("../data/greeks_data.db")
    df = pd.read_sql_query(sql, conn)
    conn.close()
    return df

def main():
    question = "How many AAPL contracts are in the table?"
    print(f"\n🔍 Question: {question}")

    try:
        sql = generate_sql_from_question(question)
        print(f"\n🧠 Generated SQL:\n{sql}")

        result = run_query(sql)

        if result.empty:
            print("\n📭 No results found.")
        else:
            print("\n📊 Top 5 rows:")
            print(result.head(5).to_markdown(index=False))

    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()

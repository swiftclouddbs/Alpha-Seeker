# AlphaSeeker

A modular research platform for options analysis and trade simulation.

## Project Structure


---

## Typical Workflows

- `daily_data_pull/`  
  Run these scripts after market close to update the local database.

- `trade_engine/`  
  Test trade strategies, simulate profits, and scan the database for opportunities.

- `portal_apps/`  
  Visualize historical performance and screening results.

- `main.py`  
  Launches a full pipeline — from data updates to trade candidate summaries.

---

## Getting Started

1. Clone the repo
2. Install Python packages:

```bash
pip install pandas numpy yfinance gradio


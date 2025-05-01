# Get rfr 
import requests
import sqlite3
from datetime import datetime

FRED_API_KEY = "9cd645eb239269635fda3cc2a21f823d"
FRED_SERIES_ID = "DGS3MO"
START_DATE = "2025-04-29"
END_DATE = datetime.today().strftime("%Y-%m-%d")

url = "https://api.stlouisfed.org/fred/series/observations"
params = {
    "series_id": FRED_SERIES_ID,
    "api_key": FRED_API_KEY,
    "file_type": "json",
    "observation_start": START_DATE,
    "observation_end": END_DATE
}

response = requests.get(url, params=params)
observations = response.json()["observations"]

conn = sqlite3.connect("../data/greeks_data.db")
cursor = conn.cursor()

for obs in observations:
    try:
        rfr = float(obs["value"]) / 100  # Percent to decimal
        cursor.execute("INSERT OR REPLACE INTO risk_free_rates (date, rate) VALUES (?, ?)", (obs["date"], rfr))
    except ValueError:
        continue  # Skip non-numeric entries

conn.commit()
conn.close()
print(f"✅ Stored {len(observations)} daily risk-free rates from {START_DATE} to {END_DATE}")

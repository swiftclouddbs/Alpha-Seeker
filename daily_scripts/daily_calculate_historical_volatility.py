import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime

# Connect to SQLite database
conn = sqlite3.connect("../data/greeks_data.db")
cursor = conn.cursor()

# Function to calculate log returns
def calculate_log_returns(prices):
    return np.log(prices / prices.shift(1))

# Function to calculate annualized historical volatility
def calculate_annualized_hv(returns, window):
    rolling_std = returns.rolling(window=window).std()
    annualized_vol = rolling_std * np.sqrt(252)  # Annualize by multiplying by sqrt(252)
    return annualized_vol

# Function to fetch data from the historical_data table
def fetch_data_from_db(ticker):
    cursor.execute("""
        SELECT date, close FROM historical_data WHERE ticker = ? ORDER BY date
    """, (ticker,))
    rows = cursor.fetchall()
    data = pd.DataFrame(rows, columns=["date", "close"])
    data['date'] = pd.to_datetime(data['date'])
    data.set_index('date', inplace=True)
    return data

# Function to store HV in the database
def store_hv_in_db(db_path, ticker, hv_dict):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    for date, values in hv_dict.items():
        cursor.execute("""
            INSERT OR REPLACE INTO historical_volatility 
            (ticker, data_date, hv_10, hv_20, hv_30, hv_60)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (ticker, date, values['hv_10'], values['hv_20'], values['hv_30'], values['hv_60']))

    conn.commit()
    conn.close()

# Function to compute and store historical volatility for tickers
def compute_and_store_hv(tickers, db_path='../data/greeks_data.db'):
    for ticker in tickers:
        print(f"📈 Fetching data for: {ticker}")
        data = fetch_data_from_db(ticker)
        
        if len(data) < 60:  # Skip if not enough data
            print(f"⚠️ Not enough data for {ticker}. Skipping.")
            continue
        
        # Calculate log returns
        data['log_returns'] = calculate_log_returns(data['close'])
        
        # Calculate HV for different windows
        hv_10 = calculate_annualized_hv(data['log_returns'], 10)
        hv_20 = calculate_annualized_hv(data['log_returns'], 20)
        hv_30 = calculate_annualized_hv(data['log_returns'], 30)
        hv_60 = calculate_annualized_hv(data['log_returns'], 60)
        
        # Create a dictionary for storing HV data
        hv_dict = {}
        for date in hv_10.index:
            date_str = date.strftime('%Y-%m-%d')
            hv_dict[date_str] = {
                'hv_10': hv_10[date],
                'hv_20': hv_20[date],
                'hv_30': hv_30[date],
                'hv_60': hv_60[date],
            }
        
        # Store HV data in the database
        store_hv_in_db(db_path, ticker, hv_dict)
        print(f"✅ Stored HV data for {ticker}")

# Main function to run the HV computation
if __name__ == '__main__':
    tickers = ["SPY", "QQQ", "IWM", "TLT", "XLF", "GLD", "SLV", "HYG",
    "TQQQ", "SOXL", "EEM", "GDX", "KWEB", "EWZ", "TSLL", "KRE", "FXI",
    "SQQQ", "XLE", "ARKK", "NVDA", "TSLA", "AAPL", "MSTR", "PLTR", "AMZN",
    "HTZ", "IBIT", "AMD", "META", "INTC", "GOOGL", "NFLX", "GME", "UNH",
    "SOFI", "MARA", "SMCI", "BABA", "COIN"]
    compute_and_store_hv(tickers)

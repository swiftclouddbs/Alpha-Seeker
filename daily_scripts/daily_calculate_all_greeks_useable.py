#Script works but not have safeguards to ensure older data is not modified.

import sqlite3
from datetime import datetime
import numpy as np
from scipy.stats import norm

# --- Black-Scholes Greeks Calculation ---
def calculate_greeks(S, K, T, r, sigma, option_type='call'):
    if T <= 0 or sigma <= 0:
        return {'delta': 0, 'gamma': 0, 'vega': 0, 'theta': 0, 'rho': 0}

    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)

    if option_type == 'call':
        delta = norm.cdf(d1)
        theta = (-S * norm.pdf(d1) * sigma / (2 * np.sqrt(T))) - r * K * np.exp(-r * T) * norm.cdf(d2)
        rho = K * T * np.exp(-r * T) * norm.cdf(d2)
    else:
        delta = -norm.cdf(-d1)
        theta = (-S * norm.pdf(d1) * sigma / (2 * np.sqrt(T))) + r * K * np.exp(-r * T) * norm.cdf(-d2)
        rho = -K * T * np.exp(-r * T) * norm.cdf(-d2)

    gamma = norm.pdf(d1) / (S * sigma * np.sqrt(T))
    vega = S * norm.pdf(d1) * np.sqrt(T)

    return {
        'delta': delta,
        'gamma': gamma,
        'vega': vega / 100,
        'theta': theta / 365,
        'rho': rho / 100
    }

# --- Main Processing Function ---
def calculate_and_store_greeks_for_all(db_path='../data/greeks_data.db'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    inserted = 0
    skipped = 0

    query = """
        SELECT option_id, ticker, expiration_date, strike, option_type,
               last_price, option_price, implied_volatility, fetch_date
        FROM options_data
        WHERE option_price IS NOT NULL 
          AND implied_volatility IS NOT NULL 
          AND fetch_date <= expiration_date
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    for row in rows:
        (
            option_data_id, ticker, exp_date, strike, option_type,
            underlying_price, option_price, iv, fetch_date
        ) = row

        try:
            # Fetch the risk-free rate for the current fetch_date, fallback to latest available
            cursor.execute("SELECT rate FROM risk_free_rates WHERE date <= ? ORDER BY date DESC LIMIT 1", (fetch_date,))
            r_result = cursor.fetchone()

            if not r_result:
                print(f"⚠️ Skipped option_id {option_data_id}: No risk-free rate available")
                skipped += 1
                continue

            r = r_result[0]
            T = (datetime.strptime(exp_date, "%Y-%m-%d") - datetime.strptime(fetch_date, "%Y-%m-%d")).days / 365.0

            if T <= 0:
                print(f"⚠️ Skipped option_id {option_data_id}: Expired or zero T (fetch: {fetch_date}, expiry: {exp_date})")
                skipped += 1
                continue

            # Check for existing Greeks
            cursor.execute("""
                SELECT 1 FROM greeks WHERE option_id = ? AND fetch_date = ?
            """, (option_data_id, fetch_date))
            if cursor.fetchone():
                print(f"ℹ️ Already exists in greeks: option_id {option_data_id} for {fetch_date}")
                skipped += 1
                continue

            greeks = calculate_greeks(
                S=underlying_price,
                K=strike,
                T=T,
                r=r,
                sigma=iv,
                option_type=option_type.lower()
            )

            # --- Calculate theoretical Black-Scholes price ---

            d1 = (np.log(underlying_price / strike) + (r + 0.5 * iv**2) * T) / (iv * np.sqrt(T))
            d2 = d1 - iv * np.sqrt(T)

            if option_type.lower() == 'call':
                bs_price = underlying_price * norm.cdf(d1) - strike * np.exp(-r * T) * norm.cdf(d2)
            else:  # put
                bs_price = strike * np.exp(-r * T) * norm.cdf(-d2) - underlying_price * norm.cdf(-d1)

            bs_diff = bs_price - option_price if option_price is not None else None

            # --- Update options_data with bs_price and bs_diff ---
            cursor.execute("""
                UPDATE options_data
                SET bs_price = ?, bs_diff = ?
                WHERE option_id = ?
            """, (bs_price, bs_diff, option_data_id))

            # --- Insert Greeks ---
            cursor.execute("""
                INSERT INTO greeks (
                    option_id, ticker, expiry, call_put, fetch_date,
                    delta, gamma, vega, theta, rho, strike,
                    underlying_price, days_to_expiry, risk_free_rate, implied_volatility
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                option_data_id, ticker, exp_date, option_type, fetch_date,
                greeks['delta'], greeks['gamma'], greeks['vega'],
                greeks['theta'], greeks['rho'], strike,
                underlying_price, T, r, iv
            ))

            inserted += 1

        except Exception as e:
            print(f"⚠️ Skipped row {option_data_id} due to error: {e}")
            skipped += 1

    conn.commit()
    conn.close()
    print(f"\n🎯 Done. Inserted: {inserted}, Skipped: {skipped}")



# --- Entry Point ---
if __name__ == "__main__":
    calculate_and_store_greeks_for_all()

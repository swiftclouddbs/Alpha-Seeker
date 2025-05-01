import sqlite3
from datetime import datetime
import numpy as np
from scipy.stats import norm

# --- User Input: Set this to the date you want to process ---
TARGET_DATE = "2025-04-30"  # <-- change this as needed (YYYY-MM-DD)

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
def calculate_and_store_greeks_for_date(db_path='../data/greeks_data.db', target_date=TARGET_DATE):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    inserted = 0
    skipped = 0

    # --- Query Options for the Target Date ---
    query = """
        SELECT option_id, ticker, expiration_date, strike, option_type,
               last_price, option_price, implied_volatility, data_date
        FROM options_data
        WHERE option_price IS NOT NULL 
          AND implied_volatility IS NOT NULL 
          AND data_date = ?
          AND expiration_date >= ?
    """
    cursor.execute(query, (target_date, target_date))
    rows = cursor.fetchall()

    print(f"📊 Total options to process: {len(rows)} for {target_date}")

    for idx, row in enumerate(rows):
        (
            option_id, ticker, exp_date, strike, option_type,
            underlying_price, option_price, iv, data_date
        ) = row

        try:
            # --- Risk-Free Rate Lookup ---
            cursor.execute("SELECT rate FROM risk_free_rates WHERE date <= ? ORDER BY date DESC LIMIT 1", (data_date,))
            r_result = cursor.fetchone()

            if not r_result:
                print(f"[SKIP] {option_id} - No risk-free rate for {data_date}")
                skipped += 1
                continue

            r = r_result[0]

            # --- Time to Expiry Calculation ---
            T = (datetime.strptime(exp_date, "%Y-%m-%d") - datetime.strptime(data_date, "%Y-%m-%d")).days / 365.0
            if T <= 0:
                print(f"[SKIP] {option_id} - Expired or same-day option. T={T}")
                skipped += 1
                continue

            # --- Duplicate Check ---
            cursor.execute("SELECT 1 FROM greeks WHERE option_id = ?", (option_id,))
            if cursor.fetchone():
                skipped += 1
                if skipped % 1000 == 0:
                    print(f"[SKIP] Duplicate found: {option_id}")
                continue

            # --- Calculation ---
            greeks = calculate_greeks(
                S=underlying_price,
                K=strike,
                T=T,
                r=r,
                sigma=iv,
                option_type=option_type.lower()
            )

            d1 = (np.log(underlying_price / strike) + (r + 0.5 * iv**2) * T) / (iv * np.sqrt(T))
            d2 = d1 - iv * np.sqrt(T)

            bs_price = (
                underlying_price * norm.cdf(d1) - strike * np.exp(-r * T) * norm.cdf(d2)
                if option_type.lower() == 'call'
                else strike * np.exp(-r * T) * norm.cdf(-d2) - underlying_price * norm.cdf(-d1)
            )

            bs_diff = bs_price - option_price

            # --- Update and Insert ---
            cursor.execute("""
                UPDATE options_data
                SET bs_price = ?, bs_diff = ?
                WHERE option_id = ?
            """, (bs_price, bs_diff, option_id))

            cursor.execute("""
                INSERT INTO greeks (
                    option_id, ticker, expiry, call_put,
                    delta, gamma, vega, theta, rho, strike,
                    underlying_price, days_to_expiry, risk_free_rate, implied_volatility
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                option_id, ticker, exp_date, option_type,
                greeks['delta'], greeks['gamma'], greeks['vega'],
                greeks['theta'], greeks['rho'], strike,
                underlying_price, T, r, iv
            ))

            inserted += 1
            if inserted % 1000 == 0:
                print(f"✅ Inserted {inserted}...")

        except Exception as e:
            print(f"[ERROR] {option_id} - {e}")
            skipped += 1

    conn.commit()
    conn.close()
    print(f"\n🎯 Finished! Inserted: {inserted}, Skipped: {skipped}")

# --- Entry Point ---
if __name__ == "__main__":
    calculate_and_store_greeks_for_date()

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
               last_price, option_price, implied_volatility, data_date
        FROM options_data
        WHERE option_price IS NOT NULL 
          AND implied_volatility IS NOT NULL 
          AND data_date <= expiration_date
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    for idx, row in enumerate(rows, 1):
        (
            option_data_id, ticker, exp_date, strike, option_type,
            underlying_price, option_price, iv, data_date
        ) = row

        try:
            # Get risk-free rate valid for data_date
            cursor.execute(
                "SELECT rate FROM risk_free_rates WHERE date <= ? ORDER BY date DESC LIMIT 1",
                (data_date,)
            )
            r_result = cursor.fetchone()

            if not r_result:
                print(f"⚠️ Skipped option_id {option_data_id}: No risk-free rate for {data_date}")
                skipped += 1
                continue

            r = r_result[0]
            T = (datetime.strptime(exp_date, "%Y-%m-%d") - datetime.strptime(data_date, "%Y-%m-%d")).days / 365.0

            if T <= 0:
                print(f"⚠️ Skipped option_id {option_data_id}: Expired or zero T (data_date: {data_date}, expiry: {exp_date})")
                skipped += 1
                continue

            # Check for existing Greeks entry
            cursor.execute("""
                SELECT 1 FROM greeks WHERE option_id = ? AND fetch_date = ?
            """, (option_data_id, data_date))
            if cursor.fetchone():
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

            # Black-Scholes price calculation
            d1 = (np.log(underlying_price / strike) + (r + 0.5 * iv**2) * T) / (iv * np.sqrt(T))
            d2 = d1 - iv * np.sqrt(T)

            if option_type.lower() == 'call':
                bs_price = underlying_price * norm.cdf(d1) - strike * np.exp(-r * T) * norm.cdf(d2)
            else:
                bs_price = strike * np.exp(-r * T) * norm.cdf(-d2) - underlying_price * norm.cdf(-d1)

            bs_diff = bs_price - option_price if option_price is not None else None

            # Update options_data with BS info
            cursor.execute("""
                UPDATE options_data
                SET bs_price = ?, bs_diff = ?
                WHERE option_id = ?
            """, (bs_price, bs_diff, option_data_id))

            # Insert Greek values
            cursor.execute("""
                INSERT INTO greeks (
                    option_id, ticker, expiry, call_put, fetch_date,
                    delta, gamma, vega, theta, rho, strike,
                    underlying_price, days_to_expiry, risk_free_rate, implied_volatility
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                option_data_id, ticker, exp_date, option_type, data_date,
                greeks['delta'], greeks['gamma'], greeks['vega'],
                greeks['theta'], greeks['rho'], strike,
                underlying_price, T, r, iv
            ))

            inserted += 1

            if idx % 1000 == 0:
                print(f"📊 Processed {idx} rows so far... Inserted: {inserted}, Skipped: {skipped}")

        except Exception as e:
            print(f"⚠️ Error on option_id {option_data_id}: {e}")
            skipped += 1

    conn.commit()
    conn.close()
    print(f"\n✅ Finished. Total Inserted: {inserted}, Skipped: {skipped}")

# --- Entry Point ---
if __name__ == "__main__":
    calculate_and_store_greeks_for_all()

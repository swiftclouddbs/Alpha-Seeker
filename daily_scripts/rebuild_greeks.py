import sqlite3

def rebuild_greeks_table(db_path='../data/greeks_data.db'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Step 1: Backup current greeks table
        print("🔹 Backing up 'greeks' table to 'greeks_backup'...")
        cursor.execute("DROP TABLE IF EXISTS greeks_backup;")
        cursor.execute("CREATE TABLE greeks_backup AS SELECT * FROM greeks;")
        conn.commit()

        # Step 2: Drop old greeks table
        print("🔹 Dropping old 'greeks' table...")
        cursor.execute("DROP TABLE IF EXISTS greeks;")
        conn.commit()

        # Step 3: Create new greeks table without fetch_date
        print("🔹 Creating new 'greeks' table (clean schema)...")
        cursor.execute("""
            CREATE TABLE greeks (
                option_id TEXT PRIMARY KEY,
                ticker TEXT,
                expiry TEXT,
                call_put TEXT,
                delta REAL,
                gamma REAL,
                vega REAL,
                theta REAL,
                rho REAL,
                strike REAL,
                underlying_price REAL,
                days_to_expiry REAL,
                risk_free_rate REAL,
                implied_volatility REAL
            );
        """)
        conn.commit()

        # Step 4: Copy data into new greeks table
        print("🔹 Inserting backed-up data into new 'greeks' table...")

        # Select only the columns that match new schema
        cursor.execute("""
            INSERT INTO greeks (
                option_id, ticker, expiry, call_put, delta, gamma, vega, theta, rho,
                strike, underlying_price, days_to_expiry, risk_free_rate, implied_volatility
            )
            SELECT
                option_id, ticker, expiry, call_put, delta, gamma, vega, theta, rho,
                strike, underlying_price, days_to_expiry, risk_free_rate, implied_volatility
            FROM greeks_backup;
        """)
        conn.commit()

##        # Step 5: Optional - Drop the backup table
##        print("🔹 Dropping 'greeks_backup' table (optional cleanup)...")
##        cursor.execute("DROP TABLE IF EXISTS greeks_backup;")
##        conn.commit()

        print("\n✅ Rebuild complete! 'greeks' table is now clean and ready.")

    except Exception as e:
        print(f"❌ Error during rebuild: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    rebuild_greeks_table()

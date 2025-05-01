import sqlite3

def archive_expired_data(db_path='greeks_data.db'):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # --- Archive expired options_data ---
    cur.execute("SELECT COUNT(*) FROM options_data;")
    options_before = cur.fetchone()[0]

    cur.execute('''
        CREATE TABLE IF NOT EXISTS options_archive AS
        SELECT * FROM options_data WHERE 0;
    ''')

    cur.execute('''
        INSERT INTO options_archive
        SELECT * FROM options_data
        WHERE DATE(expiration_date) < DATE('now');
    ''')

    cur.execute('''
        DELETE FROM options_data
        WHERE DATE(expiration_date) < DATE('now');
    ''')

    cur.execute("SELECT COUNT(*) FROM options_data;")
    options_after = cur.fetchone()[0]

    print(f"Options Data — before: {options_before}, after: {options_after}")


    # --- Archive expired greeks ---
    cur.execute("SELECT COUNT(*) FROM greeks;")
    greeks_before = cur.fetchone()[0]

    cur.execute('''
        CREATE TABLE IF NOT EXISTS greeks_archive AS
        SELECT * FROM greeks WHERE 0;
    ''')

    cur.execute('''
        INSERT INTO greeks_archive
        SELECT * FROM greeks
        WHERE DATE(expiry) < DATE('now');
    ''')

    cur.execute('''
        DELETE FROM greeks
        WHERE DATE(expiry) < DATE('now');
    ''')

    cur.execute("SELECT COUNT(*) FROM greeks;")
    greeks_after = cur.fetchone()[0]

    print(f"Greeks Data — before: {greeks_before}, after: {greeks_after}")

    conn.commit()
    conn.close()

# Run the archiving process
archive_expired_data()

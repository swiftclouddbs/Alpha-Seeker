import sqlite3

def count_interesting_bs_diff(db_path='../data/greeks_data.db'):
    conn = sqlite3.connect(db_path)

    query = """
        SELECT 
            COUNT(*) AS num_interesting_trades
        FROM 
            candidate_trades ct
        LEFT JOIN 
            feature_store fs ON ct.option_id = fs.option_id
        WHERE 
            fs.bs_diff IS NOT NULL
            AND ABS(fs.bs_diff) > 0.25;
    """

    cursor = conn.cursor()
    cursor.execute(query)
    count = cursor.fetchone()[0]
    conn.close()

    return count

if __name__ == "__main__":
    result = count_interesting_bs_diff()
    print(f"Number of interesting candidate trades: {result}")

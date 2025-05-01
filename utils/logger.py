import sqlite3
from datetime import datetime

def log_event(script_name, event_type, message, db_path='alpha_seeker.db'):
    conn = sqlite3.connect(../data/greeks_data.db)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO pipeline_log (script_name, event_type, message, timestamp)
        VALUES (?, ?, ?, ?)
    ''', (script_name, event_type, message, datetime.now()))
    conn.commit()
    conn.close()


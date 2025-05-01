import os
import sqlite3
from datetime import datetime

DB_PATH = os.path.abspath('/Users/dev/AlphaSeeker/data/greeks_data.db')

def log_event(script_name, event_type, message, db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO pipeline_log (script_name, event_type, message, timestamp)
        VALUES (?, ?, ?, ?)
    ''', (script_name, event_type, message, datetime.now()))
    conn.commit()
    conn.close()


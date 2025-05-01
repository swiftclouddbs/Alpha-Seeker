import os

# Automatically determine the project root, no hardcoding
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Path to your SQLite database
DATABASE_PATH = os.path.join(BASE_DIR, 'data', 'greeks_data.db')

print(f"[CONFIG] BASE_DIR is set to: {BASE_DIR}")
print(f"[CONFIG] DATABASE_PATH is set to: {DATABASE_PATH}")


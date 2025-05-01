# Daily data store updates go here

# runner.py
import subprocess
import os

# Define your scripts in the order you want them to run
scripts = [
    "daily_rfr_pull.py",
    "daily_historical_volatility.py",
    "daily_historical_data.py",
    "daily_volatility.py",
    "daily_options_ingest.py",
    "daily_calculate_all_greeks.py",
    "daily_archive_options+greeks.py"
]

# Adjust this to the full path if needed
SCRIPT_DIR = os.path.join(os.path.dirname(__file__), "daily_data_pull")

def run_script(script_name):
    script_path = os.path.join(SCRIPT_DIR, script_name)
    print(f"\n🚀 Running {script_name}")
    try:
        subprocess.run(["python3", script_path], check=True)
        print(f"✅ Finished {script_name}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running {script_name}: {e}")

if __name__ == '__main__':
    print("📅 Starting daily data update sequence...")

    for script in scripts:
        run_script(script)

    print("\n🎉 All daily data scripts completed.")


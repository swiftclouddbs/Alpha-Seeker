#!/bin/bash

scripts=(
  "daily_rfr_pull.py"
  "daily_options_ingest.py"
  "daily_junk_removal.py"
  "daily_archive_expired_greeks.py"
  "daily_calculate_all_greeks_target_date.py"
  "daily_historical.py"
  "daily_calculate_historical_volatility.py"
)

for script in "${scripts[@]}"; do
  echo "Running $script..."
  python3 "$script"
  sleep 3
done


#!/bin/bash
# Script to transfer large tables in the background

echo "Starting transfer of PHYSICIAN_RX_FULL_YEAR at $(date)"
echo "This will take approximately 60-90 minutes to complete"
echo "Running in background with nohup..."

# Run the transfer in background with nohup
nohup poetry run python projects/007-snowflake-bq-transfer/dh_snowflake_bigquery_singlefile.py --table PHYSICIAN_RX_FULL_YEAR > physician_rx_transfer.log 2>&1 &

# Get the process ID
PID=$!
echo "Transfer started with PID: $PID"
echo "You can monitor progress with: tail -f physician_rx_transfer.log"
echo "To check if still running: ps -p $PID"
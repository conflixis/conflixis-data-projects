#!/bin/bash
# Run analysis with proper logging

# Create logs directory if it doesn't exist
mkdir -p logs

# Generate timestamp for log file
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOGFILE="logs/analysis_${TIMESTAMP}.log"

# Run the analysis
echo "Running analysis, logging to: ${LOGFILE}"
/home/incent/conflixis-data-projects/venv/bin/python cli.py analyze "$@" 2>&1 | tee "${LOGFILE}"

echo "Analysis complete. Log saved to: ${LOGFILE}"
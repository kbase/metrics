#!/usr/bin/env bash

# Define the source and destination directories
LOG_DIR="/mnt/metrics_logs"
PREVIOUS_LOGS_DIR="$LOG_DIR/previous_logs"

# Ensure the destination directory exists
mkdir -p "$PREVIOUS_LOGS_DIR"

# Get yesterday's date in the format YYYYMMDD
YESTERDAY=$(date -d "yesterday" +%Y%m%d)

# Move all log files from yesterday to the previous_logs directory
for file in "$LOG_DIR"/*_"$YESTERDAY".log; do
  if [ -f "$file" ]; then
    mv "$file" "$PREVIOUS_LOGS_DIR/"
    echo "Moved $file to $PREVIOUS_LOGS_DIR/"
  fi
done

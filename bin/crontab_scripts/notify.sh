#!/usr/bin/env bash

# Define directories and Slack webhook URL
LOG_DIR="/mnt/metrics_logs/previous_logs"
SLACK_WEBHOOK_URL="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"

# Get today's and yesterday's dates in YYYYMMDD format
TODAY=$(date +%Y%m%d)
YESTERDAY=$(date -d "yesterday" +%Y%m%d)

# Search for log files containing today's or yesterday's date
FILES=$(find "$LOG_DIR" -type f \( -name "*_$TODAY.log" -o -name "*_$YESTERDAY.log" \))

# Initialize an empty array to store files with matches
MATCHED_FILES=()

# Check if any files were found
if [ -n "$FILES" ]; then
    # Search for "exception" or "traceback" in the found files
    for FILE in $FILES; do
        if grep -qiE "exception|traceback" "$FILE"; then
            MATCHED_FILES+=("$FILE")
        fi
    done

    # If matches are found, send a Slack notification
    if [ ${#MATCHED_FILES[@]} -gt 0 ]; then
        MESSAGE="Found 'exception' or 'traceback' in the following log files:\n"
        for FILE in "${MATCHED_FILES[@]}"; do
            MESSAGE+="$(basename "$FILE")\n"
        done
        
        # Send message to Slack
        curl -X POST -H 'Content-type: application/json' \
             --data "{\"text\":\"$MESSAGE\"}" \
             "$SLACK_WEBHOOK_URL"
        
        echo "Notification sent to Slack."
    else
        echo "No 'exception' or 'traceback' found in log files."
    fi
else
    echo "No log files found for $TODAY or $YESTERDAY."
fi

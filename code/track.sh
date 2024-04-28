#!/bin/bash

# Make sure to replace this with the actual PID or use pgrep inside the loop
PID=$(pgrep -f ExternalSort.exe)

# Check if the process is running
if [ -z "$PID" ]; then
    echo "Process not running."
    exit 1
fi

# Output file for memory usage data
OUTPUT_FILE="memory_usage.txt"

# Header for output file
echo "Timestamp, RSS, VSZ" > $OUTPUT_FILE

# Loop to collect memory usage at intervals
while true; do
    if ! ps -p $PID > /dev/null; then
        echo "Process ended."
        break
    fi

    # Extract RSS and VSZ values (memory usage metrics)
    MEM_USAGE=$(ps -p $PID -o rss=,vsz= | awk '{print $1","$2}')

    # Get current timestamp
    TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")

    # Append data to the file
    echo "$TIMESTAMP, $MEM_USAGE" >> $OUTPUT_FILE

    # Wait for 1 second (or more depending on how often you want to record)
    sleep 1i
done

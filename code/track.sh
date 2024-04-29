#!/bin/bash

./ExternalSort.exe -c 11000000 -s 1024 -o trace10G &


# Make sure to replace this with the actual PID or use pgrep inside the loop
PID=$(pgrep -f ExternalSort.exe)

# Check if the process is running
if [ -z "$PID" ]; then
    echo "Process not running."
    exit 1
fi

# Output file for memory usage data
OUTPUT_FILE="memcpu_10G.txt"

# Header for output file
echo "Timestamp, RSS (KB), VSZ (KB), CPU (%)" > $OUTPUT_FILE

# Loop to collect memory and CPU usage at intervals
while true; do
    if ! ps -p $PID > /dev/null; then
        echo "Process ended."
        break
    fi

    # Extract RSS, VSZ, and CPU usage values
    USAGE_DATA=$(ps -p $PID -o rss=,vsz=,pcpu= | awk '{print $1","$2","$3}')

    # Get current timestamp
    TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")

    # Append data to the file
    echo "$TIMESTAMP, $USAGE_DATA" >> $OUTPUT_FILE

    # Wait for 1 second (or more depending on how often you want to record)
    sleep 1
done

./plot.py $OUTPUT_FILE

# Generate a plot from the output file
gnuplot -persist -e "datafile='$OUTPUT_FILE'" plot_memcpu.gp

# End of script
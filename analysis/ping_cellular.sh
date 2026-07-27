#!/bin/bash

# Define your target here
TARGET="128.200.20.22"
PORT="5001"

# Generate file name using script execution start time
FILENAME="nc_log_$(date +'%Y%m%d_%H%M%S').txt"

echo "Logging millisecond latency to ${FILENAME}... Press [CTRL+C] to stop."

while true; do
    # Get exact epoch time in milliseconds before execution
    START_TIME=$(python3 -c 'import time; print(int(time.time() * 1000))')

    # Run netcat command
    RESULT=$(nc -zv -w 2 "$TARGET" "$PORT" 2>&1)

    # Get exact epoch time in milliseconds after execution
    END_TIME=$(python3 -c 'import time; print(int(time.time() * 1000))')

    # Calculate difference
    LATENCY=$((END_TIME - START_TIME))
    TIMESTAMP=$(date "+[%Y-%m-%d %H:%M:%S]")

    # Log everything to the file
    echo "$TIMESTAMP - Target: $TARGET:$PORT - Status: $RESULT - Latency: ${LATENCY}ms" >> "$FILENAME"

    sleep 1
done

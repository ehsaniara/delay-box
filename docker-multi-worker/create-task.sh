#!/bin/bash

# number of tasks
max=1000

timestamp=$(date +%s000)
executionTimestamp=$((timestamp + 5000))
echo "timestamp: $timestamp, executionTimestamp: $executionTimestamp"

jsonBody='{"parameter":{"executionTimestamp":"EXECUTION_TIMESTAMP","taskType":"SHELL_CMD"},"pyload":"ZWNobyAiY3VycmVudDogJChkYXRlICslczAwMCki"}'
json_string=${jsonBody/EXECUTION_TIMESTAMP/$executionTimestamp}

# Function to send curl request
send_request() {
    # create the task
    curl -X POST http://localhost:8088/api/task -H "Content-Type: application/json" -d $json_string
}

for i in `seq 2 $max`
do
  send_request "$URL" &
done

# Wait for all background processes to complete
wait
echo ""
echo "$max tasks created."

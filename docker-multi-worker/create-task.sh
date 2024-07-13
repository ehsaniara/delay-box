#!/bin/bash

# number of tasks
max=10

timestamp=$(date +%s000)

#task will be run in 5 second
executionTimestamp=$((timestamp + 5000))
echo "timestamp: $timestamp, executionTimestamp: $executionTimestamp"
payload=$(echo 'echo "current: $(date +%s000)"' |base64)

json_string='{"parameter":{"executionTimestamp":"EXECUTION_TIMESTAMP","taskType":"SHELL_CMD"},"pyload":"PAYLOAD"}'
json_string=${json_string/EXECUTION_TIMESTAMP/$executionTimestamp}
json_string=${json_string/PAYLOAD/$payload}
echo "json_string: $json_string"

# Function to send curl request
send_request() {
    # create the task
    curl -X POST http://localhost:8088/api/task -H "Content-Type: application/json" -d $json_string
}

for i in `seq 1 $max`
do
  send_request "$URL" &
done

# Wait for all background processes to complete
wait
echo ""
echo "$max tasks created."

#!/bin/bash

# Replace with your topics name from config_multi_worker.yaml
topics=("Scheduler" "TaskExecution")

PARTITIONS=3
REPLICATION_FACTOR=1
BOOTSTRAP_SERVER=kafka-ts:9092


# Wait for Kafka to be ready
echo "Waiting for Kafka to start..."
while ! echo "exit" | nc -z kafka-ts 9092; do sleep 5; done


# Loop through the array of topics and create each one
for TOPIC_NAME in "${topics[@]}"
do
  echo "Creating topic $TOPIC_NAME..."
  kafka-topics --create --topic $TOPIC_NAME --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --if-not-exists --bootstrap-server $BOOTSTRAP_SERVER
  echo "Topic $TOPIC_NAME created."
done

#!/bin/bash
# Create Kafka topic if it does not exist
KAFKA_BROKER=${KAFKA_BROKER:-localhost:9092}
TOPIC=${KAFKA_TOPIC:-stock_data}

# Check if the topic already exists
if ! kafka-topics.sh --list --bootstrap-server $KAFKA_BROKER | grep -q $TOPIC; then
  kafka-topics.sh --create --bootstrap-server $KAFKA_BROKER --replication-factor 1 --partitions 1 --topic $TOPIC
  echo "Kafka topic '$TOPIC' created."
else
  echo "Kafka topic '$TOPIC' already exists."
fi
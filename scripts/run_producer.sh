#!/usr/bin/env bash
# Simple script to run the Kafka producer in background.
# Update BROKER, TOPIC and CSV_PATH as needed.
BROKER=${BROKER:-localhost:9092}
TOPIC=${TOPIC:-moth_clickstream}
CSV_PATH=${CSV_PATH:-./data/click_stream_rt.csv}

python3 -m moth.kafka_producer_runner --broker "$BROKER" --topic "$TOPIC" --csv "$CSV_PATH" &
echo "Producer started (PID: $!)"

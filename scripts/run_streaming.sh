#!/usr/bin/env bash
# Script to run the streaming job locally using 'spark-submit' (adjust SPARK_HOME if needed)
SPARK_SUBMIT=${SPARK_SUBMIT:-spark-submit}
KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-localhost:9092}
INPUT_TOPIC=${INPUT_TOPIC:-moth_clickstream}
CHECKPOINT=${CHECKPOINT:-/tmp/moth_checkpoint}
OUTPUT_PARQUET=${OUTPUT_PARQUET:-./output/parquet}

$SPARK_SUBMIT --master local[4] -m moth.streaming_job -- --kafka_bootstrap $KAFKA_BOOTSTRAP --input_topic $INPUT_TOPIC --checkpoint $CHECKPOINT --output_parquet_dir $OUTPUT_PARQUET &
echo "Streaming job started (PID: $!)"

SPARK_SUBMIT=${SPARK_SUBMIT:-spark-submit}
KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-localhost:9092}
INPUT_TOPIC=${INPUT_TOPIC:-moth_clickstream}
CHECKPOINT=${CHECKPOINT:-/tmp/moth_checkpoint}
OUTPUT_PARQUET=${OUTPUT_PARQUET:-./output/parquet}

$SPARK_SUBMIT \
  --master local[4] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 \
  moth/streaming_job_runner.py \
  --kafka_bootstrap $KAFKA_BOOTSTRAP \
  --input_topic $INPUT_TOPIC \
  --checkpoint $CHECKPOINT \
  --output_parquet $OUTPUT_PARQUET &

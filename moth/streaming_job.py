"""Spark Structured Streaming job.

This module defines a function `run_streaming` which reads from a Kafka topic,
joins static tables, runs a model prediction (placeholder), and writes outputs to parquet and Kafka.
"""
from pyspark.sql import SparkSession, functions as F, types as T
import json

def run_streaming(spark: SparkSession, kafka_bootstrap: str = 'localhost:9092', input_topic: str = 'moth_clickstream', checkpoint_location: str = '/tmp/moth_checkpoint', model_predict_fn=None, output_parquet_dir: str = './output/parquet'):
    """Start a structured streaming job.

    model_predict_fn: a callable that accepts a DataFrame and returns a DataFrame with a 'prediction' column.
    """
    # Read from Kafka
    df = (
        spark.readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', kafka_bootstrap)
        .option('subscribe', input_topic)
        .option('startingOffsets', 'earliest')
        .load()
    )

    # Kafka value is bytes (JSON string). Parse into columns
    schema = T.StructType()  # flexible; user can parse further
    # We parse value as string and then use from_json when schema is known
    raw = df.selectExpr("CAST(value AS STRING) as value", 'timestamp')
    parsed = raw.withColumn('json', F.from_json('value', T.MapType(T.StringType(), T.StringType())))
    # Flatten the map into columns dynamically
    cols = parsed.select('json').schema

    # For demo, keep value as json string and add event_time
    stream = parsed.withColumn('event_time', F.to_timestamp(F.col('json')['ts'].cast('long')))

    # Optionally, call model predict function
    if model_predict_fn is not None:
        predicted = model_predict_fn(stream)
    else:
        # Add a placeholder prediction column of 0/1 randomly
        predicted = stream.withColumn('prediction', F.lit(0))

    # Write predictions to parquet for downstream consumption
    query = (
        predicted.writeStream
        .format('parquet')
        .option('path', output_parquet_dir)
        .option('checkpointLocation', checkpoint_location)
        .outputMode('append')
        .start()
    )

    return query

if __name__ == '__main__':
    spark = SparkSession.builder.appName('MOTH-Streaming-Demo').master('local[4]').getOrCreate()
    q = run_streaming(spark)
    q.awaitTermination()

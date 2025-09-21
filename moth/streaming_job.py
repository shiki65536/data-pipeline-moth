"""Spark Structured Streaming job."""

from pyspark.sql import SparkSession, functions as F, types as T
import argparse

def run_streaming(
    spark: SparkSession,
    kafka_bootstrap: str = "localhost:9092",
    input_topic: str = "moth_clickstream",
    checkpoint_location: str = "/tmp/moth_checkpoint",
    model_predict_fn=None,
    output_parquet_dir: str = "./output/parquet"
):
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", input_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    raw = df.selectExpr("CAST(value AS STRING) as value", "timestamp")
    parsed = raw.withColumn(
        "json",
        F.from_json("value", T.MapType(T.StringType(), T.StringType()))
    )

    stream = parsed.withColumn(
        "event_time", F.to_timestamp(F.col("json")["ts"].cast("long"))
    )

    if model_predict_fn is not None:
        predicted = model_predict_fn(stream)
    else:
        predicted = stream.withColumn("prediction", F.lit(0))

    query = (
        predicted.writeStream
        .format("parquet")
        .option("path", output_parquet_dir)
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .start()
    )

    return query


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka_bootstrap", default="localhost:9092")
    parser.add_argument("--input_topic", default="moth_clickstream")
    parser.add_argument("--checkpoint", default="/tmp/moth_checkpoint")
    parser.add_argument("--output_parquet", default="./output/parquet")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("MOTH-Streaming-Demo")
        .master("local[4]")
        .getOrCreate()
    )

    q = run_streaming(
        spark,
        kafka_bootstrap=args.kafka_bootstrap,
        input_topic=args.input_topic,
        checkpoint_location=args.checkpoint,
        output_parquet_dir=args.output_parquet,
    )
    q.awaitTermination()

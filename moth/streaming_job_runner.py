"""Command-line runner for streaming_job.run_streaming"""
import argparse
import pickle, os
from pyspark.sql import SparkSession
from moth import spark_utils, streaming_job

def load_dummy_model(path):
    with open(path, 'rb') as f:
        return pickle.load(f)

def model_predict_fn_builder(model):
    # Returns a function that accepts a DataFrame and returns a DataFrame with 'prediction' column.
    def predict(df):
        from pyspark.sql import functions as F, types as T
        # model is a dict with threshold; we will compute numeric sum of json values if present
        @F.udf(T.IntegerType())
        def predict_udf(json_map):
            try:
                if json_map is None:
                    return 0
                s = 0.0
                for v in json_map.values():
                    try:
                        s += float(v)
                    except Exception:
                        pass
                return 1 if s > model.get('threshold', 1.0) else 0
            except Exception:
                return 0
        return df.withColumn('prediction', predict_udf(F.col('json')))
    return predict

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--kafka_bootstrap', default='localhost:9092')
    parser.add_argument('--input_topic', default='moth_clickstream')
    parser.add_argument('--checkpoint', default='/tmp/moth_checkpoint')
    parser.add_argument('--output_parquet', default='./output/parquet')
    parser.add_argument('--model', default='./models/best_model/model.pkl')
    args = parser.parse_args()

    model = load_dummy_model(args.model)
    predict_fn = model_predict_fn_builder(model)

    spark = spark_utils.create_spark('MOTH-Streaming-Runner', master='local[4]')
    q = streaming_job.run_streaming(spark, kafka_bootstrap=args.kafka_bootstrap, input_topic=args.input_topic, checkpoint_location=args.checkpoint, model_predict_fn=predict_fn, output_parquet_dir=args.output_parquet)
    q.awaitTermination()

if __name__ == '__main__':
    main()

# data-pipeline-moth

End-to-end data pipeline using PySpark + Kafka to predict eCommerce sales and customer segmentation.

## Structure

- `moth/` : core Python package with reusable functions
- `notebooks/` : demo notebooks to illustrate usage
- `scripts/` : simple helper scripts to run producer / streaming
- `data/` : put your CSV dataset files here (not included due to size)
- `models/` : saved ML model(s)

## Quick start

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Place CSVs in `data/` (category.csv, users.csv, product.csv, sales.csv, click_stream_rt.csv)

3. Start Kafka broker (example using Docker):

```bash
# start zookeeper
docker run -d --name zookeeper -p 2181:2181 zookeeper:3.7
# start kafka (uses wurstmeister image; update as needed)
docker run -d --name kafka -p 9092:9092 --env KAFKA_ZOOKEEPER_CONNECT=host.docker.internal:2181 --env KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 --env KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 wurstmeister/kafka

```

4. Create topic:

```bash
docker exec -it kafka bash -c "kafka-topics.sh --create --topic moth_clickstream --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1"
```

5. Run producer (example):

```bash
bash scripts/run_producer.sh
```

6. Start streaming job (example) — requires spark-submit in PATH:

```bash
bash scripts/run_streaming.sh
```

7. Use notebooks in `notebooks/` to explore results and visualizations.

## Running the streaming runner directly (python -m)

You can run the streaming runner as a module (this will load the dummy model stored in `models/best_model/model.pkl`):

```bash
python3 -m moth.streaming_job_runner --kafka_bootstrap localhost:9092 --input_topic moth_clickstream --checkpoint /tmp/moth_checkpoint --output_parquet ./output/parquet --model ./models/best_model/model.pkl
```

## Notes

- Notebooks are demos and call into the `moth` package.
- The dummy model included is simplistic. Replace `models/best_model/model.pkl` with your trained model and update `streaming_job_runner` to build a proper UDF for predictions.
- Adjust Docker commands for your OS (the kafka docker images often require extra network configuration).

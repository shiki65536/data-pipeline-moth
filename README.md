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

# MOTH E-Commerce Data Pipeline

End-to-end data pipeline using PySpark + Kafka for real-time eCommerce sales prediction and customer segmentation.

## Architecture

```
CSV Data → Kafka Producer → Kafka Topic → Spark Streaming → ML Prediction → Visualization
```

## Project Structure

```
data-pipeline-moth/
├── moth/                    # Core Python package
│   ├── spark_utils.py       # Spark session utilities
│   ├── batch_analysis.py    # Batch processing functions
│   ├── kafka_producer.py    # Kafka data producer
│   ├── streaming_job.py     # Spark Streaming job
│   └── visualisation.py     # Data visualization helpers
├── notebooks/               # Demo Jupyter notebooks
├── scripts/                 # Shell scripts for automation
├── data/                    # CSV dataset files (add your own)
├── models/                  # Trained ML models
└── docker-compose.yml       # Kafka environment setup
```

## Quick Start

### 1. Install Dependencies

```bash
pip install -e .
```

### 2. Prepare Data

Place the following CSV files in `data/`:

- `category.csv` - Product categories
- `users.csv` - Customer information
- `product.csv` - Product details
- `sales.csv` - Transaction records
- `click_stream_rt.csv` - Real-time clickstream data

### 3. Start Kafka Environment

```bash
# Start Kafka + Zookeeper
docker-compose up -d

# Create topic
docker exec -it kafka kafka-topics.sh \
  --create --topic moth_clickstream \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

### 4. Run Pipeline

**Option A: Using scripts**

```bash
# Start producer
bash scripts/run_producer.sh

# Start streaming job (new terminal)
bash scripts/run_streaming.sh
```

**Option B: Direct module execution**

```bash
# Producer
python -m moth.kafka_producer_runner \
  --broker localhost:9092 \
  --topic moth_clickstream \
  --csv ./data/click_stream_rt.csv

# Consumer (new terminal)
python -m moth.streaming_job_runner \
  --kafka_bootstrap localhost:9092 \
  --input_topic moth_clickstream \
  --model ./models/best_model/model.pkl
```

### 5. Explore Results

Open notebooks in `notebooks/` to view analysis and visualizations:

- `batch_analysis.ipynb` - Batch analysis demo
- `streaming_demo.ipynb` - Streaming pipeline demo

## Troubleshooting

**Check if Kafka is working:**

```bash
# List topics
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Test consumer
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic moth_clickstream \
  --from-beginning
```

## Running the streaming runner directly (python -m)

You can run the streaming runner as a module (this will load the dummy model stored in `models/best_model/model.pkl`):

```bash
python3 -m moth.streaming_job_runner --kafka_bootstrap localhost:9092 --input_topic moth_clickstream --checkpoint /tmp/moth_checkpoint --output_parquet ./output/parquet --model ./models/best_model/model.pkl
```

## Notes

- Notebooks are demos and call into the `moth` package.
- The dummy model included is simplistic. Replace `models/best_model/model.pkl` with your trained model and update `streaming_job_runner` to build a proper UDF for predictions.
- Adjust Docker commands for your OS (the kafka docker images often require extra network configuration).

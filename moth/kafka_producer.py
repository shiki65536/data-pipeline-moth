"""Kafka producer to simulate clickstream data.

This module provides a simple file-based/randomized producer. For real Kafka,
install 'kafka-python' and configure brokers accordingly.
"""
import csv, time, json, random
from kafka import KafkaProducer

def create_producer(broker: str = "localhost:9092") -> KafkaProducer:
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def stream_from_csv(producer: KafkaProducer, topic: str, csv_path: str, batch_min: int = 500, batch_max: int = 1000, pause_s: int = 5):
    """Stream random batches from csv_path into Kafka topic every `pause_s` seconds.
    Adds 'ts' field (unix timestamp seconds) and sends rows as JSON strings.
    Does not load entire CSV into memory; reads rows sequentially and cycles.
    """
    def row_generator(path):
        while True:
            with open(path, 'r', newline='', encoding='utf-8') as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    yield row
    gen = row_generator(csv_path)
    while True:
        batch_size = random.randint(batch_min, batch_max)
        ts_start = int(time.time())
        # Spread batch evenly across pause_s seconds
        per_second = max(1, batch_size // pause_s)
        for i in range(batch_size):
            try:
                row = next(gen)
            except StopIteration:
                return
            # Add ts field
            ts = ts_start + (i // per_second)
            row['ts'] = ts
            producer.send(topic, row)
        producer.flush()
        time.sleep(pause_s)

if __name__ == '__main__':
    # Example usage (requires Kafka broker running)
    broker = 'localhost:9092'
    topic = 'moth_clickstream'
    csv_path = './data/click_stream_rt.csv'
    p = create_producer(broker)
    stream_from_csv(p, topic, csv_path)

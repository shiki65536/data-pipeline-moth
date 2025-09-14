"""Simple kafka consumer visualisation helpers.

This module uses kafka-python to consume topics and convert messages to pandas for plotting.
"""
import json, time
from kafka import KafkaConsumer
import pandas as pd

def consume_to_dataframe(broker: str, topic: str, timeout_ms: int = 10000, max_messages: int = None):
    consumer = KafkaConsumer(topic, bootstrap_servers=[broker], auto_offset_reset='earliest', consumer_timeout_ms=timeout_ms, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    rows = []
    for i, msg in enumerate(consumer):
        rows.append(msg.value)
        if max_messages and i+1 >= max_messages:
            break
    consumer.close()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)

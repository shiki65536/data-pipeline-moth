"""Command-line runner for kafka_producer module."""
import argparse
from .kafka_producer import create_producer, stream_from_csv

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--broker', default='localhost:9092')
    parser.add_argument('--topic', default='moth_clickstream')
    parser.add_argument('--csv', default='./data/click_stream_rt.csv')
    args = parser.parse_args()
    p = create_producer(args.broker)
    stream_from_csv(p, args.topic, args.csv)

if __name__ == '__main__':
    main()

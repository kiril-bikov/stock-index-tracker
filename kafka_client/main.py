from consumer import start_consumer
from helpers import create_kafka_topic

if __name__ == '__main__':
    create_kafka_topic()
    start_consumer()
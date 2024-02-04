from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
import os
import sys

def get_parent_dir():
    return os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

def add_parent_dir():
    sys.path.append(get_parent_dir())

add_parent_dir()

from utils import load_yml_config, create_logger, create_logger, load_yml_config
config = load_yml_config()
logger = create_logger()

kafka_server = config['kafka_server']
kafka_topic = config['kafka_topic']

def create_kafka_topic(topic=kafka_topic):
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=kafka_server
        )

        if kafka_topic in admin_client.list_topics():
            logger.warn(f'Kafka topic {kafka_topic} already exists.')
            return

        topic_list = []
        topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logger.info(f'Created kafka topic {kafka_topic}')
    except Exception as e:
        logger.error(f'An error occurred while creating Kafka topic: {e}.')

def create_kafka_consumer():
    try:
        return KafkaConsumer(kafka_topic,
                            bootstrap_servers=[kafka_server],
                            value_deserializer=lambda x: loads(x.decode('utf-8')))
    except Exception as e:
        logger.error(f'An error occurred while initializing the Kafka consumer: {e}.')

def create_kafka_producer():
    try:
        return KafkaProducer(bootstrap_servers=kafka_server,
                            value_serializer=lambda x: dumps(x).encode('utf-8'))
    except Exception as e:
        logger.error(f'An error occurred while initializing the Kafka producer: {e}.')
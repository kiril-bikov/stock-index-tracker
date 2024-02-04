from helpers import create_kafka_consumer, add_parent_dir, get_parent_dir
add_parent_dir()
parent_dir=get_parent_dir()
from utils import create_logger, load_yml_config
from cassandra_db.helpers import insert_index_data

logger = create_logger()
config = load_yml_config(f'{parent_dir}/config.yml')
consumer = create_kafka_consumer()
indexes = config['indexes']

def process_message(message):
    try:
        item = message.value

        for index in indexes:
            insert_index_data(index, item)

    except Exception as e:
        logger.error(f'An error occurred while processing message in consumer: {e}.')

def start_consumer():
    for message in consumer:
        process_message(message)

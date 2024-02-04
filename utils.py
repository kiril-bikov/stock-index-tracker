import logging
import yaml
from datetime import datetime

def load_yml_config(config_file_path=f'config.yml'):
    with open(config_file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

config = load_yml_config()

def create_logger(logger_name='tickers_logger', message_format='%(asctime)s - %(message)s', datefmt=config['date_format']):
    logger = logging.getLogger(logger_name)
    logging.basicConfig(format=message_format, datefmt=datefmt)
    logger.setLevel(logging.INFO)

    return logger

def is_correct_date_format(date_string, date_format=config['date_format']):
    if not isinstance(date_string, str):
        return False
    try:
        datetime.strptime(date_string, date_format)
        return True
    except ValueError:
        return False

def validate_json_body(request, *required_keys):
    json_body = request.json
    missing_keys = [key for key in required_keys if key not in json_body]
    if missing_keys:
        raise ValueError(f"Missing required parameter(s): {', '.join(missing_keys)}")
    return json_body






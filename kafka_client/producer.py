from datetime import datetime
import time
from kafka_client.helpers import create_kafka_producer, add_parent_dir, get_parent_dir
from cassandra_db.helpers import create_cassandra_session, get_unique_tickers
add_parent_dir()
parent_dir = get_parent_dir()
from utils import load_yml_config, create_logger
from scrapper import download_ticker_data

config = load_yml_config()
logger = create_logger()
session = create_cassandra_session()

date_format = config['date_format']
indexes = config['indexes']
historic_start_date = config['historic_start_date']
kafka_topic = config['kafka_topic']

def produce_data(producer, value):
	try:
		producer.send(kafka_topic, value)
	except:
		logger.info(f'Producer - Could not process ticker data {value["Ticker_id"]}.')

async def download_index_data(process_in_real_time=False, start_date=historic_start_date):
	unique_tickers = get_unique_tickers()
	producer = create_kafka_producer()

	for ticker in unique_tickers:
		# Yahoo finance allows only 120 requests per minute
		time.sleep(0.5)

		date_now = datetime.now()
		date_now = date_now.strftime(date_format)
		if process_in_real_time:
			start_date = ticker.last_updated

		ticker_data = download_ticker_data(ticker.ticker_id, ticker.company, start_date, date_now)

		if ticker_data is not None and not ticker_data.empty:
			for _, item in ticker_data.iterrows():
				item.Date = item.Date.strftime(date_format)
				produce_data(producer, item.to_dict())

			logger.info(f'Producer - Sent ticker data {ticker.ticker_id}.')


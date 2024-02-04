from cassandra.cluster import Cluster
import os, sys

def add_parent_dir():
    parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    sys.path.append(parent_dir)

add_parent_dir()

from cassandra_db.index_table  import IndexTable
from cassandra_db.tickers_table import TickersTable
from cassandra_db.index_entry import IndexEntry

from utils import load_yml_config, create_logger
config = load_yml_config()
logger = create_logger()

def create_cassandra_session(cassandra_keyspace=config['cassandra_keyspace']):
    cluster = Cluster(protocol_version=5)
    return cluster.connect(cassandra_keyspace)

session=create_cassandra_session()

def create_index_tables(indexes=config['indexes'], session=session):
    for index_name in indexes:
        try:
            table_name = f'{index_name}_index'
            tickers_table = IndexTable(session, table_name)
            tickers_table.create_table()
            logger.info(f'Index table {index_name} created.')
        except Exception as e:
            logger.error(f'Could not create index table {index_name} due to error: {e}.')

def create_tickers_tables(indexes=config['indexes'], session=session):
    for index_name in indexes:
        try:
            table_name = f'{index_name}_tickers'
            tickers_table = TickersTable(session, table_name)
            tickers_table.create_table()
            logger.info(f'Ticker table {table_name} created.')
        except Exception as e:
            logger.error(f'Could not create ticker table {table_name} due to error: {e}.')

def insert_index_data(index_name, ticker):
    tickers_table_name = f'{index_name}_tickers'
    index_table_name = f'{index_name}_index'

    try:
        tickers_table = TickersTable(session, tickers_table_name)
        ticker_row = tickers_table.retrieve_ticker(ticker_id=ticker['Ticker_id'])
    except Exception as e:
        logger.error(f'Could not retrieve ticker data due to error: {e}.')

    if len(ticker_row) > 0:
        try:
            index_entry = IndexEntry(ticker_id=ticker['Ticker_id'], company=ticker['Company'], close=ticker['Close'], date=ticker['Date'], high=ticker['High'], low=ticker['Low'], open_amount=ticker['Open'], volume=ticker['Volume'])
            index_entry.insert_in_table(session, index_table_name)
            logger.info(f"Consumer - Inserted {index_entry.ticker_id} data into {index_table_name}.")
        except Exception as e:
            logger.error(f'Could not insert {index_entry.ticker_id} data into {index_table_name} due to error: {e}.')

        try:
            tickers_table.update_ticker(date=index_entry.date, ticker_id=ticker['Ticker_id'])
            logger.info(f"Consumer - Updated {index_entry.ticker_id} in {tickers_table_name} to {index_entry.date}.")
        except Exception as e:
            logger.error(f'Could not update {index_entry.ticker_id} in {tickers_table_name} due to error: {e}.')

def get_unique_tickers(indexes=config['indexes']):
	unique_tickers = set()
	try:
		for index in indexes:
			table_name = f'{index}_tickers'
			tickers_table = TickersTable(session, table_name)
			tickers = tickers_table.retrieve_all_tickers()
			unique_tickers.update(tickers)

		logger.info(f'Successfully obtained all unique tickers from indexes.')
		return unique_tickers

	except Exception as e:
		logger.info(f'Could not obtain all unique tickers from indexes due to error: {e}.')

def empty_index_exists(indexes=config['indexes']):
	try:
		for index in indexes:
			table_name = f'{index}_index'
			tickers_table = IndexTable(session, table_name)
			if tickers_table.is_empty():
				return True

	except Exception as e:
		logger.info(f'Could not obtain all unique tickers from indexes due to error: {e}.')
		return True

	return False




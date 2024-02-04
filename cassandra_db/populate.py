from helpers import create_tickers_tables, create_index_tables, add_parent_dir
add_parent_dir()
from utils import create_logger
from scrapper import scrape_tickers
logger = create_logger()

if __name__ == '__main__':
    create_tickers_tables()
    scrape_tickers()
    create_index_tables()
    logger.info('Successfully populated cassandra db.')
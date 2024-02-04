from datetime import datetime, time as dt_time
from kafka_client.producer import download_index_data
from utils import create_logger
from cassandra_db.helpers import empty_index_exists
import asyncio

logger = create_logger()

async def in_stock_market_hours():
    now = datetime.now()

    if now.weekday() < 5:  # Monday to Friday
        start_time = dt_time(8, 0)  # 8:00 AM
        end_time = dt_time(16, 30)  # 4:30 PM

        if start_time <= now.time() <= end_time:
            return True

    return False

async def schedule_index_data_download(process_in_real_time=False):
	if empty_index_exists():
			logger.info(f'Starting download of historic index data.')
			await download_index_data()
			logger.info(f'Finished downloading historic index data.')
	else:
		logger.info(f'Historic index data already downloaded.')

	while process_in_real_time:
		if await in_stock_market_hours():
			logger.info(f'Starting download of real-time index data.')
			await download_index_data(process_in_real_time)
			logger.info(f'Finished downloading real-time index data.')

		else:
			logger.info('Outside of Stock Exchange hours. Retrying in 10 minutes.')
			await asyncio.sleep(600)
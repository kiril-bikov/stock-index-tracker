from scheduler import schedule_index_data_download
import asyncio
from utils import load_yml_config
config = load_yml_config()

if __name__ == '__main__':
	process_in_real_time = config['process_in_real_time']
	asyncio.run(schedule_index_data_download(process_in_real_time))
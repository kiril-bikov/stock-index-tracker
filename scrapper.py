import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
from io import StringIO
from datetime import datetime
from cassandra_db.ticker import Ticker
from utils import load_yml_config, create_logger
from cassandra_db.helpers import create_cassandra_session
import yfinance as yf

config = load_yml_config()
logger = create_logger()
session = create_cassandra_session()

date_format = config['date_format']
indexes = config['indexes']

def get_html_soup(url):
    request = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    return bs(request.text, "lxml")

def get_dow_jones():
    url = config['dow_jones_urls'][0]
    soup = get_html_soup(url)
    stats = soup.find('table', class_='tablepress tablepress-id-42 tablepress-responsive')
    pulled_df = pd.read_html(StringIO(str(stats)))[0]
    pulled_df.rename(columns={'Symbol': 'Ticker'}, inplace=True)
    return pulled_df

def get_sp_500():
    url = config['sp_500_urls'][0]
    soup = get_html_soup(url)
    stats = soup.find('table', class_='table table-hover table-borderless table-sm')
    df = pd.read_html(StringIO(str(stats)))[0]
    df.rename(columns={'Symbol': 'Ticker'}, inplace=True)
    return df

def get_nasdaq_100():
    df = pd.DataFrame()
    urls = config['nasdaq_100_urls']
    for url in urls:
        soup = get_html_soup(url)
        stats = soup.find('table', class_='mdc-data-table__table')
        temp = pd.read_html(StringIO(str(stats)))[0]
        df = df._append(temp, ignore_index=True)
    return df

def call_function_by_name(function_name):
    func = globals().get(function_name)
    if callable(func):
        return func()
    else:
        logger.error(f"No function named '{function_name}'.")

def scrape_tickers():
	now_time = datetime.now().strftime(date_format)

	for index_name in indexes:
		table_name = f'{index_name}_tickers'
		index_pd = call_function_by_name(f'get_{index_name}')

		for _, row in index_pd.iterrows():
			ticker_id = row['Ticker'].replace(';', '')
			company = row['Company']
			try:
				ticker = Ticker(last_updated=now_time, ticker_id=ticker_id, company=company)
				ticker.insert_in_table(session, table_name)
			except Exception as e:
				logger.error(f'Could not insert ticker {ticker_id} due to error: {e}.')

		logger.info(f'Scrapped tickers for index {index_name}.')

def download_ticker_data(ticker_id, company, start_date, end_date):
	try:
		ticker_data = yf.download(ticker_id, datetime.strptime(start_date, date_format), datetime.strptime(end_date, date_format), progress=False)

		if ticker_data.empty:
			return

		ticker_data.reset_index(inplace=True)
		ticker_data['Ticker_id'] = ticker_id
		ticker_data['Company'] = company

		logger.info(f'Yahoo Finance - Downloaded stock data for {ticker_id} for the period {start_date} - {end_date}.')
		return ticker_data

	except Exception as e:
		logger.error(f'Yahoo Finance - Could not download stock data for {ticker_id} due to error: {e}.')
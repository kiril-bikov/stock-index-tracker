from flask import Flask, jsonify
from flask import request
from cassandra_db.tickers_table import TickersTable
from cassandra_db.index_table import IndexTable
from cassandra_db.helpers import create_cassandra_session, load_yml_config
from utils import is_correct_date_format, validate_json_body
from datetime import datetime

session=create_cassandra_session()
config=load_yml_config()
app = Flask(__name__)

@app.route('/tickers', methods=['GET'])
def list_tickers_in_index():
    try:
        json_body = validate_json_body(request, 'index')

        index = json_body['index']
        if not isinstance(index, str):
            raise ValueError("Index must be a string.")

        if index not in config['indexes']:
            raise ValueError(f"Index {index} is not supported.")

        tickers_table_name = f'{index}_tickers'
        tickers_table = TickersTable(session, tickers_table_name)
        tickers = tickers_table.list_ticker_ids()
        response = [row.ticker_id for row in tickers]
        response.sort()

        return jsonify(response), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/index/types', methods=['GET'])
def list_index_types():
    try:
        response = config['indexes']
        response.sort()

        return jsonify(response), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/index/data', methods=['GET'])
def list_index_data():
    try:
        json_body = validate_json_body(request, 'index', 'ticker_id')

        index = json_body['index']
        if not isinstance(index, str):
            raise ValueError("Index must be a string.")

        if index not in config['indexes']:
            raise ValueError(f"Index {index} is not supported.")

        ticker_id = json_body['ticker_id']
        if not isinstance(index, str):
            raise ValueError("Ticker must be a string.")

        tickers_table_name = f'{index}_tickers'
        tickers_table = TickersTable(session, tickers_table_name)
        tickers = tickers_table.list_ticker_ids()
        tickers_list = [row.ticker_id for row in tickers]

        if ticker_id not in tickers_list:
            raise ValueError(f"Ticker {ticker_id} is not in index {index}.")

        date_format = config['date_format']
        start_date = config['historic_start_date']
        if 'start_date' in json_body:
            start_date = json_body['start_date']
            if not is_correct_date_format(start_date):
                raise ValueError(f"Start date is not in correct format. Please use a string in the format {date_format}.")

        end_date = datetime.now().strftime(date_format)
        if 'end_date' in json_body:
            end_date = json_body['end_date']
            if not is_correct_date_format(end_date):
                raise ValueError(f"End date is not in correct format. Please use the format {date_format}.")

        index_table_name = f'{index}_index'
        index_table = IndexTable(session, index_table_name)
        ticker_data = index_table.retrieve_ticker_data(ticker_id, start_date, end_date)

        response = [row._asdict() for row in ticker_data]

        return jsonify(response), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=config['api_debug_mode'])

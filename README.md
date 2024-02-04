# Stock Index Tracker

Stock Index Tracker is a system for processing and storing historic and real-time data from popular stock market indexes.

Currently supported indexes: S&P 500, Dow Jones Industrial Average, Nasdaq-100.

## Features

- Scrape ticker symbols for an index from the internet.
- Store index structure in a database.
- Download and store historic tickers data for any selected period based on an index.
- Obtain real-life tickers data from an index during stock exchange hours.
- Retrieve the stored index data via an API.

## Technology Stack

Stock Index Tracker is a Python project that uses the following technologies:

- [Apache Cassandra](https://cassandra.apache.org/doc/latest/) - An open-source NoSQL distributed database.
- [Apache Kafka](https://kafka.apache.org/)- An open-source distributed event streaming platform.
- [Flask](https://flask.palletsprojects.com/en/3.0.x/) - A web framework.

## Installation

Stock Index Tracker requires [Python 3](https://www.python.org/downloads/), [Kafka](https://kafka.apache.org/quickstart) and [Cassandra](https://cassandra.apache.org/doc/latest/cassandra/installing/installing.html) to run.

Install the project dependencies:

```sh
pip install -r requirements.txt
```

## Setup

To run the project, follow the steps below:

* Create a Cassandra keyspace. Update the `config.yml` file by setting `cassandra_keyspace` to your new keyspace.

* Populate cassandra with initial data on index structure by running:
    ```sh
    python cassandra_db/populate.py
    ```

* Start the kafka consumer with:
    ```sh
    python kafka_client/main.py
    ```

* In a separate terminal, download historic tickers data for the indexes:
    ```sh
    python main.py
    ```

To use the API, run:
```sh
python api.py
```

You can also run the project to download real-time tickers data from the indexes. To do that, change the `process_in_real_time` in the `config.yml` to true and start `main.py`. This will activate a scheduler that continously downloads all tickers data for the indexes while the stock exchange market is open.

## License

GNU General Public License
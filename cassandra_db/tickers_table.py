class TickersTable:
    def __init__(self, session, table_name):
        self.session = session
        self.table_name = table_name

    def create_table(self):
        create_table_query = f"""
        CREATE TABLE if not exists {self.table_name} (
            last_updated TIMESTAMP,
            ticker_id TEXT,
            company TEXT,
            PRIMARY KEY (ticker_id)
        );
        """
        self.session.execute(create_table_query)

    def retrieve_ticker(self, ticker_id):
        query = f"SELECT * FROM {self.table_name} WHERE ticker_id='{ticker_id}';"
        return self.session.execute(query).current_rows

    def retrieve_all_tickers(self):
        query = f"SELECT * FROM {self.table_name};"
        return self.session.execute(query)

    def update_ticker(self, date, ticker_id):
        query = f"UPDATE {self.table_name} SET last_updated='{date}' WHERE ticker_id='{ticker_id}'"
        self.session.execute(query)

    def list_ticker_ids(self):
        query = f"SELECT ticker_id FROM {self.table_name};"
        return self.session.execute(query)
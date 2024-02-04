class IndexTable:
    def __init__(self, session, table_name):
        self.session = session
        self.table_name = table_name

    def create_table(self):
        create_table_query = f"""
        CREATE TABLE if not exists {self.table_name} (
            ticker_id TEXT,
            company TEXT,
            date TIMESTAMP,
            close FLOAT,
            high FLOAT,
            low FLOAT,
            open FLOAT,
            volume FLOAT,
            PRIMARY KEY(ticker_id, date)
        )
        WITH CLUSTERING ORDER BY (date DESC);
        """
        self.session.execute(create_table_query)

    def is_empty(self):
        query = f"SELECT * FROM {self.table_name} LIMIT 1"
        rows = self.session.execute(query)
        is_empty = True if rows.one() is None else False
        return is_empty

    def retrieve_ticker_data(self, ticker_id, start_date, end_date):
        query = f"""
        SELECT * FROM {self.table_name}
        WHERE ticker_id='{ticker_id}' AND
        date>='{start_date}' AND date<='{end_date}';
        """
        return self.session.execute(query)


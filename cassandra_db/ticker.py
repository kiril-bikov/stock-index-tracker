class Ticker:
    def __init__(self, last_updated, ticker_id, company):
        self.last_updated = last_updated
        self.ticker_id = ticker_id
        self.company = company

    def insert_in_table(self, session, table_name):
        query = f"""
        INSERT INTO {table_name} (last_updated, ticker_id, company)
        VALUES (%s, %s, %s)
        """
        session.execute(query, (self.last_updated, self.ticker_id, self.company))
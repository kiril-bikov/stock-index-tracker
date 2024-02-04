class IndexEntry:
    def __init__(self, ticker_id, company, close, date, high, low, open_amount, volume):
        self.ticker_id = ticker_id
        self.company = company
        self.date = date
        self.close = close
        self.high = high
        self.low = low
        self.open_amount = open_amount
        self.volume = volume

    def insert_in_table(self, session, table_name):
        query = f"""
        INSERT INTO {table_name}
        (ticker_id, company, date, close, high, low, open, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        session.execute(query, (self.ticker_id, self.company, self.date, self.close, self.high, self.low, self.open_amount, self.volume))


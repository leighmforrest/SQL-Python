import sqlite3
import csv
from pathlib import Path


def read_csv_data(file_path):
    """Read data from CSV and prepare it for insertion."""
    raw_data = []
    data = []
    print(f"Reading {file_path}")
    with open(file_path, newline="") as file:
        reader = csv.DictReader(file)
        raw_data = [
            {k: v for k, v in row.items() if k.strip() and v.strip()} for row in reader
        ]

    for row in raw_data:
        date = row["Date"]
        filtered_row = {k: v for k, v in row.items() if k != "Date"}
        for currency, rate in filtered_row.items():
            data.append({"date": date, "symbol": currency, "rate": rate})

    return data


def get_currency_set(data):
    return {row["symbol"] for row in data}


def drop_tables(con, cur):
    queries = [
        "DROP TABLE IF EXISTS rate_currency;",
        "DROP TABLE IF EXISTS rate;",
        "DROP TABLE IF EXISTS currency;",
    ]
    for query in queries:
        cur.execute(query)
    con.commit()


def create_tables(cur, con):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS currency (
            id INTEGER PRIMARY KEY,
            symbol TEXT NOT NULL UNIQUE
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rate (
            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            date DATE,
            rate FLOAT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rate_currency (
            currency_id INTEGER,
            rate_id INTEGER,
            FOREIGN KEY (currency_id) REFERENCES currency (id),
            FOREIGN KEY (rate_id) REFERENCES rate (id),
            PRIMARY KEY (currency_id, rate_id)
        )
    """)
    con.commit()


class Rate:
    def __init__(self, date, rate):
        self.date = date
        self.rate = rate
        self.id = None
    
    def save(self, cur, con):
        cur.execute(
            "INSERT INTO rate (date, rate) VALUES (?, ?)",
            (self.date, self.rate)
        )
        self.id = cur.lastrowid
        con.commit()
    
    def link_currency(self, currency_id, cur, con):
        if self.id:
            cur.execute(
                "INSERT OR IGNORE INTO rate_currency (currency_id, rate_id) VALUES (?, ?)",
                (currency_id, self.id)
            )
            con.commit()
            print(f"{currency_id} is set to {self.rate}")
    
    def get(self, cur):
        res = cur.execute(
            """
            SELECT rate.id, rate.date, rate.rate, currency.symbol
            FROM rate
            JOIN rate_currency ON rate.id = rate_currency.rate_id
            JOIN currency ON rate_currency.currency_id = currency.id
            WHERE rate.id = ?
            """, (self.id,)
        )
        return res.fetchall()
    
    @classmethod
    def get_all_by_symbol(cls, symbol, cur):
        currency_id = Currency.get_currency_id(symbol, cur)

        res = cur.execute(
            """
            SELECT rate.id, rate.date, rate.rate, currency.symbol
            FROM rate
            JOIN rate_currency ON rate.id = rate_currency.rate_id
            JOIN currency ON rate_currency.currency_id = currency.id
            WHERE rate_currency.currency_id = ?
            """, (currency_id,)
        )
        return res.fetchall()


class Currency:
    def __init__(self, symbol):
        self.symbol = symbol
        self.id = None

    def save(self, cur, con):
        cur.execute(
            "INSERT OR IGNORE INTO currency (symbol) VALUES (?)", 
            (self.symbol,)
        )
        self.id = cur.lastrowid or self.get_currency_id(self.symbol, cur)
        con.commit()
    
    @classmethod
    def get_currency_id(cls, symbol, cur):
        res = cur.execute(
            "SELECT id FROM currency WHERE symbol = ?", (symbol,)
        )
        result = res.fetchone()
        return result[0] if result else None

    def get(self, cur):
        res = cur.execute(
            "SELECT symbol, id FROM currency WHERE symbol = ?",
            (self.symbol,)
        )
        return dict(res.fetchone())


if __name__ == "__main__":
    csv_path = Path("data.csv")
    data = read_csv_data(csv_path)
    currency_set = get_currency_set(data)

    connection = sqlite3.connect("euro.sqlite3")
    cursor = connection.cursor()

    drop_tables(connection, cursor)
    create_tables(cursor, connection)

    # Insert currencies and map symbols to their IDs
    currency_objects = {}
    for symbol in currency_set:
        currency = Currency(symbol)
        currency.save(cursor, connection)
        currency_objects[symbol] = currency.id
    
    print(currency_objects)

    # Run a subset of data
    for item in data[:10000]:
        currency_id = currency_objects[item["symbol"]]
        rate = Rate(date=item["date"], rate=item["rate"])
        rate.save(cursor, connection)
        rate.link_currency(currency_id, cursor, connection)

    usd_rates = Rate.get_all_by_symbol('USD', cursor)
    
    # Print usd data
    print("\n\nPrinting USD data...\n\n")
    for rate in usd_rates:
        print(f"{rate[1]}|{rate[2]}")
    connection.close()

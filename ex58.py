import csv
import sqlite3
from pathlib import Path


def read_csv_data(file_path):
    """Read data csv and get the data ready for insertion."""
    raw_data = []
    data = []
    print(f"Reading {file_path}")
    with open(file_path, newline="") as file:
        reader = csv.DictReader(file)

        # Collect each filtered row in a list comprehension
        raw_data = [
            {k: v for k, v in row.items() if k.strip() and v.strip()} for row in reader
        ]

    for row in raw_data:
        # store date
        date = row["Date"]
        # pull out the date
        filtered_row = {k: v for k, v in row.items() if k != "Date"}

        for currency, rate in filtered_row.items():
            # turn each key into currency, value into rate, date into date
            data.append({"date": date, "currency": currency, "rate": rate})

    return data


def get_currency_set(data):
    currencies = {row["currency"] for row in data}
    return currencies


def drop_rate_table(con, cur):
    query = """
    DROP TABLE IF EXISTS rate;
"""
    cur.execute(query)
    con.commit()


def drop_currency_table(con, cur):
    query = """
    DROP TABLE IF EXISTS currency;
"""
    cur.execute(query)
    con.commit()


def create_rate_table(con, cur):
    query = """
    CREATE TABLE IF NOT EXISTS rate
    (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    date DATE, currency_id INTEGER, rate FLOAT);
"""
    cur.execute(query)
    con.commit()


def create_currency_table(con, cur):
    query = """
    CREATE TABLE IF NOT EXISTS currency
    (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, currency TEXT);
"""
    cur.execute(query)
    con.commit()


def display_rates(cur):
    res = cur.execute(
        """
    SELECT r.date, c.currency, r.rate
    FROM rate r
    JOIN currency c on r.currency_id = c.id
    WHERE r.rate IS NOT NULL;
    """
    )

    for items in res.fetchall():
        print(f"{items[0]}|{items[1]}|{items[2]}")


def insert_rate_record(con, cur, rate_dict_list, currencies):
    for record in rate_dict_list:
        currency_id = currencies[record["currency"]]
        record["currency_id"] = currency_id
        del record["currency"]

    cur.executemany(
        "INSERT INTO rate (date, currency_id, rate) VALUES(:date, :currency_id, :rate)",
        rate_dict_list,
    )
    con.commit()


def insert_currency_record(con, cur, currency_set):
    currency_data = [{"currency": currency} for currency in currency_set]
    cur.executemany("INSERT INTO currency (currency) VALUES(:currency)", currency_data)
    con.commit()


def get_currency_data(cur):
    res = cur.execute(
        """
    SELECT currency, id as currency_id
    FROM currency;
    """
    )

    return dict(res.fetchall())


def update_na_to_null(con, cur):
    cur.execute(
        """
        UPDATE rate
        SET rate=NULL
        WHERE rate='N/A'
        """
    )
    con.commit()


def get_2022_usd_avg(cur):
    res = cur.execute(
        """SELECT AVG(rate.rate)
           FROM rate, currency
           WHERE rate.currency_id = currency.id
           AND currency.currency = 'USD'
           AND strftime('%Y', rate.date) = '2022';"""
    )

    return res.fetchone()


def study_drill_4(cur):
    res = cur.execute(
        """SELECT count(*) as total, currency.currency
           FROM rate, currency
           WHERE rate.currency_id = currency.id
           AND rate.rate is null
           GROUP BY currency.currency
           ORDER BY total DESC;"""
    )

    return res.fetchall()


def get_jpy_min(cur):
    res = cur.execute(
        """SELECT min(rate.rate) FROM rate, currency
           WHERE currency.currency='JPY';"""
    )

    return res.fetchone()


if __name__ == "__main__":
    csv_path = Path(__file__).parent / "data.csv"
    con = sqlite3.connect("euro.sqlite3")
    cur = con.cursor()

    drop_rate_table(con, cur)
    drop_currency_table(con, cur)
    create_rate_table(con, cur)
    create_currency_table(con, cur)
    rate_records = read_csv_data(csv_path)
    currency_set = get_currency_set(rate_records)
    insert_currency_record(con, cur, currency_set)
    currencies = get_currency_data(cur)
    insert_rate_record(con, cur, rate_records, currencies)
    update_na_to_null(con, cur)
    display_rates(cur)

    avg_2022 = get_2022_usd_avg(cur)
    print(f"The average for USD in 2022 is {avg_2022[0]}")

    jpy_min = get_jpy_min(cur)
    print(f"The minimum rate of JPY for all years is {jpy_min[0]}")

    for total, currency in study_drill_4(cur):
        print(f"{total:5}|{currency}")

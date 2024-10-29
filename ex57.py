import shutil
import sqlite3
from datetime import datetime, timedelta

con = sqlite3.connect("euro.sqlite3")
cur = con.cursor()


restore = lambda: shutil.copyfile("./euro_backup.sqlite3", "./euro.sqlite3")


def run_many(con, cur, query, params=[]):
    res = cur.executemany(query, params)
    con.commit()

    return res


def run_transaction(con, cur, query, params=[]):
    try:
        res = cur.execute(query, params)
        con.commit()
        return res
    except sqlite3.Error as e:
        con.rollback()


def insert_record(con, cur, timeframe, dollar_value):
    query = """
    INSERT INTO euro (date, USD) 
    VALUES (date(:timeframe), :dollar_value);
    """

    params = [{"timeframe": timeframe, "dollar_value": dollar_value}]
    run_many(con, cur, query, params)


def update_record(con, cur, dollar_value, date):
    query = """
            UPDATE euro
            SET USD = :dollar_value, date = :date
            WHERE date(date) > date('2024-01-01') 
            """
    params = [{"date": date, "dollar_value": dollar_value}]
    run_many(con, cur, query, params)


def select_above_insert(cur):
    res = cur.execute(
        """
    SELECT date, USD
    FROM euro
    WHERE date(date) > date('2024-01-01')
    """
    )

    for items in res.fetchall():
        print(f"{items[0]}|{items[1]}")


def delete(con, cur):
    query = "DELETE FROM euro WHERE USD=1.1215"
    run_many(con, cur, query)


def delete_transaction(con, cur):
    query = """
SELECT count(*) FROM euro;
BEGIN TRANSACTION;

DELETE FROM euro;

ROLLBACK TRANSACTION;

SELECT count(*) FROM euro
    """
    res = run_transaction(con, cur, query)
    print(res)


if __name__ == "__main__":
    date_today = (datetime.now()).strftime("%Y-%m-%d")
    date_tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    update_date = (datetime(2048, 1, 1)).strftime("%Y-%m-%d")

    insert_record(con, cur, date_today, 1.09)
    insert_record(con, cur, date_tomorrow, 1.01)
    update_record(con, cur, 100, update_date)
    select_above_insert(cur)
    delete(con, cur)
    delete_transaction(con, cur)
    con.close()
    restore()

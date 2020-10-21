import os
import sqlite3
from datetime import datetime
import time

db_path = os.path.abspath(os.path.join(
    os.path.dirname(__file__), 'quote_history_aggregator.db'))
db_sql_path = os.path.abspath(os.path.join(
    os.path.dirname(__file__), 'setup.sql'))

def init():
    with open(db_sql_path) as f:
        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()
        cursor.executescript(f.read())

        connection.commit()
        connection.close()

def add_instruments(data: dict) -> None:
    connection = sqlite3.connect(db_path)
    cur = connection.cursor()
    
    # store data = {'description': 'est similique constituam', 'isin': 'DC0454155462'}
    timestamp = datetime.now().astimezone().replace(microsecond=0).isoformat()
    rowdata = [data['isin'], data['description'], timestamp]
    cur.execute("INSERT INTO instruments(isin, description, timestamp) VALUES (?, ?, ?)", rowdata)
    # log_message("INSERT INTO instruments(isin, description, timestamp): " + ','.join(rowdata))
    
    connection.commit()
    connection.close()

def del_instruments(data: dict) -> None:
    connection = sqlite3.connect(db_path)
    cur = connection.cursor()
    
    # delete data = {'description': 'est similique constituam', 'isin': 'DC0454155462'}
    cur.execute("DELETE FROM instruments WHERE isin = ?", [data['isin']])
    # log_message("DELETE FROM instruments WHERE isin = " + data['isin'])
    
    connection.commit()
    connection.close()

def add_quotes(data: dict) -> None:
    connection = sqlite3.connect(db_path)
    cur = connection.cursor()
    
    # store data = { "price": 1317.8947, "isin": "LK5537038107" }
    timestamp = datetime.now().astimezone().replace(microsecond=0).isoformat()
    rowdata = [data['isin'], data['price'], timestamp]
    cur.execute("INSERT INTO quotes(isin, price, timestamp) VALUES (?, ?, ?)", rowdata)
    # log_message("INSERT INTO quotes(isin, price, timestamp)")
    
    connection.commit()
    connection.close()

def fetch_price_of_all_instruments() -> dict:
     #  fetch from db
    connection = sqlite3.connect(db_path)
    cur = connection.cursor()

    sql_statement = """SELECT 
	                        instruments.isin, latest_prices.price
                        FROM
                            instruments
                        LEFT OUTER JOIN (SELECT
                                isin, price , MAX(timestamp) as price_timestamp
                        FROM
                            quotes
                        GROUP BY
                            isin) as latest_prices on latest_prices.isin = instruments.isin;"""
    cur.execute(sql_statement)
    rows = cur.fetchall()
    connection.close()
    return dict(rows)

def fetch_instrument_prices(isin: str) -> list:
    # fetch from db
    connection = sqlite3.connect(db_path)
    cur = connection.cursor()

    sql_statement = """
    SELECT 
	    price, strftime('%Y-%m-%d %H:%M:00', timestamp) AS candle_interval 
    FROM 
	    quotes 
    WHERE isin = ? AND timestamp >= Datetime('now', '-30 minutes', 'localtime');
    """
    cur.execute(sql_statement, (isin, ))
    rows = cur.fetchall()
    connection.close()
    return rows

def remove_db():
    if os.path.exists(db_path):
            os.remove(db_path)
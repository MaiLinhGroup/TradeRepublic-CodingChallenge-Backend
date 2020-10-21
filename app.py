import asyncio
import logging
import websockets
import sqlite3
import os
import json
from datetime import datetime
import time
from aiohttp import web

logging.basicConfig(level=logging.INFO)

db_path = os.path.abspath(os.path.join(
    os.path.dirname(__file__), 'quote_history_aggregator.db'))
db_sql_path = os.path.abspath(os.path.join(
    os.path.dirname(__file__), 'setup.sql'))

# Corountines
async def consume_instruments(host: str, port: int) -> None:
    ws_uri = f"ws://{host}:{port}/instruments"
    async with websockets.connect(ws_uri) as ws:
        async for message in ws:
            message = json.loads(message)
            if message['type'] == "ADD":
                add_instruments(message['data'])
            else: 
                del_instruments(message['data'])


async def consume_quotes(host: str, port: int) -> None:
    ws_uri = f"ws://{host}:{port}/quotes"
    async with websockets.connect(ws_uri) as ws:
        async for message in ws:
            message = json.loads(message)
            add_quotes(message['data'])

async def get_instruments_prices(request):
    return web.Response(text=json.dumps(fetch_price_of_all_instruments()))


async def get_price_history(request):
    isin = request.match_info['isin']

    rows = fetch_instrument_prices(isin)
    results = create_candle_sticks(rows)

    return web.Response(text=json.dumps(results))

async def web_server():
    app = web.Application()
    app.router.add_get('/prices', get_instruments_prices)
    app.router.add_get('/prices/{isin}', get_price_history)
    return app

# functions
def init():
    with open(db_sql_path) as f:
        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()
        cursor.executescript(f.read())

        connection.commit()
        connection.close()

def log_message(message: str) -> None:
    logging.info(f"Message: {message}")

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

def create_candle(row: list) -> dict:
    return {
        "openTimestamp": row[1], 
        "openPrice": row[0],
        "highPrice": row[0],
        "lowPrice": row[0],
        "closePrice": row[0],
        "closeTimestamp":row[1],
    }

def create_candle_sticks(rows: list) -> list:
    results = []

    current = create_candle(rows[0])
    for row in rows:
        (price, timestamp) = row

        if timestamp != current['openTimestamp']:
            current['closeTimestamp'] = timestamp
            results.append(current)
            current = create_candle(row)
        
        current['lowPrice'] = min(price, current['lowPrice'])
        current['highPrice'] = max(price, current['highPrice'])
        
        # last price of each row is always close price
        current['closePrice'] = price

    results.append(current)

    return results

if __name__ == "__main__":
    print('Create/Initialise db')
    init()
    loop = asyncio.get_event_loop()
    try:
        asyncio.ensure_future(consume_instruments(host="localhost", port=8080))
        asyncio.ensure_future(consume_quotes(host="localhost", port=8080))
        web.run_app(web_server(), host='127.0.0.1', port=8081)
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()
        if os.path.exists(db_path):
            os.remove(db_path)

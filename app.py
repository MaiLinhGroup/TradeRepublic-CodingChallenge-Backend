import asyncio
import logging
import websockets
import sqlite3
import os
import json
from datetime import datetime
import time




logging.basicConfig(level=logging.INFO)

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
            pass
            # log_message(message)
            # business logic here

def log_message(message: str) -> None:
    logging.info(f"Message: {message}")

def add_instruments(data: dict) -> None:
    connection = sqlite3.connect(db_path)
    cur = connection.cursor()
    
    # store data = {'description': 'est similique constituam', 'isin': 'DC0454155462'}
    timestamp = datetime.now().astimezone().replace(microsecond=0).isoformat()
    rowdata = [data['isin'], data['description'], timestamp]
    cur.execute("INSERT INTO instruments(isin, description, timestamp) VALUES (?, ?, ?)", rowdata)
    log_message("INSERT INTO instruments(isin, description, timestamp): " + ','.join(rowdata))
    
    connection.commit()
    connection.close()

def del_instruments(data: dict) -> None:
    connection = sqlite3.connect(db_path)
    cur = connection.cursor()
    
    # delete data = {'description': 'est similique constituam', 'isin': 'DC0454155462'}
    cur.execute("DELETE FROM instruments WHERE isin = ?", [data['isin']])
    log_message("DELETE FROM instruments WHERE isin = " + data['isin'])
    
    connection.commit()
    connection.close()

def add_quotes(data: dict) -> None:
    connection = sqlite3.connect(db_path)
    cur = connection.cursor()
    
    # store data = {'description': 'est similique constituam', 'isin': 'DC0454155462'}
    timestamp = datetime.now().astimezone().replace(microsecond=0).isoformat()
    rowdata = [data['isin'], data['description'], timestamp]
    cur.execute("INSERT INTO instruments(isin, description, timestamp) VALUES (?, ?, ?)", rowdata)
    
    connection.commit()
    connection.close()

# def del_quotes(data: dict) -> None:
#     connection = sqlite3.connect(db_path)
#     cur = connection.cursor()
    
#     # delete data = {'description': 'est similique constituam', 'isin': 'DC0454155462'}
#     cur.execute("DELETE FROM quotes WHERE isin = ?", [data['isin']])
#     log_message("DELETE FROM instruments WHERE isin = " + data['isin'])
    
#     connection.commit()
    

if __name__ == "__main__":
    print('Create/Initialise db')
    init()
    loop = asyncio.get_event_loop()
    try:
        asyncio.ensure_future(consume_instruments(host="localhost", port=8080))
        asyncio.ensure_future(consume_quotes(host="localhost", port=8080))
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()
        if os.path.exists(db_path):
            os.remove(db_path)

import asyncio
import logging
import websockets
import sqlite3
import os

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
            log_message(message)
            # business logic here

async def consume_quotes(host: str, port: int) -> None:
    ws_uri = f"ws://{host}:{port}/quotes"
    async with websockets.connect(ws_uri) as ws:
        async for message in ws:
            log_message(message)
            # business logic here

def log_message(message: str) -> None:
    logging.info(f"Message: {message}")

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

import asyncio
import logging
import websockets
import json
from aiohttp import web
import db

logging.basicConfig(level=logging.INFO)

# Corountines
async def consume_instruments(host: str, port: int) -> None:
    ws_uri = f"ws://{host}:{port}/instruments"
    async with websockets.connect(ws_uri) as ws:
        async for message in ws:
            message = json.loads(message)
            if message['type'] == "ADD":
                db.add_instruments(message['data'])
            else: 
                db.del_instruments(message['data'])


async def consume_quotes(host: str, port: int) -> None:
    ws_uri = f"ws://{host}:{port}/quotes"
    async with websockets.connect(ws_uri) as ws:
        async for message in ws:
            message = json.loads(message)
            db.add_quotes(message['data'])

async def get_instruments_prices(request):
    return web.Response(text=json.dumps(db.fetch_price_of_all_instruments()))


async def get_price_history(request):
    isin = request.match_info['isin']

    rows = db.fetch_instrument_prices(isin)
    results = create_candle_sticks(rows)

    return web.Response(text=json.dumps(results))

async def web_server():
    app = web.Application()
    app.router.add_get('/prices', get_instruments_prices)
    app.router.add_get('/prices/{isin}', get_price_history)
    return app

# functions
def log_message(message: str) -> None:
    logging.info(f"Message: {message}")

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
    db.init()
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
        db.remove_db()

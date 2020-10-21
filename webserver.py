from aiohttp import web
import db
import asyncio
import json


# coroutines
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

def run():
    web.run_app(web_server(), host='127.0.0.1', port=8081)
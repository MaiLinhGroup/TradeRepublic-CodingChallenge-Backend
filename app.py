import asyncio
import websockets
import json
import db
import webserver

HOST = "localhost"
WS_PORT = 8080
WEBSERVER_PORT = 8081

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

if __name__ == "__main__":
    print('Create/Initialise db')
    db.init()
    loop = asyncio.get_event_loop()
    try:
        asyncio.ensure_future(consume_instruments(host=HOST, port=WS_PORT))
        asyncio.ensure_future(consume_quotes(host=HOST, port=WS_PORT))
        webserver.run(HOST, WEBSERVER_PORT)
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()
        db.remove_db()

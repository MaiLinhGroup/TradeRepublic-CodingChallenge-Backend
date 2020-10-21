import asyncio
import logging
import websockets

logging.basicConfig(level=logging.INFO)

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
    loop = asyncio.get_event_loop()
    try:
        asyncio.ensure_future(consume_instruments(host="localhost", port=8080))
        asyncio.ensure_future(consume_quotes(host="localhost", port=8080))
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()

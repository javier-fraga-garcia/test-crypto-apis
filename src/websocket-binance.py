import sys
import asyncio
import websockets
import json
from pydantic import BaseModel, Field


class TradeEvent(BaseModel):
    event_type: str = Field(alias="e")
    event_time: int = Field(alias="E")
    symbol: str = Field(alias="s")
    trade_id: int = Field(alias="t")
    price: float = Field(alias="p")
    quantity: float = Field(alias="q")
    m: bool = Field(alias="m")


class MultiTradeStreamResponse(BaseModel):
    stream: str
    data: TradeEvent


async def trade_stream(url: str) -> None:
    async with websockets.connect(url) as ws:
        print(f"Conectado a {url}\n")

        async for message in ws:
            res = json.loads(message)
            trade = TradeEvent(**res)

            print(trade)


async def multi_stream(url: str, tickers: list[str]) -> None:
    async with websockets.connect(url) as ws:
        subscribe_msg = json.dumps({"method": "SUBSCRIBE", "params": tickers, "id": 1})

        await ws.send(subscribe_msg)
        confirmation = json.loads(await ws.recv())
        print(f"Suscripción: {confirmation}\n")

        async for message in ws:
            res = json.loads(message)
            trade = MultiTradeStreamResponse(**res)
            print(trade)


if __name__ == "__main__":
    SINGLE_STREAM_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    MULTI_STREAM_URL = "wss://stream.binance.com:9443/stream"
    MULTI_STREAM_TICKERS = ["btcusdt@trade", "ethusdt@trade"]

    try:
        # asyncio.run(trade_stream(SINGLE_STREAM_URL))
        asyncio.run(multi_stream(MULTI_STREAM_URL, MULTI_STREAM_TICKERS))
    except KeyboardInterrupt:
        print("Programa cancelado por el usuario")
        sys.exit(1)
    except Exception as e:
        print("Algo salió mal\n")
        print(e)
        sys.exit(1)

import sys
import asyncio
import random
import time
import json
import websockets
import sqlite3
import zlib
import uuid

async def ingest_raw_data(ws_url: str, tickers: list[str], batch_size: int = 1000, flush_interval: float = 5.0):
    con = sqlite3.connect('raw_events.db')
    con.execute('PRAGMA journal_mode=WAL;')
    con.execute('PRAGMA synchronous=NORMAL;')

    con.execute('''
        CREATE TABLE IF NOT EXISTS events (
        batch_id TEXT,
        time REAL,
        stream_type TEXT,
        payload BLOB
    )
    ''')

    con.commit()

    buffer = []
    last_flush = time.time()

    print(f'Conectando a {ws_url}...')

    try:
        async with websockets.connect(ws_url) as websocket:
            subscribe_msg = json.dumps({"method": "SUBSCRIBE", "params": tickers, "id": random.randint(1, 10000)})
            await websocket.send(subscribe_msg)
            confirmation = json.loads(await websocket.recv())
            print(f"Suscripción: {confirmation}")
            async for message in websocket:
                now = time.time()
                compress_payload = zlib.compress(message.encode('utf-8'))
                buffer.append((now, compress_payload))    
                if len(buffer) >= batch_size or (now - last_flush) >= flush_interval:

                    batch_id = str(uuid.uuid4())
                    batch_data = [(batch_id, ts, 'agg_trade', payload) for ts, payload in buffer]

                    con.executemany('INSERT INTO events (batch_id, time, stream_type, payload) VALUES (?, ?, ?, ?)', batch_data)
                    con.commit()
                    print(f'[{now}] Insertado lote de {len(buffer)} eventos.')

                    buffer.clear()

                    last_flush = now
    except websockets.exceptions.ConnectionClosed:
        print('Conexión con el websocket cerrada')
    finally:
        if buffer:
            batch_id = str(uuid.uuid4())
            batch_data = [(batch_id, ts, 'agg_trade', payload) for ts, payload in buffer]
            con.executemany('INSERT INTO events (batch_id, time, stream_type, payload) VALUES (?, ?, ?, ?)', buffer)
            con.commit()
            print(f'Guardado lote final con {len(buffer)} eventos')
        con.close()

if __name__ == '__main__':
    try:
        asyncio.run(ingest_raw_data(ws_url='wss://stream.binance.com:9443/stream', tickers=["btcusdt@aggTrade", "ethusdt@aggTrade", "solusdt@aggTrade"]))
    except KeyboardInterrupt:
        print('El usuario ha cancelado el proceso. Saliendo...')
        sys.exit(0)
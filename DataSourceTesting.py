import asyncio
import json
import websockets

async def producer():
    uri = "wss://wss.whitebit.com/ws"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                # 1. Подписка на trades (публичный канал)
                await ws.send(json.dumps({
                    "id": 1,
                    "method": "trades_subscribe",
                    "params": ["BTC_USDT"]
                }))
                
                # 2. Пинг в фоне (каждые 50 сек)
                async def ping_loop():
                    while True:
                        await asyncio.sleep(50)
                        await ws.send(json.dumps({"id": 0, "method": "ping", "params": []}))
                
                ping_task = asyncio.create_task(ping_loop())
                
                # 3. Основной цикл чтения
                async for message in ws:
                    data = json.loads(message)
                    # Здесь будет отправка в Kafka (позже)
                    print(data)  # пока просто смотрим
                    
        except (websockets.exceptions.ConnectionClosed, OSError):
            print("Соединение упало, переподключаемся...")
            await asyncio.sleep(2)  # потом заменим на backoff

asyncio.run(producer())
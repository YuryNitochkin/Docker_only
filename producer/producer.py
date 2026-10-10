import asyncio
import json
import os

import websockets
from confluent_kafka import Producer

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "raw-data"
WS_URL = "wss://wss.whitebit.com/ws"


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}]")


async def producer_loop():
    kafka_producer = Producer({"bootstrap.servers": KAFKA_SERVERS})

    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                print("Connected to WhiteBIT")

                # Подписка на trades
                await ws.send(json.dumps({
                    "id": 1,
                    "method": "trades_subscribe",
                    "params": ["BTC_USDT"]
                }))

                # Пинг каждые 50 секунд
                async def ping_loop():
                    while True:
                        await asyncio.sleep(50)
                        await ws.send(json.dumps({
                            "id": 0,
                            "method": "ping",
                            "params": []
                        }))

                ping_task = asyncio.create_task(ping_loop())

                try:
                    async for message in ws:
                        data = json.loads(message)

                        # Игнорируем pong и прочие служебные сообщения
                        if data.get("method") != "trades_update":
                            continue

                        # Отправляем в Kafka
                        kafka_producer.produce(
                            topic=TOPIC,
                            value=json.dumps(data).encode("utf-8"),
                            callback=delivery_report,
                        )
                        kafka_producer.poll(0)

                finally:
                    ping_task.cancel()

        except (websockets.exceptions.ConnectionClosed, OSError) as e:
            print(f"Connection lost: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(producer_loop())
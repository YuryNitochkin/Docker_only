import json
import os
import signal

import duckdb
from confluent_kafka import Consumer, KafkaError

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = "raw-data"
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/warehouse.duckdb")

running = True


def shutdown(signum, frame):
    global running
    print("Получен сигнал завершения, останавливаемся...")
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def init_db(con):
    """Создаём таблицу для сырых JSON-сообщений."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_trades (
            payload      JSON,
            ingested_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("Таблица raw_trades готова.")


def main():
    print(f"Подключаемся к Kafka: {KAFKA_SERVERS}")
    consumer = Consumer({
        "bootstrap.servers": KAFKA_SERVERS,
        "group.id": "duckdb-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC])

    print(f"Открываем DuckDB: {DUCKDB_PATH}")
    con = duckdb.connect(DUCKDB_PATH)
    init_db(con)

    print("Начинаем читать сообщения...")
    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"Ошибка Kafka: {msg.error()}")
            continue

        try:
            raw = msg.value().decode("utf-8")
            data = json.loads(raw)

            # Пишем только trades_update, остальное игнорируем
            if data.get("method") != "trades_update":
                continue

            con.execute(
                "INSERT INTO raw_trades (payload) VALUES (?)",
                [raw],
            )
            print("Записано 1 сообщение в raw_trades")
        except Exception as e:
            print(f"Ошибка обработки сообщения: {e}")

    print("Закрываем соединения...")
    consumer.close()
    con.close()
    print("Готово.")


if __name__ == "__main__":
    main()
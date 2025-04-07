import hashlib
import os
from dotenv import load_dotenv
from my_venv.src.schemas import EventBatch, DedupeResult, Event

from clickhouse_driver import Client
from datetime import datetime

load_dotenv()

''' Запускаю clickhouse в Docker с гарантированно отключённой аутентификацией: 
docker run -d `
    --name clickhouse-server `
    -p 8123:8123 -p 9000:9000 `
    -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 `
    -e CLICKHOUSE_USER=default `
    -e CLICKHOUSE_PASSWORD="" `
    clickhouse/clickhouse-server:latest
'''

# Чтоб гарантировано подключиться сделаем так
class ClickHouseRepository:
    def __init__(self):
        self.client = Client(
            host="localhost",  # Явно указываем хост
            port=9000,  # Явно указываем порт
            user="default",  # Явно указываем пользователя
            password="",  # Явно пустой пароль
            settings={'connect_timeout': 10}  # Таймаут подключения
        )
        self._init_db()

# class ClickHouseRepository:
#     def __init__(self):
#         self.client = Client(
#             host=os.getenv("CLICKHOUSE_HOST", "localhost"),
#             port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
#             user=os.getenv("CLICKHOUSE_USER", "default"),
#             password=os.getenv("CLICKHOUSE_PASSWORD", ""),
#             settings={'connect_timeout': 3}
#         )
#         self._init_db()

    def _init_db(self):
        self.client.execute("""  
        CREATE TABLE IF NOT EXISTS event_hashes (  
            hash String,  
            timestamp DateTime DEFAULT now()  
        ) ENGINE = ReplacingMergeTree()  
        ORDER BY hash  
        TTL timestamp + INTERVAL 7 DAY  
        """)


    def check_hash(self, event_hash: str) -> bool:
        # Проверяем, существует ли хеш
        result = self.client.execute(
            "SELECT 1 FROM event_hashes WHERE hash = %(hash)s LIMIT 1",
            {'hash': event_hash}
        )
        return len(result) > 0

    def insert_hash(self, event_hash: str):
        # Вставляем хеш в ClickHouse через запрос с параметрами
        self.client.execute(
            "INSERT INTO event_hashes (hash) VALUES (%(hash)s)",
            {'hash': event_hash}
        )

    def deduplicate_events(self, event_batch: EventBatch) -> DedupeResult:
        duplicate_hashes = []
        unique_count = 0

        for event in event_batch.events:
            # Генерируем стабильный хеш в виде строки
            hash_input = f"{event.client_id}_{event.event_datetime.isoformat()}".encode('utf-8')
            event_hash = hashlib.sha256(hash_input).hexdigest()

            if self.check_hash(event_hash):
                duplicate_hashes.append(event_hash)
            else:
                self.insert_hash(event_hash)
                unique_count += 1

        return DedupeResult(
            status="Processed",
            duplicates=len(duplicate_hashes),
            unique_events=unique_count,
            duplicate_hashes=duplicate_hashes
        )


# Это для меня
if __name__ == "__main__":
    repo = ClickHouseRepository()

    # Батч событий
    events = EventBatch(events=[
        Event(event_name="Event1", client_id="123", event_datetime=datetime.now(), payload={"data": "value1"}),
        Event(event_name="Event2", client_id="123", event_datetime=datetime.now(), payload={"data": "value2"}),
        Event(event_name="Event1", client_id="123", event_datetime=datetime.now(), payload={"data": "value1"}),

    ])

    # Обрабатываем события
    result = repo.deduplicate_events(events)
    print(f"Dedupe Result: {result.model_dump_json(indent=4)}")
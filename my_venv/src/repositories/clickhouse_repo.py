from clickhouse_driver import Client
from datetime import datetime
from dotenv import load_dotenv
from my_venv.src.schemas import EventBatch, DedupeResult, Event

load_dotenv()


class ClickHouseRepository:
    def __init__(self):
        self.client = Client(host="localhost")
        self._init_db()

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
        # Вставляем хеш в ClickHouse через параметризированный запрос
        self.client.execute(
            "INSERT INTO event_hashes (hash) VALUES (%(hash)s)",
            {'hash': event_hash}
        )

    def deduplicate_events(self, event_batch: EventBatch) -> DedupeResult:
        duplicate_hashes = []
        unique_count = 0

        for event in event_batch.events:
            # Генерируем хеш события (например, на основе client_id и event_datetime)
            event_hash = hash((event.client_id, event.event_datetime.isoformat()))

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
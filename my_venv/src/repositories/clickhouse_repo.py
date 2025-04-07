import hashlib
from clickhouse_driver import Client
from datetime import datetime
from my_venv.src.schemas import Event, EventBatch, DedupeResponse
import logging

from my_venv.src.utils.logger import logger

''' Запускаю clickhouse в Docker с гарантированно отключённой аутентификацией: 
docker run -d `
    --name clickhouse-server `
    -p 8123:8123 -p 9000:9000 `
    -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 `
    -e CLICKHOUSE_USER=default `
    -e CLICKHOUSE_PASSWORD="" `
    clickhouse/clickhouse-server:latest
'''


class ClickHouseRepository:
    def __init__(self):
        self.client = Client(
            host='localhost',
            port=9000,
            user='default',
            password='',
            settings={
                'max_block_size': 100000,
                'use_client_time_zone': True
            }
        )
        self._init_db()

    def _init_db(self):
        try:
            self.client.execute("""
            CREATE TABLE IF NOT EXISTS event_hashes (
                hash String,
                timestamp DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree()
            ORDER BY hash
            TTL timestamp + INTERVAL 7 DAY
            """)
            logger.info("Table initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing table: {e}")
            raise

    def _generate_hash(self, event: Event) -> str:
        client_id = event.client_id or event.user_id or ""
        data = f"{event.event_name}:{client_id}:{event.event_datetime.isoformat()}"
        return hashlib.sha256(data.encode()).hexdigest()

    def deduplicate_batch(self, event_batch: EventBatch) -> DedupeResponse:
        unique_count = 0
        duplicate_count = 0

        for event in event_batch.events:
            try:
                event_hash = self._generate_hash(event)

                # Правильный синтаксис запроса с параметрами
                result = self.client.execute(
                    "SELECT 1 FROM event_hashes WHERE hash = %(hash)s LIMIT 1",
                    {'hash': event_hash}
                )

                if not result:
                    # Вставка с правильным синтаксисом параметров
                    self.client.execute(
                        "INSERT INTO event_hashes (hash) VALUES (%(hash)s)",
                        {'hash': event_hash}
                    )
                    unique_count += 1
                    logger.debug(f"New event: {event.event_name}")
                else:
                    duplicate_count += 1
                    logger.debug(f"Duplicate: {event.event_name}")

            except Exception as e:
                logger.error(f"Error processing event: {e}")
                duplicate_count += 1
                continue

        return DedupeResponse(
            status='completed',
            processed_events=len(event_batch.events),
            duplicates=duplicate_count,
            unique_events=unique_count
        )


if __name__ == "__main__":
    # Тестовые данные с корректными датами
    test_events = {
        "events": [
            {
                "event_name": "video_play",
                "event_datetime": datetime.now().isoformat(),
                "user_id": "user123",
                "payload": {"video_id": "xyz123"}
            },
            {
                "event_name": "video_play",
                "event_datetime": datetime.now().isoformat(),
                "client_id": "user123",
                "payload": {"video_id": "xyz123"}
            },
            {
                "event_name": "app_launch",
                "event_datetime": datetime.now().isoformat(),
                "client_id": "user456"
            }
        ]
    }

    # Инициализация и тестирование
    repo = ClickHouseRepository()
    batch = EventBatch(**test_events)
    result = repo.deduplicate_batch(batch)

    print("\nResults:")
    print(f"Processed: {result.processed_events}")
    print(f"Unique: {result.unique_events}")
    print(f"Duplicates: {result.duplicates}")
    print(f"Status: {result.status}")
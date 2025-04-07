import logging
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError

from src.repositories.redis_repo import logger

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
        # self.client = Client(
        #     host='localhost',
        #     port=8123,
        #     user='default',
        #     password='',
        #     settings={'use_client_time_zone': True}
        # )
        self.client = Client(host='localhost', port=9000)
        self._ensure_table_structure()

    def _ensure_table_structure(self):
        """Гарантирует правильную структуру таблицы"""
        try:
            # Проверяем существование таблицы
            if not self._table_exists():
                self._create_table()
            else:
                self._validate_table_structure()
        except ClickHouseError as e:
            logger.error(f"Table verification failed: {e}")
            raise

    def _table_exists(self) -> bool:
        """Проверяет существование таблицы"""
        result = self.client.execute(
            "EXISTS TABLE event_hashes"
        )
        return bool(result[0][0]) if result else False

    def _create_table(self):
        """Создает таблицу с правильной структурой"""
        self.client.execute("""
        CREATE TABLE event_hashes (
            hash String,
            user_id String,
            client_id String,
            event_name String,
            normalized_datetime DateTime,
            timestamp DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree()
        ORDER BY hash
        """)
        logger.info("Table created successfully")

    def _validate_table_structure(self):
        """Проверяет соответствие структуры таблицы"""
        expected_columns = {
            'hash', 'user_id', 'client_id',
            'event_name', 'normalized_datetime', 'timestamp'
        }
        result = self.client.execute(
            "SELECT name FROM system.columns WHERE table = 'event_hashes'"
        )
        existing_columns = {row[0] for row in result}

        if not expected_columns.issubset(existing_columns):
            logger.error(f"Table structure mismatch. Expected: {expected_columns}, found: {existing_columns}")
            raise ValueError("Table structure mismatch")

    def _init_db(self):
        """Инициализация таблицы с правильной структурой"""
        try:
            self.client.execute("""
            CREATE TABLE IF NOT EXISTS event_hashes (
                hash String,
                user_id String,
                client_id String,
                event_name String,
                normalized_datetime DateTime,
                timestamp DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree()
            ORDER BY hash
            """)
            logger.info("ClickHouse table initialized")
        except ClickHouseError as e:
            logger.error(f"Table initialization error: {e}")
            raise

    def check_hash(self, event_hash: str) -> bool:
        """Проверка наличия хэша (синхронный метод)"""
        try:
            result = self.client.execute(
                "SELECT 1 FROM event_hashes FINAL WHERE hash = %(hash)s LIMIT 1",
                {'hash': event_hash}
            )
            return bool(result)
        except ClickHouseError as e:
            logger.error(f"Error checking hash: {e}")
            return False

    def insert_hash(self, event_hash: str, event: dict) -> None:
        """Вставка хэша (синхронный метод)"""
        try:
            self.client.execute(
                """INSERT INTO event_hashes 
                (hash, user_id, client_id, event_name, normalized_datetime) 
                VALUES""",
                [{
                    'hash': event_hash,
                    'user_id': event.get('user_id', ''),
                    'client_id': event.get('client_id', ''),
                    'event_name': event['event_name'],
                    'normalized_datetime': event['event_datetime']
                }]
            )
            logger.debug(f"Inserted hash: {event_hash}")
        except ClickHouseError as e:
            logger.error(f"Error inserting hash: {e}")
            raise

    def delete_hash(self, event_hash: str) -> None:
        """Удаление хэша (синхронный метод)"""
        try:
            self.client.execute(
                "ALTER TABLE event_hashes DELETE WHERE hash = %(hash)s",
                {'hash': event_hash}
            )
            logger.debug(f"Deleted hash: {event_hash}")
        except ClickHouseError as e:
            logger.error(f"Error deleting hash: {e}")
            raise

    def close(self):
        """Закрытие соединения"""
        self.client.disconnect()
"""
Репозиторий для работы с ClickHouse - хранение и проверка событий
"""
from clickhouse_driver import Client
from src.repositories.redis_repo import logger

# Явно указываем user и password для анонимного пользователя, для запуска из Docker
class ClickHouseRepository:
    def __init__(self, host: str = 'localhost', port: int = 9000, user: str = 'default', password: str = 'password'):
        """
        Инициализация подключения к ClickHouse.

        Args:
            host: Хост ClickHouse сервера
            port: Порт для подключения (по умолчанию 9000 - native protocol)
            user: Имя пользователя для подключения
            password: Пароль для подключения
        """
        self.client = Client(host=host, port=port, user=user, password=password)
        self._ensure_table_structure()

    def _ensure_table_structure(self):
        """Проверяет и создает таблицу при необходимости"""
        if not self._table_exists():
            self._create_table()

    def _table_exists(self) -> bool:
        """Проверяет существование таблицы event_hashes"""
        result = self.client.execute("EXISTS TABLE event_hashes")
        return bool(result[0][0]) if result else False

    def _create_table(self):
        """Создает таблицу для хранения хэшей событий"""
        self.client.execute("""  
        CREATE TABLE event_hashes (  
            hash String,  
            user_id String,  
            client_id String,  
            event_name String,  
            event_datetime DateTime,  
            timestamp DateTime DEFAULT now()  
        ) ENGINE = ReplacingMergeTree()  
        ORDER BY (event_datetime, hash)  
        """)  # Сортировка по дате и хэшу.
        logger.info("ClickHouse table created")

    def check_hash(self, event_hash: str) -> bool:
        """
        Проверяет наличие хэша в базе.

        Args:
            event_hash: SHA256 хэш события

        Returns:
            bool: True если хэш найден
        """
        result = self.client.execute(
            "SELECT 1 FROM event_hashes WHERE hash = %(hash)s LIMIT 1",
            {'hash': event_hash}
        )
        return bool(result)

    def insert_hash(self, event_hash: str, event: dict):
        """
        Сохраняет хэш и данные события в базу.

        Args:
            event_hash: SHA256 хэш события
            event: Словарь с данными события
        """
        self.client.execute(
            """INSERT INTO event_hashes   
            (hash, user_id, client_id, event_name, event_datetime)   
            VALUES""",
            [{
                'hash': event_hash,
                'user_id': event.get('user_id', ''),
                'client_id': event.get('client_id', ''),
                'event_name': event['event_name'],
                'event_datetime': event['event_datetime']
            }]
        )

    def close(self):
        """Закрывает соединение с ClickHouse"""
        self.client.disconnect()
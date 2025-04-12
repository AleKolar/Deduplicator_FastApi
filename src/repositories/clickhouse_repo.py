from clickhouse_driver import Client
from clickhouse_driver.errors import Error
from typing import Dict
import logging

from fastApiProject_Deduplicator.src.config import CLICKHOUSE_TABLE, CLICKHOUSE_HOST, CLICKHOUSE_BUFFER_SIZE
from fastApiProject_Deduplicator.src.utils.logger import logger


class ClickHouseRepo:
    def __init__(self):
        self.client = Client(host=CLICKHOUSE_HOST)
        self.buffer = []
        self._init_table()

    def _init_table(self):
        try:
            self.client.execute(f"""  
                CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE} (  
                    event_hash String,  
                    event_name LowCardinality(String),  
                    profile_id LowCardinality(String),  
                    event_datetime DateTime,  
                    content_id LowCardinality(String),  
                    event_date Date  
                ) ENGINE = MergeTree()  
                ORDER BY (event_date, event_hash)  
                TTL event_date + INTERVAL 7 DAY  
                SETTINGS storage_policy = 'compressed'  
            """)
        except Error as e:
            logger.error(f"ClickHouse error: {e}")
            raise

    def add_to_buffer(self, event: Dict):
        self.buffer.append(event)
        if len(self.buffer) >= CLICKHOUSE_BUFFER_SIZE:
            self.flush()

    def flush(self):
        if not self.buffer:
            return
        try:
            self.client.execute(
                f'INSERT INTO {CLICKHOUSE_TABLE} (*) VALUES',
                self.buffer
            )
            self.buffer.clear()
        except Error as e:
            logging.error(f"ClickHouse insert failed: {e}")
            raise




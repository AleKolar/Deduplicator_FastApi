from ..repositories.clickhouse_repo import ClickHouseRepository
from ..repositories.redis_repo import RedisRepository
from ..utils.hashing import generate_json_hash
import asyncio

class Deduplicator:
    def __init__(self):
        self.redis = RedisRepository()
        self.ch = ClickHouseRepository()
        # self.lock = asyncio.Lock()

    async def is_duplicate(self, event: dict) -> bool:
        event_hash = generate_json_hash(event)

        # Сначала проверяем в Redis (быстро)
        if await self.redis.check_hash(event_hash):
            return True

        # Если нет в Redis, ищем в ClickHouse
        return await self.ch.check_hash(event_hash)

    async def mark_as_processed(self, event: dict):
        event_hash = generate_json_hash(event)
        await asyncio.gather(
            self.redis.store_hash(event_hash),
            self.ch.insert_hash(event_hash))
import asyncio
from typing import Any, Coroutine

from src.repositories.redis_repo import RedisRepository
from src.repositories.clickhouse_repo import ClickHouseRepository
from src.repositories.hasher_repo import EventHasher
from src.services.preparation_service import DataPreparationService
from src.utils.logger import logger


class Deduplicator:
    def __init__(self):
        self.redis = RedisRepository()
        self.ch = ClickHouseRepository()
        self.hasher = EventHasher()
        self._initialized = False

    async def initialize(self) -> None:
        try:
            await self.redis.connect()
            self._initialized = True
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            raise

    async def is_duplicate(self, event: dict) -> bool | Coroutine[Any, Any, bool]:
        if not self._initialized:
            raise RuntimeError("Deduplicator not initialized")

        event_hash = self.hasher.generate_hash(event)

        try:
            if await self.redis.check_hash(event_hash):
                return True

            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None,
                self.ch.check_hash,
                event_hash
            )
        except Exception as e:
            logger.error(f"Duplicate check failed: {e}")
            raise

    async def mark_as_processed(self, event: dict) -> None:
        if not self._initialized:
            raise RuntimeError("Deduplicator not initialized")

        event_hash = self.hasher.generate_hash(event)

        try:
            await asyncio.gather(
                self.redis.store_hash(event_hash),
                asyncio.get_event_loop().run_in_executor(
                    None,
                    self.ch.insert_hash,
                    event_hash,
                    event
                )
            )
        except Exception as e:
            logger.error(f"Failed to mark as processed: {e}")
            raise

    async def close(self) -> None:
        try:
            await self.redis.close()
        except Exception as e:
            logger.error(f"Error closing deduplicator: {e}")
        finally:
            self._initialized = False


async def main():
    """Пример использования с предподготовкой"""
    prep_service = DataPreparationService()
    deduplicator = Deduplicator()

    try:
        # Предзагрузка данных за последний день
        await prep_service.prepare_redis_cache(days=1)

        await deduplicator.initialize()

        test_event = {
            'user_id': 'user123',
            'client_id': 'client456',
            'event_name': 'page_view',
            'event_datetime': '2023-01-01 12:00:00'
        }

        if not await deduplicator.is_duplicate(test_event):
            await deduplicator.mark_as_processed(test_event)

    finally:
        await deduplicator.close()


if __name__ == "__main__":
    asyncio.run(main())
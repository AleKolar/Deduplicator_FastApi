import asyncio
from datetime import datetime, timedelta
from typing import List

from src.repositories.clickhouse_repo import ClickHouseRepository
from src.repositories.redis_repo import RedisRepository

from src.utils.logger import logger


class DataPreparationService:
    def __init__(self):
        self.redis = RedisRepository()
        self.ch = ClickHouseRepository()

    async def prepare_redis_cache(self, days: int = 1) -> None:
        """Предзагрузка последних N дней данных в Redis"""
        try:
            # Подключаемся к хранилищам
            await asyncio.gather(
                self.redis.connect(),
                self.ch.connect()
            )

            # Получаем хэши
            hashes = await self._get_recent_hashes(days)

            if hashes:
                # Используем новый метод batch_store_hashes
                await self.redis.batch_store_hashes(hashes)
                logger.info(f"Successfully preloaded {len(hashes)} hashes to Redis")
            else:
                logger.warning("No hashes found for preloading")

        except Exception as e:
            logger.error(f"Preparation failed: {e}")
            raise
        finally:
            # Всегда закрываем соединения
            await asyncio.gather(
                self.redis.close(),
                self.ch.close(),
                return_exceptions=True  # Игнорируем ошибки закрытия
            )

    async def _get_recent_hashes(self, days: int) -> List[str]:
        """Получение хэшей за последние N дней из ClickHouse"""
        end_date = datetime.datetime.utcnow()
        start_date = end_date - timedelta(days=days)

        query = """
        SELECT hash FROM event_hashes 
        WHERE timestamp >= %(start_date)s 
          AND timestamp <= %(end_date)s
        """

        try:
            result = await self.ch.execute_query(query, {
                'start_date': start_date.strftime('%Y-%m-%d %H:%M:%S'),
                'end_date': end_date.strftime('%Y-%m-%d %H:%M:%S')
            })
            return [row[0] for row in result] if result else []
        except Exception as e:
            logger.error(f"Error getting recent hashes: {e}")
            return []


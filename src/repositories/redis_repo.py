""" Redis для кеширования хэшей событий """
import os
from datetime import timedelta
import logging
from typing import Optional, Union
from redis.asyncio import Redis
from redis.exceptions import RedisError, ConnectionError

logger = logging.getLogger(__name__)

class RedisRepository:
    def __init__(self):
        self.url = os.getenv("REDIS_URL", "redis://localhost:6379")
        self.redis: Optional[Redis] = None
        self.ttl = timedelta(days=7)
        self._is_connected = False

    async def connect(self) -> None:
        if self._is_connected:
            return

        try:
            self.redis = Redis.from_url(
                self.url,
                socket_timeout=5,
                socket_connect_timeout=5,
                decode_responses=True
            )
            if not await self.redis.ping():
                raise ConnectionError("Redis ping failed")
            self._is_connected = True
            logger.info("Redis connection established")
        except Exception as e:
            await self._safe_close()
            logger.error(f"Redis connection failed: {e}")
            raise

# import os
# from datetime import timedelta
# import logging
# from typing import Optional, List, Union
#
# try:
#     # Попробуем импорт для redis >= 4.0
#     from redis.asyncio import Redis
#     from redis.exceptions import RedisError, ConnectionError
#
#     ASYNCIO_REDIS = True
# except ImportError:
#     # Fallback для старых версий
#     try:
#         import aioredis
#         from aioredis import Redis
#         from aioredis.exceptions import RedisError, ConnectionError
#
#         ASYNCIO_REDIS = False
#     except ImportError:
#         raise ImportError("Требуется redis>=4.0 или aioredis")



    async def check_hash(self, event_hash: str) -> Union[int, bool]:
        if not self._is_connected or self.redis is None:
            raise RuntimeError("Redis connection not established")

        try:
            return await self.redis.exists(f"event:{event_hash}")
        except RedisError as e:
            logger.error(f"Error checking hash: {e}")
            return False

    async def store_hash(self, event_hash: str) -> None:
        if not self._is_connected or self.redis is None:
            raise RuntimeError("Redis connection not established")

        try:
            await self.redis.setex(
                f"event:{event_hash}",
                int(self.ttl.total_seconds()),
                "1"
            )
        except RedisError as e:
            logger.error(f"Error storing hash: {e}")
            raise

    async def close(self) -> None:
        await self._safe_close()

    async def _safe_close(self) -> None:
        try:
            if self.redis:
                await self.redis.close()
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
        finally:
            self.redis = None
            self._is_connected = False

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


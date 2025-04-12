import os

from redis import Redis
from redis.exceptions import RedisError
import logging

from fastApiProject_Deduplicator.src.config import REDIS_HOST, REDIS_PORT, REDIS_TTL_DAYS



class RedisRepo:
    def __init__(self):
        self.redis = Redis.from_url(os.getenv("REDIS_URL"))
        self.ttl = REDIS_TTL_DAYS * 86400

    def increment_counter(self, key: str) -> int:
        try:
            count = self.redis.incr(key)
            if count == 1:
                self.redis.expire(key, self.ttl)
            return count
        except RedisError as e:
            logging.error(f"Redis error: {e}")
            raise

    def key_exists(self, key: str) -> bool:
        return self.redis.exists(key) == 1


import os

import aioredis
from datetime import timedelta
from aioredis import Redis
from dotenv import load_dotenv

load_dotenv()

class RedisRepository:
    def __init__(self):
        self.url = os.getenv("REDIS_URL")
        self.redis = Redis.from_url(self.url)
        self.ttl_days = timedelta(days=7)

    async def connect(self):
        self.redis = await aioredis.from_url("redis://redis")

    async def check_hash(self, event_hash: str) -> bool:
        return await self.redis.exists(f"event:{event_hash}")

    async def store_hash(self, event_hash: str):
        await self.redis.setex(f"event:{event_hash}", self.ttl, 1)
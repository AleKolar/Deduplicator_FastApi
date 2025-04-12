from redis.asyncio import Redis

from fastApiProject_Deduplicator.src.utils.event_hasher import EventHasher


class DeduplicationService:
    def __init__(self, redis: Redis, hasher=EventHasher):
        self.redis = redis
        self.hasher = hasher

    async def process_event_dedup(self, event: dict, ttl: int = 604800) -> bool:
        """
        ## Атомарная проверка/маркировка события,
        возвращает True для уникальных событий, False для дублей
        """
        event_hash = await self.hasher.generate_hash(event)

        result = await self.redis.set(
            name=event_hash,
            value="1",
            ex=ttl,
            nx=True
        )

        return result is not None



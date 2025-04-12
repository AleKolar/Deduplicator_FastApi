""" Kласс дедупликации с гарантированной атомарностью операций:
    Пример : Приходит 6 одинаковых событий, значит уникальное из 6 одно!
"""
import json
import mmh3
from redis.asyncio import Redis
from fastapi import FastAPI


class DeduplicationService:
    def __init__(self, redis: Redis):
        self.redis = redis

    async def _normalize_event(self, event: dict) -> bytes:
        """Сериализация с каноническим представлением JSON"""
        return json.dumps(
            event,
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=False
        ).encode('utf-8')

    async def process_event(self, event: dict, ttl: int = 604800) -> bool:
        """
        Атомарная проверка и маркировка события.
        Возвращает True для уникальных событий, False для дублей.
        """
        normalized = await self._normalize_event(event)
        event_hash = f"evt:{mmh3.hash128(normalized):x}"

        # Атомарная операция SETNX + EXPIRE
        result = await self.redis.set(
            name=event_hash,
            value="1",
            ex=ttl,
            nx=True
        )

        return result is not None




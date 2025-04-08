"""
Репозиторий/Утилиты для нормализации событий и генерации хэшей
"""
from datetime import datetime
import hashlib
import orjson
from typing import Dict, Any
from zoneinfo import ZoneInfo

DEFAULT_TIMEZONE = ZoneInfo("UTC")
SUPPORTED_DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y%m%d %H%M%S",
]


class EventHasher:
    @staticmethod
    def generate_hash(event: Dict[str, Any]) -> str:
        """
        Генерирует детерминированный хэш события после нормализации

        Args:
            event: Словарь с данными события

        Returns:
            str: SHA256 хэш в hex-формате
        """
        normalized = {
            'user_id': str(event.get('user_id', '')),
            'client_id': str(event.get('client_id', '')),
            'event_name': str(event['event_name']),
            'event_datetime': EventHasher._normalize_datetime(
                event['event_datetime']
            ),
        }
        return hashlib.sha256(
            orjson.dumps(normalized, option=orjson.OPT_SORT_KEYS)
        ).hexdigest()

    @staticmethod
    def _normalize_datetime(dt: Any) -> str:
        """
        Приводит дату к строке в UTC

        Поддерживает:
        - Строки в различных форматах
        - Объекты datetime
        - Timestamp (int/float)
        """
        if isinstance(dt, datetime):
            return dt.astimezone(DEFAULT_TIMEZONE).isoformat()

        if isinstance(dt, (int, float)):
            return datetime.fromtimestamp(dt, DEFAULT_TIMEZONE).isoformat()

        if isinstance(dt, str):
            for fmt in SUPPORTED_DATETIME_FORMATS:
                try:
                    parsed = datetime.strptime(dt, fmt)
                    return parsed.astimezone(DEFAULT_TIMEZONE).isoformat()
                except ValueError:
                    continue

        raise ValueError(f"Unsupported datetime format: {type(dt)} {dt}")
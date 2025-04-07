from datetime import datetime
import hashlib
import json
from src.services.metrics import DATETIME_FORMATS, DEFAULT_TIMEZONE

class EventHasher:
    @staticmethod
    def generate_hash(event: dict) -> str:
        """Генерирует уникальный хэш события"""
        normalized = {
            'user_id': str(event.get('user_id', '')),
            'client_id': str(event.get('client_id', '')),
            'event_name': str(event['event_name']),
            'datetime': EventHasher._normalize_datetime(event['event_datetime'])
        }
        return hashlib.sha256(
            json.dumps(normalized, sort_keys=True).encode()
        ).hexdigest()

    @staticmethod
    def _normalize_datetime(dt_str: str) -> str:
        """Нормализует дату в UTC"""
        for fmt in DATETIME_FORMATS:
            try:
                dt = datetime.strptime(dt_str, fmt)
                return dt.astimezone(DEFAULT_TIMEZONE).isoformat()
            except ValueError:
                continue
        raise ValueError(f"Unsupported datetime format: {dt_str}")
import hashlib
import orjson

def generate_json_hash(event: dict) -> str:
    """
    Генерирует SHA-256 хеш от ВСЕГО содержимого JSON,
    сортируя ключи для стабильности.
    """
    # Сериализуем с сортировкой ключей
    sorted_json = orjson.dumps(event, option=orjson.OPT_SORT_KEYS)
    return hashlib.sha256(sorted_json).hexdigest()
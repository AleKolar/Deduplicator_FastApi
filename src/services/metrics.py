# Настройки валидации
from datetime import timedelta

EMPTY_VALUES = ["", None, "null", "NULL", "None", "[]", "{}", "0"]
KEY_FIELDS = ["event_name", "user_id", "client_id", "ip_address"]
IGNORED_FIELDS = ["timestamp", "metadata", "processing_time"]

# Форматы даты
DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%d %H:%M:%S%z",
    "%d.%m.%Y %H:%M:%S"
]

# Настройки TTL
DEFAULT_TIMEZONE = "UTC"
DEFAULT_TTL = timedelta(days=7)  # Для Redis
CLICKHOUSE_TTL_DAYS = 30  # Для ClickHouse

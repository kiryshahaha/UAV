import redis
import json
from datetime import timedelta
from functools import lru_cache
import os


# Настройка Redis
redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=int(os.getenv('REDIS_DB', 0)),
    decode_responses=True
)

def get_cache_key(endpoint: str, **params):
    """Генерация ключа кэша"""
    key_parts = [endpoint]
    for k, v in sorted(params.items()):
        key_parts.append(f"{k}:{v}")
    return "|".join(key_parts)

def get_cached_data(key: str):
    """Получить данные из кэша"""
    try:
        cached = redis_client.get(key)
        if cached:
            # Явно приводим к строке
            return json.loads(str(cached))
        return None
    except Exception:
        return None

def set_cached_data(key: str, data: dict, expire_minutes: int = 30):
    """Сохранить данные в кэш"""
    try:
        # Сериализуем данные, обрабатывая datetime объекты
        def json_serializer(obj):
            if hasattr(obj, 'isoformat'):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        
        serialized_data = json.dumps(data, default=json_serializer, ensure_ascii=False)
        redis_client.setex(key, timedelta(minutes=expire_minutes), serialized_data)
    except Exception as e:
        print(f"Ошибка кэширования: {e}")
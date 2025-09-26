import redis
import json
from datetime import timedelta
from typing import Any
from datetime import datetime, date

# Redis кэш
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Функции кэширования
def get_cache_key(endpoint: str, **kwargs) -> str:
    """Генерация ключа кэша"""
    params = "_".join(f"{k}_{v}" for k, v in sorted(kwargs.items()))
    return f"api_{endpoint}_{params}"

def get_cached_data(key: str) -> Any:
    """Получить данные из кэша"""
    try:
        cached = redis_client.get(key)
        if cached is not None:
            if isinstance(cached, (str, bytes, bytearray)):
                return json.loads(cached)
            else:
                return cached
        return None
    except (json.JSONDecodeError, TypeError) as e:
        print(f"Error decoding cached data for key {key}: {e}")
        return None

def set_cached_data(key: str, data: Any, expire_minutes: int = 5):
    """Сохранить данные в кэш с обработкой datetime"""
    
    def serialize_obj(obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, list):
            return [serialize_obj(item) for item in obj]
        if isinstance(obj, dict):
            return {k: serialize_obj(v) for k, v in obj.items()}
        return obj

    try:
        serialized_data = serialize_obj(data)
        redis_client.setex(
            key, 
            timedelta(minutes=expire_minutes), 
            json.dumps(serialized_data, ensure_ascii=False)
        )
    except Exception as e:
        print(f"Error caching data for key {key}: {e}")
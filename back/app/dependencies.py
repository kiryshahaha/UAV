from fastapi import Depends
from sqlalchemy.orm import Session
import redis
import json
from datetime import timedelta
from typing import TypeVar, Any, Callable
import functools

# Redis кэш
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def get_redis():
    return redis_client

T = TypeVar('T')

def cache_response(key: str, expire_minutes: int = 5):
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            # Проверяем кэш
            cached = redis_client.get(key)
            if cached is not None:
                # Явно приводим к строке
                return json.loads(str(cached))
            
            # Выполняем функцию и кэшируем результат
            result = func(*args, **kwargs)
            redis_client.setex(key, timedelta(minutes=expire_minutes), json.dumps(result))
            return result
        return wrapper
    return decorator
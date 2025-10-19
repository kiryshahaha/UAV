import redis
import json
from datetime import timedelta
from functools import lru_cache
import os
from fastapi import Request, Depends, HTTPException
from sqlalchemy.orm import Session
from table_service import TableService
from database import get_db
import uuid

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


def get_session_id(request: Request) -> str:
    """Получить или создать ID сессии"""
    session_id = request.cookies.get("session_id")
    if not session_id:
        session_id = str(uuid.uuid4())
    return session_id

def get_current_table(request: Request, db: Session = Depends(get_db)) -> str:
    """Получить текущую таблицу для запроса"""
    session_id = get_session_id(request)
    table_service = TableService(db)
    current_table = table_service.get_current_table(session_id)
    
    if not current_table:
        raise HTTPException(status_code=404, detail="Нет доступных таблиц")
    
    return current_table

def get_table_service(db: Session = Depends(get_db)) -> TableService:
    """Получить сервис таблиц"""
    return TableService(db)

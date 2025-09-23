from fastapi import FastAPI, HTTPException, Depends, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, func, distinct, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, date, timedelta
import os
from dotenv import load_dotenv
import json
import redis
import sys

# Добавляем путь для импорта модулей app
sys.path.append(os.path.dirname(__file__))

load_dotenv()

# Настройка базы данных
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'postgres')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Модель базы данных
class Flight(Base):
    __tablename__ = "flights"
    
    id = Column(Integer, primary_key=True, index=True)
    message_type = Column(String(10), default="FPL")
    aircraft_id = Column(String(50))
    aircraft_type = Column(String(50))
    departure_aerodrome = Column(String(10))
    destination_aerodrome = Column(String(10))
    departure_time = Column(String(10))
    route = Column(Text)
    region = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)

# Pydantic модели
class FlightBase(BaseModel):
    message_type: str = "FPL"
    aircraft_id: Optional[str] = None
    aircraft_type: Optional[str] = None
    departure_aerodrome: Optional[str] = None
    destination_aerodrome: Optional[str] = None
    departure_time: Optional[str] = None
    route: Optional[str] = None
    region: str

class FlightResponse(FlightBase):
    id: int
    created_at: datetime
    
    class Config:
        from_attributes = True

class RegionStats(BaseModel):
    region: str
    flights_count: int
    drones_count: int

class AnalyticsResponse(BaseModel):
    total_flights: int
    total_regions: int
    total_drones: int
    period: str
    last_updated: datetime
    top_regions: List[RegionStats]

# Dependency для БД
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Redis кэш
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Создание приложения
app = FastAPI(title="БВС API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Создание таблиц
@app.on_event("startup")
async def startup_event():
    """Запускается при старте FastAPI"""
    Base.metadata.create_all(bind=engine)
    print("🚀 Запуск БВС API...")
    
    # Автоматическая проверка и настройка данных
    db = SessionLocal()
    try:
        from auto_setup import auto_setup
        auto_setup(db)
    except Exception as e:
        print(f"⚠️ Ошибка автоматической настройки: {e}")
    finally:
        db.close()

# Вспомогательные функции
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

# 🔥 КЛЮЧЕВЫЕ ЭНДПОИНТЫ ДЛЯ ФРОНТЕНДА

@app.get("/")
async def root():
    return {"message": "БВС API работает", "version": "1.0.0"}

@app.get("/health")
async def health():
    return {"status": "OK", "timestamp": datetime.now()}

# АДМИНИСТРАТИВНЫЕ ЭНДПОИНТЫ
@app.post("/api/admin/parse-excel")
async def parse_excel_file(background_tasks: BackgroundTasks):
    """Запустить парсинг Excel файла в фоновом режиме"""
    try:
        # Динамический импорт парсера
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'excel_to_postgres'))
        from main import main_standard
        
        def run_parser():
            try:
                print("🔄 Запуск парсера Excel...")
                main_standard()
                print("✅ Парсинг завершен успешно")
            except Exception as e:
                print(f"❌ Ошибка парсинга: {e}")
        
        background_tasks.add_task(run_parser)
        
        return {
            "status": "started", 
            "message": "Парсинг Excel запущен в фоновом режиме",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except ImportError as e:
        raise HTTPException(status_code=500, detail=f"Ошибка импорта парсера: {e}")

@app.post("/api/admin/migrate-data")
async def migrate_parser_data(background_tasks: BackgroundTasks):
    """Мигрировать данные из таблиц парсера в таблицу flights"""
    try:
        from data_integrator import DataIntegrator
        
        def run_migration():
            try:
                print("🔄 Запуск миграции данных...")
                integrator = DataIntegrator()
                migrated_count = integrator.migrate_all_tables()
                print(f"✅ Миграция завершена. Перенесено записей: {migrated_count}")
            except Exception as e:
                print(f"❌ Ошибка миграции: {e}")
        
        background_tasks.add_task(run_migration)
        
        return {
            "status": "started",
            "message": "Миграция данных запущена в фоновом режиме",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка миграции: {e}")

@app.get("/api/admin/available-tables")
async def get_available_tables(db: Session = Depends(get_db)):
    """Получить список всех таблиц в БД"""
    try:
        result = db.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """))
        tables = [row[0] for row in result]
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения таблиц: {e}")

# ОСНОВНЫЕ ЭНДПОИНТЫ ДАННЫХ
@app.get("/api/analytics/dashboard")
async def get_dashboard_analytics(
    period: str = Query("30d", description="Период: 7d, 30d, 90d, 1y, all"),
    db: Session = Depends(get_db)
):
    """Основная аналитика для дашборда"""
    cache_key = get_cache_key("dashboard", period=period)
    cached = get_cached_data(cache_key)
    if cached:
        return cached
    
    # Проверяем наличие данных
    total_flights_count = db.query(Flight).count()
    
    if total_flights_count == 0:
        return {
            "status": "no_data",
            "message": "В базе данных нет записей о полетах",
            "suggestion": "Запустите парсинг Excel и миграцию данных через /api/admin/parse-excel и /api/admin/migrate-data",
            "total_flights": 0,
            "total_regions": 0,
            "total_drones": 0,
            "period": period,
            "last_updated": datetime.utcnow().isoformat(),
            "top_regions": []
        }
    
    # Рассчитываем дату начала периода
    today = datetime.utcnow().date()
    if period == "7d":
        start_date = today - timedelta(days=7)
    elif period == "30d":
        start_date = today - timedelta(days=30)
    elif period == "90d":
        start_date = today - timedelta(days=90)
    elif period == "1y":
        start_date = today - timedelta(days=365)
    else:
        start_date = date(2000, 1, 1)  # все данные
    
    # Общая статистика
    total_flights = db.query(Flight).filter(Flight.created_at >= start_date).count()
    total_regions = db.query(Flight.region).filter(Flight.created_at >= start_date).distinct().count()
    total_drones = db.query(Flight.aircraft_id).filter(Flight.created_at >= start_date).distinct().count()
    
    # Топ регионов по количеству полетов
    top_regions_query = db.query(
        Flight.region,
        func.count(Flight.id).label('flights_count'),
        func.count(distinct(Flight.aircraft_id)).label('drones_count')
    ).filter(Flight.created_at >= start_date).group_by(Flight.region).order_by(func.count(Flight.id).desc()).limit(10)
    
    top_regions = [
        {"region": r.region, "flights_count": r.flights_count, "drones_count": r.drones_count}
        for r in top_regions_query.all()
    ]
    
    response = {
        "status": "success",
        "total_flights": total_flights,
        "total_regions": total_regions,
        "total_drones": total_drones,
        "period": period,
        "last_updated": datetime.utcnow().isoformat(),
        "top_regions": top_regions
    }
    
    set_cached_data(cache_key, response)
    return response


# 2. ДАННЫЕ ДЛЯ КАРТЫ РЕГИОНОВ
@app.get("/api/visualizations/map")
async def get_map_data(db: Session = Depends(get_db)):
    cache_key = get_cache_key("map_data")
    cached = get_cached_data(cache_key)
    if cached:
        return cached

    region_coordinates = {
        "moscow": {"lat": 55.7558, "lon": 37.6176},
        "spb": {"lat": 59.9343, "lon": 30.3351},
        "kaliningrad": {"lat": 54.7104, "lon": 20.4522},
        "rostov": {"lat": 47.2357, "lon": 39.7015},
        "samara": {"lat": 53.1959, "lon": 50.1002},
        "ekaterinburg": {"lat": 56.8389, "lon": 60.6057},
        "novosibirsk": {"lat": 55.0084, "lon": 82.9357},
        "krasnoyarsk": {"lat": 56.0184, "lon": 92.8672},
        "irkutsk": {"lat": 52.2864, "lon": 104.2807},
        "yakutsk": {"lat": 62.0278, "lon": 129.7312},
        "magadan": {"lat": 59.5602, "lon": 150.7986},
        "habarovsk": {"lat": 48.4802, "lon": 135.0719},
        "simferopol": {"lat": 44.9521, "lon": 34.1024}
    }

    region_stats = (
        db.query(
            Flight.region,
            func.count(Flight.id).label('flights_count'),
            func.count(distinct(Flight.aircraft_id)).label('drones_count')
        )
        .group_by(Flight.region)
        .all()
    )

    map_data = []
    for stat in region_stats:
        coords = region_coordinates.get(stat.region, {"lat": 55.7558, "lon": 37.6176})
        map_data.append({
            "region": stat.region,
            "lat": coords["lat"],
            "lon": coords["lon"],
            "flights_count": stat.flights_count,
            "drones_count": stat.drones_count,
            "intensity": min(stat.flights_count // 10, 100)
        })

    set_cached_data(cache_key, map_data)
    return map_data


# 3. ДАННЫЕ ДЛЯ ГРАФИКОВ
@app.get("/api/visualizations/charts")
async def get_charts_data(
    chart_type: str = Query("regions", description="regions, timeline, aircraft"),
    db: Session = Depends(get_db)
):
    cache_key = get_cache_key("charts", chart_type=chart_type)
    cached = get_cached_data(cache_key)
    if cached:
        return cached

    if chart_type == "regions":
        data = (
            db.query(Flight.region, func.count(Flight.id).label('flights'))
            .group_by(Flight.region)
            .order_by(func.count(Flight.id).desc())
            .limit(15)
            .all()
        )
        chart_data = {
            "type": "bar",
            "labels": [d.region for d in data],
            "datasets": [{
                "label": "Количество полетов",
                "data": [d.flights for d in data],
                "backgroundColor": "rgba(54, 162, 235, 0.6)"
            }]
        }

    elif chart_type == "timeline":
        data = (
            db.query(func.date(Flight.created_at).label('date'), func.count(Flight.id).label('flights'))
            .group_by(func.date(Flight.created_at))
            .order_by(func.date(Flight.created_at))
            .limit(30)
            .all()
        )
        chart_data = {
            "type": "line",
            "labels": [d.date.isoformat() for d in data],
            "datasets": [{
                "label": "Полетов в день",
                "data": [d.flights for d in data],
                "borderColor": "rgba(75, 192, 192, 1)",
                "fill": False
            }]
        }

    elif chart_type == "aircraft":
        data = (
            db.query(Flight.aircraft_type, func.count(Flight.id).label('flights'))
            .filter(Flight.aircraft_type.isnot(None))
            .group_by(Flight.aircraft_type)
            .order_by(func.count(Flight.id).desc())
            .limit(10)
            .all()
        )
        chart_data = {
            "type": "pie",
            "labels": [d.aircraft_type or "Не указан" for d in data],
            "datasets": [{
                "data": [d.flights for d in data],
                "backgroundColor": [f"hsl({i * 360 / len(data)}, 70%, 50%)" for i in range(len(data))]
            }]
        }
    else:
        raise HTTPException(status_code=400, detail="Неверный тип графика")

    set_cached_data(cache_key, chart_data)
    return chart_data


# 4. ПОИСК И ФИЛЬТРАЦИЯ ПОЛЕТОВ
@app.get("/api/flights/search")
async def search_flights(
    q: Optional[str] = Query(None, description="Поиск по ID дрона или типу ВС"),
    region: Optional[str] = Query(None),
    aircraft_type: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, le=100),
    db: Session = Depends(get_db)
):
    query = db.query(Flight)

    if q:
        query = query.filter(
            (Flight.aircraft_id.ilike(f"%{q}%")) |
            (Flight.aircraft_type.ilike(f"%{q}%"))
        )
    if region:
        query = query.filter(Flight.region == region)
    if aircraft_type:
        query = query.filter(Flight.aircraft_type == aircraft_type)

    total = query.count()
    flights = query.offset((page - 1) * page_size).limit(page_size).all()

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "results": [FlightResponse.model_validate(f) for f in flights]  # Поддержка Pydantic v2
    }


# 5. ДЕТАЛЬНАЯ СТАТИСТИКА ПО РЕГИОНУ
@app.get("/api/regions/{region}/analytics")
async def get_region_analytics(region: str, db: Session = Depends(get_db)):
    cache_key = get_cache_key("region_analytics", region=region)
    cached = get_cached_data(cache_key)
    if cached:
        return cached

    stats = (
        db.query(
            func.count(Flight.id).label('total_flights'),
            func.count(distinct(Flight.aircraft_id)).label('unique_drones'),
            func.min(Flight.created_at).label('first_flight'),
            func.max(Flight.created_at).label('last_flight')
        )
        .filter(Flight.region == region)
        .first()
    )

    popular_aircraft = (
        db.query(Flight.aircraft_type, func.count(Flight.id).label('count'))
        .filter(Flight.region == region, Flight.aircraft_type.isnot(None))
        .group_by(Flight.aircraft_type)
        .order_by(func.count(Flight.id).desc())
        .limit(5)
        .all()
    )

    daily_activity = (
        db.query(func.date(Flight.created_at).label('date'), func.count(Flight.id).label('flights'))
        .filter(Flight.region == region)
        .group_by(func.date(Flight.created_at))
        .order_by(func.date(Flight.created_at))
        .limit(30)
        .all()
    )

    response = {
        "region": region,
        "total_flights": stats.total_flights if stats else 0,
        "unique_drones": stats.unique_drones if stats else 0,
        "first_flight": stats.first_flight.isoformat() if stats and stats.first_flight else None,
        "last_flight": stats.last_flight.isoformat() if stats and stats.last_flight else None,
        "popular_aircraft": [{"type": a.aircraft_type, "count": a.count} for a in popular_aircraft],
        "daily_activity": [{"date": d.date.isoformat(), "flights": d.flights} for d in daily_activity]
    }

    set_cached_data(cache_key, response)
    return response


# Для обратной совместимости
@app.get("/flights/", response_model=List[FlightResponse])
async def get_flights(
    region: Optional[str] = Query(None),
    aircraft_type: Optional[str] = Query(None),
    limit: int = Query(default=100, le=1000),
    db: Session = Depends(get_db)
):
    query = db.query(Flight)
    if region:
        query = query.filter(Flight.region == region)
    if aircraft_type:
        query = query.filter(Flight.aircraft_type == aircraft_type)
    flights = query.limit(limit).all()
    return [FlightResponse.model_validate(f) for f in flights]


@app.get("/statistics/overview")
async def get_statistics(db: Session = Depends(get_db)):
    total_flights = db.query(Flight).count()
    total_regions = db.query(Flight.region).distinct().count()

    top_regions = (
        db.query(Flight.region, func.count(Flight.id).label('count'))
        .group_by(Flight.region)
        .order_by(func.count(Flight.id).desc())
        .limit(5)
        .all()
    )

    top_aircraft = (
        db.query(Flight.aircraft_type, func.count(Flight.id).label('count'))
        .filter(Flight.aircraft_type.isnot(None))
        .group_by(Flight.aircraft_type)
        .order_by(func.count(Flight.id).desc())
        .limit(5)
        .all()
    )

    return {
        "total_flights": total_flights,
        "total_regions": total_regions,
        "top_regions": [{"region": r.region, "count": r.count} for r in top_regions],
        "top_aircraft_types": [{"aircraft_type": r.aircraft_type, "count": r.count} for r in top_aircraft]
    }


# Запуск сервера
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
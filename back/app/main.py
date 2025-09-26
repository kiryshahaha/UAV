from fastapi import FastAPI, HTTPException, Depends, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func, text
from sqlalchemy.orm import Session
from typing import Optional, List
from datetime import datetime, date, timedelta
import os
import sys
import asyncio

# Добавляем текущую директорию в путь для импортов
sys.path.append(os.path.dirname(__file__))

# Импортируем из наших модулей
from models import Flight, FlightResponse, AnalyticsResponse, RegionStats, Base
from database import engine, SessionLocal, get_db
from dependencies import get_cache_key, get_cached_data, set_cached_data

# Создание приложения
app = FastAPI(title="БВС API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Флаг для отслеживания статуса миграции
migration_status = {"running": False, "completed": False, "error": None}

async def run_auto_migration():
    """Асинхронная автоматическая миграция"""
    try:
        migration_status["running"] = True
        print("🔄 Запуск автоматической миграции в фоне...")
        
        from data_integrator import DataIntegrator
        integrator = DataIntegrator()
        migrated_count = integrator.migrate_all_tables()
        
        migration_status["completed"] = True
        migration_status["running"] = False
        print(f"✅ Автоматическая миграция завершена. Перенесено: {migrated_count} записей")
        
    except Exception as e:
        migration_status["error"] = str(e)
        migration_status["running"] = False
        print(f"❌ Ошибка автоматической миграции: {e}")

@app.on_event("startup")
async def startup_event():
    """Запускается при старте FastAPI"""
    Base.metadata.create_all(bind=engine)

    from migrations import upgrade_database
    upgrade_database()
    print("🚀 Запуск БВС API...")
    
    # Быстрая проверка данных без блокировки
    db = SessionLocal()
    try:
        flight_count = db.query(Flight).count()
        print(f"📊 Найдено записей в таблице flights: {flight_count}")
        
        if flight_count == 0:
            # Проверяем наличие таблиц парсера
            result = db.execute(text("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name LIKE '%excel_data%'
            """))
            parser_tables = [row[0] for row in result]
            
            if parser_tables:
                print(f"📋 Найдены таблицы парсера: {len(parser_tables)}")
                # Запускаем миграцию в фоне без блокировки старта сервера
                asyncio.create_task(run_auto_migration())
            else:
                print("💡 Таблицы парсера не найдены. Используйте /api/admin/parse-excel")
        else:
            print("✅ Данные готовы к использованию")
            
    except Exception as e:
        print(f"⚠️ Ошибка при запуске: {e}")
    finally:
        db.close()

@app.get("/migration-status")
async def get_migration_status():
    """Статус автоматической миграции"""
    return migration_status

# Базовые эндпоинты
@app.get("/")
async def root():
    return {
        "message": "БВС API работает", 
        "version": "1.0.0",
        "migration_status": migration_status
    }

@app.get("/health")
async def health():
    return {"status": "OK", "timestamp": datetime.now()}
# Административные эндпоинты
@app.post("/api/admin/parse-excel")
async def parse_excel_file(background_tasks: BackgroundTasks):
    """Запустить парсинг Excel файла в фоновом режиме"""
    try:
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

# Основные эндпоинты данных
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
        func.count(func.distinct(Flight.aircraft_id)).label('drones_count')
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

# Остальные эндпоинты (добавьте их аналогично)
@app.get("/flights/")
async def get_flights(
    region: Optional[str] = Query(None),
    aircraft_type: Optional[str] = Query(None),
    limit: int = Query(default=100, le=1000),
    db: Session = Depends(get_db)
):
    # SQL запрос который точно вернет все существующие колонки
    base_query = "SELECT * FROM flights WHERE 1=1"
    params = {}
    
    if region:
        base_query += " AND region = :region"
        params['region'] = region
    if aircraft_type:
        base_query += " AND aircraft_type = :aircraft_type"
        params['aircraft_type'] = aircraft_type
        
    base_query += f" LIMIT {limit}"
    
    result = db.execute(text(base_query), params)
    columns = result.keys()
    flights = result.fetchall()
    
    # Динамически создаем dict на основе реальных колонок
    return [dict(zip(columns, flight)) for flight in flights]


@app.get("/statistics/overview")
async def get_statistics(db: Session = Depends(get_db)):
    total_flights = db.query(Flight).count()
    total_regions = db.query(Flight.region).distinct().count()
    top_regions = db.query(Flight.region, func.count(Flight.id).label('count')).group_by(Flight.region).order_by(func.count(Flight.id).desc()).limit(5).all()
    top_aircraft = db.query(Flight.aircraft_type, func.count(Flight.id).label('count')).filter(Flight.aircraft_type.isnot(None)).group_by(Flight.aircraft_type).order_by(func.count(Flight.id).desc()).limit(5).all()
    
    return {
        "total_flights": total_flights,
        "total_regions": total_regions,
        "top_regions": [{"region": r.region, "count": r.count} for r in top_regions],
        "top_aircraft_types": [{"aircraft_type": r.aircraft_type, "count": r.count} for r in top_aircraft]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
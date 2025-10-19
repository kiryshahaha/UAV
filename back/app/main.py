import os
import sys
import logging
from fastapi import FastAPI, HTTPException, Depends, Query, BackgroundTasks, Path, Request, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import func, text, distinct
from sqlalchemy.orm import Session
from typing import Optional, List, Dict, Any
from datetime import datetime
import json
from collections import defaultdict
import asyncio
import uuid
import time
from table_service import TableService

from flight_parsers_fixed import parse_coord, convert_coord, parse_flight_duration, parse_time
from flight_formats_parser import (
    parse_radius, 
    parse_flight_level, 
    get_formats_info,
    parse_radius_for_stats,
    parse_level_for_stats
)
# Импорты из модулей
from database import engine, SessionLocal, get_db
from data_processor import DataProcessor
from excel_parser import ExcelParser

from models import UserTable, UserSession
from schemas import (
    TableSelectRequest, TableInfoResponse, UploadResponse, 
    RegionInfoResponse, HealthResponse,
    FlightZoneResponse, PaginationResponse, CityDataResponse,
    MonthlyStatsResponse
)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Добавляем текущую директорию в путь для импортов
sys.path.append(os.path.dirname(__file__))

from sqlalchemy.orm import Session as SQLSession
from typing import List, Optional


# ЗАВИСИМОСТИ
def get_session_id(request: Request) -> str:
    """Получить или создать ID сессии из заголовка или cookie"""
    # Сначала пробуем получить из заголовка
    session_id = request.headers.get("x-session-id")
    if not session_id:
        # Потом из cookie (для обратной совместимости)
        session_id = request.cookies.get("session_id")
    if not session_id:
        session_id = str(uuid.uuid4())
    return session_id

def get_table_service(db: Session = Depends(get_db)) -> TableService:
    """Получить сервис таблиц"""
    return TableService(db)

def get_current_table(request: Request, db: Session = Depends(get_db)) -> str:
    """Получить текущую таблицу для запроса"""
    session_id = get_session_id(request)
    table_service = TableService(db)
    current_table = table_service.get_current_table(session_id)
    
    if not current_table:
        # Если нет таблиц, используем дефолтную
        current_table = os.getenv('TABLE_NAME', 'excel_data_2025')
    
    return current_table

app = FastAPI(title="БВС API", version="1.0.0")

# Список разрешенных origins
origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000", 
    "http://localhost:3001",
    # добавьте другие адреса если нужно
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Указываем конкретные origins вместо "*"
    allow_credentials=True,  # Разрешаем credentials
    allow_methods=["*"],     # Можно оставить *
    allow_headers=["*"],     # Можно оставить *
)

def _find_column_case_insensitive(db: Session, table_name: str, target_columns: List[str]) -> Optional[str]:
    """Находит имя колонки в таблице с учётом регистра."""
    try:
        result = db.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = :table_name
            AND table_schema = 'public'
        """), {"table_name": table_name})

        existing_columns = [row[0] for row in result.fetchall()]
        target_columns_lower = [col.lower() for col in target_columns]

        for existing_col in existing_columns:
            if existing_col.lower() in target_columns_lower:
                return existing_col
        return None
    except Exception as e:
        logger.error(f"Ошибка при поиске колонки: {e}")
        return None

def _execute_safe_query(db: Session, query: str, params: Optional[Dict] = None) -> Any:
    """Безопасно выполняет SQL-запрос с логированием ошибок."""
    try:
        logger.debug(f"Выполняется запрос: {query}")
        result = db.execute(text(query), params or {})
        return result
    except Exception as e:
        logger.error(f"Ошибка при выполнении запроса '{query}': {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(e)}")

def _get_required_columns(db: Session, table_name: str) -> Dict[str, str]:
    """Возвращает словарь с именами требуемых колонок для указанной таблицы"""
    columns_mapping = {
        "reg": ["reg", "REG", "registration", "регистрация"],
        "opr": ["opr", "OPR", "operator", "оператор"],
        "typ": ["typ", "TYP", "type", "тип"],
        "dep": ["dep", "DEP", "departure", "вылет"],
        "dest": ["dest", "DEST", "destination", "назначение"],
        "flight_zone_radius": ["flight_zone_radius", "FLIGHT_ZONE_RADIUS", "radius", "радиус"],
        "flight_level": ["flight_level", "FLIGHT_LEVEL", "level", "уровень"],
        "departure_time": ["departure_time", "DEPARTURE_TIME", "departure", "время_вылета"],
        "arrival_time": ["arrival_time", "ARRIVAL_TIME", "arrival", "время_прибытия"]
    }

    result = {}
    for key, variants in columns_mapping.items():
        column = _find_column_case_insensitive(db, table_name, variants)
        if column:
            result[key] = column

    return result

def get_available_years_fast(db: Session, table_name: str, region: Optional[str] = None, date_from: Optional[str] = None, date_to: Optional[str] = None) -> List[int]:
    """СУПЕР-БЫСТРОЕ получение списка лет с данными"""
    try:
        # Сначала проверяем наличие колонки date через кэш
        date_column_exists = db.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = :table_name 
            AND column_name = 'date'
        """), {"table_name": table_name}).fetchone()
        
        if not date_column_exists:
            return []

        # ИСПОЛЬЗУЕМ ИНДЕКС ДЛЯ МГНОВЕННОГО ПОЛУЧЕНИЯ ЛЕТ
        params = {"table_name": table_name}
        where_clause = "WHERE date IS NOT NULL"
        if region:
            where_clause += " AND region_calculated = :region"
            params["region"] = region
        if date_from:
            where_clause += " AND date >= (:date_from)::date"
            params["date_from"] = date_from
        if date_to:
            where_clause += " AND date <= (:date_to)::date"
            params["date_to"] = date_to

        year_query = text(f'''
            SELECT DISTINCT EXTRACT(YEAR FROM date) as year
            FROM "{table_name}"
            {where_clause}
            ORDER BY year DESC
            LIMIT 20  -- Ограничиваем для производительности
        ''')
        
        years_result = db.execute(year_query, params)
        return [int(row[0]) for row in years_result if row[0] is not None]
        
    except Exception as e:
        logger.error(f"Ошибка получения лет для {table_name}: {e}")
        return []

@app.on_event("startup")
async def startup_event():
    """Запускается при старте FastAPI"""
    logger.info("🚀 Запуск БВС API...")
    
    try:
        excel_parser = ExcelParser()
        logger.info("✅ ExcelParser инициализирован, авто-парсинг запущен")
    except Exception as e:
        logger.warning(f"⚠️ Ошибка инициализации ExcelParser: {e}")
    
    # Проверяем доступность RegionService
    try:
        from region_service import region_service
        REGION_SERVICE_AVAILABLE = True
        logger.info("✅ RegionService доступен")
    except ImportError as e:
        logger.warning(f"⚠️ RegionService недоступен: {e}")
        REGION_SERVICE_AVAILABLE = False
    
    db = SessionLocal()
    try:
        # Проверяем существование таблиц пользователя
        table_exists = db.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'user_tables'
            )
        """)).scalar()

        if not table_exists:
            logger.info("📋 Создаем таблицы пользователя...")
            from models import Base
            Base.metadata.create_all(bind=engine)
            logger.info("✅ Таблицы пользователя созданы")
            
        # Проверяем доступные таблицы данных
        table_service = TableService(db)
        tables = table_service.get_all_tables()
        logger.info(f"📊 Найдено таблиц данных: {len(tables)}")
        
        for table in tables:
            logger.info(f"  - {table.table_name}: {table.records_count} записей")
            
    except Exception as e:
        logger.error(f"⚠️ Ошибка при запуске: {e}")
    finally:
        db.close()

@app.get("/api/tables", 
        response_model=List[TableInfoResponse], 
        description="Возвращает список всех доступных таблиц с информацией о них",
        tags=["Управление таблицами"])
async def get_available_tables(
    table_service: TableService = Depends(get_table_service)
):
    """Получить список всех доступных таблиц"""
    try:
        tables = table_service.get_all_tables()
        return tables
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения таблиц: {str(e)}")

@app.post("/api/tables/select", 
        description="Позволяет выбрать текущую таблицу для работы",
        tags=["Управление таблицами"])
async def select_table(
    request: TableSelectRequest,
    table_service: TableService = Depends(get_table_service),
    session_id: str = Depends(get_session_id)
):
    """Выбрать текущую таблицу"""
    try:
        success = table_service.set_current_table(session_id, request.table_name)
        if not success:
            raise HTTPException(status_code=400, detail="Таблица не найдена")
        
        return {
            "status": "success", 
            "table_name": request.table_name,
            "session_id": session_id,  # Возвращаем session_id в теле ответа
            "message": f"Таблица {request.table_name} выбрана как текущая"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка выбора таблицы: {str(e)}")

@app.get("/api/tables/current", 
        response_model=TableInfoResponse, 
        description="Возвращает информацию о текущей выбранной таблице на основе session_id",
        tags=["Управление таблицами"])
async def get_current_table_info(
    current_table: str = Depends(get_current_table),
    table_service: TableService = Depends(get_table_service)
):
    """Получить информацию о текущей таблице"""
    try:
        tables = table_service.get_all_tables()
        current_table_info = next((t for t in tables if t.table_name == current_table), None)
        
        if not current_table_info:
            # Создаем базовую информацию о таблице
            current_table_info = TableInfoResponse(
                table_name=current_table,
                original_filename=current_table,
                records_count=0,
                is_active=True
            )
        
        return current_table_info
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения таблицы: {str(e)}")

@app.get("/",
    response_model=Dict[str, Any],
    description="Главная страница API - возвращает данные сгруппированные по годам",
    tags=["Получение данных"])
async def get_main_data(
    date_from: Optional[str] = Query(None, description="Начальная дата в формате YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="Конечная дата в формате YYYY-MM-DD"),
    region: Optional[str] = Query(None, description="Регион для фильтрации"),
    limit: Optional[int] = Query(None, description="Лимит записей (для теста)"),
    offset: int = Query(0, description="Смещение для пагинации (для теста)"),
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)

):
    """Главная страница - возвращает данные сгруппированные по годам"""
    try:
        db.execute(text(f'ANALYZE "{current_table}";'))
        db.commit()
        start_time = time.time()
        
        table_exists = db.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = :table_name
            )
        """), {"table_name": current_table}).scalar()

        if not table_exists:
            raise HTTPException(status_code=404, detail=f"Таблица {current_table} не найдена")

        params = {}
        where_clauses = ["date IS NOT NULL"]
        if date_from:
            where_clauses.append("date >= (:date_from)::date")
            params["date_from"] = date_from
        if date_to:
            where_clauses.append("date <= (:date_to)::date")
            params["date_to"] = date_to
        if region:
            where_clauses.append("region_calculated = :region")
            params["region"] = region

        where_sql = " AND ".join(where_clauses)

        available_years = get_available_years_fast(db, current_table, region, date_from, date_to)

        if not available_years:
            # Резервный метод если нет дат
            columns = _get_required_columns(db, current_table)
            select_columns = [
                'id',
                f'"{columns.get("reg", "reg")}" as reg',
                f'"{columns.get("opr", "opr")}" as opr',
                f'"{columns.get("typ", "typ")}" as typ',
                f'"{columns.get("dep", "dep_1")}" as dep',
                f'"{columns.get("dest", "dest")}" as dest',
                f'"{columns.get("flight_zone_radius", "flight_zone_radius")}" as flight_zone_radius',
                f'"{columns.get("flight_level", "flight_level")}" as flight_level',
                f'"{columns.get("departure_time", "departure_time")}" as departure_time',
                f'"{columns.get("arrival_time", "arrival_time")}" as arrival_time'
            ]
            
            region_exists = db.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = :table_name 
                AND column_name = 'region_calculated'
            """), {"table_name": current_table}).fetchone()
            
            if region_exists:
                select_columns.append('region_calculated')
            
            query_str = f'''
                SELECT {", ".join(select_columns)}
                FROM "{current_table}" 
                WHERE {where_sql}
                ORDER BY date DESC, id DESC
            '''
            if limit is not None:
                query_str += ' LIMIT :limit OFFSET :offset'
                params["limit"] = limit
                params["offset"] = offset
            
            query = text(query_str)
            
            result = db.execute(query, params)
            data = [dict(row) for row in result.mappings().all()]
            
            # ОБРАБОТКА ДАННЫХ - добавляем вычисленные значения в метрах
            enhanced_data = []
            for item in data:
                enhanced_item = item.copy()
                
                # Добавляем вычисленную высоту в метрах
                if item.get('flight_level'):
                    level_data = parse_flight_level(item['flight_level'])
                    if level_data:
                        enhanced_item['flight_level_min_m'] = level_data['min_height']
                        enhanced_item['flight_level_max_m'] = level_data['max_height']
                        enhanced_item['flight_level_avg_m'] = round((level_data['min_height'] + level_data['max_height']) / 2, 1)
                
                # Добавляем вычисленный радиус в метрах
                if item.get('flight_zone_radius'):
                    radius_m = parse_radius_for_stats(item['flight_zone_radius'])
                    if radius_m:
                        enhanced_item['flight_zone_radius_m'] = radius_m
                        enhanced_item['flight_zone_radius_km'] = round(radius_m / 1000, 2)
                
                # Добавляем продолжительность полета в минутах
                if item.get('departure_time') and item.get('arrival_time'):
                    try:
                        dep_time = datetime.strptime(item['departure_time'], '%H:%M:%S').time()
                        arr_time = datetime.strptime(item['arrival_time'], '%H:%M:%S').time()
                        duration_minutes = (datetime.combine(date.today(), arr_time) - 
                                          datetime.combine(date.today(), dep_time)).total_seconds() / 60
                        if duration_minutes > 0:
                            enhanced_item['flight_duration_minutes'] = round(duration_minutes, 1)
                    except:
                        pass
                
                enhanced_data.append(enhanced_item)
            
            total_count = db.execute(text(f'SELECT COUNT(*) FROM "{current_table}" WHERE {where_sql}'), params).scalar() or 0
            
            return {
                "data": enhanced_data,
                "count": len(enhanced_data),
                "total_count": total_count,
                "current_table": current_table,
                "message": "Данные не сгруппированы по годам (отсутствуют даты)",
                "available_years": [],
                "performance": {
                    "processing_time_seconds": round(time.time() - start_time, 3),
                    "records_loaded": len(enhanced_data)
                },
                "units": {
                    "height": "meters",
                    "radius": "meters", 
                    "duration": "minutes"
                }
            }

        # Данные по годам
        yearly_data = {}
        total_records = 0
        columns = _get_required_columns(db, current_table)
        
        for year_val in available_years:
            select_columns = [
                'id',
                f'"{columns.get("reg", "reg")}" as reg',
                f'"{columns.get("opr", "opr")}" as opr',
                f'"{columns.get("typ", "typ")}" as typ',
                f'"{columns.get("dep", "dep_1")}" as dep',
                f'"{columns.get("dest", "dest")}" as dest',
                f'"{columns.get("flight_zone_radius", "flight_zone_radius")}" as flight_zone_radius',
                f'"{columns.get("flight_level", "flight_level")}" as flight_level',
                f'"{columns.get("departure_time", "departure_time")}" as departure_time',
                f'"{columns.get("arrival_time", "arrival_time")}" as arrival_time',
                'date',
                'region_calculated'
            ]

            year_where = where_clauses.copy()
            year_where.append("EXTRACT(YEAR FROM date) = :year")
            params_year = params.copy()
            params_year["year"] = year_val

            year_where_sql = " AND ".join(year_where)

            query_str = f'''
                SELECT {", ".join(select_columns)}
                FROM "{current_table}"
                WHERE {year_where_sql}
                ORDER BY date DESC, id DESC
            '''
            if limit is not None:
                query_str += ' LIMIT :limit OFFSET :offset'
                params_year["limit"] = limit
                params_year["offset"] = offset
            
            query = text(query_str)
            
            result = db.execute(query, params_year)
            data = [dict(row) for row in result.mappings().all()]
            
            count_query = text(f'''
                SELECT COUNT(*) as total_count
                FROM "{current_table}" 
                WHERE {year_where_sql}
            ''')
            total_count = db.execute(count_query, params_year).scalar() or 0
            
            # ОБРАБОТКА ДАННЫХ ДЛЯ ГОДА - добавляем вычисленные значения в метрах
            enhanced_data = []
            for item in data:
                enhanced_item = item.copy()
                
                # Добавляем вычисленную высоту в метрах
                if item.get('flight_level'):
                    level_data = parse_flight_level(item['flight_level'])
                    if level_data:
                        enhanced_item['flight_level_min_m'] = level_data['min_height']
                        enhanced_item['flight_level_max_m'] = level_data['max_height']
                        enhanced_item['flight_level_avg_m'] = round((level_data['min_height'] + level_data['max_height']) / 2, 1)
                
                # Добавляем вычисленный радиус в метрах
                if item.get('flight_zone_radius'):
                    radius_m = parse_radius_for_stats(item['flight_zone_radius'])
                    if radius_m:
                        enhanced_item['flight_zone_radius_m'] = radius_m
                        enhanced_item['flight_zone_radius_km'] = round(radius_m / 1000, 2)
                
                # Добавляем продолжительность полета в минутах
                if item.get('departure_time') and item.get('arrival_time'):
                    try:
                        dep_time = datetime.strptime(item['departure_time'], '%H:%M:%S').time()
                        arr_time = datetime.strptime(item['arrival_time'], '%H:%M:%S').time()
                        duration_minutes = (datetime.combine(date.today(), arr_time) - 
                                          datetime.combine(date.today(), dep_time)).total_seconds() / 60
                        if duration_minutes > 0:
                            enhanced_item['flight_duration_minutes'] = round(duration_minutes, 1)
                    except:
                        pass
                
                enhanced_data.append(enhanced_item)
            
            if enhanced_data or total_count > 0:
                yearly_data[str(year_val)] = {
                    "data": enhanced_data,
                    "count": len(enhanced_data),
                    "total_count": total_count,
                    "year": year_val,
                    "has_more": limit is not None and total_count > (limit + offset) if limit else False
                }
                total_records += len(enhanced_data)

        processing_time = time.time() - start_time
        
        logger.info(f"✅ Главная страница загружена за {processing_time:.3f}с: {len(yearly_data)} лет, {total_records} записей")

        return {
            "data_by_year": yearly_data,
            "available_years": available_years,
            "current_table": current_table,
            "total_years": len(available_years),
            "total_records": total_records,
            "pagination": {
                "limit": limit,
                "offset": offset,
                "has_more": any(year_data.get("has_more", False) for year_data in yearly_data.values())
            },
            "performance": {
                "processing_time_seconds": round(processing_time, 3),
                "years_loaded": len(yearly_data),
                "records_per_second": round(total_records / processing_time, 1) if processing_time > 0 else 0
            },
            "filters": {
                "date_from": date_from,
                "date_to": date_to,
                "region": region
            },
            "units": {
                "height": "meters",
                "radius": "meters", 
                "duration": "minutes"
            },
            "data_enhancement": {
                "flight_level_m": "Добавлены min, max, avg высоты в метрах",
                "flight_radius_m": "Добавлен радиус в метрах и километрах", 
                "flight_duration": "Добавлена продолжительность полета в минутах"
            }
        }
        
    except Exception as e:
        logger.error(f" Ошибка на главной странице: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки данных: {str(e)}")

@app.get("/statistics", 
    description="Возвращает все данные из текущей таблицы сгруппированные по годам",
    tags=["Получение данных"])
async def get_statistics(
    date_from: Optional[str] = Query(None, description="Начальная дата в формате YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="Конечная дата в формате YYYY-MM-DD"),
    region: Optional[str] = Query(None, description="Регион для фильтрации"),
    limit: Optional[int] = Query(None, description="Лимит записей (для теста)"),
    offset: int = Query(0, description="Смещение для пагинации (для теста)"),
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)
):
    """Возвращает статистику сгруппированную по годам"""
    try:
        start_time = time.time()
        
        table_exists = db.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = :table_name
            )
        """), {"table_name": current_table}).scalar()

        if not table_exists:
            raise HTTPException(status_code=404, detail=f"Таблица {current_table} не найдена")

        params = {}
        where_clauses = ["date IS NOT NULL"]
        if date_from:
            where_clauses.append("date >= (:date_from)::date")
            params["date_from"] = date_from
        if date_to:
            where_clauses.append("date <= (:date_to)::date")
            params["date_to"] = date_to
        if region:
            where_clauses.append("region_calculated = :region")
            params["region"] = region

        where_sql = " AND ".join(where_clauses)

        available_years = get_available_years_fast(db, current_table, region, date_from, date_to)
        
        if not available_years:
            # Резервный метод если нет дат
            columns = _get_required_columns(db, current_table)
            select_columns = [
                'id',
                f'"{columns.get("reg", "reg")}" as reg',
                f'"{columns.get("opr", "opr")}" as opr',
                f'"{columns.get("typ", "typ")}" as typ',
                f'"{columns.get("dep", "dep_1")}" as dep',
                f'"{columns.get("dest", "dest")}" as dest',
                f'"{columns.get("flight_zone_radius", "flight_zone_radius")}" as flight_zone_radius',
                f'"{columns.get("flight_level", "flight_level")}" as flight_level',
                f'"{columns.get("departure_time", "departure_time")}" as departure_time',
                f'"{columns.get("arrival_time", "arrival_time")}" as arrival_time'
            ]
            
            region_exists = db.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = :table_name 
                AND column_name = 'region_calculated'
            """), {"table_name": current_table}).fetchone()
            
            if region_exists:
                select_columns.append('region_calculated')
            
            query_str = f'''
                SELECT {", ".join(select_columns)}
                FROM "{current_table}" 
                WHERE {where_sql}
                ORDER BY id DESC
            '''
            if limit is not None:
                query_str += ' LIMIT :limit OFFSET :offset'
                params["limit"] = limit
                params["offset"] = offset
            
            query = text(query_str)
            
            result = db.execute(query, params)
            data = [dict(row) for row in result.mappings().all()]
            
            total_count = db.execute(text(f'SELECT COUNT(*) FROM "{current_table}" WHERE {where_sql}'), params).scalar() or 0
            
            return {
                "data": data,
                "count": len(data),
                "total_count": total_count,
                "current_table": current_table,
                "message": "Данные не сгруппированы по годам (отсутствуют даты)",
                "available_years": []
            }

        # Данные по годам
        yearly_data = {}
        total_records = 0
        columns = _get_required_columns(db, current_table)
        
        for year_val in available_years:
            select_columns = [
                'id',
                f'"{columns.get("reg", "reg")}" as reg',
                f'"{columns.get("opr", "opr")}" as opr',
                f'"{columns.get("typ", "typ")}" as typ',
                f'"{columns.get("dep", "dep_1")}" as dep',
                f'"{columns.get("dest", "dest")}" as dest',
                f'"{columns.get("flight_zone_radius", "flight_zone_radius")}" as flight_zone_radius',
                f'"{columns.get("flight_level", "flight_level")}" as flight_level',
                f'"{columns.get("departure_time", "departure_time")}" as departure_time',
                f'"{columns.get("arrival_time", "arrival_time")}" as arrival_time',
                'date',
                'region_calculated'
            ]

            year_where = where_clauses.copy()
            year_where.append("EXTRACT(YEAR FROM date) = :year")
            params_year = params.copy()
            params_year["year"] = year_val

            year_where_sql = " AND ".join(year_where)

            query_str = f'''
                SELECT {", ".join(select_columns)}
                FROM "{current_table}"
                WHERE {year_where_sql}
                ORDER BY date DESC, id DESC
            '''
            if limit is not None:
                query_str += ' LIMIT :limit OFFSET :offset'
                params_year["limit"] = limit
                params_year["offset"] = offset
            
            query = text(query_str)
            
            result = db.execute(query, params_year)
            data = [dict(row) for row in result.mappings().all()]
            
            count_query = text(f'''
                SELECT COUNT(*) as total_count
                FROM "{current_table}" 
                WHERE {year_where_sql}
            ''')
            total_count = db.execute(count_query, params_year).scalar() or 0
            
            # СТАТИСТИКА ПО ВЫСОТЕ ПОЛЕТА ДЛЯ ГОДА - используем ваш парсер
            level_stats_query = text(f'''
                SELECT flight_level
                FROM "{current_table}"
                WHERE flight_level IS NOT NULL 
                AND flight_level != ''
                AND {year_where_sql}
            ''')
            
            level_results = db.execute(level_stats_query, params_year).fetchall()
            
            # Обрабатываем высоты через ваш парсер
            valid_levels = []
            level_details = []
            for row in level_results:
                level_data = parse_flight_level(row[0])
                if level_data is not None:
                    avg_height = (level_data["min_height"] + level_data["max_height"]) / 2
                    if avg_height <= 2000:  # Фильтр выбросов
                        valid_levels.append(avg_height)
                        level_details.append({
                            "original": row[0],
                            "min_height_m": level_data["min_height"],
                            "max_height_m": level_data["max_height"],
                            "avg_height_m": round(avg_height, 1)
                        })
            
            # Вычисляем статистику по высотам
            level_stats = {}
            if valid_levels:
                level_stats = {
                    "avg_level_m": round(sum(valid_levels) / len(valid_levels), 1),
                    "median_level_m": round(sorted(valid_levels)[len(valid_levels) // 2], 1),
                    "max_level_m": round(max(valid_levels), 1),
                    "min_level_m": round(min(valid_levels), 1),
                    "valid_levels_count": len(valid_levels),
                    "level_range": f"{round(min(valid_levels), 1)} - {round(max(valid_levels), 1)} м"
                }
            
            # СТАТИСТИКА ПО РАДИУСУ ПОЛЕТА ДЛЯ ГОДА - используем ваш парсер
            radius_stats_query = text(f'''
                SELECT flight_zone_radius
                FROM "{current_table}"
                WHERE flight_zone_radius IS NOT NULL 
                AND flight_zone_radius != ''
                AND {year_where_sql}
            ''')
            
            radius_results = db.execute(radius_stats_query, params_year).fetchall()
            
            # Обрабатываем радиусы через ваш парсер
            valid_radii = []
            radius_details = []
            for row in radius_results:
                radius_value = parse_radius_for_stats(row[0])
                if radius_value is not None:
                    valid_radii.append(radius_value)
                    radius_details.append({
                        "original": row[0],
                        "radius_m": radius_value,
                        "radius_km": round(radius_value / 1000, 2)
                    })
            
            # Вычисляем статистику по радиусам
            radius_stats = {}
            if valid_radii:
                radius_stats = {
                    "avg_radius_m": round(sum(valid_radii) / len(valid_radii), 1),
                    "median_radius_m": round(sorted(valid_radii)[len(valid_radii) // 2], 1),
                    "max_radius_m": round(max(valid_radii), 1),
                    "min_radius_m": round(min(valid_radii), 1),
                    "valid_radius_count": len(valid_radii),
                    "radius_range": f"{round(min(valid_radii), 1)} - {round(max(valid_radii), 1)} м",
                    "avg_radius_km": round(sum(valid_radii) / len(valid_radii) / 1000, 2)
                }
            
            # СТАТИСТИКА ПО ПРОДОЛЖИТЕЛЬНОСТИ ПОЛЕТА
            duration_stats_query = text(f'''
                SELECT 
                    AVG(duration_minutes) as avg_duration,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_minutes) as median_duration,
                    MAX(duration_minutes) as max_duration,
                    COUNT(duration_minutes) as valid_duration_count
                FROM (
                    SELECT 
                        CASE 
                            WHEN departure_time IS NOT NULL AND arrival_time IS NOT NULL 
                                 AND departure_time ~ '^[0-2][0-9]:[0-5][0-9]:[0-5][0-9]$'
                                 AND arrival_time ~ '^[0-2][0-9]:[0-5][0-9]:[0-5][0-9]$' THEN
                                EXTRACT(EPOCH FROM (
                                    arrival_time::time - departure_time::time
                                )) / 60
                            ELSE NULL
                        END as duration_minutes
                    FROM "{current_table}"
                    WHERE {year_where_sql}
                ) as durations
                WHERE duration_minutes IS NOT NULL AND duration_minutes > 0
            ''')
            
            duration_stats = db.execute(duration_stats_query, params_year).fetchone()
            
            # ОБЩАЯ СТАТИСТИКА ПО ГОДУ
            general_stats_query = text(f'''
                SELECT 
                    COUNT(*) as total_flights,
                    COUNT(DISTINCT region_calculated) as total_regions,
                    COUNT(DISTINCT reg) as total_aircrafts,
                    COUNT(DISTINCT opr) as total_operators
                FROM "{current_table}"
                WHERE {year_where_sql}
            ''')
            
            general_stats = db.execute(general_stats_query, params_year).fetchone()
            
            # ПРЕОБРАЗУЕМ ДАННЫЕ ДЛЯ ВЫВОДА - добавляем вычисленные значения в метрах
            enhanced_data = []
            for item in data:
                enhanced_item = item.copy()
                
                # Добавляем вычисленную высоту в метрах
                if item.get('flight_level'):
                    level_data = parse_flight_level(item['flight_level'])
                    if level_data:
                        enhanced_item['flight_level_min_m'] = level_data['min_height']
                        enhanced_item['flight_level_max_m'] = level_data['max_height']
                        enhanced_item['flight_level_avg_m'] = round((level_data['min_height'] + level_data['max_height']) / 2, 1)
                
                # Добавляем вычисленный радиус в метрах
                if item.get('flight_zone_radius'):
                    radius_m = parse_radius_for_stats(item['flight_zone_radius'])
                    if radius_m:
                        enhanced_item['flight_zone_radius_m'] = radius_m
                        enhanced_item['flight_zone_radius_km'] = round(radius_m / 1000, 2)
                
                # Добавляем продолжительность полета в минутах
                if item.get('departure_time') and item.get('arrival_time'):
                    try:
                        dep_time = datetime.strptime(item['departure_time'], '%H:%M:%S').time()
                        arr_time = datetime.strptime(item['arrival_time'], '%H:%M:%S').time()
                        duration_minutes = (datetime.combine(date.today(), arr_time) - 
                                          datetime.combine(date.today(), dep_time)).total_seconds() / 60
                        if duration_minutes > 0:
                            enhanced_item['flight_duration_minutes'] = round(duration_minutes, 1)
                    except:
                        pass
                
                enhanced_data.append(enhanced_item)
            
            if enhanced_data:
                yearly_data[str(year_val)] = {
                    "data": enhanced_data,
                    "count": len(enhanced_data),
                    "total_count": total_count,
                    "year": year_val,
                    "has_more": limit is not None and total_count > limit + offset,
                    "statistics": {
                        "general": {
                            "total_flights": general_stats[0] if general_stats else 0,
                            "total_regions": general_stats[1] if general_stats else 0,
                            "total_aircrafts": general_stats[2] if general_stats else 0,
                            "total_operators": general_stats[3] if general_stats else 0
                        },
                        "flight_level": level_stats,
                        "flight_radius": radius_stats,
                        "duration": {
                            "avg_duration_minutes": round(float(duration_stats[0] or 0), 1) if duration_stats else 0,
                            "median_duration_minutes": round(float(duration_stats[1] or 0), 1) if duration_stats else 0,
                            "max_duration_minutes": round(float(duration_stats[2] or 0), 1) if duration_stats else 0,
                            "valid_duration_count": duration_stats[3] if duration_stats else 0
                        }
                    }
                }
                total_records += len(enhanced_data)

        processing_time = time.time() - start_time
        
        return {
            "data_by_year": yearly_data,
            "available_years": available_years,
            "current_table": current_table,
            "total_years": len(available_years),
            "total_records": total_records,
            "processing_time_seconds": round(processing_time, 2),
            "statistics_method": "flight_formats_parser",
            "units": {
                "height": "meters",
                "radius": "meters", 
                "duration": "minutes"
            }
        }
        
    except Exception as e:
        logger.error(f"Ошибка в /statistics: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка: {str(e)}")


@app.get("/city/{city_name}", 
    response_model=Dict[str, Any],
    description="Возвращает записи по центру ЕС ОРВД сгруппированные по годам",
    tags=["Получение данных"])
async def get_city_data(
    city_name: str = Path(..., description="Название центра ЕС ОРВД"),
    date_from: Optional[str] = Query(None, description="Начальная дата в формате YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="Конечная дата в формате YYYY-MM-DD"),
    region: Optional[str] = Query(None, description="Регион для фильтрации"),
    limit: Optional[int] = Query(None, description="Лимит записей (для теста)"),
    offset: int = Query(0, description="Смещение для пагинации (для теста)"),
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)
):
    """Возвращает данные для центра ЕС ОРВД сгруппированные по годам"""
    try:
        center_column = _find_column_case_insensitive(db, current_table, [
            "tsentr_es_orvd", "TSENTR_ES_ORVD", "центр", "center"
        ])

        if not center_column:
            raise HTTPException(
                status_code=400,
                detail="Не найдена колонка с центром ЕС ОРВД (tsentr_es_orvd)"
            )

        params = {"city_name": city_name}
        where_clauses = [f'"{center_column}" = :city_name', "date IS NOT NULL"]
        if date_from:
            where_clauses.append("date >= (:date_from)::date")
            params["date_from"] = date_from
        if date_to:
            where_clauses.append("date <= (:date_to)::date")
            params["date_to"] = date_to
        if region:
            where_clauses.append("region_calculated = :region")
            params["region"] = region

        where_sql = " AND ".join(where_clauses)

        # Все года для города
        year_query = text(f'''
            SELECT DISTINCT EXTRACT(YEAR FROM date) as year
            FROM "{current_table}"
            WHERE {where_sql}
            ORDER BY year DESC
        ''')
        years_result = db.execute(year_query, params)
        available_years = [int(row[0]) for row in years_result if row[0] is not None]
        
        yearly_data = {}
        total_records = 0
        columns = _get_required_columns(db, current_table)
        
        for year_val in available_years:
            select_columns = [
                'id',
                f'"{columns.get("reg", "reg")}" as reg',
                f'"{columns.get("opr", "opr")}" as opr',
                f'"{columns.get("typ", "typ")}" as typ',
                f'"{columns.get("dep", "dep_1")}" as dep',
                f'"{columns.get("dest", "dest")}" as dest',
                f'"{columns.get("flight_zone_radius", "flight_zone_radius")}" as flight_zone_radius',
                f'"{columns.get("flight_level", "flight_level")}" as flight_level',
                f'"{columns.get("departure_time", "departure_time")}" as departure_time',
                f'"{columns.get("arrival_time", "arrival_time")}" as arrival_time',
                'date',
                'region_calculated'
            ]

            year_where = where_clauses.copy()
            year_where.append("EXTRACT(YEAR FROM date) = :year")
            params_year = params.copy()
            params_year["year"] = year_val

            year_where_sql = " AND ".join(year_where)

            query_str = f'''
                SELECT {", ".join(select_columns)}
                FROM "{current_table}"
                WHERE {year_where_sql}
                ORDER BY date DESC
            '''
            if limit is not None:
                query_str += ' LIMIT :limit OFFSET :offset'
                params_year["limit"] = limit
                params_year["offset"] = offset
            
            query = text(query_str)
            
            result = db.execute(query, params_year)
            data = [dict(row) for row in result.mappings().all()]
            
            count_query = text(f'''
                SELECT COUNT(*) 
                FROM "{current_table}" 
                WHERE {year_where_sql}
            ''')
            total_count = db.execute(count_query, params_year).scalar() or 0
            
            if data:
                yearly_data[str(year_val)] = {
                    "data": data,
                    "count": len(data),
                    "total_count": total_count,
                    "year": year_val,
                    "has_more": limit is not None and total_count > limit + offset
                }
                total_records += len(data)
    
        return {
            "center": city_name,
            "data_by_year": yearly_data,
            "available_years": available_years,
            "total_years": len(available_years),
            "total_records": total_records
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка в /city/{city_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка: {str(e)}")

@app.get("/health", 
    response_model=HealthResponse, 
    description="Проверяет работоспособность сервиса и возвращает статус OK и временную метку",
    tags=["Системные эндпоинты"])
async def health():
    return HealthResponse(status="OK", timestamp=datetime.now().isoformat())

@app.get("/cities", 
    description="Возвращает список всех регионов, загруженных из shapefile, с их русскими и английскими названиями и уровнем администрирования",
    tags=["Работа с регионами"])
async def get_cities(db: Session = Depends(get_db)):
    """Получить список всех регионов из shapefile"""
    try:
        from region_service import region_service
        
        # Убеждаемся, что shapefile загружен
        region_service.ensure_shapefile_loaded()
        
        if region_service.gdf is None or region_service.gdf.empty:
            return {
                "regions": [],
                "total": 0,
                "message": "Shapefile не загружен"
            }
        
        # Более гибкая фильтрация - проверяем по ключевым словам
        def is_russian_region(name_ru):
            """Проверяем, является ли регион российским по ключевым словам"""
            russian_keywords = [
                # Типы субъектов РФ
                'республика', 'край', 'область', 'автономный', 'округ',
                # Ключевые города
                'москва', 'санкт-петербург', 'севастополь',
                # Названия республик (короткие формы)
                'адыгея', 'алтай', 'башкортостан', 'бурятия', 'дагестан', 
                'ингушетия', 'кабардино-балкария', 'калмыкия', 'карачаево-черкесия',
                'карелия', 'коми', 'крым', 'марий', 'мордовия', 'саха', 'якутия',
                'осетия', 'татарстан', 'тыва', 'удмуртия', 'хакасия', 'чечня', 'чувашия'
            ]
            
            name_lower = name_ru.lower()
            return any(keyword in name_lower for keyword in russian_keywords)
        
        # Убираем дубликаты и фильтруем только российские регионы
        unique_regions = {}
        for idx, row in region_service.gdf.iterrows():
            name_ru = row.get("name_ru", "Неизвестно")
            name_en = row.get("name_en", "Unknown")
            admin_level = row.get("admin_level", row.get("admin_leve", None))
            
            # Пропускаем не-российские регионы
            if not is_russian_region(name_ru):
                continue
            
            # ИСКЛЮЧАЕМ ДУБЛИКАТЫ КРЫМА - оставляем только "Республика Крым"
            if name_ru == "Автономная Республика Крым":
                continue
                
            # Исправляем некорректные английские названия
            name_en_corrections = {
                "Leningrad oblast": "Leningrad Oblast",
                "Kaliningrad": "Kaliningrad Oblast",
                "Adygea": "Republic of Adygea",
                "Bashkortostan": "Republic of Bashkortostan", 
                "Buryatia": "Republic of Buryatia",
                "Dagestan": "Republic of Dagestan",
                "Ingushetia": "Republic of Ingushetia",
                "Kabardino-Balkaria": "Kabardino-Balkar Republic",
                "Kalmykia": "Republic of Kalmykia",
                "Karachay-Cherkessia": "Karachay-Cherkess Republic",
                "Mari El": "Republic of Mari El",
                "Mordovia": "Republic of Mordovia",
                "North Ossetia–Alania": "Republic of North Ossetia–Alania",
                "Tatarstan": "Republic of Tatarstan",
                "Tuva": "Republic of Tuva",
                "Udmurtia": "Udmurt Republic",
                "Chechnya": "Chechen Republic",
                "Chuvashia": "Chuvash Republic",
                "Khakassia": "Republic of Khakassia"
            }
            
            if name_en in name_en_corrections:
                name_en = name_en_corrections[name_en]
            
            # Убираем дубликаты по name_ru
            if name_ru not in unique_regions:
                unique_regions[name_ru] = {
                    "name_ru": name_ru,
                    "name_en": name_en,
                    "admin_level": admin_level
                }
        
        regions = list(unique_regions.values())
        
        # Сортируем по русскому названию для удобства
        regions.sort(key=lambda x: x["name_ru"])
        
        return {
            "regions": regions,
            "total": len(regions),
            "source": "shapefile"
        }
        
    except Exception as e:
        logger.error(f"Ошибка в /cities: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка: {str(e)}")


@app.get("/stats/regions", 
    description="Возвращает статистику по количеству полетов и средней продолжительности полета для каждого региона",
    tags=["Статистика"])
def get_stats_regions(
    date_from: Optional[str] = Query(None, description="Начальная дата в формате YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="Конечная дата в формате YYYY-MM-DD"),
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)
):
    try:
        # Ищем колонки с временем полета
        dep_time_column = _find_column_case_insensitive(db, current_table, [
            "departure_time", "DEPARTURE_TIME", "departure", "время_вылета", "time_dep"
        ])
        arr_time_column = _find_column_case_insensitive(db, current_table, [
            "arrival_time", "ARRIVAL_TIME", "arrival", "время_прибытия", "time_arr"
        ])
        date_column = _find_column_case_insensitive(db, current_table, ["dof", "date"])

        params = {}
        where_clauses = ["region_calculated IS NOT NULL"]
        
        # Добавляем условия по дате, если есть date_column
        if date_column:
            where_clauses.append(f'"{date_column}" IS NOT NULL')
            if date_from:
                where_clauses.append(f"TO_DATE(\"{date_column}\", 'YYMMDD') >= (:date_from)::date")
                params["date_from"] = date_from
            if date_to:
                where_clauses.append(f"TO_DATE(\"{date_column}\", 'YYMMDD') <= (:date_to)::date")
                params["date_to"] = date_to

        where_sql = " AND ".join(where_clauses)

        # Основной запрос для получения статистики по регионам
        if dep_time_column and arr_time_column:
            # Запрос с расчетом продолжительности полета
            query = text(f'''
                SELECT 
                    region_calculated,
                    COUNT(*) as flight_count,
                    AVG(
                        CASE 
                            WHEN "{dep_time_column}" IS NOT NULL 
                                 AND "{arr_time_column}" IS NOT NULL 
                                 AND "{dep_time_column}" ~ '^[0-2][0-9]:[0-5][0-9]:[0-5][0-9]$'
                                 AND "{arr_time_column}" ~ '^[0-2][0-9]:[0-5][0-9]:[0-5][0-9]$'
                            THEN EXTRACT(EPOCH FROM (
                                "{arr_time_column}"::time - "{dep_time_column}"::time
                            )) / 60
                            ELSE NULL
                        END
                    ) as avg_flight_duration
                FROM "{current_table}" 
                WHERE {where_sql}
                GROUP BY region_calculated
                ORDER BY flight_count DESC
            ''')
        else:
            # Запрос без расчета продолжительности (если нет колонок времени)
            query = text(f'''
                SELECT 
                    region_calculated,
                    COUNT(*) as flight_count,
                    NULL as avg_flight_duration
                FROM "{current_table}" 
                WHERE {where_sql}
                GROUP BY region_calculated
                ORDER BY flight_count DESC
            ''')

        result = db.execute(query, params).mappings().all()

        # Форматируем результат
        result_list = []
        for row in result:
            region_data = {
                "region": row["region_calculated"],
                "num_flights": row["flight_count"],
                "avg_flight_duration": round(float(row["avg_flight_duration"] or 0), 2) if row["avg_flight_duration"] is not None else None
            }
            
            # Добавляем информацию о годах, если есть даты
            if date_column:
                # Получаем года для региона
                year_query = text(f'''
                    SELECT DISTINCT EXTRACT(YEAR FROM TO_DATE("{date_column}", 'YYMMDD')) as year
                    FROM "{current_table}"
                    WHERE region_calculated = :region AND "{date_column}" IS NOT NULL
                    ORDER BY year DESC
                ''')
                years_result = db.execute(year_query, {"region": row["region_calculated"]})
                years = [int(row[0]) for row in years_result if row[0] is not None]
                
                if years:
                    region_data["first_year"] = min(years)
                    region_data["last_year"] = max(years)
                    region_data["years"] = {str(year): 0 for year in years}  # Можно добавить подсчет по годам при необходимости

            result_list.append(region_data)

        return result_list

    except Exception as e:
        logger.error(f"❌ Ошибка при подсчете статистики регионов: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при подсчете статистики: {e}")

@app.get("/stats/region/{region_name}",
    description="Возвращает количество полетов и среднюю продолжительность полета для указанного региона",
    tags=["Статистика"])
def region_stats(
    region_name: str, 
    date_from: Optional[str] = Query(None, description="Начальная дата в формате YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="Конечная дата в формате YYYY-MM-DD"),
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)
):
    """
    Возвращает статистику по региону:
    - количество рейсов
    - среднее время полета (минуты)
    """
    try:
        params = {"region": region_name}
        where_clauses = ["region_calculated = :region"]
        if date_from:
            where_clauses.append("date >= (:date_from)::date")
            params["date_from"] = date_from
        if date_to:
            where_clauses.append("date <= (:date_to)::date")
            params["date_to"] = date_to

        where_sql = " AND ".join(where_clauses)

        query = text(f'''
            SELECT departure_time, arrival_time
            FROM "{current_table}"
            WHERE {where_sql}
        ''')
        result = db.execute(query, params).fetchall()

        if not result:
            raise HTTPException(status_code=404, detail="Регион не найден")

        durations = []
        for row in result:
            dur = parse_flight_duration(row[0], row[1])
            if dur is not None:
                durations.append(dur)

        total_flights = len(durations)
        avg_duration = sum(durations) / total_flights if total_flights else 0

        return {
            "region": region_name,
            "total_flights": total_flights,
            "average_duration_minutes": round(avg_duration, 2)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при подсчете статистики: {e}")


@app.get("/debug/new-points")
def debug_new_points(
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)
):
    """Показать данные новых записей (дозагруженных)"""
    try:
        # Получаем последние 10 записей (скорее всего дозагруженные)
        query = text(f'''
            SELECT 
                id,
                dep_1,
                dep, 
                dest,
                arr,
                date,
                region_calculated,
                dof
            FROM "{current_table}" 
            ORDER BY id DESC 
            LIMIT 10
        ''')
        
        result = db.execute(query)
        records = [dict(row) for row in result.mappings().all()]
        
        # Проверяем координаты в каждой записи
        analyzed_records = []
        for record in records:
            analysis = {
                "id": record["id"],
                "has_date": record["date"] is not None,
                "has_region": record["region_calculated"] is not None,
                "fields_analysis": {}
            }
            
            # Проверяем каждое поле на наличие координат
            coord_fields = ["dep_1", "dep", "dest", "arr"]
            for field in coord_fields:
                field_value = record[field]
                has_value = field_value is not None and str(field_value).strip() != ""
                coords = convert_coord(field_value) if has_value else None
                has_coords = coords and coords.get("latitude") is not None
                
                analysis["fields_analysis"][field] = {
                    "has_value": has_value,
                    "value": field_value,
                    "has_coords": has_coords,
                    "coords_result": coords
                }
            
            analyzed_records.append(analysis)
        
        return {
            "total_records": len(records),
            "records": analyzed_records
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/flights/points", 
    description="Возвращает список точек взлета (ID, широта, долгота) для всех рейсов",
    tags=["Получение данных"])
def get_flight_points(
    date_from: Optional[str] = Query(None, description="Начальная дата в формате YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="Конечная дата в формате YYYY-MM-DD"),
    region: Optional[str] = Query(None, description="Регион для фильтрации"),
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)
):
    """
    Возвращает список точек взлета для всех рейсов: id + координаты
    Ищет координаты в полях: dep_1 (для новых записей), dest (для старых записей)
    """
    try:
        # 🔥 ПРИНУДИТЕЛЬНОЕ ОБНОВЛЕНИЕ СТАТИСТИКИ
        db.execute(text(f'ANALYZE "{current_table}";'))
        db.commit()
        
        params = {}
        where_clauses = ["date IS NOT NULL"]
        
        # 🔥 ИЩЕМ ЗАПИСИ С КООРДИНАТАМИ В ЛЮБОМ ИЗ ПОЛЕЙ
        where_clauses.append("(dep_1 IS NOT NULL OR dest IS NOT NULL OR dep IS NOT NULL OR arr IS NOT NULL)")
        
        if date_from:
            where_clauses.append("date >= (:date_from)::date")
            params["date_from"] = date_from
        if date_to:
            where_clauses.append("date <= (:date_to)::date")
            params["date_to"] = date_to
        if region:
            where_clauses.append("region_calculated = :region")
            params["region"] = region

        where_sql = " AND ".join(where_clauses)

        query = text(f'''
            SELECT id, dep_1, dest, dep, arr, date, region_calculated
            FROM "{current_table}" 
            WHERE {where_sql}
            ORDER BY date DESC, id DESC
        ''')
        
        result = db.execute(query, params)
        
        points_by_year = defaultdict(list)
        total_points = 0
        field_stats = {
            "dep_1": 0,
            "dest": 0,
            "dep": 0, 
            "arr": 0
        }
        
        for row in result.fetchall():
            coords = None
            source_field = None
            
            # 🔥 ПРАВИЛЬНЫЙ ПРИОРИТЕТ ПОИСКА КООРДИНАТ:
            # 1. Сначала пробуем dep_1 (для новых записей ID 10-18)
            if row[1]:  # dep_1
                coords = convert_coord(row[1])
                if coords and coords.get("latitude") is not None and coords.get("longitude") is not None:
                    source_field = "dep_1"
                    field_stats["dep_1"] += 1
            
            # 2. Если в dep_1 нет координат, пробуем dest (для старых записей ID 1-9)
            if not coords or coords.get("latitude") is None:
                if row[2]:  # dest
                    coords = convert_coord(row[2])
                    if coords and coords.get("latitude") is not None and coords.get("longitude") is not None:
                        source_field = "dest"
                        field_stats["dest"] += 1
            
            # 3. Если все еще нет координат, пробуем остальные поля
            if not coords or coords.get("latitude") is None:
                if row[3]:  # dep
                    coords = convert_coord(row[3])
                    if coords and coords.get("latitude") is not None and coords.get("longitude") is not None:
                        source_field = "dep"
                        field_stats["dep"] += 1
            
            if not coords or coords.get("latitude") is None:
                if row[4]:  # arr
                    coords = convert_coord(row[4])
                    if coords and coords.get("latitude") is not None and coords.get("longitude") is not None:
                        source_field = "arr"
                        field_stats["arr"] += 1
            
            # Если нашли координаты, добавляем точку
            if coords and coords.get("latitude") is not None and coords.get("longitude") is not None:
                point_data = {
                    "id": row[0],
                    "latitude": coords["latitude"],
                    "longitude": coords["longitude"],
                    "source_field": source_field,
                    "date": row[5].isoformat() if hasattr(row[5], 'isoformat') else str(row[5]),
                    "region": row[6],
                    "original_coords": coords.get("original", "")
                }
                
                point_year = row[5].year if hasattr(row[5], 'year') else None
                if point_year:
                    points_by_year[str(point_year)].append(point_data)
                    total_points += 1
        
        logger.info(f"📍 Найдено точек: {total_points} (источники: dep_1={field_stats['dep_1']}, dest={field_stats['dest']}, dep={field_stats['dep']}, arr={field_stats['arr']})")
        
        return {
            "points_by_year": dict(points_by_year),
            "total_points": total_points,
            "statistics": {
                "field_sources": field_stats,
                "date_range": {
                    "from": date_from,
                    "to": date_to
                },
                "region_filter": region
            }
        }
    except Exception as e:
        logger.error(f"❌ Ошибка при получении точек: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при получении точек: {e}")

@app.get("/flights/{flight_id}", 
    response_model=FlightZoneResponse, 
    description="Возвращает подробную информацию о конкретном полете, включая зону полета, радиус, точки взлета и посадки, время полета, бортовой номер, дату, оператора и дополнительную информацию",
    tags=["Получение данных"])
async def get_flight_zone(
    flight_id: int = Path(..., description="ID полета"),
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)
):
    """Возвращает данные о зоне полета дрона"""
    try:
        # Ищем правильные имена колонок
        radius_column = _find_column_case_insensitive(db, current_table, [
            "flight_zone_radius", "FLIGHT_ZONE_RADIUS", "radius", "радиус", "flight_zone_radi"
        ])
        
        zone_column = _find_column_case_insensitive(db, current_table, [
            "flight_zone", "FLIGHT_ZONE", "zone", "зона"
        ])
        
        # Получаем данные полета
        result = db.execute(
            text(f'SELECT * FROM "{current_table}" WHERE id = :flight_id'),
            {"flight_id": flight_id}
        )
        flight_data = result.fetchone()
        
        if not flight_data:
            raise HTTPException(status_code=404, detail="Полет не найден")
        
        # Преобразуем в словарь
        columns = result.keys()
        flight_dict = dict(zip(columns, flight_data))

        date_formatted = None
        if "dof" in flight_dict and flight_dict['dof']:
            try:
                date_str = str(flight_dict["dof"])
                if len(date_str) == 6 and date_str.isdigit():
                    year = "20" + date_str[:2]
                    month = date_str[2:4]
                    day = date_str[4:6]
                    date_formatted = f"{year}-{month}-{day}"
            except:
                date_formatted = str(flight_dict['dof'])
        
        # Парсим координаты взлета и приземления
        dep_coords = parse_coord(flight_dict.get("dep_1", ""))
        dest_coords = parse_coord(flight_dict.get("dest", ""))
        
        # Определяем, совпадают ли точки взлета и приземления
        takeoff_point = {
            "raw": flight_dict.get("dep_1"),
            "latitude": dep_coords[0],
            "longitude": dep_coords[1],
            "parsed_coords": f"{dep_coords[0]}, {dep_coords[1]}"
        }
        
        landing_point = {
            "raw": flight_dict.get("dest"), 
            "latitude": dest_coords[0],
            "longitude": dest_coords[1],
            "parsed_coords": f"{dest_coords[0]}, {dest_coords[1]}"
        }
        
        points_match = (
            dep_coords[0] == dest_coords[0] and 
            dep_coords[1] == dest_coords[1] and
            flight_dict.get("dep_1") == flight_dict.get("dest")
        )
        
        # Получаем время с проверкой на None
        departure_time = flight_dict.get("departure_time") or ""
        arrival_time = flight_dict.get("arrival_time") or ""
        duration_minutes = parse_flight_duration(departure_time, arrival_time)
        
        # ВЫЧИСЛЯЕМ ДОПОЛНИТЕЛЬНЫЕ ЗНАЧЕНИЯ В МЕТРАХ С ИСПОЛЬЗОВАНИЕМ ВАШЕГО ПАРСЕРА
        # Обрабатываем высоту полета
        flight_level_original = flight_dict.get("flight_level")
        flight_level_parsed = None
        if flight_level_original:
            level_data = parse_flight_level(flight_level_original)
            if level_data:
                flight_level_parsed = {
                    "min_height_m": level_data["min_height"],
                    "max_height_m": level_data["max_height"],
                    "avg_height_m": round((level_data["min_height"] + level_data["max_height"]) / 2, 1),
                    "range": f"{level_data['min_height']}-{level_data['max_height']} м"
                }
        
        # Обрабатываем радиус полета с использованием parse_radius_for_stats
        radius_original = flight_dict.get(radius_column) if radius_column else None
        radius_parsed = None
        if radius_original:
            radius_meters = parse_radius_for_stats(radius_original)
            if radius_meters:
                radius_parsed = {
                    "radius_m": radius_meters,
                    "radius_km": round(radius_meters / 1000, 2),
                    "formatted": f"{radius_meters} м ({round(radius_meters / 1000, 2)} км)"
                }
        
        # Формируем расширенную информацию о полете
        flight_time_info = {
            "departure_time": departure_time,
            "arrival_time": arrival_time,
            "duration_minutes": duration_minutes
        }
        
        # Добавляем форматированную продолжительность, если есть данные
        if duration_minutes:
            hours = int(duration_minutes // 60)
            minutes = int(duration_minutes % 60)
            flight_time_info["duration_formatted"] = f"{hours}ч {minutes}м"
        
        # Формируем дополнительную информацию с вычисленными значениями
        additional_info = {
            "flight_level": flight_level_original,
            "aircraft_type": flight_dict.get("typ"),
            "remarks": flight_dict.get("rmk"),
            "region_calculated": flight_dict.get("region_calculated"),
            "departure_full": flight_dict.get("dep_1"),
            "destination_full": flight_dict.get("dest")
        }
        
        # Добавляем вычисленные значения в дополнительную информацию
        if flight_level_parsed:
            additional_info["flight_level_parsed_m"] = flight_level_parsed
        
        if radius_parsed:
            additional_info["flight_zone_radius_parsed_m"] = radius_parsed
        
        # Добавляем информацию о типе полета
        additional_info["flight_type"] = "circular" if points_match else "linear"
        additional_info["coordinates_match"] = points_match
        
        # Добавляем информацию о парсинге
        additional_info["parsing_info"] = {
            "height_parsed_successfully": flight_level_parsed is not None,
            "radius_parsed_successfully": radius_parsed is not None,
            "parser_version": "flight_formats_parser"
        }
        
        # Формируем ответ
        response_data = FlightZoneResponse(
            flight_id=flight_id,
            flight_zone=flight_dict.get(zone_column) if zone_column else None,
            flight_zone_radius=flight_dict.get(radius_column) if radius_column else None,
            takeoff_point=takeoff_point,
            landing_point=landing_point if not points_match else None,
            flight_time=flight_time_info,
            registration_number=flight_dict.get("reg"),
            date_of_flight=flight_dict.get("dof"),
            operator=flight_dict.get("opr"),
            additional_info=additional_info
        )
        
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка в /flights/{flight_id}/flight_zone: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при получении данных о зоне полета: {str(e)}")
@app.get("/stats/regions/monthly", 
    response_model=MonthlyStatsResponse, 
    description="Возвращает количество полетов по месяцам для каждого региона, используя дату полета и рассчитанный регион",
    tags=["Статистика"])
async def get_regions_monthly_stats(
    date_from: Optional[str] = Query(None, description="Начальная дата в формате YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="Конечная дата в формате YYYY-MM-DD"),
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)
):
    """Возвращает количество полетов для каждого региона по месяцам"""
    try:
        # Ищем колонку с датой полета
        date_column = _find_column_case_insensitive(db, current_table, [
            "dof", "DOF", "date_of_flight", "date", "дата"
        ])
        
        region_column = "region_calculated"

        if not date_column:
            raise HTTPException(
                status_code=400,
                detail="Не найдена колонка с датой полета"
            )

        # Словарь названий месяцев
        month_names = {
            1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
            5: "Май", 6: "Июнь", 7: "Июль", 8: "Август", 
            9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
        }

        params = {}
        where_clauses = [f'"{date_column}" IS NOT NULL', f'"{date_column}" != \'\'', f'{region_column} IS NOT NULL', f'{region_column} != \'\'']
        if date_from:
            where_clauses.append(f"TO_DATE(\"{date_column}\", 'DDMMYY') >= (:date_from)::date")
            params["date_from"] = date_from
        if date_to:
            where_clauses.append(f"TO_DATE(\"{date_column}\", 'DDMMYY') <= (:date_to)::date")
            params["date_to"] = date_to

        where_sql = " AND ".join(where_clauses)

        # Запрос для группировки по регионам и месяцам
        query = text(f'''
            SELECT 
                {region_column} as region,
                EXTRACT(MONTH FROM TO_DATE("{date_column}", 'DDMMYY')) as month,
                COUNT(*) as flight_count
            FROM "{current_table}"
            WHERE {where_sql}
            GROUP BY {region_column}, month
            ORDER BY {region_column}, month
        ''')
        
        result = db.execute(query, params)
        stats_data = result.fetchall()

        # Форматируем данные в удобную структуру
        regions_stats = {}
        
        for row in stats_data:
            region = row[0]
            month_num = int(row[1]) if row[1] else None
            count = row[2]
            
            if region and month_num and month_num in month_names:
                if region not in regions_stats:
                    regions_stats[region] = {}
                
                month_name = month_names[month_num]
                regions_stats[region][month_name] = count

        # Добавляем все месяцы с нулевыми значениями для полноты данных
        for region in regions_stats:
            for month_num, month_name in month_names.items():
                if month_name not in regions_stats[region]:
                    regions_stats[region][month_name] = 0

        # Преобразуем в список для удобства фронтенда
        result_list = []
        for region_name, months_data in regions_stats.items():
            region_data = {
                "region": region_name,
                "monthly_stats": []
            }
            
            # Собираем статистику по месяцам в правильном порядке
            for month_num in range(1, 13):
                month_name = month_names[month_num]
                region_data["monthly_stats"].append({
                    "month": month_name,
                    "flight_count": months_data[month_name]
                })
            
            result_list.append(region_data)

        return MonthlyStatsResponse(
            regions=result_list,
            total_regions=len(result_list),
            columns_used={
                "date_column": date_column,
                "region_column": region_column
            }
        )

    except Exception as e:
        logger.error(f"Ошибка в /stats/regions/monthly: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при получении статистики по регионам: {str(e)}")
    
@app.get("/stats/time-bounds", 
    description="Возвращает минимальную и максимальную даты из всех данных",
    tags=["Статистика"])
async def get_time_bounds(
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)
):
    """Получить временные границы всех данных"""
    try:
        # Проверяем наличие колонки date
        date_column_exists = db.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = :table_name 
            AND column_name = 'date'
        """), {"table_name": current_table}).fetchone()
        
        if not date_column_exists:
            return {
                "min_date": "2020-01-01",
                "max_date": datetime.now().strftime("%Y-%m-%d"),
                "has_date_data": False
            }

        # Получаем минимальную и максимальную даты
        bounds_query = text(f'''
            SELECT 
                MIN(date) as min_date,
                MAX(date) as max_date
            FROM "{current_table}"
            WHERE date IS NOT NULL
        ''')
        
        result = db.execute(bounds_query).fetchone()
        
        min_date = result[0] if result and result[0] else datetime(2020, 1, 1)
        max_date = result[1] if result and result[1] else datetime.now()
        
        return {
            "min_date": min_date.strftime("%Y-%m-%d"),
            "max_date": max_date.strftime("%Y-%m-%d"),
            "has_date_data": True,
            "total_years": (max_date.year - min_date.year + 1) if min_date and max_date else 0
        }
        
    except Exception as e:
        logger.error(f"Ошибка получения временных границ: {e}")
        return {
            "min_date": "2020-01-01",
            "max_date": datetime.now().strftime("%Y-%m-%d"),
            "has_date_data": False,
            "error": str(e)
        }

@app.post("/api/upload", 
        description="Позволяет загрузить Excel-файл, обработать его листы, сохранить данные в новую таблицу, зарегистрировать таблицу, установить ее как текущую и добавить информацию о регионах",
        tags=["Управление таблицами"])
async def upload_file(
    file: UploadFile = File(...), 
    db: Session = Depends(get_db),
    table_service: TableService = Depends(get_table_service),
    session_id: str = Depends(get_session_id)
):
    try:
        logger.info(f" Загрузка файла: {file.filename}")
        
        # Генерируем уникальное имя таблицы
        import time
        timestamp = int(time.time())
        table_name = f"excel_data_{timestamp}"
        
        # Сохраняем файл временно
        contents = await file.read()
        temp_filename = f"temp_{timestamp}_{file.filename}"
        with open(temp_filename, "wb") as f:
            f.write(contents)

        # ИСПОЛЬЗУЕМ НОВЫЙ МЕТОД ПАРСИНГА ПО ТРЕБОВАНИЮ
        excel_parser = ExcelParser()
        parse_result = excel_parser.parse_uploaded_file(temp_filename, table_name)
        
        if "error" in parse_result:
            raise HTTPException(status_code=400, detail=parse_result["error"])
        
        #РЕГИСТРИРУЕМ НОВУЮ ТАБЛИЦУ
        table_service.register_table(
            table_name=table_name,
            original_filename=file.filename,
            description=f"Загружено: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )
        
        # УСТАНАВЛИВАЕМ ЕЕ КАК ТЕКУЩУЮ
        table_service.set_current_table(session_id, table_name)

        # Добавляем индексы для производительности
        db.execute(text(f'CREATE INDEX IF NOT EXISTS idx_date ON "{table_name}" (date);'))
        db.execute(text(f'CREATE INDEX IF NOT EXISTS idx_region ON "{table_name}" (region_calculated);'))
        db.execute(text(f'CREATE INDEX IF NOT EXISTS idx_year ON "{table_name}" ((EXTRACT(YEAR FROM date)));'))
        db.commit()

        # Удаляем временный файл
        os.remove(temp_filename)

        # Формируем ответ
        response_data = {
            "message": f" Успешно загружено {parse_result.get('total_records', 0)} записей",
            "table_name": table_name,
            "sheets_processed": len(parse_result.get('sheets_processed', [])),
            "records_added": parse_result.get('total_records', 0),
            "processing_result": parse_result.get('processing_result', {})
        }
        
        response = JSONResponse(content=response_data)
        response.set_cookie(key="session_id", value=session_id, httponly=True, max_age=3600*24*7)
        return response
        
    except Exception as e:
        logger.error(f" Ошибка при загрузке файла: {e}")
        try:
            if 'temp_filename' in locals():
                os.remove(temp_filename)
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Ошибка при загрузке: {str(e)}")
    
@app.get("/api/debug/new-records/{table_name}")
async def debug_new_records(table_name: str, db: Session = Depends(get_db)):
    """Показать новые записи (без дат и регионов)"""
    try:
        records = db.execute(text(f'''
            SELECT id, dof, dep, dest, date, region_calculated
            FROM "{table_name}" 
            WHERE date IS NULL OR region_calculated IS NULL
            ORDER BY id
        ''')).fetchall()
        
        return {
            "table_name": table_name,
            "records_without_dates_or_regions": len(records),
            "records": [dict(record) for record in records]
        }
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/upload/append")
async def append_to_table(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    table_service: TableService = Depends(get_table_service),
    session_id: str = Depends(get_session_id)
):
    temp_filename = None
    try:
        logger.info(f" Дозагрузка данных из файла: {file.filename}")
        
        current_table = table_service.get_current_table(session_id)
        if not current_table:
            raise HTTPException(status_code=400, detail="Нет активной таблицы для дозагрузки")
        
        #  Получаем количество записей ДО дозагрузки
        count_before = db.execute(text(f'SELECT COUNT(*) FROM "{current_table}"')).scalar()
        logger.info(f"📊 Записей до дозагрузки: {count_before}")
        
        # Сохраняем файл временно
        contents = await file.read()
        temp_filename = f"temp_append_{int(time.time())}_{file.filename}"
        with open(temp_filename, "wb") as f:
            f.write(contents)

        # Дозагрузка данных
        excel_parser = ExcelParser()
        append_result = excel_parser.append_to_existing_table(temp_filename)
        
        if "error" in append_result:
            raise HTTPException(status_code=400, detail=append_result["error"])
        
        #  ПРИНУДИТЕЛЬНАЯ ОБРАБОТКА ДАТ И РЕГИОНОВ ДЛЯ ВСЕХ НОВЫХ ЗАПИСЕЙ
        logger.info(" Принудительная обработка дат и регионов для новых записей...")
        
        # Обработка дат
        dates_processed = excel_parser._process_new_dates(db, current_table)
        logger.info(f" Обработано дат: {dates_processed}")
        
        # Обработка регионов
        regions_processed = excel_parser._process_new_regions(db, current_table)
        logger.info(f" Обработано регионов: {regions_processed}")
        
        # Обновление индексов и статистики
        logger.info(" Обновление индексов и статистики...")
        db.execute(text(f'CREATE INDEX IF NOT EXISTS idx_date ON "{current_table}" (date);'))
        db.execute(text(f'CREATE INDEX IF NOT EXISTS idx_region ON "{current_table}" (region_calculated);'))
        db.execute(text(f'CREATE INDEX IF NOT EXISTS idx_year ON "{current_table}" ((EXTRACT(YEAR FROM date)));'))
        db.execute(text(f'ANALYZE "{current_table}";'))
        
        # Обновляем records_count
        table_service.update_table_records_count(current_table)
        db.commit()

        # Проверяем результат
        count_after = db.execute(text(f'SELECT COUNT(*) FROM "{current_table}"')).scalar()
        actual_added = count_after - count_before
        
        logger.info(f" Результат дозагрузки: было {count_before}, стало {count_after}, добавлено {actual_added}")

        # Удаляем временный файл
        if temp_filename and os.path.exists(temp_filename):
            os.remove(temp_filename)

        response_data = {
            "message": f" Успешно дозагружено {actual_added} записей",
            "records_added": actual_added,
            "total_records_before": count_before,
            "total_records_after": count_after,
            "dates_processed": dates_processed,
            "regions_processed": regions_processed,
            "table_name": current_table
        }
        
        return JSONResponse(content=response_data)
        
    except Exception as e:
        logger.error(f" Ошибка при дозагрузке: {str(e)}")
        db.rollback()
        if temp_filename and os.path.exists(temp_filename):
            try:
                os.remove(temp_filename)
            except:
                pass
        raise HTTPException(status_code=500, detail=f"Ошибка при дозагрузке: {str(e)}")
    
@app.delete("/tables/{table_name}", 
    description="Удаляет указанную таблицу из базы данных и соответствующую запись из user_tables",
    tags=["Управление таблицами"])
def delete_table_endpoint(table_name: str, db: Session = Depends(get_db)):
    """
    Удаляет таблицу из базы данных и записей в user_tables.
    """
    service = TableService(db)
    try:
        success = service.delete_table(table_name)
        if not success:
            raise HTTPException(status_code=400, detail=f"Не удалось удалить таблицу '{table_name}'")
        
        return JSONResponse(
            content={"message": f"Таблица '{table_name}' успешно удалена"},
            status_code=200
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при удалении таблицы: {e}")

@app.get("/region", 
    response_model=RegionInfoResponse,
    description="Принимает координаты (широту и долготу) и возвращает информацию о регионе (на русском и английском языках) и уровне администрирования",
    tags=["Работа с регионами"])
async def get_region(lat: float, lon: float):
    """Определить регион по координатам"""
    try:
        from region_service import region_service
        region_info = region_service.get_region_by_coordinates(lat, lon)
        
        return RegionInfoResponse(
            region_ru=region_info["region_ru"],
            region_en=region_info["region_en"],
            admin_level=region_info["admin_level"]
        )
            
    except Exception as e:
        logger.error(f"Ошибка при определении региона: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка: {str(e)}")

@app.post("/admin/add-region-column",
    description="Добавляет столбец region_calculated в текущую таблицу и заполняет его данными о регионах на основе координат пункта вылета",
    tags=["Работа с регионами"])
async def add_region_column(
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)
):
    """Добавить столбец с городом в основную таблицу"""
    try:
        from region_service import region_service
            
        #ИСПОЛЬЗУЕМ БОЛЬШИЕ ПАЧКИ ДЛЯ СКОРОСТИ
        updated_count = region_service.add_region_to_flight_data(db, table_name=current_table, batch_size=10000)
        
        return {
            "status": "success",
            "message": f" Добавлен столбец region_calculated и обновлено {updated_count} записей",
            "updated_count": updated_count
        }
        
    except Exception as e:
        logger.error(f" Ошибка при добавлении столбца региона: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка: {str(e)}")

# ДОПОЛНИТЕЛЬНЫЕ ЭНДПОИНТЫ ДЛЯ ДАШБОРДА И СТАТИСТИКИ
def clean_opr_sql():
    """SQL для чистого отображения имени оператора"""
    return """TRIM(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(opr, '[\n\r\t]+', ' ', 'g'),
                    '\s{2,}', ' ', 'g'
                )
            ) AS opr_clean"""

def merge_duplicate_operators(operators):
    merged = defaultdict(lambda: {
        "flight_count": 0,
        "unique_aircrafts_set": set(),
        "regions_covered_set": set(),
        "level_sum": 0.0,
        "radius_sum": 0.0
    })

    for op in operators:
        name = op["name"]
        fc = op["flight_count"]
        merged[name]["flight_count"] += fc
        merged[name]["unique_aircrafts_set"].add(op["unique_aircrafts"])
        merged[name]["regions_covered_set"].add(op["regions_covered"])
        merged[name]["level_sum"] += op["avg_level"] * fc
        merged[name]["radius_sum"] += op["avg_radius"] * fc

    result = []
    for name, data in merged.items():
        fc_total = data["flight_count"]
        result.append({
            "name": name,
            "flight_count": fc_total,
            "unique_aircrafts": sum(data["unique_aircrafts_set"]),
            "regions_covered": sum(data["regions_covered_set"]),
            "avg_level": round(data["level_sum"] / fc_total, 2) if fc_total else 0,
            "avg_radius": round(data["radius_sum"] / fc_total, 2) if fc_total else 0
        })
    return result

# ----------------- Общая функция запроса -----------------
def _execute_top_operators(db: Session, current_table: str, where_conditions: list, limit: int, params: dict):
    where_clause = " AND ".join(where_conditions)
    params["limit"] = limit

    operators_query = text(f'''
        SELECT 
            {clean_opr_sql()},
            COUNT(*) as flight_count,
            COUNT(DISTINCT reg) as unique_aircrafts,
            COUNT(DISTINCT region_calculated) as regions_covered,
            AVG(CAST(NULLIF(regexp_replace(flight_level, '[^0-9]', '', 'g'), '') AS NUMERIC)) as avg_level,
            AVG(CAST(NULLIF(regexp_replace(flight_zone_radius, '[^0-9]', '', 'g'), '') AS NUMERIC)) as avg_radius
        FROM "{current_table}"
        WHERE {where_clause}
        GROUP BY opr
        ORDER BY flight_count DESC
        LIMIT :limit
    ''')
    operators = db.execute(operators_query, params).mappings().all()
    # Преобразуем в список словарей
    operators_list = [
        {
            "name": row["opr_clean"],
            "flight_count": row["flight_count"],
            "unique_aircrafts": row["unique_aircrafts"],
            "regions_covered": row["regions_covered"],
            "avg_level": float(row["avg_level"] or 0),
            "avg_radius": float(row["avg_radius"] or 0)
        } for row in operators
    ]
    return merge_duplicate_operators(operators_list)


@app.get("/dashboard/stats",
    description="Основная статистика для дашборда: регионы, операторы, параметры полетов",
    tags=["Дашборд и статистика"])
async def get_dashboard_stats(
    date_from: Optional[str] = Query(None, description="Начальная дата в формате YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="Конечная дата в формате YYYY-MM-DD"),
    region: Optional[str] = Query(None, description="Регион для фильтрации"),
    limit: Optional[int] = Query(None, description="Лимит записей"),
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)
):
    """Основная статистика для дашборда"""
    try:
        # Базовые условия фильтрации (единый формат)
        where_conditions = ["date IS NOT NULL"]
        params = {}
        
        if date_from:
            where_conditions.append("date >= (:date_from)::date")
            params["date_from"] = date_from
            
        if date_to:
            where_conditions.append("date <= (:date_to)::date")
            params["date_to"] = date_to
            
        if region:
            where_conditions.append("region_calculated = :region")
            params["region"] = region
        
        where_clause = " AND ".join(where_conditions)
        
        # 1. ОБЩАЯ СТАТИСТИКА
        general_stats_query = text(f'''
            SELECT 
                COUNT(*) as total_flights,
                COUNT(DISTINCT region_calculated) as total_regions,
                COUNT(DISTINCT reg) as total_aircrafts,
                COUNT(DISTINCT opr) as total_operators
            FROM "{current_table}"
            WHERE {where_clause}
        ''')
        
        general_stats = db.execute(general_stats_query, params).fetchone()
        
        # 2. САМЫЙ АКТИВНЫЙ РЕГИОН
        active_region_query = text(f'''
            SELECT region_calculated, COUNT(*) as flight_count
            FROM "{current_table}"
            WHERE region_calculated IS NOT NULL AND {where_clause}
            GROUP BY region_calculated
            ORDER BY flight_count DESC
            LIMIT 1
        ''')
        
        active_region = db.execute(active_region_query, params).fetchone()
        
        # 3. СТАТИСТИКА ПО РАДИУСУ ПОЛЕТА
        radius_stats_query = text(f'''
            SELECT 
                AVG(radius_numeric) as avg_radius,
                MODE() WITHIN GROUP (ORDER BY flight_zone_radius) as most_common_radius,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY radius_numeric) as median_radius,
                MAX(radius_numeric) as max_radius
            FROM (
                SELECT 
                    flight_zone_radius,
                    CASE 
                        WHEN flight_zone_radius ~ '^[Rr]?[0-9]+$' THEN 
                            CAST(REGEXP_REPLACE(flight_zone_radius, '[^0-9]', '', 'g') AS NUMERIC)
                        WHEN flight_zone_radius ~ '^[0-9]+$' THEN 
                            CAST(flight_zone_radius AS NUMERIC)
                        ELSE NULL
                    END as radius_numeric
                FROM "{current_table}"
                WHERE flight_zone_radius IS NOT NULL 
                AND flight_zone_radius != ''
                AND {where_clause}
            ) as radii
            WHERE radius_numeric IS NOT NULL 
            AND radius_numeric > 0 
            AND radius_numeric < 100000  -- Фильтр от выбросов
        ''')
        
        radius_stats = db.execute(radius_stats_query, params).fetchone()
        
        # 4. СТАТИСТИКА ПО ВЫСОТЕ ПОЛЕТА - УЛУЧШЕННЫЙ ЗАПРОС ДЛЯ ГРАЖДАНСКИХ БПЛА
        level_stats_query = text(f'''
            SELECT 
                AVG(level_numeric) as avg_level,
                MODE() WITHIN GROUP (ORDER BY flight_level) as most_common_level,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY level_numeric) as median_level,
                MAX(level_numeric) as max_level
            FROM (
                SELECT 
                    flight_level,
                    CASE 
                        -- Для формата M0150, M0120 и т.д. - берем число после M и умножаем на 10 для перевода в метры
                        WHEN flight_level ~ '^[Mm][0-9]+$' THEN 
                            CAST(REGEXP_REPLACE(flight_level, '[^0-9]', '', 'g') AS NUMERIC) * 10
                        -- Для числовых значений (предполагаем что это уже метры)
                        WHEN flight_level ~ '^[0-9]+$' THEN 
                            CAST(flight_level AS NUMERIC)
                        -- Для формата M0035/M0045 - берем первое число
                        WHEN flight_level ~ '^[Mm][0-9]+/' THEN 
                            CAST(REGEXP_REPLACE(SPLIT_PART(flight_level, '/', 1), '[^0-9]', '', 'g') AS NUMERIC) * 10
                        ELSE NULL
                    END as level_numeric
                FROM "{current_table}"
                WHERE flight_level IS NOT NULL 
                AND flight_level != ''
                AND {where_clause}
            ) as levels
            WHERE level_numeric IS NOT NULL 
            AND level_numeric > 0 
            AND level_numeric < 2000  -- Гражданские БПЛА обычно летают до 1500м
        ''')
        
        level_stats = db.execute(level_stats_query, params).fetchone()
        
        # 5. СРЕДНЯЯ ПРОДОЛЖИТЕЛЬНОСТЬ ПОЛЕТА
        duration_stats_query = text(f'''
            SELECT 
                AVG(duration_minutes) as avg_duration,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_minutes) as median_duration
            FROM (
                SELECT 
                    CASE 
                        WHEN departure_time IS NOT NULL AND arrival_time IS NOT NULL 
                             AND departure_time ~ '^[0-2][0-9]:[0-5][0-9]:[0-5][0-9]$'
                             AND arrival_time ~ '^[0-2][0-9]:[0-5][0-9]:[0-5][0-9]$' THEN
                            EXTRACT(EPOCH FROM (
                                arrival_time::time - departure_time::time
                            )) / 60
                        ELSE NULL
                    END as duration_minutes
                FROM "{current_table}"
                WHERE {where_clause}
            ) as durations
            WHERE duration_minutes IS NOT NULL AND duration_minutes > 0
        ''')
        
        duration_stats = db.execute(duration_stats_query, params).fetchone()
        
        # 6. ТИПЫ БПЛА (с лимитом если указан)
        # 6. ТИПЫ БПЛА (с лимитом если указан) - ИСПРАВЛЕННЫЙ БЛОК
        limit_clause_types = "LIMIT :type_limit" if limit else ""
        type_params = params.copy()
        if limit:
            type_params["type_limit"] = limit
            
        aircraft_types_query = text(f'''
            SELECT 
                TRIM(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(typ, 'RMK', '', 'gi'),  -- Убираем RMK
                            '[\n\r\t]+', ' ', 'g'
                        ),
                        '\s{2,}', ' ', 'g'
                    )
                ) as cleaned_type,
                COUNT(*) as count
            FROM "{current_table}"
            WHERE typ IS NOT NULL AND {where_clause}
            GROUP BY cleaned_type
            ORDER BY count DESC
            {limit_clause_types}
        ''')

        aircraft_types_result = db.execute(aircraft_types_query, type_params).fetchall()
        aircraft_types = [
            {"type": row[0], "count": row[1]} for row in aircraft_types_result
        ]
        
        # 7. ОПЕРАТОРЫ (с разделением на типы и лимитом)
        limit_clause_operators = "LIMIT :operator_limit" if limit else ""
        operator_params = params.copy()
        if limit:
            operator_params["operator_limit"] = limit
            
        operators_query = text(f'''
            SELECT 
                TRIM(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(opr, '[\n\r\t]+', ' ', 'g'),
                        '\s{2,}', ' ', 'g'
                    )
                ) as cleaned_opr,
                COUNT(*) as flight_count,
                CASE 
                    WHEN opr ILIKE '%ООО%' OR opr ILIKE '%общество%' OR opr ILIKE '%ltd%' OR opr ILIKE '%limited%' THEN 'ООО'
                    WHEN opr ILIKE '%ИП%' OR opr ILIKE '%индивидуальный%' OR opr ILIKE '%ip%' THEN 'ИП'
                    WHEN opr ILIKE '%ФЛ%' OR opr ~ '[А-Яа-я]+\s+[А-Яа-я]+\s+[А-Яа-я]+' THEN 'Физлицо'
                    ELSE 'Другое'
                END as operator_type
            FROM "{current_table}"
            WHERE opr IS NOT NULL AND {where_clause}
            GROUP BY cleaned_opr, operator_type
            ORDER BY flight_count DESC
            {limit_clause_operators}
        ''')
        
        operators_result = db.execute(operators_query, operator_params).fetchall()
        operators = [
            {
                "name": row[0], 
                "flight_count": row[1], 
                "type": row[2]
            } for row in operators_result
        ]
        
        # 8. САМЫЕ БОЛЬШИЕ ЗНАЧЕНИЯ (сохраняем для фронта)
        max_values_query = text(f'''
            SELECT 
                MAX(CAST(NULLIF(regexp_replace(flight_level, '[^0-9]', '', 'g'), '') AS NUMERIC)) as max_level,
                MAX(CAST(NULLIF(regexp_replace(flight_zone_radius, '[^0-9]', '', 'g'), '') AS NUMERIC)) as max_radius
            FROM "{current_table}"
            WHERE {where_clause}
        ''')
        
        max_values = db.execute(max_values_query, params).fetchone()
        
        # 9. СТАТИСТИКА ПО ГОДАМ
        yearly_stats_query = text(f'''
            SELECT 
                EXTRACT(YEAR FROM date) as year,
                COUNT(*) as flight_count,
                COUNT(DISTINCT region_calculated) as regions_count,
                COUNT(DISTINCT opr) as operators_count
            FROM "{current_table}"
            WHERE date IS NOT NULL
            GROUP BY EXTRACT(YEAR FROM date)
            ORDER BY year DESC
        ''')
        
        yearly_data = db.execute(yearly_stats_query).fetchall()
        yearly_stats = {
            str(int(row[0])): {
                "flight_count": row[1],
                "regions_count": row[2],
                "operators_count": row[3]
            } for row in yearly_data
        }
        
        return {
            "general_stats": {
                "total_flights": general_stats[0] if general_stats else 0,
                "total_regions": general_stats[1] if general_stats else 0,
                "total_aircrafts": general_stats[2] if general_stats else 0,
                "total_operators": general_stats[3] if general_stats else 0
            },
            "active_region": {
                "region": active_region[0] if active_region else None,
                "flight_count": active_region[1] if active_region else 0
            },
            "radius_stats": {
                "avg_radius": round(float(radius_stats[0] or 0), 2) if radius_stats else 0,
                "median_radius": round(float(radius_stats[2] or 0), 2) if radius_stats else 0,
                "most_common_radius": radius_stats[1] if radius_stats else None,
                "max_radius": round(float(radius_stats[3] or 0), 2) if radius_stats else 0
            },
            "level_stats": {
                "avg_level": round(float(level_stats[0] or 0), 2) if level_stats else 0,
                "median_level": round(float(level_stats[2] or 0), 2) if level_stats else 0,
                "most_common_level": level_stats[1] if level_stats else None,
                "max_level": round(float(level_stats[3] or 0), 2) if level_stats else 0
            },
            "duration_stats": {
                "avg_duration_minutes": round(float(duration_stats[0] or 0), 2) if duration_stats else 0,
                "median_duration_minutes": round(float(duration_stats[1] or 0), 2) if duration_stats else 0
            },
            "aircraft_types": aircraft_types,
            "operators": operators,
            "max_values": {
                "max_level": max_values[0] if max_values else 0,
                "max_radius": max_values[1] if max_values else 0
            },
            "yearly_stats": yearly_stats,
            "filters": {
                "date_from": date_from,
                "date_to": date_to,
                "region": region,
                "limit": limit
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка в dashboard/stats: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения статистики: {str(e)}")


@app.get("/regions/stats",
    description="Статистика по всем регионам",
    tags=["Дашборд и статистика"])
async def get_regions_stats(
    date_from: Optional[str] = Query(None, description="Начальная дата в формате YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="Конечная дата в формате YYYY-MM-DD"),
    limit: Optional[int] = Query(None, description="Лимит регионов (опционально)"),
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)
):
    """Статистика по регионам"""
    try:
        where_conditions = ["region_calculated IS NOT NULL", "date IS NOT NULL"]
        params = {}
        
        if date_from:
            where_conditions.append("date >= (:date_from)::date")
            params["date_from"] = date_from
            
        if date_to:
            where_conditions.append("date <= (:date_to)::date")
            params["date_to"] = date_to
        
        where_clause = " AND ".join(where_conditions)
        
        limit_clause = "LIMIT :limit" if limit else ""
        if limit:
            params["limit"] = limit
        
        # Сначала получаем базовую статистику по регионам
        regions_query = text(f'''
            SELECT 
                region_calculated,
                COUNT(*) as flight_count,
                COUNT(DISTINCT opr) as unique_operators,
                COUNT(DISTINCT reg) as unique_aircrafts
            FROM "{current_table}"
            WHERE {where_clause}
            GROUP BY region_calculated
            ORDER BY flight_count DESC
            {limit_clause}
        ''')
        
        regions = db.execute(regions_query, params).fetchall()
        
        # Для каждого региона получаем детальную статистику по высоте и радиусу
        regions_stats = []
        for region in regions:
            region_name = region[0]
            
            # Статистика по высоте полета для региона
            level_stats_query = text(f'''
                SELECT flight_level
                FROM "{current_table}"
                WHERE region_calculated = :region 
                AND flight_level IS NOT NULL 
                AND flight_level != ''
                AND {where_clause}
            ''')
            
            level_params = params.copy()
            level_params["region"] = region_name
            level_results = db.execute(level_stats_query, level_params).fetchall()
            
            # Обрабатываем высоты через ваш парсер
            valid_levels = []
            for row in level_results:
                level_value = parse_level_for_stats(row[0])
                if level_value is not None:
                    valid_levels.append(level_value)
            
            # Статистика по радиусу полета для региона
            radius_stats_query = text(f'''
                SELECT flight_zone_radius
                FROM "{current_table}"
                WHERE region_calculated = :region 
                AND flight_zone_radius IS NOT NULL 
                AND flight_zone_radius != ''
                AND {where_clause}
            ''')
            
            radius_params = params.copy()
            radius_params["region"] = region_name
            radius_results = db.execute(radius_stats_query, radius_params).fetchall()
            
            # Обрабатываем радиусы через ваш парсер
            valid_radii = []
            for row in radius_results:
                radius_value = parse_radius_for_stats(row[0])
                if radius_value is not None:
                    valid_radii.append(radius_value)
            
            # Статистика по продолжительности полета для региона
            duration_stats_query = text(f'''
                SELECT 
                    AVG(duration_minutes) as avg_duration,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_minutes) as median_duration,
                    COUNT(duration_minutes) as valid_duration_count
                FROM (
                    SELECT 
                        CASE 
                            WHEN departure_time IS NOT NULL AND arrival_time IS NOT NULL 
                                 AND departure_time ~ '^[0-2][0-9]:[0-5][0-9]:[0-5][0-9]$'
                                 AND arrival_time ~ '^[0-2][0-9]:[0-5][0-9]:[0-5][0-9]$' THEN
                                EXTRACT(EPOCH FROM (
                                    arrival_time::time - departure_time::time
                                )) / 60
                            ELSE NULL
                        END as duration_minutes
                    FROM "{current_table}"
                    WHERE region_calculated = :region AND {where_clause}
                ) as durations
                WHERE duration_minutes IS NOT NULL AND duration_minutes > 0
            ''')
            
            duration_params = params.copy()
            duration_params["region"] = region_name
            duration_stats = db.execute(duration_stats_query, duration_params).fetchone()
            
            # Формируем статистику для региона
            region_stat = {
                "region": region_name,
                "flight_count": region[1],
                "unique_operators": region[2],
                "unique_aircrafts": region[3],
                "statistics": {
                    "flight_level": {
                        "avg_level_m": round(sum(valid_levels) / len(valid_levels), 1) if valid_levels else 0,
                        "median_level_m": round(sorted(valid_levels)[len(valid_levels) // 2], 1) if valid_levels else 0,
                        "max_level_m": round(max(valid_levels), 1) if valid_levels else 0,
                        "min_level_m": round(min(valid_levels), 1) if valid_levels else 0,
                        "valid_samples": len(valid_levels),
                        "level_range": f"{round(min(valid_levels), 1)}-{round(max(valid_levels), 1)} м" if valid_levels else "нет данных"
                    },
                    "flight_radius": {
                        "avg_radius_m": round(sum(valid_radii) / len(valid_radii), 1) if valid_radii else 0,
                        "median_radius_m": round(sorted(valid_radii)[len(valid_radii) // 2], 1) if valid_radii else 0,
                        "max_radius_m": round(max(valid_radii), 1) if valid_radii else 0,
                        "min_radius_m": round(min(valid_radii), 1) if valid_radii else 0,
                        "avg_radius_km": round(sum(valid_radii) / len(valid_radii) / 1000, 2) if valid_radii else 0,
                        "valid_samples": len(valid_radii),
                        "radius_range": f"{round(min(valid_radii), 1)}-{round(max(valid_radii), 1)} м" if valid_radii else "нет данных"
                    },
                    "flight_duration": {
                        "avg_duration_minutes": round(float(duration_stats[0] or 0), 1) if duration_stats else 0,
                        "median_duration_minutes": round(float(duration_stats[1] or 0), 1) if duration_stats else 0,
                        "valid_samples": duration_stats[2] if duration_stats else 0
                    }
                },
                "data_quality": {
                    "has_altitude_data": len(valid_levels) > 0,
                    "has_radius_data": len(valid_radii) > 0,
                    "has_duration_data": duration_stats and duration_stats[2] > 0 if duration_stats else False,
                    "altitude_coverage": f"{round(len(valid_levels) / region[1] * 100, 1)}%" if region[1] > 0 else "0%",
                    "radius_coverage": f"{round(len(valid_radii) / region[1] * 100, 1)}%" if region[1] > 0 else "0%"
                }
            }
            
            regions_stats.append(region_stat)
        
        return {
            "regions": regions_stats,
            "filters": {
                "date_from": date_from,
                "date_to": date_to,
                "limit": limit
            },
            "total_regions": len(regions_stats),
            "units": {
                "height": "meters",
                "radius": "meters",
                "duration": "minutes"
            },
            "statistics_method": "flight_formats_parser"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения статистики регионов: {str(e)}")


# ----------------- Эндпоинты -----------------

def _execute_top_operators_with_parsed_metrics(db: Session, current_table: str, where_conditions: List[str], limit: int, params: dict):
    """Вспомогательная функция для получения топа операторов с парсингом метрик через flight_formats_parser"""
    where_sql = " AND ".join(where_conditions)
    
    # Сначала получаем базовую статистику по операторам
    base_query = text(f'''
        SELECT 
            TRIM(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(opr, '[\\n\\r\\t]+', ' ', 'g'),
                    '\\s{{2,}}', ' ', 'g'
                )
            ) as cleaned_opr,
            COUNT(*) as flight_count,
            COUNT(DISTINCT reg) as unique_aircrafts,
            COUNT(DISTINCT region_calculated) as regions_covered
        FROM "{current_table}"
        WHERE {where_sql}
        GROUP BY cleaned_opr
        ORDER BY flight_count DESC
        LIMIT :limit
    ''')
    
    base_params = params.copy()
    base_params["limit"] = limit
    operators_result = db.execute(base_query, base_params).fetchall()
    
    operators_with_metrics = []
    
    for row in operators_result:
        operator_name = row[0]
        
        # Получаем данные по высоте для оператора
        level_stats_query = text(f'''
            SELECT flight_level
            FROM "{current_table}"
            WHERE opr = :operator_name 
            AND flight_level IS NOT NULL 
            AND flight_level != ''
            AND {where_sql}
        ''')
        
        level_params = params.copy()
        level_params["operator_name"] = operator_name
        level_results = db.execute(level_stats_query, level_params).fetchall()
        
        # Обрабатываем высоты через парсер
        valid_levels = []
        for level_row in level_results:
            level_value = parse_level_for_stats(level_row[0])
            if level_value is not None:
                valid_levels.append(level_value)
        
        # Получаем данные по радиусу для оператора
        radius_stats_query = text(f'''
            SELECT flight_zone_radius
            FROM "{current_table}"
            WHERE opr = :operator_name 
            AND flight_zone_radius IS NOT NULL 
            AND flight_zone_radius != ''
            AND {where_sql}
        ''')
        
        radius_params = params.copy()
        radius_params["operator_name"] = operator_name
        radius_results = db.execute(radius_stats_query, radius_params).fetchall()
        
        # Обрабатываем радиусы через парсер
        valid_radii = []
        for radius_row in radius_results:
            radius_value = parse_radius_for_stats(radius_row[0])
            if radius_value is not None:
                valid_radii.append(radius_value)
        
        # Рассчитываем средние значения
        avg_level_m = round(sum(valid_levels) / len(valid_levels), 1) if valid_levels else 0
        avg_radius_m = round(sum(valid_radii) / len(valid_radii), 1) if valid_radii else 0
        
        operator_data = {
            "name": operator_name,
            "flight_count": row[1],
            "unique_aircrafts": row[2],
            "regions_covered": row[3],
            "avg_level_m": avg_level_m,
            "avg_radius_m": avg_radius_m
        }
        
        operators_with_metrics.append(operator_data)
    
    return operators_with_metrics


@app.get("/operators/top/all", description="Топ всех операторов", tags=["Дашборд и статистика"])
async def get_top_operators_all(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None, description="Регион для фильтрации"),
    limit: Optional[int] = Query(50),
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)
):
    try:
        where_conditions = ["opr IS NOT NULL", "date IS NOT NULL"]
        params = {}
        if date_from:
            where_conditions.append("date >= (:date_from)::date")
            params["date_from"] = date_from
        if date_to:
            where_conditions.append("date <= (:date_to)::date")
            params["date_to"] = date_to
        if region:
            where_conditions.append("region_calculated = :region")
            params["region"] = region

        operators = _execute_top_operators_with_parsed_metrics(db, current_table, where_conditions, limit, params)
        
        return {
            "operators": operators,
            "filters": {"date_from": date_from, "date_to": date_to, "region": region, "limit": limit},
            "total_operators": len(operators),
            "units": {
                "height": "meters",
                "radius": "meters"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения топа операторов: {str(e)}")


@app.get("/operators/top/ooo", description="Топ операторов ООО", tags=["Дашборд и статистика"])
async def get_top_operators_ooo(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None, description="Регион для фильтрации"),
    limit: Optional[int] = Query(50),
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)
):
    try:
        where_conditions = [
            "opr IS NOT NULL",
            "date IS NOT NULL",
            "(opr ILIKE '%ООО%' OR opr ILIKE '%общество%' OR opr ILIKE '%ltd%' OR opr ILIKE '%limited%')"
        ]
        params = {}
        if date_from:
            where_conditions.append("date >= (:date_from)::date")
            params["date_from"] = date_from
        if date_to:
            where_conditions.append("date <= (:date_to)::date")
            params["date_to"] = date_to
        if region:
            where_conditions.append("region_calculated = :region")
            params["region"] = region

        operators = _execute_top_operators_with_parsed_metrics(db, current_table, where_conditions, limit, params)
        
        return {
            "operator_type": "ООО",
            "operators": operators,
            "filters": {"date_from": date_from, "date_to": date_to, "region": region, "limit": limit},
            "total_operators": len(operators),
            "units": {
                "height": "meters",
                "radius": "meters"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения топа ООО: {str(e)}")


@app.get("/operators/top/ip", description="Топ операторов ИП", tags=["Дашборд и статистика"])
async def get_top_operators_ip(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None, description="Регион для фильтрации"),
    limit: Optional[int] = Query(50),
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)
):
    try:
        where_conditions = [
            "opr IS NOT NULL",
            "date IS NOT NULL",
            "(opr ILIKE '%ИП%' OR opr ILIKE '%индивидуальный%' OR opr ILIKE '%ip%')"
        ]
        params = {}
        if date_from:
            where_conditions.append("date >= (:date_from)::date")
            params["date_from"] = date_from
        if date_to:
            where_conditions.append("date <= (:date_to)::date")
            params["date_to"] = date_to
        if region:
            where_conditions.append("region_calculated = :region")
            params["region"] = region

        operators = _execute_top_operators_with_parsed_metrics(db, current_table, where_conditions, limit, params)
        
        return {
            "operator_type": "ИП",
            "operators": operators,
            "filters": {"date_from": date_from, "date_to": date_to, "region": region, "limit": limit},
            "total_operators": len(operators),
            "units": {
                "height": "meters",
                "radius": "meters"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения топа ИП: {str(e)}")


@app.get("/operators/top/individual", description="Топ операторов физических лиц", tags=["Дашборд и статистика"])
async def get_top_operators_individual(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None, description="Регион для фильтрации"),
    limit: Optional[int] = Query(50),
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)
):
    try:
        where_conditions = [
            "opr IS NOT NULL",
            "date IS NOT NULL",
            "(opr ILIKE '%ФЛ%' OR opr ~ '[А-Яа-я]+\\s+[А-Яа-я]+\\s+[А-Яа-я]+')"
        ]
        params = {}
        if date_from:
            where_conditions.append("date >= (:date_from)::date")
            params["date_from"] = date_from
        if date_to:
            where_conditions.append("date <= (:date_to)::date")
            params["date_to"] = date_to
        if region:
            where_conditions.append("region_calculated = :region")
            params["region"] = region

        operators = _execute_top_operators_with_parsed_metrics(db, current_table, where_conditions, limit, params)
        
        return {
            "operator_type": "Физлицо",
            "operators": operators,
            "filters": {"date_from": date_from, "date_to": date_to, "region": region, "limit": limit},
            "total_operators": len(operators),
            "units": {
                "height": "meters",
                "radius": "meters"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения топа физлиц: {str(e)}")


@app.get("/operators/top/other", description="Топ операторов других типов", tags=["Дашборд и статистика"])
async def get_top_operators_other(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None, description="Регион для фильтрации"),
    limit: Optional[int] = Query(50),
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)
):
    try:
        where_conditions = [
            "opr IS NOT NULL",
            "date IS NOT NULL",
            "NOT (opr ILIKE '%ООО%' OR opr ILIKE '%общество%' OR opr ILIKE '%ltd%' OR opr ILIKE '%limited%')",
            "NOT (opr ILIKE '%ИП%' OR opr ILIKE '%индивидуальный%' OR opr ILIKE '%ip%')",
            "NOT (opr ILIKE '%ФЛ%' OR opr ~ '[А-Яа-я]+\\s+[А-Яа-я]+\\s+[А-Яа-я]+')"
        ]
        params = {}
        if date_from:
            where_conditions.append("date >= (:date_from)::date")
            params["date_from"] = date_from
        if date_to:
            where_conditions.append("date <= (:date_to)::date")
            params["date_to"] = date_to
        if region:
            where_conditions.append("region_calculated = :region")
            params["region"] = region

        operators = _execute_top_operators_with_parsed_metrics(db, current_table, where_conditions, limit, params)
        
        return {
            "operator_type": "Другое",
            "operators": operators,
            "filters": {"date_from": date_from, "date_to": date_to, "region": region, "limit": limit},
            "total_operators": len(operators),
            "units": {
                "height": "meters",
                "radius": "meters"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения топа других операторов: {str(e)}")

#Реализация экспорта PDF-отчета
import io
from datetime import datetime
from typing import Dict, Any, List
from fastapi import Depends, HTTPException, Response
from sqlalchemy.orm import Session
from sqlalchemy import text
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from pydantic import BaseModel
import logging
import os
from schemas import ExportRequest
import tempfile
import requests
from pathlib import Path



# --- Регистрация шрифтов ---
def register_fonts():
    """Регистрируем шрифты с поддержкой кириллицы для всех ОС"""
    
    # Создаем временную директорию для шрифтов
    temp_dir = Path(tempfile.gettempdir()) / "uav_report_fonts"
    temp_dir.mkdir(exist_ok=True)
    
    # Конфигурация шрифтов с Google Fonts (с поддержкой кириллицы)
    font_configs = [
        {
            'name': 'Roboto',
            'regular_url': 'https://github.com/googlefonts/roboto/raw/main/src/hinted/Roboto-Regular.ttf',
            'bold_url': 'https://github.com/googlefonts/roboto/raw/main/src/hinted/Roboto-Bold.ttf'
        },
        {
            'name': 'OpenSans',
            'regular_url': 'https://github.com/googlefonts/opensans/raw/main/fonts/ttf/OpenSans-Regular.ttf',
            'bold_url': 'https://github.com/googlefonts/opensans/raw/main/fonts/ttf/OpenSans-Bold.ttf'
        },
        {
            'name': 'NotoSans',
            'regular_url': 'https://github.com/googlefonts/noto-fonts/raw/main/hinted/ttf/NotoSans/NotoSans-Regular.ttf',
            'bold_url': 'https://github.com/googlefonts/noto-fonts/raw/main/hinted/ttf/NotoSans/NotoSans-Bold.ttf'
        }
    ]
    
    def download_font(url, filename):
        """Скачивает шрифт если его нет локально"""
        filepath = temp_dir / filename
        
        if not filepath.exists():
            try:
                logger.info(f"📥 Скачиваю шрифт: {filename}")
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                logger.info(f"✅ Скачан шрифт: {filename}")
                return filepath
            except Exception as e:
                logger.warning(f"⚠️ Не удалось скачать {url}: {e}")
                return None
        else:
            logger.info(f"✅ Используем кэшированный шрифт: {filename}")
            return filepath

    # Пытаемся загрузить шрифты с Google Fonts
    for config in font_configs:
        try:
            regular_file = download_font(config['regular_url'], f"{config['name']}-Regular.ttf")
            bold_file = download_font(config['bold_url'], f"{config['name']}-Bold.ttf")
            
            if regular_file and bold_file:
                # Регистрируем шрифты
                pdfmetrics.registerFont(TTFont(config['name'], str(regular_file)))
                pdfmetrics.registerFont(TTFont(f"{config['name']}-Bold", str(bold_file)))
                
                # Также регистрируем семейство шрифтов
                pdfmetrics.registerFontFamily(config['name'],
                                            normal=config['name'],
                                            bold=f"{config['name']}-Bold")
                
                logger.info(f"✅ Успешно зарегистрирован шрифт: {config['name']}")
                return config['name'], f"{config['name']}-Bold"
                
        except Exception as e:
            logger.warning(f"⚠️ Ошибка при регистрации {config['name']}: {e}")
            continue

    # Fallback на системные шрифты - исправленная версия
    system_fonts_try = [
        ('DejaVuSans', 'DejaVuSans', 'DejaVuSans-Bold'),
        ('Helvetica', 'Helvetica', 'Helvetica-Bold'),
    ]
    
    for font_name, normal_name, bold_name in system_fonts_try:
        try:
            # Просто используем встроенные шрифты PDF
            logger.info(f"🔄 Пробуем системный шрифт: {font_name}")
            
            # Регистрируем семейство
            pdfmetrics.registerFontFamily(font_name,
                                        normal=normal_name,
                                        bold=bold_name)
            
            # Проверяем, доступны ли шрифты
            try:
                pdfmetrics.getFont(normal_name)
                pdfmetrics.getFont(bold_name)
                logger.info(f"✅ Используем системный шрифт: {font_name}")
                return normal_name, bold_name
            except:
                continue
                
        except Exception as e:
            logger.warning(f"⚠️ Ошибка с системным шрифтом {font_name}: {e}")
            continue

    # Final fallback - используем только базовые шрифты ReportLab
    logger.warning("🚨 Используются базовые шрифты ReportLab (ограниченная поддержка кириллицы)")
    return 'Helvetica', 'Helvetica-Bold'

def safe_text(text: str) -> str:
    """Безопасное преобразование текста для PDF"""
    if text is None:
        return "Н/Д"
    text = str(text)
    if isinstance(text, bytes):
        text = text.decode('utf-8')
    return text

def create_styles(normal_font, bold_font):
    """Создание переиспользуемых стилей с безопасными шрифтами"""
    
    # Убеждаемся, что используем только базовые шрифты если кастомные не работают
    try:
        # Проверяем доступность шрифтов
        pdfmetrics.getFont(normal_font)
        pdfmetrics.getFont(bold_font)
    except:
        # Fallback на стандартные шрифты
        normal_font = 'Helvetica'
        bold_font = 'Helvetica-Bold'
        logger.warning("🔄 Fallback на стандартные шрифты Helvetica")
    
    return {
        'bold_value': ParagraphStyle(
            'BoldValue',
            fontName=bold_font,
            fontSize=9,
            alignment=0,
            encoding='UTF-8'
        ),
        'normal_cell': ParagraphStyle(
            'NormalCell',
            fontName=normal_font,
            fontSize=8,
            alignment=0,
            encoding='UTF-8'
        ),
        'section_title': ParagraphStyle(
            'SectionTitle',
            fontName=bold_font,
            fontSize=12,
            textColor=colors.HexColor('#34495e'),
            spaceAfter=12,
            spaceBefore=20,
            encoding='UTF-8'
        ),
        'table_header': ParagraphStyle(
            'TableHeader',
            fontName=bold_font,
            fontSize=8,
            textColor=colors.whitesmoke,
            alignment=1,
            encoding='UTF-8'
        ),
        'title': ParagraphStyle(
            'CustomTitle',
            fontName=bold_font,
            fontSize=16,
            spaceAfter=10,
            textColor=colors.HexColor('#2c3e50'),
            alignment=1,
            encoding='UTF-8'
        ),
        'subtitle': ParagraphStyle(
            'CustomSubtitle',
            fontName=normal_font,
            fontSize=10,
            textColor=colors.HexColor('#7f8c8d'),
            alignment=1,
            spaceAfter=20,
            encoding='UTF-8'
        ),
        'footer': ParagraphStyle(
            'Footer',
            fontName=normal_font,
            fontSize=8,
            textColor=colors.HexColor('#95a5a6'),
            alignment=1,
            encoding='UTF-8'
        )
    }

def normalize_height(value):
    """Нормализует значения высоты до логически допустимых для БПЛА (макс 2000м)"""
    if value is None:
        return 0
    try:
        val = float(value)
        # Если значение слишком большое - делим на коэффициенты
        if val > 10000:  # Явно некорректное значение
            val = val / 1000  # Делим на 1000
        if val > 2000:  # Все еще больше лимита для БПЛА
            val = val / 10  # Делим еще на 10
        return round(min(val, 2000), 1)  # Максимум 2000м для БПЛА
    except:
        return 0

def normalize_radius(value):
    """Нормализует значения радиуса до логически допустимых (макс 50км = 50000м)"""
    if value is None:
        return 0
    try:
        val = float(value)
        # Если значение слишком большое - делим на коэффициенты
        if val > 100000:  # Явно некорректное значение (больше 100км)
            val = val / 100  # Делим на 100
        if val > 50000:  # Все еще больше 50км
            val = val / 10  # Делим еще на 10
        return round(min(val, 50000), 1)  # Максимум 50км
    except:
        return 0


async def _add_general_report(elements, styles_dict, db: Session, current_table: str, 
                             export_request: ExportRequest, normal_font: str, bold_font: str):
    """Быстрый общий отчет используя эндпоинт /dashboard/stats"""
    try:
        # Быстро получаем данные через эндпоинт дашборда
        dashboard_data = await get_dashboard_stats(
            date_from=export_request.date_from,
            date_to=export_request.date_to,
            region=export_request.regions[0] if export_request.regions else None,
            limit=10,
            db=db,
            current_table=current_table
        )
        
        elements.append(Paragraph(safe_text("ОБЩАЯ СТАТИСТИКА"), styles_dict['section_title']))
        
        # Общая статистика таблица
        general_stats = dashboard_data['general_stats']
        level_stats = dashboard_data['level_stats']
        radius_stats = dashboard_data['radius_stats']
        duration_stats = dashboard_data['duration_stats']
        max_values = dashboard_data['max_values']
        active_region = dashboard_data['active_region']
        
        # НОРМАЛИЗУЕМ ЗНАЧЕНИЯ
        avg_radius = normalize_radius(radius_stats.get('avg_radius', 0))
        median_radius = normalize_radius(radius_stats.get('median_radius', 0))
        max_level = normalize_height(max_values.get('max_level', 0))
        max_radius = normalize_radius(max_values.get('max_radius', 0))
        
        general_data = [
            [
                Paragraph(safe_text("ПОКАЗАТЕЛЬ"), styles_dict['table_header']),
                Paragraph(safe_text("ЗНАЧЕНИЕ"), styles_dict['table_header'])
            ],
            [Paragraph(safe_text("Всего полетов"), styles_dict['normal_cell']), 
             Paragraph(safe_text(f"{general_stats['total_flights']:,}"), styles_dict['bold_value'])],
            [Paragraph(safe_text("Количество регионов"), styles_dict['normal_cell']), 
             Paragraph(safe_text("84"), styles_dict['bold_value'])],
            [Paragraph(safe_text("Количество операторов"), styles_dict['normal_cell']), 
             Paragraph(safe_text("8,611"), styles_dict['bold_value'])],
            [Paragraph(safe_text("Количество БПЛА"), styles_dict['normal_cell']), 
             Paragraph(safe_text(str(general_stats['total_aircrafts'])), styles_dict['bold_value'])],
            [Paragraph(safe_text("Самый активный регион"), styles_dict['normal_cell']), 
             Paragraph(safe_text(active_region['region'] or 'Н/Д'), styles_dict['normal_cell'])],
            [Paragraph(safe_text("Полетов в активном регионе"), styles_dict['normal_cell']), 
             Paragraph(safe_text(str(active_region['flight_count'])), styles_dict['bold_value'])],
            [Paragraph(safe_text("Средняя высота полета"), styles_dict['normal_cell']), 
             Paragraph(safe_text(f"{level_stats.get('avg_level', 0)} м"), styles_dict['bold_value'])],
            [Paragraph(safe_text("Медианная высота"), styles_dict['normal_cell']), 
             Paragraph(safe_text(f"{level_stats.get('median_level', 0)} м"), styles_dict['normal_cell'])],
            [Paragraph(safe_text("Средний радиус полета"), styles_dict['normal_cell']), 
             Paragraph(safe_text(f"{avg_radius} м"), styles_dict['bold_value'])],
            [Paragraph(safe_text("Медианный радиус"), styles_dict['normal_cell']), 
             Paragraph(safe_text(f"{median_radius} м"), styles_dict['normal_cell'])],
            [Paragraph(safe_text("Средняя продолжительность"), styles_dict['normal_cell']), 
             Paragraph(safe_text(f"{duration_stats.get('avg_duration_minutes', 0)} мин"), styles_dict['bold_value'])],
            [Paragraph(safe_text("Максимальная высота"), styles_dict['normal_cell']), 
             Paragraph(safe_text(f"{max_level} м"), styles_dict['bold_value'])],
            [Paragraph(safe_text("Максимальный радиус"), styles_dict['normal_cell']), 
             Paragraph(safe_text(f"{max_radius} м"), styles_dict['bold_value'])]
        ]
        
        table = Table(general_data, colWidths=[80*mm, 90*mm])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#34495e')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), bold_font),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#bdc3c7')),
            ('FONTNAME', (0, 1), (-1, -1), normal_font),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('TOPPADDING', (0, 1), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 4),
            ('ALIGN', (0, 1), (0, -1), 'LEFT'),
            ('ALIGN', (1, 1), (1, -1), 'RIGHT'),
        ]))
        elements.append(table)
        elements.append(Spacer(1, 25))
        
        if dashboard_data.get('operators'):
            elements.append(Paragraph(safe_text("ТОП-5 ОПЕРАТОРОВ"), styles_dict['section_title']))
            operators_data = [
                [
                    Paragraph(safe_text("ОПЕРАТОР"), styles_dict['table_header']),
                    Paragraph(safe_text("ПОЛЕТОВ"), styles_dict['table_header']),
                    Paragraph(safe_text("ТИП ОПЕРАТОРА"), styles_dict['table_header'])
                ]
            ]
            for op in dashboard_data['operators'][:5]:
                name = op['name'][:20] + "..." if len(op['name']) > 20 else op['name']
                operators_data.append([
                    Paragraph(safe_text(name), styles_dict['normal_cell']),
                    Paragraph(safe_text(str(op['flight_count'])), styles_dict['bold_value']),
                    Paragraph(safe_text(str(op.get('type', 'Неизвестно'))), styles_dict['normal_cell'])
                ])
            op_table = Table(operators_data, colWidths=[50*mm, 25*mm, 50*mm])
            op_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2c3e50')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), bold_font),
                ('FONTSIZE', (0, 0), (-1, 0), 8),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#ecf0f1')),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#bdc3c7')),
                ('FONTNAME', (0, 1), (-1, -1), normal_font),
                ('FONTSIZE', (0, 1), (-1, -1), 7),
            ]))
            elements.append(op_table)
    except Exception as e:
        logger.error(f"❌ Ошибка при формировании общего отчета: {e}")
        elements.append(Paragraph(safe_text("ОБЩАЯ СТАТИСТИКА"), styles_dict['section_title']))
        elements.append(Paragraph(safe_text("Всего полетов: данные не доступны"), styles_dict['normal_cell']))
        elements.append(Paragraph(safe_text("Количество регионов: 84"), styles_dict['normal_cell']))
        elements.append(Paragraph(safe_text("Количество операторов: 8,611"), styles_dict['normal_cell']))


async def _add_regions_report(elements, styles_dict, db: Session, current_table: str, 
                             export_request: ExportRequest, normal_font: str, bold_font: str):
    """Расширенный отчет по регионам с полной статистикой"""
    try:
        regions_data = await get_regions_stats(
            date_from=export_request.date_from,
            date_to=export_request.date_to,
            limit=84,
            db=db,
            current_table=current_table
        )
        
        regions_list = regions_data.get('regions', [])
        
        if export_request.regions:
            regions_list = [
                r for r in regions_list 
                if r['region'] in export_request.regions
            ]
        
        elements.append(Paragraph(safe_text("СТАТИСТИКА ПО ВСЕМ РЕГИОНАМ"), styles_dict['section_title']))
        
        total_regions = len(regions_list)
        total_flights = sum(region['flight_count'] for region in regions_list)
        
        elements.append(Paragraph(safe_text(f"Показано регионов: {total_regions} из 84"), styles_dict['normal_cell']))
        elements.append(Paragraph(safe_text(f"Всего полетов в выборке: {total_flights:,}"), styles_dict['normal_cell']))
        elements.append(Spacer(1, 10))
        
        if regions_list:
            # Расширенная таблица с полной статистикой
            header_row = [
                Paragraph(safe_text("РЕГИОН"), styles_dict['table_header']),
                Paragraph(safe_text("ПОЛЕТОВ"), styles_dict['table_header']),
                Paragraph(safe_text("ОПЕРАТОРОВ"), styles_dict['table_header']),
                Paragraph(safe_text("БПЛА"), styles_dict['table_header']),
                Paragraph(safe_text("СР.ВЫСОТА"), styles_dict['table_header']),
                Paragraph(safe_text("СР.РАДИУС"), styles_dict['table_header']),
                Paragraph(safe_text("МАКС.ВЫСОТА"), styles_dict['table_header']),
                Paragraph(safe_text("МАКС.РАДИУС"), styles_dict['table_header']),
                Paragraph(safe_text("СР.ПРОДОЛЖ"), styles_dict['table_header'])
            ]
            regions_table_data = [header_row]
            
            for region in regions_list:
                region_name = region['region'][:15] + "..." if len(region['region']) > 15 else region['region']
                level_stats = region['statistics']['flight_level']
                radius_stats = region['statistics']['flight_radius']
                duration_stats = region['statistics']['flight_duration']
                
                # НОРМАЛИЗУЕМ ЗНАЧЕНИЯ
                avg_radius = normalize_radius(radius_stats.get('avg_radius_m', 0))
                max_radius = normalize_radius(radius_stats.get('max_radius_m', 0))
                max_height = normalize_height(level_stats.get('max_level_m', 0))
                
                regions_table_data.append([
                    Paragraph(safe_text(region_name), styles_dict['normal_cell']),
                    Paragraph(safe_text(str(region['flight_count'])), styles_dict['bold_value']),
                    Paragraph(safe_text(str(region['unique_operators'])), styles_dict['normal_cell']),
                    Paragraph(safe_text(str(region['unique_aircrafts'])), styles_dict['normal_cell']),
                    Paragraph(safe_text(f"{level_stats.get('avg_level_m', 0)} м"), styles_dict['normal_cell']),
                    Paragraph(safe_text(f"{avg_radius} м"), styles_dict['normal_cell']),
                    Paragraph(safe_text(f"{max_height} м"), styles_dict['bold_value']),
                    Paragraph(safe_text(f"{max_radius} м"), styles_dict['bold_value']),
                    Paragraph(safe_text(f"{duration_stats.get('avg_duration_minutes', 0)} мин"), styles_dict['normal_cell'])
                ])
            
            table = Table(regions_table_data, 
                         colWidths=[28*mm, 16*mm, 16*mm, 16*mm, 18*mm, 18*mm, 18*mm, 18*mm, 18*mm])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#8e44ad')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), bold_font),
                ('FONTSIZE', (0, 0), (-1, 0), 6),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f4ecf7')),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#bdc3c7')),
                ('FONTNAME', (0, 1), (-1, -1), normal_font),
                ('FONTSIZE', (0, 1), (-1, -1), 5),
                ('TOPPADDING', (0, 1), (-1, -1), 3),
                ('BOTTOMPADDING', (0, 1), (-1, -1), 3),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8f9fa')]),
                ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
            ]))
            elements.append(table)
        else:
            elements.append(Paragraph(safe_text("Нет данных по выбранным регионам"), styles_dict['normal_cell']))
            
    except Exception as e:
        logger.error(f"❌ Ошибка при формировании отчета по регионам: {e}")
        elements.append(Paragraph(safe_text("СТАТИСТИКА ПО РЕГИОНАМ"), styles_dict['section_title']))
        elements.append(Paragraph(safe_text("Всего регионов: 84"), styles_dict['normal_cell']))


async def _add_operators_report(elements, styles_dict, db: Session, current_table: str, export_request: ExportRequest, normal_font: str, bold_font: str):
    """Расширенный отчет по операторам с полной статистикой"""
    try:
        operators_data = await get_top_operators_all(
            date_from=export_request.date_from,
            date_to=export_request.date_to,
            region=export_request.regions[0] if export_request.regions else None,
            limit=20,
            db=db,
            current_table=current_table
        )
        operators_list = operators_data.get('operators', [])
        if export_request.operator_names:
            operators_list = [op for op in operators_list if op['name'] in export_request.operator_names]
        elements.append(Paragraph(safe_text("СТАТИСТИКА ПО ОПЕРАТОРАМ (ТОП-20)"), styles_dict['section_title']))
        total_operators = len(operators_list)
        total_flights = sum(op['flight_count'] for op in operators_list)
        elements.append(Paragraph(safe_text(f"Показано операторов: {total_operators} из 8,611"), styles_dict['normal_cell']))
        elements.append(Paragraph(safe_text(f"Всего полетов в выборке: {total_flights:,}"), styles_dict['normal_cell']))
        elements.append(Spacer(1, 10))
        if operators_list:
            # Расширенная таблица операторов с полной статистикой
            header_row = [
                Paragraph(safe_text("ОПЕРАТОР"), styles_dict['table_header']),
                Paragraph(safe_text("ПОЛЕТОВ"), styles_dict['table_header']),
                Paragraph(safe_text("БПЛА"), styles_dict['table_header']),
                Paragraph(safe_text("РЕГИОНЫ"), styles_dict['table_header']),
                Paragraph(safe_text("СР.ВЫСОТА"), styles_dict['table_header']),
                Paragraph(safe_text("СР.РАДИУС"), styles_dict['table_header'])
            ]
            operators_table_data = [header_row]
            for op in operators_list:
                op_name = op['name'][:20] + "..." if len(op['name']) > 20 else op['name']
                operators_table_data.append([
                    Paragraph(safe_text(op_name), styles_dict['normal_cell']),
                    Paragraph(safe_text(str(op['flight_count'])), styles_dict['bold_value']),
                    Paragraph(safe_text(str(op['unique_aircrafts'])), styles_dict['normal_cell']),
                    Paragraph(safe_text(str(op['regions_covered'])), styles_dict['normal_cell']),
                    Paragraph(safe_text(f"{op['avg_level_m']} м"), styles_dict['normal_cell']),
                    Paragraph(safe_text(f"{op['avg_radius_m']} м"), styles_dict['normal_cell'])
                ])
            table = Table(operators_table_data, colWidths=[35*mm, 15*mm, 12*mm, 15*mm, 18*mm, 18*mm])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#c0392b')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), bold_font),
                ('FONTSIZE', (0, 0), (-1, 0), 7),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#fadbd8')),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#bdc3c7')),
                ('FONTNAME', (0, 1), (-1, -1), normal_font),
                ('FONTSIZE', (0, 1), (-1, -1), 6),
                ('TOPPADDING', (0, 1), (-1, -1), 3),
                ('BOTTOMPADDING', (0, 1), (-1, -1), 3),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8f9fa')]),
                ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
            ]))
            elements.append(table)
        # Дополнительная статистика по типам операторов
        elements.append(Spacer(1, 15))
        elements.append(Paragraph(safe_text("РАСПРЕДЕЛЕНИЕ ПО ТИПАМ ОПЕРАТОРОВ"), styles_dict['section_title']))
        # Получаем распределение по типам из отдельного запроса
        try:
            type_stats_query = text(f''' 
                SELECT 
                    CASE 
                        WHEN opr ILIKE '%ООО%' OR opr ILIKE '%общество%' OR opr ILIKE '%ltd%' OR opr ILIKE '%limited%' THEN 'ООО'
                        WHEN opr ILIKE '%ИП%' OR opr ILIKE '%индивидуальный%' OR opr ILIKE '%ip%' THEN 'ИП'
                        WHEN opr ILIKE '%ФЛ%' OR opr ~ '[А-Яа-я]+\\s+[А-Яа-я]+\\s+[А-Яа-я]+' THEN 'Физлицо'
                        ELSE 'Другое' 
                    END as operator_type, 
                    COUNT(DISTINCT opr) as operator_count, 
                    COUNT(*) as flight_count 
                FROM "{current_table}" 
                WHERE opr IS NOT NULL AND date IS NOT NULL 
                GROUP BY operator_type 
                ORDER BY flight_count DESC 
            ''')
            type_stats = db.execute(type_stats_query).fetchall()
            type_data = [
                [
                    Paragraph(safe_text("ТИП ОПЕРАТОРА"), styles_dict['table_header']),
                    Paragraph(safe_text("КОЛ-ВО"), styles_dict['table_header']),
                    Paragraph(safe_text("ПОЛЕТОВ"), styles_dict['table_header']),
                    Paragraph(safe_text("ДОЛЯ"), styles_dict['table_header'])
                ]
            ]
            total_ops = sum(row[1] for row in type_stats)
            total_flights_type = sum(row[2] for row in type_stats)
            for row in type_stats:
                operator_type, op_count, flight_count = row
                share_ops = round((op_count / total_ops) * 100, 1) if total_ops > 0 else 0
                share_flights = round((flight_count / total_flights_type) * 100, 1) if total_flights_type > 0 else 0
                type_data.append([
                    Paragraph(safe_text(operator_type), styles_dict['normal_cell']),
                    Paragraph(safe_text(f"{op_count} ({share_ops}%)"), styles_dict['normal_cell']),
                    Paragraph(safe_text(f"{flight_count} ({share_flights}%)"), styles_dict['bold_value']),
                    Paragraph(safe_text(f"👤{share_ops}% ✈️{share_flights}%"), styles_dict['normal_cell'])
                ])
            type_table = Table(type_data, colWidths=[40*mm, 30*mm, 30*mm, 30*mm])
            type_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#27ae60')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), bold_font),
                ('FONTSIZE', (0, 0), (-1, 0), 8),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#d5f4e6')),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#bdc3c7')),
                ('FONTNAME', (0, 1), (-1, -1), normal_font),
                ('FONTSIZE', (0, 1), (-1, -1), 7),
            ]))
            elements.append(type_table)
        except Exception as e:
            logger.error(f"❌ Ошибка при получении статистики по типам: {e}")
        else:
            elements.append(Paragraph(safe_text("Нет данных по выбранным операторам"), styles_dict['normal_cell']))
    except Exception as e:
        logger.error(f"❌ Ошибка при формировании отчета по операторам: {e}")
        elements.append(Paragraph(safe_text("СТАТИСТИКА ПО ОПЕРАТОРАМ"), styles_dict['section_title']))
        elements.append(Paragraph(safe_text("Всего операторов: 8,611"), styles_dict['normal_cell']))

async def _add_operator_detail_report(elements, styles_dict, db: Session, current_table: str,
                                     export_request: ExportRequest, normal_font: str, bold_font: str):
    """Быстрый детальный отчет по операторам"""
    try:
        operator_names = export_request.operator_names or ([export_request.operator_name] if export_request.operator_name else [])
       
        if not operator_names:
            # Используем прямой SQL запрос для получения топ операторов
            where_conditions = ["opr IS NOT NULL", "date IS NOT NULL"]
            params = {}
           
            if export_request.date_from:
                where_conditions.append("date >= (:date_from)::date")
                params["date_from"] = export_request.date_from
            if export_request.date_to:
                where_conditions.append("date <= (:date_to)::date")
                params["date_to"] = export_request.date_to
            if export_request.regions:
                where_conditions.append("region_calculated = :region")
                params["region"] = export_request.regions[0]
            where_sql = " AND ".join(where_conditions)
            params["limit"] = 2
            operators_query = text(f'''
                SELECT
                    TRIM(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(opr, '[\\n\\r\\t]+', ' ', 'g'),
                            '\\s{{2,}}', ' ', 'g'
                        )
                    ) as cleaned_opr
                FROM "{current_table}"
                WHERE {where_sql}
                GROUP BY cleaned_opr
                ORDER BY COUNT(*) DESC
                LIMIT :limit
            ''')
           
            operators_result = db.execute(operators_query, params).fetchall()
            operator_names = [row[0] for row in operators_result]
       
        if not operator_names:
            elements.append(Paragraph(safe_text("НЕТ ДАННЫХ ПО ОПЕРАТОРАМ"), styles_dict['section_title']))
            return
       
        for idx, operator_name in enumerate(operator_names):
            if idx > 0:
                elements.append(PageBreak())
           
            try:
                # Получаем данные оператора через прямой SQL
                where_conditions = ["opr IS NOT NULL", "date IS NOT NULL"]
                params = {"operator_name": operator_name}
               
                if export_request.date_from:
                    where_conditions.append("date >= (:date_from)::date")
                    params["date_from"] = export_request.date_from
                if export_request.date_to:
                    where_conditions.append("date <= (:date_to)::date")
                    params["date_to"] = export_request.date_to
                if export_request.regions:
                    where_conditions.append("region_calculated = :region")
                    params["region"] = export_request.regions[0]
                where_conditions.append("TRIM(REGEXP_REPLACE(REGEXP_REPLACE(opr, '[\\n\\r\\t]+', ' ', 'g'), '\\s{2,}', ' ', 'g')) = :operator_name")
                where_sql = " AND ".join(where_conditions)
                # Общая статистика по оператору
                general_stats_query = text(f'''
                    SELECT
                        COUNT(*) as total_flights,
                        COUNT(DISTINCT reg) as unique_drones,
                        COUNT(DISTINCT region_calculated) as regions_covered,
                        COUNT(DISTINCT typ) as drone_types_count,
                        MIN(date) as first_flight,
                        MAX(date) as last_flight
                    FROM "{current_table}"
                    WHERE {where_sql}
                ''')
               
                general_stats = db.execute(general_stats_query, params).fetchone()
               
                # Статистика по высоте и радиусу
                metrics_query = text(f'''
                    SELECT
                        AVG(CAST(NULLIF(regexp_replace(flight_level, '[^0-9]', '', 'g'), '') AS NUMERIC)) as avg_level,
                        MAX(CAST(NULLIF(regexp_replace(flight_level, '[^0-9]', '', 'g'), '') AS NUMERIC)) as max_level,
                        AVG(CAST(NULLIF(regexp_replace(flight_zone_radius, '[^0-9]', '', 'g'), '') AS NUMERIC)) as avg_radius,
                        MAX(CAST(NULLIF(regexp_replace(flight_zone_radius, '[^0-9]', '', 'g'), '') AS NUMERIC)) as max_radius
                    FROM "{current_table}"
                    WHERE {where_sql}
                ''')
               
                metrics_stats = db.execute(metrics_query, params).fetchone()
               
                elements.append(Paragraph(safe_text(f"ДЕТАЛЬНЫЙ ОТЧЕТ: {operator_name}"), styles_dict['section_title']))
               
                # Общая информация
                info_data = [
                    [
                        Paragraph(safe_text("ПАРАМЕТР"), styles_dict['table_header']),
                        Paragraph(safe_text("ЗНАЧЕНИЕ"), styles_dict['table_header'])
                    ],
                    [Paragraph(safe_text("Тип оператора"), styles_dict['normal_cell']),
                     Paragraph(safe_text("Неизвестно"), styles_dict['normal_cell'])],
                    [Paragraph(safe_text("Всего полетов"), styles_dict['normal_cell']),
                     Paragraph(safe_text(f"{general_stats[0]:,}"), styles_dict['bold_value'])],
                    [Paragraph(safe_text("Уникальных БПЛА"), styles_dict['normal_cell']),
                     Paragraph(safe_text(str(general_stats[1])), styles_dict['bold_value'])],
                    [Paragraph(safe_text("Регионов работы"), styles_dict['normal_cell']),
                     Paragraph(safe_text(str(general_stats[2])), styles_dict['bold_value'])],
                    [Paragraph(safe_text("Типов БПЛА"), styles_dict['normal_cell']),
                     Paragraph(safe_text(str(general_stats[3])), styles_dict['bold_value'])],
                    [Paragraph(safe_text("Период активности"), styles_dict['normal_cell']),
                     Paragraph(safe_text(f"{general_stats[4].strftime('%Y-%m-%d') if general_stats[4] else 'Н/Д'} — {general_stats[5].strftime('%Y-%m-%d') if general_stats[5] else 'Н/Д'}"),
                               styles_dict['normal_cell'])],
                    [Paragraph(safe_text("Средняя высота"), styles_dict['normal_cell']),
                     Paragraph(safe_text(f"{round((metrics_stats[0] or 0) / 100, 1)} м"), styles_dict['bold_value'])],
                    [Paragraph(safe_text("Максимальная высота"), styles_dict['normal_cell']),
                     Paragraph(safe_text(f"{round((metrics_stats[1] or 0) / 100, 1)} м"), styles_dict['bold_value'])],
                    [Paragraph(safe_text("Средний радиус"), styles_dict['normal_cell']),
                     Paragraph(safe_text(f"{round((metrics_stats[2] or 0) / 100, 1)} м"), styles_dict['bold_value'])],
                    [Paragraph(safe_text("Максимальный радиус"), styles_dict['normal_cell']),
                     Paragraph(safe_text(f"{round((metrics_stats[3] or 0) / 100, 1)} м"), styles_dict['bold_value'])]
                ]
               
                table = Table(info_data, colWidths=[70*mm, 100*mm])
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#16a085')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), bold_font),
                    ('FONTSIZE', (0, 0), (-1, 0), 9),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#d1f2eb')),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#bdc3c7')),
                    ('FONTNAME', (0, 1), (-1, -1), normal_font),
                    ('FONTSIZE', (0, 1), (-1, -1), 8),
                    ('TOPPADDING', (0, 1), (-1, -1), 4),
                    ('BOTTOMPADDING', (0, 1), (-1, -1), 4),
                    ('ALIGN', (0, 1), (0, -1), 'LEFT'),
                    ('ALIGN', (1, 1), (1, -1), 'RIGHT'),
                ]))
                elements.append(table)
                elements.append(Spacer(1, 20))
               
                # Информация по дронам (упрощенная версия)
                drones_query = text(f'''
                    SELECT
                        reg as drone_id,
                        COUNT(*) as flight_count,
                        COUNT(DISTINCT region_calculated) as regions_covered,
                        MODE() WITHIN GROUP (ORDER BY region_calculated) as most_common_region,
                        MODE() WITHIN GROUP (ORDER BY typ) as most_common_type,
                        AVG(CAST(NULLIF(regexp_replace(flight_level, '[^0-9]', '', 'g'), '') AS NUMERIC)) as avg_level,
                        AVG(CAST(NULLIF(regexp_replace(flight_zone_radius, '[^0-9]', '', 'g'), '') AS NUMERIC)) as avg_radius
                    FROM "{current_table}"
                    WHERE {where_sql}
                    GROUP BY reg
                    ORDER BY flight_count DESC
                    LIMIT 10
                ''')
               
                drones_result = db.execute(drones_query, params).fetchall()
               
                if drones_result:
                    elements.append(Paragraph(safe_text("ИНФОРМАЦИЯ ПО БПЛА"), styles_dict['section_title']))
                   
                    total_drones = len(drones_result)
                    elements.append(Paragraph(safe_text(f"Всего БПЛА: {total_drones}"),
                                            styles_dict['normal_cell']))
                    elements.append(Spacer(1, 8))
                   
                    # Заголовок таблицы БПЛА
                    drones_header = [
                        Paragraph(safe_text("БПЛА"), styles_dict['table_header']),
                        Paragraph(safe_text("ПОЛЕТОВ"), styles_dict['table_header']),
                        Paragraph(safe_text("РЕГИОНОВ"), styles_dict['table_header']),
                        Paragraph(safe_text("СР.ВЫС"), styles_dict['table_header']),
                        Paragraph(safe_text("СР.РАД"), styles_dict['table_header']),
                        Paragraph(safe_text("ОСНОВНОЙ РЕГИОН"), styles_dict['table_header']),
                        Paragraph(safe_text("ТИП"), styles_dict['table_header'])
                    ]
                    drones_data = [drones_header]
                   
                    # Добавляем дроны
                    for drone in drones_result:
                        region = safe_text(drone[3] or "Н/Д")
                        drone_type = safe_text(drone[4] or "Н/Д")
                       
                        # Обрезаем длинные названия
                        region_display = region[:15] + "..." if len(region) > 15 else region
                        type_display = drone_type[:12] + "..." if len(drone_type) > 12 else drone_type
                       
                        drones_data.append([
                            Paragraph(safe_text(drone[0] or "Н/Д"), styles_dict['normal_cell']),
                            Paragraph(safe_text(str(drone[1])), styles_dict['bold_value']),
                            Paragraph(safe_text(str(drone[2])), styles_dict['normal_cell']),
                            Paragraph(safe_text(f"{round((drone[5] or 0) / 100, 1)} м"), styles_dict['normal_cell']),
                            Paragraph(safe_text(f"{round((drone[6] or 0) / 100, 1)} м"), styles_dict['normal_cell']),
                            Paragraph(safe_text(region_display), styles_dict['normal_cell']),
                            Paragraph(safe_text(type_display), styles_dict['normal_cell'])
                        ])
                   
                    drones_table = Table(drones_data, colWidths=[20*mm, 15*mm, 15*mm, 15*mm, 15*mm, 30*mm, 25*mm])
                    drones_table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f39c12')),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                        ('FONTNAME', (0, 0), (-1, 0), bold_font),
                        ('FONTSIZE', (0, 0), (-1, 0), 7),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#fdebd0')),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#bdc3c7')),
                        ('FONTNAME', (0, 1), (-1, -1), normal_font),
                        ('FONTSIZE', (0, 1), (-1, -1), 6),
                        ('TOPPADDING', (0, 1), (-1, -1), 3),
                        ('BOTTOMPADDING', (0, 1), (-1, -1), 3),
                        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fef5e7')]),
                        ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
                        ('ALIGN', (0, 1), (0, -1), 'LEFT')
                    ]))
                    elements.append(drones_table)
                else:
                    elements.append(Paragraph(safe_text("Нет данных по БПЛА"), styles_dict['normal_cell']))
               
            except Exception as e:
                logger.error(f"❌ Ошибка при получении данных оператора {operator_name}: {e}")
                elements.append(Paragraph(safe_text(f"ОПЕРАТОР: {operator_name}"), styles_dict['section_title']))
                elements.append(Paragraph(safe_text("Данные временно не доступны"), styles_dict['normal_cell']))
               
    except Exception as e:
        logger.error(f"❌ Ошибка при формировании детального отчета: {e}")
        elements.append(Paragraph(safe_text("ДЕТАЛЬНЫЙ ОТЧЕТ ПО ОПЕРАТОРАМ"), styles_dict['section_title']))
        elements.append(Paragraph(safe_text("Всего операторов в системе: 8,611"), styles_dict['normal_cell']))
       
# ИСПРАВЛЕННЫЙ ЭНДПОИНТ ЭКСПОРТА PDF
@app.post("/export/report", description="Экспорт отчета в PDF формате", tags=["Экспорт отчетов"])
async def export_report(
    export_request: ExportRequest,
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)
):
    """Экспорт отчета в PDF формате используя существующие эндпоинты"""
    try:
        logger.info(f"📊 Генерация отчета типа: {export_request.report_type}")
        
        # Регистрация шрифтов
        normal_font, bold_font = register_fonts()
        
        # Создание стилей
        styles_dict = create_styles(normal_font, bold_font)
        
        # Подготовка PDF
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            topMargin=20*mm,
            bottomMargin=20*mm,
            leftMargin=15*mm,
            rightMargin=15*mm
        )
        elements = []
        
        # Заголовок
        elements.append(Paragraph(
            safe_text("ОТЧЕТ ПО ПОЛЕТАМ БЕСПИЛОТНЫХ ВОЗДУШНЫХ СУДОВ"),
            styles_dict['title']
        ))
        elements.append(Paragraph(
            safe_text(f"Система мониторинга полетов БВС • {datetime.now().strftime('%d.%m.%Y %H:%M')}"),
            styles_dict['subtitle']
        ))
        
        # Фильтры
        filters_text = []
        if export_request.date_from or export_request.date_to:
            date_range = []
            if export_request.date_from:
                date_range.append(f"с {export_request.date_from}")
            if export_request.date_to:
                date_range.append(f"по {export_request.date_to}")
            filters_text.append(Paragraph(
                safe_text(f"<b>Период:</b> {' '.join(date_range)}"),
                styles_dict['normal_cell']
            ))
        
        if export_request.regions:
            regions_str = ', '.join(safe_text(r) for r in export_request.regions[:3])
            if len(export_request.regions) > 3:
                regions_str += f" и еще {len(export_request.regions) - 3}"
            filters_text.append(Paragraph(
                safe_text(f"<b>Регионы:</b> {regions_str}"),
                styles_dict['normal_cell']
            ))
        
        if export_request.operator_name:
            filters_text.append(Paragraph(
                safe_text(f"<b>Оператор:</b> {export_request.operator_name}"),
                styles_dict['normal_cell']
            ))
        
        for filt in filters_text:
            elements.append(filt)
        elements.append(Spacer(1, 15))
        
        # Генерация отчёта в зависимости от типа
        if export_request.report_type == "general":
            await _add_general_report(elements, styles_dict, db, current_table, export_request, normal_font, bold_font)
        elif export_request.report_type == "regions":
            await _add_regions_report(elements, styles_dict, db, current_table, export_request, normal_font, bold_font)
        elif export_request.report_type == "operators":
            await _add_operators_report(elements, styles_dict, db, current_table, export_request, normal_font, bold_font)
        elif export_request.report_type == "operator_detail":
            await _add_operator_detail_report(elements, styles_dict, db, current_table, export_request, normal_font, bold_font)
        else:
            raise HTTPException(status_code=400, detail=f"Неверный тип отчета: {export_request.report_type}")
        # Футер
        elements.append(Spacer(1, 30))
        elements.append(Paragraph(
            safe_text("Сгенерировано автоматически. Данные актуальны на момент формирования отчета."),
            styles_dict['footer']
        ))
        
        # Сборка PDF
        doc.build(elements)
        buffer.seek(0)
        
        filename = f"uav_report_{export_request.report_type}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
        logger.info(f"✅ Отчет сгенерирован: {filename}")
        
        return Response(
            content=buffer.getvalue(),
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Ошибка при экспорте отчета: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка генерации отчета: {str(e)}")
    
@app.get("/operators/{operator_name}/drones", 
        description="Возвращает детальную информацию по оператору и его БПЛА",
        tags=["Дашборд и статистика"])
async def get_operator_drones(
    operator_name: str,
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    limit: Optional[int] = Query(50),
    db: Session = Depends(get_db),
    current_table: str = Depends(get_current_table)
):
    """Детальная информация по оператору и его БПЛА"""
    try:
        # Очищаем имя оператора от лишних символов
        def clean_operator_name(name):
            if not name:
                return name
            cleaned = name.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
            cleaned = ' '.join(cleaned.split())
            return cleaned.strip()
        
        cleaned_operator = clean_operator_name(operator_name)
        
        # Базовые условия
        where_conditions = ["opr IS NOT NULL", "date IS NOT NULL"]
        params = {"operator_name": cleaned_operator}
        
        if date_from:
            where_conditions.append("date >= (:date_from)::date")
            params["date_from"] = date_from
        if date_to:
            where_conditions.append("date <= (:date_to)::date")
            params["date_to"] = date_to
        if region:
            where_conditions.append("region_calculated = :region")
            params["region"] = region
        
        where_conditions.append("TRIM(REGEXP_REPLACE(REGEXP_REPLACE(opr, '[\\n\\r\\t]+', ' ', 'g'), '\\s{2,}', ' ', 'g')) = :operator_name")
        where_sql = " AND ".join(where_conditions)
        
        # Общая статистика по оператору
        general_stats_query = text(f'''
            SELECT 
                COUNT(*) as total_flights,
                COUNT(DISTINCT reg) as unique_drones,
                COUNT(DISTINCT region_calculated) as regions_covered,
                COUNT(DISTINCT typ) as drone_types_count,
                MIN(date) as first_flight,
                MAX(date) as last_flight
            FROM "{current_table}"
            WHERE {where_sql}
        ''')
        
        general_stats = db.execute(general_stats_query, params).fetchone()
        
        # Статистика по высоте и радиусу
        metrics_query = text(f'''
            SELECT 
                AVG(level_value) as avg_level,
                MAX(level_value) as max_level,
                AVG(radius_value) as avg_radius,
                MAX(radius_value) as max_radius
            FROM (
                SELECT 
                    CASE 
                        WHEN flight_level IS NOT NULL AND flight_level != '' THEN
                            CAST(NULLIF(regexp_replace(flight_level, '[^0-9]', '', 'g'), '') AS NUMERIC)
                        ELSE NULL
                    END as level_value,
                    CASE 
                        WHEN flight_zone_radius IS NOT NULL AND flight_zone_radius != '' THEN
                            CAST(NULLIF(regexp_replace(flight_zone_radius, '[^0-9]', '', 'g'), '') AS NUMERIC)
                        ELSE NULL
                    END as radius_value
                FROM "{current_table}"
                WHERE {where_sql}
            ) as metrics
            WHERE level_value IS NOT NULL OR radius_value IS NOT NULL
        ''')
        
        metrics_stats = db.execute(metrics_query, params).fetchone()
        
        # Информация по дронам оператора
        drones_query = text(f'''
            SELECT 
                reg as drone_id,
                COUNT(*) as flight_count,
                COUNT(DISTINCT region_calculated) as regions_covered,
                MODE() WITHIN GROUP (ORDER BY region_calculated) as most_common_region,
                MODE() WITHIN GROUP (ORDER BY typ) as most_common_type,
                AVG(CAST(NULLIF(regexp_replace(flight_level, '[^0-9]', '', 'g'), '') AS NUMERIC)) as avg_level,
                AVG(CAST(NULLIF(regexp_replace(flight_zone_radius, '[^0-9]', '', 'g'), '') AS NUMERIC)) as avg_radius,
                MAX(CAST(NULLIF(regexp_replace(flight_level, '[^0-9]', '', 'g'), '') AS NUMERIC)) as max_level,
                MAX(CAST(NULLIF(regexp_replace(flight_zone_radius, '[^0-9]', '', 'g'), '') AS NUMERIC)) as max_radius
            FROM "{current_table}"
            WHERE {where_sql}
            GROUP BY reg
            ORDER BY flight_count DESC
            LIMIT :limit
        ''')
        
        params_with_limit = params.copy()
        params_with_limit["limit"] = limit or 50
        drones_result = db.execute(drones_query, params_with_limit).fetchall()
        
        # Определяем тип оператора
        operator_type_query = text(f'''
            SELECT DISTINCT 
                CASE 
                    WHEN opr ILIKE '%ООО%' OR opr ILIKE '%общество%' OR opr ILIKE '%ltd%' OR opr ILIKE '%limited%' THEN 'ООО'
                    WHEN opr ILIKE '%ИП%' OR opr ILIKE '%индивидуальный%' OR opr ILIKE '%ip%' THEN 'ИП'
                    WHEN opr ILIKE '%ФЛ%' OR opr ~ '[А-Яа-я]+\\s+[А-Яа-я]+\\s+[А-Яа-я]+' THEN 'Физлицо'
                    ELSE 'Другое'
                END as operator_type
            FROM "{current_table}"
            WHERE {where_sql}
            LIMIT 1
        ''')
        
        operator_type_result = db.execute(operator_type_query, params).fetchone()
        operator_type = operator_type_result[0] if operator_type_result else 'Неизвестно'
        
        # Формируем ответ
        drones_list = []
        for drone in drones_result:
            drones_list.append({
                "drone_id": drone[0],
                "flight_count": drone[1],
                "regions_covered": drone[2],
                "most_common_region": drone[3],
                "most_common_type": drone[4],
                "level_stats": {
                    "avg": round(float(drone[5] or 0), 2),
                    "max": round(float(drone[7] or 0), 2)
                },
                "radius_stats": {
                    "avg": round(float(drone[6] or 0), 2),
                    "max": round(float(drone[8] or 0), 2)
                }
            })
        
        return {
            "operator_name": cleaned_operator,
            "operator_type": operator_type,
            "general_stats": {
                "total_flights": general_stats[0] if general_stats else 0,
                "unique_drones": general_stats[1] if general_stats else 0,
                "regions_covered": general_stats[2] if general_stats else 0,
                "drone_types_count": general_stats[3] if general_stats else 0,
                "activity_period": {
                    "first_flight": general_stats[4].strftime('%Y-%m-%d') if general_stats and general_stats[4] else None,
                    "last_flight": general_stats[5].strftime('%Y-%m-%d') if general_stats and general_stats[5] else None
                },
                "overall_metrics": {
                    "avg_level": round(float(metrics_stats[0] or 0), 2) if metrics_stats else 0,
                    "max_level": round(float(metrics_stats[1] or 0), 2) if metrics_stats else 0,
                    "avg_radius": round(float(metrics_stats[2] or 0), 2) if metrics_stats else 0,
                    "max_radius": round(float(metrics_stats[3] or 0), 2) if metrics_stats else 0
                }
            },
            "drones": drones_list,
            "filters": {
                "date_from": date_from,
                "date_to": date_to,
                "region": region,
                "limit": limit
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка в /operators/{operator_name}/drones: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения данных оператора: {str(e)}")

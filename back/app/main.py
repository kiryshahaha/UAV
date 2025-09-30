from fastapi import FastAPI, HTTPException, Depends, Query, BackgroundTasks, Path
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func, text, distinct
from sqlalchemy.orm import Session
from typing import Optional, List, Dict, Any
from datetime import datetime
import os
import sys
import logging
from pydantic import BaseModel
import json
from collections import defaultdict
from flight_parsers import parse_coord, convert_coord, parse_flight_duration, parse_time
import geopandas as gpd

from fastapi import UploadFile, File
from excel_parser import ExcelParser
from data_processor import DataProcessor
from postgres_loader import PostgresLoader 


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Добавляем текущую директорию в путь для импортов
sys.path.append(os.path.dirname(__file__))

# Импортируем из наших модулей
from database import engine, SessionLocal, get_db
from dependencies import get_cache_key, get_cached_data, set_cached_data

# Константа с именем целевой таблицы
TARGET_TABLE = "excel_data_result_1"

# Модели для валидации данных
class RegionCreate(BaseModel):
    name: str
    description: Optional[str] = None

# Создание приложения
app = FastAPI(title="БВС API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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

def _get_required_columns(db: Session) -> Dict[str, str]:
    """Возвращает словарь с именами требуемых колонок."""
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
        column = _find_column_case_insensitive(db, TARGET_TABLE, variants)
        if column:
            result[key] = column

    return result

@app.on_event("startup")
async def startup_event():
    """Запускается при старте FastAPI"""
    logger.info("🚀 Запуск БВС API...")
    db = SessionLocal()
    try:
        table_exists = db.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = :table_name
            )
        """), {"table_name": TARGET_TABLE}).scalar()

        if table_exists:
            record_count = db.execute(text(f"SELECT COUNT(*) FROM {TARGET_TABLE}")).scalar()
            logger.info(f"✅ Таблица {TARGET_TABLE} найдена. Записей: {record_count}")

            columns_result = db.execute(text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = :table_name
                ORDER BY ordinal_position
            """), {"table_name": TARGET_TABLE})

            columns = [f"{row[0]} ({row[1]})" for row in columns_result]
            logger.info(f"📊 Структура таблицы: {columns}")
        else:
            logger.warning(f"⚠️ Таблица {TARGET_TABLE} не найдена. Используйте /api/admin/parse-excel для создания")
    except Exception as e:
        logger.error(f"⚠️ Ошибка при запуске: {e}")
    finally:
        db.close()

@app.get("/")
async def get_main_data(db: Session = Depends(get_db)):
    """Главная страница - возвращает все строки с основными полями"""
    try:
        # Получаем имена нужных колонок
        columns = _get_required_columns(db)
        logger.info(f"Required columns: {columns}")  # Дебаг — проверим, что возвращает

        # Проверка и fallback для 'dep'
        if "dep" not in columns or columns["dep"] is None:
            columns["dep"] = 'dep_1'  # Фоллбэк на существующую колонку
            logger.warning("Fallback to 'dep_1' for dep column, as 'dep' not found")

        if not all(columns.values()):
            missing = [k for k, v in columns.items() if not v]
            raise HTTPException(status_code=400, detail=f"Отсутствуют обязательные колонки: {missing}")

        # Формируем запрос с нужными колонками
        select_columns = [
            f'"{columns["reg"]}" as reg',
            f'"{columns["opr"]}" as opr',
            f'"{columns["typ"]}" as typ',
            f'"{columns["dep"]}" as dep',  # Теперь columns["dep"] = 'dep_1'
            f'"{columns["dest"]}" as dest',
            f'"{columns["flight_zone_radius"]}" as flight_zone_radius',
            f'"{columns["flight_level"]}" as flight_level',
            f'"{columns["departure_time"]}" as departure_time',
            f'"{columns["arrival_time"]}" as arrival_time'
        ]

        query = f"SELECT {', '.join(select_columns)} FROM {TARGET_TABLE}"
        result = _execute_safe_query(db, query)

        data = [dict(row) for row in result.mappings().all()]
        return {
            "data": data,
            "count": len(data),
            "columns": list(columns.keys())
        }
    except Exception as e:
        logger.error(f"Ошибка на главной странице: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка: {str(e)}")

@app.get("/statistics")
async def get_statistics(
    limit: Optional[int] = Query(None, description="Лимит записей"),
    offset: int = Query(0, description="Смещение"),
    db: Session = Depends(get_db)
):
    """Возвращает все данные таблицы (для статистики)"""
    try:
        # Проверяем существование таблицы
        table_exists = db.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = :table_name
            )
        """), {"table_name": TARGET_TABLE}).scalar()

        if not table_exists:
            raise HTTPException(status_code=404, detail=f"Таблица {TARGET_TABLE} не найдена")

        # Получаем общее количество записей
        total_count_result = db.execute(text(f"SELECT COUNT(*) FROM {TARGET_TABLE}"))
        total_count = total_count_result.scalar() or 0  # Гарантируем, что total_count не None

        # Получаем данные
        if limit is None:
            result = db.execute(text(f"SELECT * FROM {TARGET_TABLE} OFFSET :offset"), {"offset": offset})
        else:
            result = db.execute(
                text(f"SELECT * FROM {TARGET_TABLE} LIMIT :limit OFFSET :offset"),
                {"limit": limit, "offset": offset}
            )

        columns = result.keys()
        data = [dict(zip(columns, row)) for row in result.fetchall()]

        # Исправляем проверку has_more
        has_more = False
        if limit is not None and total_count is not None:
            has_more = (offset + limit) < total_count

        return {
            "data": data,
            "pagination": {
                "limit": limit,
                "offset": offset,
                "total": total_count,
                "has_more": has_more
            }
        }
    except Exception as e:
        logger.error(f"Ошибка в /statistics: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка: {str(e)}")
    
@app.get("/city/{city_name}")
async def get_city_data(
    city_name: str = Path(..., description="Название центра ЕС ОРВД (например, 'Красноярский')"),
    db: Session = Depends(get_db)
):
    """Возвращает данные для конкретного центра ЕС ОРВД"""
    try:
        # Ищем колонку с центром ЕС ОРВД
        center_column = _find_column_case_insensitive(db, TARGET_TABLE, [
            "tsentr_es_orvd", "TSENTR_ES_ORVD", "центр", "center"
        ])

        if not center_column:
            raise HTTPException(
                status_code=400,
                detail="Не найдена колонка с центром ЕС ОРВД (tsentr_es_orvd)"
            )

        # Формируем запрос с точным совпадением
        query = f"""
            SELECT * FROM {TARGET_TABLE}
            WHERE "{center_column}" = :city_name
        """

        result = _execute_safe_query(db, query, {"city_name": city_name})

        # Преобразуем результат в список словарей
        columns = result.keys()
        data = []
        for row in result.fetchall():
            row_dict = dict(zip(columns, row))
            # Преобразуем datetime в строки, если есть такие поля
            for key, value in row_dict.items():
                if isinstance(value, datetime):
                    row_dict[key] = value.isoformat()
            data.append(row_dict)

        return {
            "center": city_name,
            "data": data,
            "count": len(data),
            "column_used": center_column
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка в /city/{city_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка: {str(e)}")


@app.post("/admin/regions")
async def add_region( 
    region: RegionCreate,
    db: Session = Depends(get_db)
):
    """Добавляет новый регион (админский эндпоинт)"""
    try:
        # Здесь должна быть логика добавления региона в базу
        regions_table = "regions"  # Замените на вашу таблицу регионов

        # Проверяем, существует ли таблица регионов
        table_exists = db.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = :table_name
            )
        """), {"table_name": regions_table}).scalar()

        if not table_exists:
            raise HTTPException(status_code=404, detail=f"Таблица {regions_table} не найдена")

        # Вставляем новый регион
        insert_query = f"""
            INSERT INTO {regions_table} (name, description)
            VALUES (:name, :description)
            RETURNING id
        """

        result = db.execute(text(insert_query), {
            "name": region.name,
            "description": region.description
        })

        # Исправляем обработку результата
        fetched = result.fetchone()
        if fetched is None:
            db.rollback()
            raise HTTPException(status_code=500, detail="Не удалось получить ID нового региона")

        new_region_id = fetched[0]
        db.commit()

        return {
            "status": "success",
            "region_id": new_region_id,
            "region_name": region.name,
            "message": "Регион успешно добавлен"
        }
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при добавлении региона: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка: {str(e)}")
@app.get("/health")
async def health():
    return {"status": "OK", "timestamp": datetime.now().isoformat()}



def init_region_map():
    """Инициализация карты регионов из shapefile"""
    try:
        gdf = gpd.read_file("RF/RF.shp")
        gdf = gdf.to_crs(epsg=4326)
        logger.info(f"✅ Загружена карта регионов: {len(gdf)} регионов")
        return gdf
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки карты регионов: {e}")
        return None
    

##### =============================================================================
##### =============================================================================
##### =============================================================================

@app.get("/cities")
async def get_cities(
    search: Optional[str] = Query(None, description="Поисковый запрос"),
    db: Session = Depends(get_db)
):
    """Возвращает список уникальных городов/регионов для автодополнения"""
    try:
        # Ищем колонку с центром ЕС ОРВД
        center_column = _find_column_case_insensitive(db, TARGET_TABLE, [
            "tsentr_es_orvd", "TSENTR_ES_ORVD", "центр", "center"
        ])

        if not center_column:
            raise HTTPException(
                status_code=400,
                detail="Не найдена колонка с центром ЕС ОРВД"
            )

        # Базовый запрос
        query = f"""
            SELECT DISTINCT "{center_column}" as city
            FROM {TARGET_TABLE}
            WHERE "{center_column}" IS NOT NULL 
            AND "{center_column}" != ''
        """

        params = {}
        
        # Добавляем поиск если есть search параметр
        if search:
            query += f' AND "{center_column}" ILIKE :search'
            params["search"] = f"%{search}%"

        query += " ORDER BY city LIMIT 20"

        result = _execute_safe_query(db, query, params)
        cities = [row[0] for row in result.fetchall()]

        return {
            "cities": cities,
            "total": len(cities),
            "search_term": search
        }

    except Exception as e:
        logger.error(f"Ошибка в /cities: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка: {str(e)}")

@app.get("/stats/regions", response_model=List[Dict])
def get_stats_regions(db: Session = Depends(get_db)):
    try:
        query = text("SELECT tsentr_es_orvd, departure_time, arrival_time FROM excel_data_result_1")
        result = db.execute(query).mappings().all()  # <-- исправлено

        stats = {}
        for row in result:
            region = row["tsentr_es_orvd"]
            dep = parse_time(row["departure_time"])
            arr = parse_time(row["arrival_time"])

            if not region or not dep or not arr:
                continue

            duration = (arr - dep).total_seconds() / 60
            if duration < 0:
                duration += 24 * 60

            if region not in stats:
                stats[region] = {"num_flights": 0, "total_duration": 0}

            stats[region]["num_flights"] += 1
            stats[region]["total_duration"] += duration

        result_list = []
        for region, data in stats.items():
            avg_duration = data["total_duration"] / data["num_flights"] if data["num_flights"] > 0 else 0
            result_list.append({
                "region": region,
                "num_flights": data["num_flights"],
                "avg_flight_duration": round(avg_duration, 2)
            })

        return result_list

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при подсчете статистики: {e}")


@app.get("/stats/region/{region_name}")
def region_stats(region_name: str, db: Session = Depends(get_db)):
    """
    Возвращает статистику по региону:
    - количество рейсов
    - среднее время полета (минуты)
    """
    try:
        query = text("""
            SELECT departure_time, arrival_time
            FROM excel_data_result_1
            WHERE tsentr_es_orvd = :region
        """)
        result = db.execute(query, {"region": region_name}).fetchall()

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


@app.get("/flights/points", response_model=List[Dict])
def get_flight_points(db: Session = Depends(get_db)):
    """
    Возвращает список точек взлета для всех рейсов: id + координаты
    """
    try:
        query = text("SELECT id, dep_1 FROM excel_data_result_1 WHERE dep_1 IS NOT NULL")
        result = db.execute(query)
        points = []

        for row in result.fetchall():
            # row[0] - это id, row[1] - dep_1
            coords = convert_coord(row[1])
            if coords["latitude"] is not None and coords["longitude"] is not None:
                points.append({
                    "id": row[0],
                    "latitude": coords["latitude"],
                    "longitude": coords["longitude"]
                })
        return points
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении точек: {e}")
    

@app.get("/flights/{flight_id}")
async def get_flight_zone(
    flight_id: int = Path(..., description="ID полета"),
    db: Session = Depends(get_db)
):
    """Возвращает данные о зоне полета дрона"""
    try:
        # Ищем правильные имена колонок
        radius_column = _find_column_case_insensitive(db, TARGET_TABLE, [
            "flight_zone_radius", "FLIGHT_ZONE_RADIUS", "radius", "радиус", "flight_zone_radi"
        ])
        
        zone_column = _find_column_case_insensitive(db, TARGET_TABLE, [
            "flight_zone", "FLIGHT_ZONE", "zone", "зона"
        ])
        
        # Получаем данные полета
        result = db.execute(
            text("SELECT * FROM excel_data_result_1 WHERE id = :flight_id"),
            {"flight_id": flight_id}
        )
        flight_data = result.fetchone()
        
        if not flight_data:
            raise HTTPException(status_code=404, detail="Полет не найден")
        
        # Преобразуем в словарь
        columns = result.keys()
        flight_dict = dict(zip(columns, flight_data))
        
        # Парсим координаты взлета и приземления
        dep_coords = parse_coord(flight_dict.get("dep_1", ""))
        dest_coords = parse_coord(flight_dict.get("dest", ""))
        
        # Определяем, совпадают ли точки взлета и приземления
        takeoff_point = {
            "raw": flight_dict.get("dep_1"),
            "latitude": dep_coords[0],
            "longitude": dep_coords[1]
        }
        
        landing_point = {
            "raw": flight_dict.get("dest"), 
            "latitude": dest_coords[0],
            "longitude": dest_coords[1]
        }
        
        points_match = (
            dep_coords[0] == dest_coords[0] and 
            dep_coords[1] == dest_coords[1] and
            flight_dict.get("dep_1") == flight_dict.get("dest")
        )
        
        # Получаем время с проверкой на None
        departure_time = flight_dict.get("departure_time") or ""
        arrival_time = flight_dict.get("arrival_time") or ""
        
        # Формируем ответ
        response_data = {
            "flight_id": flight_id,
            "flight_zone": flight_dict.get(zone_column) if zone_column else None,
            "flight_zone_radius": flight_dict.get(radius_column) if radius_column else None,
            "takeoff_point": takeoff_point,
            "landing_point": landing_point if not points_match else None,
            "flight_time": {
                "departure_time": departure_time,
                "arrival_time": arrival_time,
                "duration_minutes": parse_flight_duration(departure_time, arrival_time)
            },
            "registration_number": flight_dict.get("reg"),
            "date_of_flight": flight_dict.get("dof"),
            "operator": flight_dict.get("opr"),
            "additional_info": {
                "flight_level": flight_dict.get("flight_level"),
                "aircraft_type": flight_dict.get("typ"),
                "remarks": flight_dict.get("rmk")
            }
        }
        
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка в /flights/{flight_id}/flight_zone: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при получении данных о зоне полета: {str(e)}")
        
@app.get("/stats/regions/monthly")
async def get_regions_monthly_stats(db: Session = Depends(get_db)):
    """Возвращает количество полетов для каждого региона по месяцам"""
    try:
        # Ищем колонку с датой полета
        date_column = _find_column_case_insensitive(db, TARGET_TABLE, [
            "dof", "DOF", "date_of_flight", "date", "дата"
        ])
        
        # Ищем колонку с регионом (центром ЕС ОРВД)
        region_column = _find_column_case_insensitive(db, TARGET_TABLE, [
            "tsentr_es_orvd", "TSENTR_ES_ORVD", "центр", "center", "region"
        ])

        if not date_column or not region_column:
            raise HTTPException(
                status_code=400,
                detail="Не найдены необходимые колонки для анализа (дата и регион)"
            )

        # Запрос для группировки по регионам и месяцам
        query = text(f"""
            SELECT 
                "{region_column}" as region,
                EXTRACT(YEAR FROM TO_DATE("{date_column}", 'DDMMYY')) as year,
                EXTRACT(MONTH FROM TO_DATE("{date_column}", 'DDMMYY')) as month,
                COUNT(*) as flight_count
            FROM {TARGET_TABLE}
            WHERE "{date_column}" IS NOT NULL 
            AND "{date_column}" != ''
            AND "{region_column}" IS NOT NULL
            AND "{region_column}" != ''
            GROUP BY "{region_column}", year, month
            ORDER BY "{region_column}", year, month
        """)

        result = db.execute(query)
        stats_data = result.fetchall()

        # Словарь названий месяцев
        month_names = {
            1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
            5: "Май", 6: "Июнь", 7: "Июль", 8: "Август", 
            9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
        }

        # Форматируем данные в удобную структуру
        formatted_data = {}
        
        for row in stats_data:
            region = row[0]
            year = int(row[1]) if row[1] else None
            month = int(row[2]) if row[2] else None
            count = row[3]
            
            if region and year and month:
                if region not in formatted_data:
                    formatted_data[region] = {}
                
                year_key = str(year)
                if year_key not in formatted_data[region]:
                    formatted_data[region][year_key] = {}
                
                month_name = month_names.get(month, f"Месяц {month}")
                formatted_data[region][year_key][month_name] = count

        # Добавляем все месяцы с нулевыми значениями для полноты данных
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        for region in formatted_data:
            for year in formatted_data[region]:
                year_int = int(year)
                # Для текущего года показываем только прошедшие месяцы
                # Для прошлых лет показываем все 12 месяцев
                max_month = current_month if year_int == current_year else 12
                
                for month_num in range(1, max_month + 1):
                    month_name = month_names[month_num]
                    if month_name not in formatted_data[region][year]:
                        formatted_data[region][year][month_name] = 0

        return {
            "stats": formatted_data,
            "total_regions": len(formatted_data),
            "columns_used": {
                "date_column": date_column,
                "region_column": region_column
            }
        }

    except Exception as e:
        logger.error(f"Ошибка в /stats/regions/monthly: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при получении статистики по регионам: {str(e)}")        

@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...), db: Session = Depends(get_db)):
    try:
        logger.info(f"Начало загрузки файла: {file.filename}")
        
        # Сохраняем файл временно
        contents = await file.read()
        temp_filename = f"temp_{file.filename}"
        with open(temp_filename, "wb") as f:
            f.write(contents)
        logger.info(f"Файл сохранен как: {temp_filename}")

        # Парсим Excel
        excel_parser = ExcelParser()
        excel_parser.excel_file_path = temp_filename
        all_sheets = excel_parser.read_all_excel_sheets()
        logger.info(f"Прочитано листов: {len(all_sheets)}")

        # Обрабатываем и загружаем данные используя существующую сессию БД
        data_processor = DataProcessor(db_session=db)
        
        total_records = 0
        for sheet_name, df in all_sheets.items():
            logger.info(f"Обработка листа: {sheet_name}, строк: {len(df)}")
            
            # Очищаем данные - используем статический метод ПРАВИЛЬНО
            df_cleaned = DataProcessor.clean_dataframe(df)  # Просто вызываем как статический метод
            logger.info(f"После очистки: {len(df_cleaned)} строк")
            
            if not df_cleaned.empty:
                # Дешифруем поля плана полета
                df_decoded = data_processor.decode_flight_plan_fields(df_cleaned)
                logger.info(f"После декодирования: {len(df_decoded)} строк")
                
                # Сохраняем в таблицу
                result = data_processor.save_to_table_with_id(df_decoded, TARGET_TABLE)
                total_records += result.get("added", 0)
                logger.info(f"Сохранено в базу: {result.get('added', 0)} записей")
            else:
                logger.warning(f"Лист {sheet_name} пуст после очистки")

        # Удаляем временный файл
        os.remove(temp_filename)
        logger.info(f"Временный файл удален")

        return {
            "message": f"Успешно загружено {len(all_sheets)} листов, {total_records} записей в {TARGET_TABLE}",
            "sheets_processed": len(all_sheets),
            "records_added": total_records
        }
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке файла: {e}", exc_info=True)
        # Убедимся, что временный файл удален даже при ошибке
        try:
            if 'temp_filename' in locals():
                os.remove(temp_filename)
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Ошибка при загрузке: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)

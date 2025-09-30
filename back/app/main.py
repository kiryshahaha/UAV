from fastapi import FastAPI, HTTPException, Depends, Query, BackgroundTasks, UploadFile, File, Path
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func, text, distinct
from sqlalchemy.orm import Session
from typing import Optional, List, Dict, Any, Union, Tuple
from datetime import datetime
import os
import sys
import logging
import shutil
from pathlib import Path
import pandas as pd
from pydantic import BaseModel
import re


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Добавляем текущую директорию в путь для импортов
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# Импортируем из наших модулей
from database import engine, SessionLocal, get_db

def parse_time(time_str: str) -> Optional[datetime]:
    """Преобразует строку 'HH:MM:SS' в datetime.time, игнорирует некорректные значения"""
    try:
        if not time_str or time_str.upper() == "ZZ:ZZ:00":
            return None
        return datetime.strptime(time_str, "%H:%M:%S")
    except Exception:
        return None

def parse_coord(coord_str: str) -> Tuple[Optional[float], Optional[float]]:
    """Парсит координаты из строки"""
    if not coord_str:
        return None, None
    match = re.match(r"(\d{2,3})(\d{2})([NS])(\d{3})(\d{2})([EW])", coord_str.strip())
    if not match:
        return None, None

    lat_deg = int(match.group(1))
    lat_min = int(match.group(2))
    lat_sign = 1 if match.group(3) == "N" else -1
    lat = lat_sign * (lat_deg + lat_min / 60.0)

    lon_deg = int(match.group(4))
    lon_min = int(match.group(5))
    lon_sign = 1 if match.group(6) == "E" else -1
    lon = lon_sign * (lon_deg + lon_min / 60.0)

    return round(lat, 6), round(lon, 6)

def convert_coord(coord: str) -> Dict[str, Optional[float]]:
    """
    Конвертирует координаты из формата '5957N02905E' в {latitude, longitude}
    """
    try:
        # Разбираем широту
        lat_deg = int(coord[0:2])
        lat_min = int(coord[2:4])
        lat_dir = coord[4].upper()
        latitude = lat_deg + lat_min / 60
        if lat_dir == 'S':
            latitude = -latitude

        # Разбираем долготу
        lon_deg = int(coord[5:8])
        lon_min = int(coord[8:10])
        lon_dir = coord[10].upper()
        longitude = lon_deg + lon_min / 60
        if lon_dir == 'W':
            longitude = -longitude

        return {"latitude": latitude, "longitude": longitude}
    except Exception:
        return {"latitude": None, "longitude": None}

def parse_flight_duration(dep: str, arr: str) -> Optional[float]:
    """
    Преобразует время отправления и прибытия в длительность в минутах.
    Возвращает None, если данные некорректные.
    """
    try:
        dep_time = datetime.strptime(dep, "%H:%M:%S")
        arr_time = datetime.strptime(arr, "%H:%M:%S")
        duration = (arr_time - dep_time).total_seconds() / 60
        # Если рейс пересекает полночь, корректируем
        if duration < 0:
            duration += 24 * 60
        return duration
    except Exception:
        return None
# Константа с именем целевой таблицы
TARGET_TABLE = "excel_data_result_1"

# Настройка пути для загрузки файлов
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

# Модели для валидации данных
class RegionCreate(BaseModel):
    name: str
    description: Optional[str] = None

class FileUploadResponse(BaseModel):
    status: str
    message: str
    filename: str
    file_path: str
    records_processed: Optional[int] = None

# Создание приложения
app = FastAPI(title="БВС API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def clean_column_name(col):
    """Очистка названий колонок"""
    if pd.isna(col):
        return "unknown"
    col = str(col)
    # Заменяем кириллицу на латиницу
    cyrillic_to_latin = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
        'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
        'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ё': 'YO',
        'Ж': 'ZH', 'З': 'Z', 'И': 'I', 'Й': 'Y', 'К': 'K', 'Л': 'L', 'М': 'M',
        'Н': 'N', 'О': 'O', 'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'U',
        'Ф': 'F', 'Х': 'H', 'Ц': 'TS', 'Ч': 'CH', 'Ш': 'SH', 'Щ': 'SCH',
        'Ъ': '', 'Ы': 'Y', 'Ь': '', 'Э': 'E', 'Ю': 'YU', 'Я': 'YA'
    }
    
    for cyr, lat in cyrillic_to_latin.items():
        col = col.replace(cyr, lat)
    
    col = col.lower()
    col = re.sub(r'[\s\-\.\/\\]+', '_', col)
    col = re.sub(r'[^a-z0-9_]', '', col)
    col = re.sub(r'_+', '_', col)
    col = col.strip('_')
    
    if not col or col[0].isdigit():
        col = f'col_{hash(col) % 10000}'
    
    return col


def process_uploaded_excel_simple(file_path: str, db: Session):
    """Обработка загруженного Excel файла"""
    try:
        logger.info(f"🔄 Начата обработка файла: {file_path}")
        
        # Импортируем processor
        from excel_processor import process_excel_with_external_parser
        
        # Запускаем парсер
        success = process_excel_with_external_parser(file_path)
        
        if success:
            # Получаем количество записей
            record_count = db.execute(text("SELECT COUNT(*) FROM excel_data_result_1")).scalar()
            logger.info(f"✅ Обработано записей: {record_count}")
            return record_count or 0
        else:
            logger.error("❌ Парсинг завершился с ошибкой")
            return 0
            
    except Exception as e:
        logger.error(f"❌ Ошибка при обработке Excel: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return 0
    
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
    """Возвращает словарь с именами требуемых колонок с fallback логикой"""
    columns_mapping = {
        "reg": ["reg", "REG", "registration", "регистрация", "борт", "бортовой номер"],
        "opr": ["opr", "OPR", "operator", "оператор", "авиакомпания", "эксплуатант"],
        "typ": ["typ", "TYP", "type", "тип", "тип всу", "воздушное судно"],
        "dep": ["dep", "DEP", "departure", "вылет", "аэропорт вылета", "откуда"],
        "dest": ["dest", "DEST", "destination", "назначение", "аэропорт назначения", "куда"],
        "flight_zone_radius": ["flight_zone_radius", "FLIGHT_ZONE_RADIUS", "radius", "радиус", "зона полета"],
        "flight_level": ["flight_level", "FLIGHT_LEVEL", "level", "уровень", "эшелон"],
        "departure_time": ["departure_time", "DEPARTURE_TIME", "departure", "время_вылета", "dep", "вылет", "время вылета"],
        "arrival_time": ["arrival_time", "ARRIVAL_TIME", "arrival", "время_прибытия", "arr", "прибытие", "время прибытия"],
        "tsentr_es_orvd": ["tsentr_es_orvd", "TSENTR_ES_ORVD", "центр", "center", "регион", "орвд"]
    }

    result = {}
    
    # Получаем все существующие колонки в таблице
    try:
        existing_columns_result = db.execute(text(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{TARGET_TABLE}' 
            AND table_schema = 'public'
        """))
        existing_columns = [row[0] for row in existing_columns_result.fetchall()]
        
        logger.info(f"Найдены колонки в таблице: {existing_columns}")
        
        # Ищем соответствия для каждой требуемой колонки
        for key, variants in columns_mapping.items():
            column_found = None
            for variant in variants:
                # Ищем точное совпадение (регистронезависимо)
                for existing_col in existing_columns:
                    if existing_col.lower() == variant.lower():
                        column_found = existing_col
                        break
                if column_found:
                    break
            
            if column_found:
                result[key] = column_found
                logger.info(f"✅ Найдена колонка для '{key}': {column_found}")
            else:
                logger.warning(f"❌ Колонка '{key}' не найдена среди вариантов: {variants}")
                
    except Exception as e:
        logger.error(f"Ошибка при получении колонок таблицы: {e}")
    
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
            logger.info(f"ℹ️ Таблица {TARGET_TABLE} не найдена. Загрузите Excel файл через /api/upload-excel")
    except Exception as e:
        logger.error(f"⚠️ Ошибка при запуске: {e}")
    finally:
        db.close()

# ЭНДПОИНТ ДЛЯ ЗАГРУЗКИ ФАЙЛОВ
@app.post("/api/upload-excel", response_model=FileUploadResponse)
async def upload_excel_file(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Эндпоинт для загрузки Excel файлов"""
    try:
        # Проверяем расширение файла
        if file.filename and not file.filename.lower().endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=400, 
                detail="Разрешены только файлы Excel (.xlsx, .xls)"
            )

        # Создаем папку если не существует
        UPLOAD_DIR.mkdir(exist_ok=True)
        
        # Сохраняем файл с абсолютным путем
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_filename = file.filename or f"unknown_{timestamp}.xlsx"
        file_path = UPLOAD_DIR / f"uploaded_{timestamp}_{safe_filename}"
        
        logger.info(f"💾 Сохранение файла в: {file_path.absolute()}")
        
        # Сохраняем файл
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Проверяем что файл сохранился
        if not file_path.exists():
            raise HTTPException(status_code=500, detail="Не удалось сохранить файл")
            
        file_size = file_path.stat().st_size
        logger.info(f"📁 Файл сохранен: {file_path} ({file_size} bytes)")

        # Запускаем обработку с АБСОЛЮТНЫМ путем
        absolute_file_path = str(file_path.absolute())
        records_processed = process_uploaded_excel_simple(absolute_file_path, db)

        return FileUploadResponse(
            status="success",
            message=f"Файл успешно загружен и обработан. Записей: {records_processed}",
            filename=safe_filename,
            file_path=absolute_file_path,
            records_processed=records_processed
        )

    except Exception as e:
        logger.error(f"❌ Ошибка при загрузке файла: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки: {str(e)}")

@app.get("/api/table-structure")
async def get_table_structure(db: Session = Depends(get_db)):
    """Возвращает полную структуру таблицы для отладки"""
    try:
        result = db.execute(text(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = '{TARGET_TABLE}' 
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """))
        
        columns = []
        for row in result:
            columns.append({
                "name": row[0],
                "type": row[1],
                "nullable": row[2]
            })
        
        # Также получаем несколько записей для примера
        sample_data = db.execute(text(f"SELECT * FROM {TARGET_TABLE} LIMIT 5")).fetchall()
        sample_columns = db.execute(text(f"SELECT * FROM {TARGET_TABLE} LIMIT 1")).keys()
        
        sample_records = []
        for row in sample_data:
            sample_records.append(dict(zip(sample_columns, row)))
        
        return {
            "table_name": TARGET_TABLE,
            "columns": columns,
            "sample_data": sample_records,
            "total_columns": len(columns)
        }
        
    except Exception as e:
        logger.error(f"Ошибка при получении структуры таблицы: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка: {str(e)}")

# ОСНОВНЫЕ ЭНДПОИНТЫ
@app.get("/")
async def get_main_data(db: Session = Depends(get_db)):
    """Главная страница - возвращает все строки с основными полями"""
    try:
        # Получаем имена нужных колонок
        columns = _get_required_columns(db)
        
        # Если не все колонки найдены, используем доступные
        available_columns = {k: v for k, v in columns.items() if v}
        
        if not available_columns:
            # Если вообще нет колонок, возвращаем все что есть
            logger.warning("Не найдены стандартные колонки, возвращаем все данные")
            result = _execute_safe_query(db, f"SELECT * FROM {TARGET_TABLE} LIMIT 100")
            data = [dict(row) for row in result.mappings().all()]
            
            # Получаем названия всех колонок
            all_columns_result = db.execute(text(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{TARGET_TABLE}' 
                ORDER BY ordinal_position
            """))
            all_columns = [row[0] for row in all_columns_result.fetchall()]
            
            return {
                "data": data,
                "count": len(data),
                "columns": all_columns,
                "warning": "Используются все доступные колонки (стандартные не найдены)"
            }

        # Формируем запрос только с найденными колонками
        select_columns = []
        for key, column_name in available_columns.items():
            select_columns.append(f'"{column_name}" as {key}')

        query = f"SELECT {', '.join(select_columns)} FROM {TARGET_TABLE}"
        result = _execute_safe_query(db, query)

        data = [dict(row) for row in result.mappings().all()]

        return {
            "data": data,
            "count": len(data),
            "columns": list(available_columns.keys()),
            "available_columns": available_columns
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
        total_count = total_count_result.scalar() or 0

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
    city_name: str,  # Убрал = Path(...)
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
# ЭНДПОИНТЫ С ПАРСИНГОМ КООРДИНАТ

@app.get("/stats/regions", response_model=List[Dict])
def get_stats_regions(db: Session = Depends(get_db)):
    """Статистика по регионам: количество рейсов и средняя длительность"""
    try:
        # Ищем колонку с центром ЕС ОРВД
        center_column = _find_column_case_insensitive(db, TARGET_TABLE, [
            "tsentr_es_orvd", "TSENTR_ES_ORVD", "центр", "center"
        ])
        
        # Ищем колонки времени
        time_columns = _get_required_columns(db)
        departure_time_col = time_columns.get("departure_time")
        arrival_time_col = time_columns.get("arrival_time")
        
        if not center_column:
            raise HTTPException(
                status_code=400,
                detail="Не найдена колонка с центром ЕС ОРВД"
            )

        # Если есть колонки времени, считаем статистику с длительностью
        if departure_time_col and arrival_time_col:
            query = text(f"""
                SELECT {center_column}, {departure_time_col}, {arrival_time_col} 
                FROM {TARGET_TABLE} 
                WHERE {departure_time_col} IS NOT NULL AND {arrival_time_col} IS NOT NULL
            """)
            result = db.execute(query).mappings().all()

            stats = {}
            for row in result:
                region = row[center_column]
                dep_time = row[departure_time_col]
                arr_time = row[arrival_time_col]

                if not region:
                    continue

                # Парсим время и вычисляем длительность
                dep = parse_time(str(dep_time)) if dep_time else None
                arr = parse_time(str(arr_time)) if arr_time else None
                
                duration = None
                if dep and arr:
                    duration = parse_flight_duration(
                        dep.strftime("%H:%M:%S") if hasattr(dep, 'strftime') else str(dep),
                        arr.strftime("%H:%M:%S") if hasattr(arr, 'strftime') else str(arr)
                    )

                if region not in stats:
                    stats[region] = {"num_flights": 0, "total_duration": 0, "flights_with_duration": 0}

                stats[region]["num_flights"] += 1
                if duration is not None:
                    stats[region]["total_duration"] += duration
                    stats[region]["flights_with_duration"] += 1

            result_list = []
            for region, data in stats.items():
                avg_duration = 0
                if data["flights_with_duration"] > 0:
                    avg_duration = data["total_duration"] / data["flights_with_duration"]
                
                result_list.append({
                    "region": region,
                    "num_flights": data["num_flights"],
                    "avg_flight_duration": round(avg_duration, 2),
                    "flights_with_duration_data": data["flights_with_duration"]
                })
        else:
            # Если нет колонок времени, считаем только количество рейсов
            query = text(f"SELECT {center_column}, COUNT(*) as flight_count FROM {TARGET_TABLE} GROUP BY {center_column}")
            result = db.execute(query).mappings().all()

            result_list = []
            for row in result:
                region = row[center_column]
                if region:
                    result_list.append({
                        "region": region,
                        "num_flights": row["flight_count"],
                        "avg_flight_duration": 0,
                        "flights_with_duration_data": 0
                    })

        return result_list

    except Exception as e:
        logger.error(f"Ошибка при подсчете статистики регионов: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при подсчете статистики: {e}")

@app.get("/stats/region/{region_name}")
def region_stats(region_name: str, db: Session = Depends(get_db)):
    """Детальная статистика по конкретному региону"""
    try:
        # Ищем колонку с центром ЕС ОРВД
        center_column = _find_column_case_insensitive(db, TARGET_TABLE, [
            "tsentr_es_orvd", "TSENTR_ES_ORVD", "центр", "center"
        ])
        
        # Ищем колонки времени
        time_columns = _get_required_columns(db)
        departure_time_col = time_columns.get("departure_time")
        arrival_time_col = time_columns.get("arrival_time")
        
        if not center_column:
            raise HTTPException(
                status_code=400,
                detail="Не найдена колонка с центром ЕС ОРВД"
            )

        # Проверяем существование региона
        region_exists = db.execute(text(f"""
            SELECT EXISTS (
                SELECT 1 FROM {TARGET_TABLE} WHERE {center_column} = :region
            )
        """), {"region": region_name}).scalar()

        if not region_exists:
            raise HTTPException(status_code=404, detail="Регион не найден")

        # Подсчитываем общее количество рейсов
        total_query = text(f"""
            SELECT COUNT(*) as flight_count
            FROM {TARGET_TABLE}
            WHERE {center_column} = :region
        """)
        total_result = db.execute(total_query, {"region": region_name}).fetchone()
        total_flights = total_result[0] if total_result else 0

        # Если есть колонки времени, вычисляем среднюю длительность
        avg_duration = 0
        if departure_time_col and arrival_time_col:
            time_query = text(f"""
                SELECT {departure_time_col}, {arrival_time_col}
                FROM {TARGET_TABLE}
                WHERE {center_column} = :region 
                AND {departure_time_col} IS NOT NULL 
                AND {arrival_time_col} IS NOT NULL
            """)
            time_result = db.execute(time_query, {"region": region_name}).fetchall()

            durations = []
            for row in time_result:
                dep_time = row[0]
                arr_time = row[1]
                
                if dep_time and arr_time:
                    duration = parse_flight_duration(
                        str(dep_time), 
                        str(arr_time)
                    )
                    if duration is not None:
                        durations.append(duration)

            if durations:
                avg_duration = sum(durations) / len(durations)

        return {
            "region": region_name,
            "total_flights": total_flights,
            "average_duration_minutes": round(avg_duration, 2),
            "flights_with_duration_data": len(durations) if 'durations' in locals() else 0
        }

    except Exception as e:
        logger.error(f"Ошибка при подсчете статистики для региона {region_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при подсчете статистики: {e}")

@app.get("/flights/points", response_model=List[Dict])
def get_flight_points(db: Session = Depends(get_db)):
    """Возвращает список точек взлета для всех рейсов: id + координаты"""
    try:
        # Ищем колонку с координатами вылета
        dep_column = _find_column_case_insensitive(db, TARGET_TABLE, [
            "dep_1", "DEP_1", "dep", "DEP", "departure_coords", "координаты_вылета"
        ])
        
        if not dep_column:
            logger.warning("Не найдена колонка с координатами вылета, возвращаем пустой список")
            return []

        query = text(f"SELECT id, {dep_column} FROM {TARGET_TABLE} WHERE {dep_column} IS NOT NULL")
        result = db.execute(query)
        points = []

        for row in result.fetchall():
            coord_str = str(row[1]) if row[1] else ""
            if coord_str:
                # Используем convert_coord для парсинга координат
                coords = convert_coord(coord_str)
                if coords["latitude"] is not None and coords["longitude"] is not None:
                    points.append({
                        "id": row[0],
                        "latitude": coords["latitude"],
                        "longitude": coords["longitude"],
                        "raw_coords": coord_str,
                        "parsed_method": "convert_coord"
                    })
                else:
                    # Альтернативный парсинг через parse_coord
                    lat, lon = parse_coord(coord_str)
                    if lat is not None and lon is not None:
                        points.append({
                            "id": row[0],
                            "latitude": lat,
                            "longitude": lon,
                            "raw_coords": coord_str,
                            "parsed_method": "parse_coord"
                        })
        
        logger.info(f"Найдено точек с координатами: {len(points)}")
        return points

    except Exception as e:
        logger.error(f"Ошибка при получении точек полетов: {e}")
        return []

@app.get("/flights/{flight_id}")
def get_flight(flight_id: int, db: Session = Depends(get_db)):
    """Детальная информация о конкретном рейсе"""
    try:
        result = db.execute(text(f"SELECT * FROM {TARGET_TABLE} WHERE id = :fid"), {"fid": flight_id})
        row = result.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Flight not found")

        colnames = result.keys()
        record = dict(zip(colnames, row))

        # Парсим координаты вылета
        dep_coords_raw = record.get("dep_1") or record.get("dep")
        dep_lat, dep_lon = parse_coord(str(dep_coords_raw)) if dep_coords_raw else (None, None)

        # Парсим координаты назначения
        dest_coords_raw = record.get("dest") or record.get("arr")
        arr_lat, arr_lon = parse_coord(str(dest_coords_raw)) if dest_coords_raw else (None, None)

        # Парсим время полета если есть данные
        dep_time = record.get("dep")
        arr_time = record.get("arr")
        flight_duration = None
        if dep_time and arr_time:
            flight_duration = parse_flight_duration(str(dep_time), str(arr_time))

        return {
            "id": record["id"],
            "region": record.get("tsentr_es_orvd"),
            "departure": {
                "raw": dep_coords_raw,
                "lat": dep_lat,
                "lon": dep_lon,
                "time": record.get("dep"),
                "parsed_success": dep_lat is not None and dep_lon is not None
            },
            "arrival": {
                "raw": dest_coords_raw,
                "lat": arr_lat,
                "lon": arr_lon,
                "time": record.get("arr"),
                "parsed_success": arr_lat is not None and arr_lon is not None
            },
            "flight_duration_minutes": flight_duration,
            "type": record.get("shr"),
            "reg_number": record.get("tsentr_es_orvd"),
            "operator": "Не указан",
            "remarks": record.get("source_sheet"),
            "flight_level": "Не указан",
            "flight_zone": "Не указан", 
            "flight_zone_radius": "Не указан",
            "dof": "Не указан",
            "sts": "Не указан",
            "source_sheet": record.get("source_sheet")
        }

    except Exception as e:
        logger.error(f"Ошибка при получении рейса {flight_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при получении данных рейса: {e}")

@app.get("/flights/{flight_id}/detailed")
def get_flight_detailed(flight_id: int, db: Session = Depends(get_db)):
    """Расширенная информация о рейсе с дополнительными данными"""
    try:
        result = db.execute(text(f"SELECT * FROM {TARGET_TABLE} WHERE id = :fid"), {"fid": flight_id})
        row = result.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Flight not found")

        colnames = result.keys()
        record = dict(zip(colnames, row))

        # Парсим координаты
        dep_coords_raw = record.get("dep_1") or record.get("dep")
        dest_coords_raw = record.get("dest") or record.get("arr")
        
        dep_lat, dep_lon = parse_coord(str(dep_coords_raw)) if dep_coords_raw else (None, None)
        arr_lat, arr_lon = parse_coord(str(dest_coords_raw)) if dest_coords_raw else (None, None)

        # Парсим время полета
        dep_time = record.get("dep")
        arr_time = record.get("arr")
        flight_duration = None
        if dep_time and arr_time:
            flight_duration = parse_flight_duration(str(dep_time), str(arr_time))

        return {
            "flight_info": {
                "id": record["id"],
                "registration": record.get("tsentr_es_orvd"),
                "operator": "Не указан",
                "type": record.get("shr"),
                "region": record.get("tsentr_es_orvd"),
                "source_sheet": record.get("source_sheet")
            },
            "departure": {
                "raw_coordinates": dep_coords_raw,
                "latitude": dep_lat,
                "longitude": dep_lon,
                "time": record.get("dep"),
                "parsed_success": dep_lat is not None and dep_lon is not None
            },
            "arrival": {
                "raw_coordinates": dest_coords_raw,
                "latitude": arr_lat,
                "longitude": arr_lon,
                "time": record.get("arr"),
                "parsed_success": arr_lat is not None and arr_lon is not None
            },
            "flight_characteristics": {
                "flight_level": "Не указан",
                "flight_zone": "Не указан",
                "flight_zone_radius": "Не указан",
                "duration_minutes": flight_duration
            },
            "additional_info": {
                "remarks": record.get("source_sheet"),
                "dof": "Не указан",
                "sts": "Не указан"
            }
        }

    except Exception as e:
        logger.error(f"Ошибка при получении детальной информации о рейсе {flight_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при получении данных рейса: {e}")
# АДМИНСКИЕ ЭНДПОИНТЫ
@app.post("/admin/regions")
async def add_region( 
    region: RegionCreate,
    db: Session = Depends(get_db)
):
    """Добавляет новый регион"""
    try:
        regions_table = "regions"

        table_exists = db.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = :table_name
            )
        """), {"table_name": regions_table}).scalar()

        if not table_exists:
            raise HTTPException(status_code=404, detail=f"Таблица {regions_table} не найдена")

        insert_query = f"""
            INSERT INTO {regions_table} (name, description)
            VALUES (:name, :description)
            RETURNING id
        """

        result = db.execute(text(insert_query), {
            "name": region.name,
            "description": region.description
        })

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
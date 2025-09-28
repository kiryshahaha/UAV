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
from parsers import parse_coord, convert_coord, parse_flight_duration, parse_time


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
        if not all(columns.values()):
            missing = [k for k, v in columns.items() if not v]
            raise HTTPException(status_code=400, detail=f"Отсутствуют обязательные колонки: {missing}")

        # Формируем запрос с нужными колонками
        select_columns = [
            f'"{columns["reg"]}" as reg',
            f'"{columns["opr"]}" as opr',
            f'"{columns["typ"]}" as typ',
            f'"{columns["dep"]}" as dep',
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
def fetch_flights() -> List[FlightInfo]:
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    query = """
        SELECT reg, opr, dof, dep_1, dest, departure_time, arrival_time, flight_zone, flight_zone_radius, rmk
        FROM your_table_name
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()

    flights: List[FlightInfo] = []
    for row in rows:
        reg, opr, dof, dep_1, dest, departure_time, arrival_time, flight_zone, flight_zone_radius, rmk = row
        # Если точка взлета и посадки совпадает, arrival_point = None
        arrival_point = dest if dest and dest != dep_1 else None

        flights.append(FlightInfo(
            reg_number=reg or "",
            operator=opr or "",
            flight_date=dof or "",
            departure_point=dep_1 or "",
            arrival_point=arrival_point,
            departure_time=departure_time or "",
            arrival_time=arrival_time or "",
            flight_zone=flight_zone,
            flight_zone_radius=flight_zone_radius,
            extra_info=rmk
        ))
    return flights

@app.get("/flights/zones", response_model=List[Dict])
def get_flight_zones(db: Session = Depends(get_db)):
    """
    Возвращает данные о зоне полета дронов.
    Если точка взлета не совпадает с точкой приземления, отображается точка приземления.
    Возвращает: координаты, длительность полета, рег.номер, дата, оператор, доп.информация.
    """
    try:
        query = text("""
            SELECT takeoff_coord, landing_coord, departure_time, arrival_time,
                   reg_number, flight_date, operator, additional_info
            FROM excel_data_result_1
        """)
        result = db.execute(query).mappings().all()

        flights = []
        for row in result:
            dep_coord = convert_coord(row["takeoff_coord"])
            arr_coord = convert_coord(row["landing_coord"])

            # Используем точку приземления, если она отличается от взлета
            zone_coord = arr_coord if dep_coord != arr_coord else dep_coord

            duration = parse_flight_duration(row["departure_time"], row["arrival_time"])

            flights.append({
                "zone": zone_coord,
                "flight_duration_minutes": duration,
                "reg_number": row.get("reg_number"),
                "date": row.get("flight_date"),
                "operator": row.get("operator"),
                "additional_info": row.get("additional_info")
            })

        return flights

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении зон полета: {e}")
    

@app.get("/flight-zone", response_model=List[DroneFlight])
def get_flight_zone():
    result = []
    for f in flights_data:
        zone = f["landing"] if f["takeoff"] != f["landing"] else f["takeoff"]
        result.append({
            "flight_zone": zone,
            "flight_time": f["flight_time"],
            "reg_number": f["reg_number"],
            "date": f["date"],
            "operator": f["operator"],
            "additional_info": f.get("additional_info")
        })
    return result

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
def get_flight(flight_id: int, db: Session = Depends(get_db)):
    result = db.execute(text("SELECT * FROM excel_data_result_1 WHERE id = :fid"), {"fid": flight_id})
    row = result.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Flight not found")

    colnames = result.keys()
    record = dict(zip(colnames, row))

    dep_lat, dep_lon = parse_coord(record.get("dep_1", ""))
    arr_lat, arr_lon = parse_coord(record.get("dest", ""))

    return {
        "id": record["id"],
        "region": record.get("tsentr_es_orvd"),
        "departure": {
            "raw": record.get("dep_1"),
            "lat": dep_lat,
            "lon": dep_lon,
            "time": record.get("departure_time"),
        },
        "arrival": {
            "raw": record.get("dest"),
            "lat": arr_lat,
            "lon": arr_lon,
            "time": record.get("arrival_time"),
        },
        "type": record.get("typ"),
        "reg_number": record.get("reg"),
        "operator": record.get("opr"),
        "remarks": record.get("rmk"),
        "flight_level": record.get("flight_level"),
        "flight_zone": record.get("flight_zone"),
        "flight_zone_radius": record.get("flight_zone_radius"),
        "dof": record.get("dof"),
        "sts": record.get("sts"),
        "source_sheet": record.get("source_sheet")
    }



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)

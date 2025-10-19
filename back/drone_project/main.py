from fastapi import FastAPI
from pydantic import BaseModel
import geopandas as gpd
from shapely.geometry import Point
import psycopg2
from datetime import datetime
import os
from pathlib import Path

# Настройка FastAPI и shapefile
app = FastAPI(title="BPLA Region Tracker")

# Получаем абсолютный путь к директории, где находится main.py
current_dir = Path(__file__).parent

# Загрузка shapefile с обработкой ошибок
try:
    shapefile_path = current_dir / "RF" / "RF.shp"
    gdf = gpd.read_file(str(shapefile_path))
    gdf = gdf.to_crs(epsg=4326)
    print(f"✅ Shapefile загружен: {shapefile_path}")
except Exception as e:
    print(f"Ошибка загрузки shapefile: {e}")
    gdf = None

# Настройка PostgreSQL с обработкой ошибок
try:
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="postgres",
        port="5432"
    )
    cursor = conn.cursor()
    print("✅ Подключение к PostgreSQL успешно")
except Exception as e:
    print(f"❌ Ошибка подключения к PostgreSQL: {e}")
    conn = None
    cursor = None

# Модель данных для POST /drone/flight
class Flight(BaseModel):
    drone_id: str
    lat: float
    lon: float
    timestamp: str  # ISO формат

# Эндпоинт 1: определить регион по координатам
@app.get("/region")
def get_region(lat: float, lon: float):
    try:
        if gdf is None:
            return {"error": "Shapefile не загружен"}
        
        point = Point(lon, lat)
        row = gdf[gdf.geometry.intersects(point)]
        if not row.empty:
            return {
                "region_ru": row.iloc[0]["name_ru"],
                "region_en": row.iloc[0]["name_en"],
                "admin_level": row.iloc[0].get("admin_level", row.iloc[0].get("admin_leve", None))
            }
        return {"region_ru": None, "region_en": None, "admin_level": None}
    except Exception as e:
        return {"error": str(e)}

# Эндпоинт 2: регистрация вылета дрона
@app.post("/drone/flight")
def add_flight(flight: Flight):
    if conn is None or cursor is None:
        return {"error": "Нет подключения к базе данных"}
    
    try:
        region_ru = region_en = admin_level = None
        
        if gdf is not None:
            point = Point(flight.lon, flight.lat)
            row = gdf[gdf.geometry.intersects(point)]
            if not row.empty:
                region_ru = row.iloc[0]["name_ru"]
                region_en = row.iloc[0]["name_en"]
                admin_level = row.iloc[0].get("admin_level", row.iloc[0].get("admin_leve", None))

        # Сохраняем в PostgreSQL
        cursor.execute("""
            INSERT INTO flights (drone_id, lat, lon, region_ru, region_en, admin_level, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (flight.drone_id, flight.lat, flight.lon, region_ru, region_en, admin_level, flight.timestamp))
        conn.commit()
        
        return {"status": "success", "region_ru": region_ru, "region_en": region_en}
    except Exception as e:
        conn.rollback()
        return {"error": str(e)}

# Эндпоинт 3: статистика по регионам
@app.get("/stats/regions")
def stats_regions():
    if cursor is None:
        return {"error": "Нет подключения к базе данных"}
    
    try:
        cursor.execute("""
            SELECT region_ru, region_en, COUNT(*) as flights
            FROM flights
            WHERE region_ru IS NOT NULL
            GROUP BY region_ru, region_en
            ORDER BY flights DESC
        """)
        result = cursor.fetchall()
        return [{"region_ru": r[0], "region_en": r[1], "flights": r[2]} for r in result]
    except Exception as e:
        return {"error": str(e)}

# Эндпоинт 4: полеты конкретного дрона
@app.get("/drone/{drone_id}/flights")
def drone_flights(drone_id: str):
    if cursor is None:
        return {"error": "Нет подключения к базе данных"}
    
    try:
        cursor.execute("""
            SELECT lat, lon, region_ru, timestamp
            FROM flights
            WHERE drone_id = %s
            ORDER BY timestamp
        """, (drone_id,))
        result = cursor.fetchall()
        return [{"lat": r[0], "lon": r[1], "region": r[2], "timestamp": r[3]} for r in result]
    except Exception as e:
        return {"error": str(e)}

# Корневой эндпоинт для проверки
@app.get("/")
def root():
    return {"message": "BPLA Region Tracker API работает!"}

# Закрытие подключения при завершении
@app.on_event("shutdown")
def shutdown_event():
    if conn:
        conn.close()
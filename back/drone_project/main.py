from fastapi import FastAPI
from pydantic import BaseModel
import geopandas as gpd
from shapely.geometry import Point
import psycopg2
from datetime import datetime

# Настройка FastAPI и shapefile
app = FastAPI(title="BPLA Region Tracker")

gdf = gpd.read_file("RF/RF.shp")
gdf = gdf.to_crs(epsg=4326)


# Настройка PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="postgres"
)
cursor = conn.cursor()


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
        point = Point(lon, lat)
        row = gdf[gdf.geometry.intersects(point)]
        if not row.empty:
            return {
                "region_ru": row.iloc[0]["name_ru"],
                "region_en": row.iloc[0]["name_en"],
                "admin_level": row.iloc[0]["admin_leve"]
            }
        return {"region_ru": None, "region_en": None, "admin_level": None}
    except Exception as e:
        return {"error": str(e)}


# Эндпоинт 2: регистрация вылета дрона
@app.post("/drone/flight")
def add_flight(flight: Flight):
    point = Point(flight.lon, flight.lat)
    row = gdf[gdf.geometry.intersects(point)]
    if not row.empty:
        region_ru = row.iloc[0]["name_ru"]
        region_en = row.iloc[0]["name_en"]
        admin_level = row.iloc[0]["admin_leve"]
    else:
        region_ru = region_en = admin_level = None

    # Сохраняем в PostgreSQL
    cursor.execute("""
        INSERT INTO flights (drone_id, lat, lon, region_ru, region_en, admin_level, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (flight.drone_id, flight.lat, flight.lon, region_ru, region_en, admin_level, flight.timestamp))
    conn.commit()
    return {"status": "success", "region_ru": region_ru, "region_en": region_en}


# Эндпоинт 3: статистика по регионам
@app.get("/stats/regions")
def stats_regions():
    cursor.execute("""
        SELECT region_ru, region_en, COUNT(*) as flights
        FROM flights
        WHERE region_ru IS NOT NULL
        GROUP BY region_ru, region_en
        ORDER BY flights DESC
    """)
    result = cursor.fetchall()
    return [{"region_ru": r[0], "region_en": r[1], "flights": r[2]} for r in result]


# Эндпоинт 4: полеты конкретного дрона
@app.get("/drone/{drone_id}/flights")
def drone_flights(drone_id: str):
    cursor.execute("""
        SELECT lat, lon, region_ru, timestamp
        FROM flights
        WHERE drone_id = %s
        ORDER BY timestamp
    """, (drone_id,))
    result = cursor.fetchall()
    return [{"lat": r[0], "lon": r[1], "region": r[2], "timestamp": r[3]} for r in result]



#print(gdf.columns)
#print(gdf.head())
#print(gdf.crs)


# parsers.py
import re
from datetime import datetime
from typing import Dict, Tuple, Optional

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
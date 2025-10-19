import re
from datetime import datetime, timedelta
from typing import Tuple, Optional, Dict

def parse_coord(coord_str: str) -> Tuple[Optional[float], Optional[float]]:
    """Парсинг координат из строки формата DDMMNDDDMME (например: 5957N02905E)"""
    try:
        if not coord_str:
            return None, None
        
        coord_str = coord_str.strip().upper()
        
        # Проверяем, соответствует ли строка шаблону
        match = re.fullmatch(r"(\d{2})(\d{2})([NS])(\d{3})(\d{2})([EW])", coord_str)
        if not match:
            return None, None
        
        lat_deg, lat_min, lat_dir, lon_deg, lon_min, lon_dir = match.groups()
        
        latitude = int(lat_deg) + int(lat_min) / 60.0
        if lat_dir == 'S':
            latitude = -latitude
        
        longitude = int(lon_deg) + int(lon_min) / 60.0
        if lon_dir == 'W':
            longitude = -longitude
        
        return round(latitude, 6), round(longitude, 6)
    except Exception:
        return None, None


def convert_coord(coord_str: str) -> Dict[str, Optional[float]]:
    """Конвертация координат в словарь"""
    lat, lon = parse_coord(coord_str)
    return {"latitude": lat, "longitude": lon}


def parse_time(time_str: str) -> Optional[datetime]:
    """Парсинг времени из строки"""
    if not time_str:
        return None
    time_str = time_str.strip()
    
    for fmt in ("%H:%M:%S", "%H:%M", "%H%M", "%H%M%S"):
        try:
            return datetime.strptime(time_str, fmt)
        except ValueError:
            continue
    return None


def parse_flight_duration(departure_time: str, arrival_time: str) -> Optional[float]:
    """Расчет длительности полета в минутах"""
    try:
        dep_time = parse_time(departure_time)
        arr_time = parse_time(arrival_time)
        if not dep_time or not arr_time:
            return None
        
        # Если прилёт раньше вылета — добавляем сутки
        if arr_time < dep_time:
            arr_time += timedelta(days=1)
        
        duration = (arr_time - dep_time).total_seconds() / 60.0
        return round(duration, 2)
    except Exception:
        return None



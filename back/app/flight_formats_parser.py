import re
from typing import Optional, Dict, Any, Tuple

def parse_radius(radius_str: str) -> Optional[int]:
    """
    Парсит радиус полета из различных форматов
    Возвращает радиус в метрах
    
    Основные форматы радиусов:
    - R0,5 = 0.5 км = 500 метров
    - R001 = 1000 метров (1 км)
    - R273 = 273 метра
    - R09032 = 9032 метра
    - R076285 = 76285 метров
    - R13 = 13 км = 13000 метров
    - R67 = 67 км = 67000 метров
    """
    if not radius_str or not isinstance(radius_str, str):
        return None
    
    # Убираем префикс R и пробелы
    clean_str = radius_str.replace('R', '').strip()
    
    # Формат с запятой (километры) - R0,5, R1,5 и т.д.
    if ',' in clean_str:
        try:
            km_value = float(clean_str.replace(',', '.'))
            return int(km_value * 1000)  # переводим в метры
        except:
            return None
    
    # Числовые форматы
    if clean_str.replace('.', '').isdigit():
        try:
            value = float(clean_str)
            # Эвристика: если число меньше 100, вероятно это км, иначе метры
            if value < 100:
                return int(value * 1000)  # км в метры
            else:
                return int(value)  # метры
        except:
            return None
    
    # Коды типа 001, 002 и т.д. (скорее всего метры)
    if clean_str.isdigit() and len(clean_str) <= 6:
        return int(clean_str)  # считаем что это метры
    
    return None

def parse_flight_level(level_str: str) -> Optional[Dict[str, int]]:
    """
    Парсит высоту полета из различных форматов
    Возвращает минимальную и максимальную высоту в метрах
    
    Основные форматы высоты:
    - M0035/M0045 = 350-450 метров (35-45 дециметров × 10)
    - M0000/M0020 = 0-200 метров
    - M0015/M0030 = 150-300 метров
    - M0040/M0060 = 400-600 метров
    - M0060/M0060 = 600 метров (фиксированная высота)
    - M0142/M0184 = 1420-1840 метров
    """
    if not level_str or not isinstance(level_str, str):
        return None
    
    # Формат MXXXX/MYYYY
    if '/' in level_str and level_str.startswith('M'):
        try:
            parts = level_str.split('/')
            min_part = parts[0].replace('M', '')
            max_part = parts[1].replace('M', '')
            
            # Конвертируем в метры (дециметры × 10)
            min_meters = int(min_part) * 10
            max_meters = int(max_part) * 10
            
            return {
                "min_height": min_meters,
                "max_height": max_meters,
                "range": f"{min_meters}-{max_meters} м"
            }
        except:
            return None
    
    return None

def parse_radius_for_stats(radius_str: str) -> Optional[int]:
    """
    Улучшенный парсер радиуса для статистики
    с фильтрацией выбросов
    """
    radius = parse_radius(radius_str)
    
    if radius is None:
        return None
    
    # Фильтр от выбросов - убираем слишком большие значения
    if radius > 100000:  # больше 100 км
        return None
    
    return radius

def parse_level_for_stats(level_str: str) -> Optional[int]:
    """
    Улучшенный парсер высоты для статистики
    с фильтрацией выбросов
    """
    level_data = parse_flight_level(level_str)
    
    if level_data is None:
        return None
    
    # Берем среднюю высоту из диапазона
    avg_height = (level_data["min_height"] + level_data["max_height"]) / 2
    
    # Фильтр от выбросов - гражданские БПЛА обычно до 1500м
    if avg_height > 2000:  # больше 2 км
        return None
    
    return int(avg_height)

def get_formats_info() -> Dict[str, Any]:
    """
    Возвращает информацию о поддерживаемых форматах
    """
    return {
        "radius_formats": {
            "description": "Основные форматы радиусов полета",
            "examples": [
                {"format": "R0,5", "meaning": "0.5 км = 500 метров", "type": "километры с запятой"},
                {"format": "R001", "meaning": "1000 метров", "type": "трехзначный код"},
                {"format": "R273", "meaning": "273 метра", "type": "числовое значение"},
                {"format": "R09032", "meaning": "9032 метра", "type": "пятизначный код"},
                {"format": "R076285", "meaning": "76285 метров", "type": "шестизначный код"},
                {"format": "R13", "meaning": "13 км = 13000 метров", "type": "короткий формат"}
            ]
        },
        "level_formats": {
            "description": "Основные форматы высоты полета",
            "examples": [
                {"format": "M0035/M0045", "meaning": "350-450 метров", "type": "диапазон высот"},
                {"format": "M0000/M0020", "meaning": "0-200 метров", "type": "диапазон высот"},
                {"format": "M0015/M0030", "meaning": "150-300 метров", "type": "диапазон высот"},
                {"format": "M0060/M0060", "meaning": "600 метров", "type": "фиксированная высота"},
                {"format": "M0142/M0184", "meaning": "1420-1840 метров", "type": "высокий диапазон"}
            ]
        }
    }

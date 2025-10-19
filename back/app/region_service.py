import geopandas as gpd
from shapely.geometry import Point
import logging
from pathlib import Path
from sqlalchemy import text
from functools import lru_cache
import time
import re

logger = logging.getLogger(__name__)

class RegionService:
    """ ОПТИМИЗИРОВАННЫЙ сервис для определения региона"""
    
    def __init__(self):
        self.gdf = None
        self.spatial_index = None
        
    def _load_shapefile(self):
        """ ОПТИМИЗИРОВАННАЯ загрузка shapefile"""
        try:
            possible_paths = [
                Path("/home/xshow/Projects/UAV/FOR_FAN/back/drone_project/RF/RF.shp"),
                Path("./drone_project/RF/RF.shp"),
                Path("../drone_project/RF/RF.shp"),
                Path("./RF/RF.shp"),
            ]
            
            shapefile_path = None
            for path in possible_paths:
                if path.exists():
                    shapefile_path = path
                    logger.info(f" Shapefile найден: {path}")
                    break
            
            if not shapefile_path:
                logger.error(" Shapefile не найден")
                self.gdf = gpd.GeoDataFrame()
                return
            
            start_time = time.time()
            self.gdf = gpd.read_file(str(shapefile_path))
            self.gdf = self.gdf.to_crs(epsg=4326)
            
            #  СОЗДАЕМ SPATIAL INDEX ДЛЯ МГНОВЕННОГО ПОИСКА
            self.spatial_index = self.gdf.sindex
            
            load_time = time.time() - start_time
            logger.info(f" Shapefile загружен за {load_time:.2f}с: {len(self.gdf)} регионов")
                    
        except Exception as e:
            logger.error(f" Ошибка загрузки shapefile: {e}")
            self.gdf = gpd.GeoDataFrame()
    
    def ensure_shapefile_loaded(self):
        """ДОБАВЛЯЕМ ЭТОТ МЕТОД - Убедиться, что shapefile загружен"""
        if self.gdf is None or self.gdf.empty:
            self._load_shapefile()
    
    def get_region_by_coordinates(self, lat: float, lon: float):
        """ СУПЕР-ОПТИМИЗИРОВАННЫЙ поиск региона"""
        try:
            self.ensure_shapefile_loaded()
            
            if self.gdf is None or self.gdf.empty:
                return {"region_ru": "Ошибка загрузки", "region_en": "Map error", "admin_level": None}
            
            point = Point(lon, lat)
            
            #  ИСПОЛЬЗУЕМ SPATIAL INDEX ДЛЯ МГНОВЕННОГО ПОИСКА
            if self.spatial_index is not None:
                possible_indices = list(self.spatial_index.intersection(point.bounds))
                if not possible_indices:
                    return {"region_ru": "Координаты вне РФ", "region_en": "Outside Russia", "admin_level": None}
                
                #  ПРОВЕРЯЕМ ТОЛЬКО КАНДИДАТОВ
                for idx in possible_indices:
                    row = self.gdf.iloc[idx]
                    try:
                        if row.geometry.contains(point):
                            return {
                                "region_ru": row.get("name_ru", "Неизвестный"),
                                "region_en": row.get("name_en", "Unknown"),
                                "admin_level": row.get("admin_level", row.get("admin_leve", None))
                            }
                    except:
                        continue
            
            return {"region_ru": "Регион не найден", "region_en": "Not found", "admin_level": None}
            
        except Exception as e:
            return {"region_ru": "Ошибка", "region_en": "Error", "admin_level": None}
    
    def add_region_to_flight_data(self, db, table_name="excel_data_2025", batch_size=10000):
        """ МАКСИМАЛЬНО ОПТИМИЗИРОВАННАЯ обработка"""
        try:
            self.ensure_shapefile_loaded()
            
            if self.gdf is None or self.gdf.empty:
                logger.error(" Shapefile не загружен")
                return 0
            
            # Проверяем существование колонки
            result = db.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = :table_name 
                AND column_name = 'region_calculated'
            """), {"table_name": table_name})
            
            column_exists = result.fetchone()
            
            if not column_exists:
                db.execute(text(f"ALTER TABLE {table_name} ADD COLUMN region_calculated VARCHAR(200)"))
                db.commit()
                logger.info(" Добавлена колонка region_calculated")
            
            #  СЧИТАЕМ ЗАПИСИ БЕЗ РЕГИОНОВ
            total_result = db.execute(text(f"""
                SELECT COUNT(*) 
                FROM {table_name} 
                WHERE region_calculated IS NULL 
                AND (dep_1 IS NOT NULL OR dest IS NOT NULL)
            """))
            total_to_process = total_result.scalar()
            
            if total_to_process == 0:
                logger.info(" Все записи уже имеют регионы")
                return 0
            
            logger.info(f" ОБРАБОТКА {total_to_process} ЗАПИСЕЙ...")
            
            updated_count = 0
            processed_count = 0
            start_time = time.time()
            
            #  ОБРАБОТКА МЕГА-ПАЧКАМИ
            while processed_count < total_to_process:
                batch_start = time.time()
                
                # Получаем пачку записей
                result = db.execute(text(f"""
                    SELECT id, dep_1, dest 
                    FROM {table_name} 
                    WHERE region_calculated IS NULL 
                    AND (dep_1 IS NOT NULL OR dest IS NOT NULL)
                    LIMIT :limit
                """), {"limit": batch_size})
                
                records = result.fetchall()
                if not records:
                    break
                
                #  ПАКЕТНАЯ ОБРАБОТКА В ПАМЯТИ
                updates = []
                for record_id, dep_coord, dest_coord in records:
                    region_name = self._get_region_for_record_fast(dep_coord, dest_coord)
                    if region_name and region_name not in ["Регион не найден", "Ошибка", "Координаты вне РФ"]:
                        updates.append((record_id, region_name))
                
                #  МАССОВЫЙ UPDATE
                if updates:
                    # Группируем по регионам для эффективности
                    region_groups = {}
                    for record_id, region_name in updates:
                        safe_region = region_name.replace("'", "''")
                        if safe_region not in region_groups:
                            region_groups[safe_region] = []
                        region_groups[safe_region].append(str(record_id))
                    
                    # Выполняем UPDATE группами
                    for region_name, id_list in region_groups.items():
                        if len(id_list) > 50:  #  ОЧЕНЬ БОЛЬШИЕ ГРУППЫ
                            id_condition = ",".join(id_list)
                            db.execute(text(f"""
                                UPDATE {table_name} 
                                SET region_calculated = '{region_name}'
                                WHERE id IN ({id_condition})
                            """))
                        else:
                            # Маленькие группы - индивидуально
                            for record_id in id_list:
                                db.execute(text(f"""
                                    UPDATE {table_name} 
                                    SET region_calculated = '{region_name}'
                                    WHERE id = {record_id}
                                """))
                    
                    db.commit()
                    updated_count += len(updates)
                
                processed_count += len(records)
                batch_time = time.time() - batch_start
                speed = len(records) / batch_time if batch_time > 0 else 0
                
                logger.info(f"⚡ Пачка: {len(records)} записей за {batch_time:.1f}с ({speed:.0f} зап/с)")
                
                if len(records) < batch_size:
                    break
            
            total_time = time.time() - start_time
            logger.info(f" ОБРАБОТКА ЗАВЕРШЕНА за {total_time:.1f}с!")
            logger.info(f" Итоги: {processed_count} обработано, {updated_count} обновлено")
            
            return updated_count
            
        except Exception as e:
            db.rollback()
            logger.error(f" Ошибка: {e}")
            return 0
    
    def _get_region_for_record_fast(self, dep_coord, dest_coord):
        """ОПТИМИЗИРОВАННАЯ функция получения региона для записи"""
        region_info = None
        
        # ОПТИМИЗАЦИЯ 7: Сначала пробуем dep_1, потом dest
        if dep_coord:
            coords = self._parse_coordinates_cached(dep_coord)
            if coords:
                lat, lon = coords
                region_info = self.get_region_by_coordinates(lat, lon)
                if region_info["region_ru"] not in ["Регион не найден", "Ошибка"]:
                    return region_info["region_ru"]
        
        if dest_coord:
            coords = self._parse_coordinates_cached(dest_coord)
            if coords:
                lat, lon = coords
                region_info = self.get_region_by_coordinates(lat, lon)
                return region_info["region_ru"]
        
        return "Регион не найден"
    
    @lru_cache(maxsize=50000)  #  УВЕЛИЧИВАЕМ КЭШ
    def _parse_coordinates_cached(self, coord_str: str):
        return self._parse_coordinates(coord_str)
    
    def _parse_coordinates(self, coord_str: str):
        """ ОПТИМИЗИРОВАННЫЙ парсинг координат"""
        try:
            if not coord_str or len(coord_str) < 11:
                return None
            
            # Формат: DDMMNDDDME (например: 5957N02905E)
            lat_str = coord_str[:5]
            lon_str = coord_str[5:11]
            
            #  БЫСТРЫЙ ПАРСИНГ БЕЗ ПРОВЕРОК
            lat_deg = int(lat_str[0:2])
            lat_min = int(lat_str[2:4])
            lat_dir = lat_str[4].upper()
            latitude = lat_deg + lat_min / 60.0
            if lat_dir == 'S':
                latitude = -latitude
            
            lon_deg = int(lon_str[0:3])
            lon_min = int(lon_str[3:5])
            lon_dir = lon_str[5].upper()
            longitude = lon_deg + lon_min / 60.0
            if lon_dir == 'W':
                longitude = -longitude
            
            return (latitude, longitude)  #  БЕЗ ОКРУГЛЕНИЯ ДЛЯ СКОРОСТИ
            
        except:
            return None
    
    def test_coordinate_parsing(self, coord_str: str):
        """Тестовая функция для проверки парсинга координат"""
        result = self._parse_coordinates(coord_str)
        if result:
            lat, lon = result
            region = self.get_region_by_coordinates(lat, lon)
            return {
                "coordinates": coord_str,
                "parsed": {"lat": lat, "lon": lon},
                "region": region
            }
        return {"coordinates": coord_str, "parsed": None, "region": None}
    

    def add_region_to_new_flight_data(self, db, table_name="excel_data_result_1", batch_size=5000):
        """ОПТИМИЗИРОВАННАЯ обработка ТОЛЬКО новых записей (без региона)"""
        try:
            self.ensure_shapefile_loaded()
            
            if self.gdf is None or self.gdf.empty:
                logger.error(" Shapefile не загружен")
                return 0
            
            # Считаем только новые записи (без региона)
            total_result = db.execute(text(f"""
                SELECT COUNT(*) 
                FROM {table_name} 
                WHERE region_calculated IS NULL 
                AND (dep_1 IS NOT NULL OR dest IS NOT NULL)
            """))
            total_to_process = total_result.scalar()
            
            if total_to_process == 0:
                logger.info(" Нет новых записей для обработки регионов")
                return 0
            
            logger.info(f" Обработка {total_to_process} НОВЫХ записей...")
            
            # Используем существующую оптимизированную логику
            return self.add_region_to_flight_data(db, table_name, batch_size)
            
        except Exception as e:
            logger.error(f" Ошибка обработки новых регионов: {e}")
            return 0

#  ДОБАВЛЯЕМ ГЛОБАЛЬНУЮ ПЕРЕМЕННУЮ
REGION_SERVICE_AVAILABLE = True

# Глобальный экземпляр
region_service = RegionService()

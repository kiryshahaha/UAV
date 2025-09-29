import os
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import pandas as pd

# Локальный fallback конфиг
class DatabaseConfig:
    def get_connection_string(self):
        """Получить строку подключения из переменных окружения"""
        DB_HOST = os.getenv('DB_HOST', 'localhost')
        DB_PORT = os.getenv('DB_PORT', '5432')
        DB_NAME = os.getenv('DB_NAME', 'postgres')
        DB_USER = os.getenv('DB_USER', 'postgres')
        DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
        
        return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

class DataIntegrator:
    def __init__(self):
        # ✅ Используем локальный конфиг вместо импорта
        self.db_config = DatabaseConfig()
        self.engine = create_engine(self.db_config.get_connection_string())
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def get_available_tables(self):
        """Получить список доступных таблиц с данными БВС"""
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND (table_name LIKE '%excel_data%' OR table_name LIKE '%aviation%')
                ORDER BY table_name
            """))
            tables = [row[0] for row in result]
            return tables
    
    def migrate_data_to_api_table(self, source_table: str, region: str):
        """Перенести данные из таблицы парсера в API таблицу"""
        with self.engine.connect() as conn:
            # Проверяем структуру исходной таблицы
            columns_result = conn.execute(text(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{source_table}'
                ORDER BY ordinal_position
            """))
            columns = [row[0] for row in columns_result]
            print(f"Колонки в таблице {source_table}: {columns}")
            
            # ✅ УБИРАЕМ создание таблицы - она уже создана FastAPI
            # Просто проверяем что колонка source_table существует
            try:
                conn.execute(text("ALTER TABLE flights ADD COLUMN IF NOT EXISTS source_table VARCHAR(100)"))
                conn.commit()
            except:
                pass  # Колонка уже существует
            
            # Маппинг колонок (улучшенный)
            column_mapping = {
                'aircraft_id': self._find_column(columns, ['reis', 'flight', 'aircraft_id', 'callsign', 'id', 'bort']),
                'aircraft_type': self._find_column(columns, ['tip_vs', 'aircraft_type', 'type', 'model', 'tip_gruppa_vs']),
                'departure_aerodrome': self._find_column(columns, ['mesto_vyleta', 'departure', 'from', 'dep', 'aerodrom_vyleta', 'a_v', 'arv']),
                'destination_aerodrome': self._find_column(columns, ['mesto_posadki', 'destination', 'to', 'arr', 'aerodrom_posadki', 'a_p', 'arp']),
                'departure_time': self._find_column(columns, ['vremya_vyleta', 'departure_time', 't_vyl', 'time', 'data_vremya_vyleta', 't_vyl_fakt']),
                'route': self._find_column(columns, ['marshrut', 'route', 'path', 'track', 'tekst_ishodnogo_marshruta', 'raion_poletov'])
            }
            
            # Строим SELECT запрос - ✅ ИСПРАВЛЯЕМ порядок колонок
            select_fields = []
            
            # Сначала фиксированные поля в правильном порядке
            select_fields.append("'FPL' as message_type")
            
            # Затем маппинг полей
            for api_field in ['aircraft_id', 'aircraft_type', 'departure_aerodrome', 
                            'destination_aerodrome', 'departure_time', 'route']:
                source_field = column_mapping.get(api_field)
                if source_field:
                    select_fields.append(f'"{source_field}" as {api_field}')
                else:
                    select_fields.append(f"NULL as {api_field}")
            
            # Регион и source_table в конце
            select_fields.extend([
                f"'{region}' as region",
                f"'{source_table}' as source_table"
            ])
            
            # Выполняем миграцию - ✅ ИСПРАВЛЯЕМ порядок в INSERT
            migrate_query = f"""
                INSERT INTO flights (
                    message_type, aircraft_id, aircraft_type, 
                    departure_aerodrome, destination_aerodrome, 
                    departure_time, route, region, source_table
                )
                SELECT {', '.join(select_fields)}
                FROM "{source_table}"
                WHERE NOT EXISTS (
                    SELECT 1 FROM flights WHERE source_table = '{source_table}'
                )
            """
            
            try:
                result = conn.execute(text(migrate_query))
                conn.commit()
                print(f"✅ Перенесено {result.rowcount} записей из {source_table}")
                return result.rowcount
            except Exception as e:
                print(f"❌ Ошибка миграции {source_table}: {e}")
                conn.rollback()
                return 0
    
    def _find_column(self, columns, possible_names):
        """Найти колонку по возможным названиям"""
        columns_lower = [col.lower() for col in columns]
        for name in possible_names:
            name_lower = name.lower()
            for i, col in enumerate(columns_lower):
                if name_lower in col or col in name_lower:
                    return columns[i]
        return None
    
    def migrate_all_tables(self):
        """Перенести данные из всех найденных таблиц"""
        tables = self.get_available_tables()
        total_migrated = 0
        
        if not tables:
            print("❌ Не найдено таблиц для миграции")
            return 0
        
        # Сначала обрабатываем таблицы с aviation (они структурированы)
        aviation_tables = [t for t in tables if 'fpl_aviation' in t]
        other_tables = [t for t in tables if 'fpl_aviation' not in t]
        
        print(f"🔍 Найдено aviation таблиц: {len(aviation_tables)}")
        print(f"🔍 Найдено обычных таблиц: {len(other_tables)}")
        
        # Сначала aviation таблицы
        for table in aviation_tables:
            try:
                region = self._extract_region_from_table_name(table)
                print(f"🔄 Обработка aviation таблицы: {table} -> регион: {region}")
                migrated = self.migrate_data_to_api_table(table, region)
                total_migrated += migrated
            except Exception as e:
                print(f"❌ Ошибка с таблицей {table}: {e}")
        
        # Затем остальные таблицы
        for table in other_tables:
            try:
                region = self._extract_region_from_table_name(table)
                print(f"🔄 Обработка таблицы: {table} -> регион: {region}")
                migrated = self.migrate_data_to_api_table(table, region)
                total_migrated += migrated
            except Exception as e:
                print(f"❌ Ошибка с таблицей {table}: {e}")
        
        print(f"\n📈 Всего перенесено: {total_migrated} записей")
        return total_migrated
    
    def _extract_region_from_table_name(self, table_name):
        """Извлечь регион из названия таблицы"""
        region_mapping = {
            'moskva': 'moscow',
            'sankt_peterburg': 'spb', 
            'kaliningrad': 'kaliningrad',
            'rostov': 'rostov',
            'samara': 'samara',
            'ekaterinburg': 'ekaterinburg',
            'tyumen': 'tyumen',
            'novosibirsk': 'novosibirsk',
            'krasnoyarsk': 'krasnoyarsk',
            'irkutsk': 'irkutsk',
            'yakutsk': 'yakutsk',
            'magadan': 'magadan',
            'habarovsk': 'habarovsk',
            'simferopol': 'simferopol'
        }
        
        table_lower = table_name.lower()
        for key, value in region_mapping.items():
            if key in table_lower:
                return value
        
        return 'unknown'

if __name__ == "__main__":
    integrator = DataIntegrator()
    
    print("Доступные таблицы:")
    tables = integrator.get_available_tables()
    for i, table in enumerate(tables, 1):
        print(f"{i}. {table}")
    
    print("\nНачинаем миграцию данных...")
    integrator.migrate_all_tables()

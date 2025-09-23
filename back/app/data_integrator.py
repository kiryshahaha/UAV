import os
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import pandas as pd

# Добавляем путь к вашей системе парсинга
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'excel_to_postgres'))

try:
    from config.database import DatabaseConfig
except ImportError:
    # Fallback конфиг если основной не доступен
    class DatabaseConfig:
        def get_connection_string(self):
            return "postgresql://postgres:postgres@localhost:5432/postgres"

class DataIntegrator:
    def __init__(self):
        self.db_config = DatabaseConfig()
        self.engine = create_engine(self.db_config.get_connection_string())
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def get_available_tables(self):
        """Получить список доступных таблиц с данными БВС"""
        with self.engine.connect() as conn:
            # Ищем таблицы, созданные системой парсинга
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
            
            # Создаем таблицу flights если не существует
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS flights (
                    id SERIAL PRIMARY KEY,
                    message_type VARCHAR(10) DEFAULT 'FPL',
                    aircraft_id VARCHAR(50),
                    aircraft_type VARCHAR(50),
                    departure_aerodrome VARCHAR(10),
                    destination_aerodrome VARCHAR(10),
                    departure_time VARCHAR(10),
                    route TEXT,
                    region VARCHAR(50),
                    source_table VARCHAR(100),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """))
            
            # Маппинг колонок (можете настроить под свои данные)
            column_mapping = {
                'aircraft_id': self._find_column(columns, ['reis', 'flight', 'aircraft_id', 'callsign', 'id']),
                'aircraft_type': self._find_column(columns, ['tip_vs', 'aircraft_type', 'type', 'model']),
                'departure_aerodrome': self._find_column(columns, ['mesto_vyleta', 'departure', 'from', 'dep', 'aerodrom_vylet']),
                'destination_aerodrome': self._find_column(columns, ['mesto_posadki', 'destination', 'to', 'arr', 'aerodrom_posadka']),
                'departure_time': self._find_column(columns, ['vremya_vyleta', 'departure_time', 't_vyl', 'time']),
                'route': self._find_column(columns, ['marshrut', 'route', 'path', 'track'])
            }
            
            # Строим SELECT запрос
            select_fields = []
            for api_field, source_field in column_mapping.items():
                if source_field:
                    select_fields.append(f'"{source_field}" as {api_field}')
                else:
                    select_fields.append(f"NULL as {api_field}")
            
            # Добавляем фиксированные поля
            select_fields.extend([
                "'FPL' as message_type",
                f"'{region}' as region",
                f"'{source_table}' as source_table"
            ])
            
            # Выполняем миграцию
            migrate_query = f"""
                INSERT INTO flights (message_type, aircraft_id, aircraft_type, 
                                   departure_aerodrome, destination_aerodrome, 
                                   departure_time, route, region, source_table)
                SELECT {', '.join(select_fields)}
                FROM "{source_table}"
                WHERE NOT EXISTS (
                    SELECT 1 FROM flights WHERE source_table = '{source_table}'
                )
            """
            
            result = conn.execute(text(migrate_query))
            conn.commit()
            
            print(f"Перенесено {result.rowcount} записей из {source_table}")
            return result.rowcount
    
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
        
        for table in tables:
            try:
                # Определяем регион по имени таблицы
                region = self._extract_region_from_table_name(table)
                migrated = self.migrate_data_to_api_table(table, region)
                total_migrated += migrated
                print(f"✅ {table} -> {migrated} записей")
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
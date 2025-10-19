from sqlalchemy import text
from database import engine

def upgrade_database():
    """Увеличиваем размеры полей для избежания усечения данных"""
    with engine.connect() as conn:
        try:
            # Увеличиваем размеры полей
            conn.execute(text("""
                ALTER TABLE flights 
                ALTER COLUMN departure_aerodrome TYPE VARCHAR(50),
                ALTER COLUMN destination_aerodrome TYPE VARCHAR(50),
                ALTER COLUMN departure_time TYPE VARCHAR(50),
                ALTER COLUMN aircraft_id TYPE VARCHAR(100);
            """))
            conn.commit()
            print("✅ Размеры полей увеличены")
        except Exception as e:
            print(f"⚠️ Ошибка изменения схемы: {e}")
            conn.rollback()

def add_region_column():
    """Добавляет столбец region_calculated в основную таблицу"""
    with engine.connect() as conn:
        try:
            # Добавляем столбец для рассчитанного региона
            conn.execute(text("""
                ALTER TABLE excel_data_result_1 
                ADD COLUMN IF NOT EXISTS region_calculated VARCHAR(200)
            """))
            conn.commit()
            print("✅ Столбец region_calculated добавлен в excel_data_result_1")
        except Exception as e:
            print(f"⚠️ Ошибка добавления столбца: {e}")
            conn.rollback()


def create_optimization_indexes():
    """Создание оптимизированных индексов для больших данных"""
    with engine.connect() as conn:
        try:
            # ОПТИМИЗИРОВАННЫЕ ИНДЕКСЫ ДЛЯ БЫСТРОГО ПОИСКА
            
            # Индекс для поиска по году
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_flights_date_year 
                ON excel_data_result_1 (EXTRACT(YEAR FROM date));
            """))
            
            # Составной индекс для региона и даты
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_flights_region_date 
                ON excel_data_result_1 (region_calculated, date);
            """))
            
            # Индекс для координат вылета
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_flights_dep_coords 
                ON excel_data_result_1 (dep_1) 
                WHERE dep_1 IS NOT NULL AND length(dep_1) >= 11;
            """))
            
            # Частичный индекс для активных записей
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_flights_active 
                ON excel_data_result_1 (id) 
                WHERE date IS NOT NULL AND region_calculated IS NOT NULL;
            """))
            
            # BRIN индекс для временных рядов (очень эффективен для больших данных)
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_flights_date_brin 
                ON excel_data_result_1 USING BRIN (date);
            """))
            
            conn.commit()
            print("✅ Оптимизированные индексы созданы")
            
        except Exception as e:
            print(f"⚠️ Ошибка создания индексов: {e}")
            conn.rollback()

if __name__ == "__main__":
    create_optimization_indexes()

if __name__ == "__main__":
    add_region_column()

if __name__ == "__main__":
    upgrade_database()
import os
import sys
from sqlalchemy import text

def auto_setup(db_session):
    """Автоматическая настройка при запуске API"""
    print("🔧 Автоматическая настройка системы...")
    
    try:
        # Проверяем наличие данных в flights
        from .main import Flight
        flight_count = db_session.query(Flight).count()
        print(f"📊 Найдено записей в таблице flights: {flight_count}")
        
        if flight_count == 0:
            print("🔄 Данных нет, проверяем доступные таблицы парсера...")
            
            # Проверяем существующие таблицы в БД
            result = db_session.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                AND table_name LIKE '%excel_data%'
            """))
            parser_tables = [row[0] for row in result]
            
            if parser_tables:
                print(f"📋 Найдены таблицы парсера: {parser_tables}")
                print("🔄 Запуск автоматической миграции...")
                
                # Импортируем и запускаем интегратор
                from .data_integrator import DataIntegrator
                integrator = DataIntegrator()
                migrated = integrator.migrate_all_tables()
                print(f"✅ Мигрировано записей: {migrated}")
                
                # Проверяем результат
                new_count = db_session.query(Flight).count()
                print(f"📊 Теперь записей в flights: {new_count}")
            else:
                print("❌ Таблицы парсера не найдены")
                print("💡 Для загрузки данных выполните:")
                print("   1. python excel_to_postgres/main.py")
                print("   2. Перезапустите API")
        
        else:
            print("✅ Данные готовы к использованию")
            
    except Exception as e:
        print(f"⚠️ Ошибка автоматической настройки: {e}")
        import traceback
        traceback.print_exc()
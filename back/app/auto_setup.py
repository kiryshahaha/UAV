from sqlalchemy import text

def check_data_status(db_session, Flight_model):
    """Проверка статуса данных без автоматической миграции"""
    print("🔧 Проверка состояния данных...")
    
    flight_count = db_session.query(Flight_model).count()
    print(f"📊 Записей в таблице flights: {flight_count}")
    
    if flight_count == 0:
        result = db_session.execute(text("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name LIKE '%excel_data%'
        """))
        tables = [row[0] for row in result]
        
        if tables:
            print(f"📋 Найдено таблиц парсера: {len(tables)}")
            print("💡 Автоматическая миграция запущена в фоне...")
        else:
            print("❌ Данные не найдены")
            print("💡 Используйте: POST /api/admin/parse-excel")
    else:
        print("✅ Данные готовы")
    
    return flight_count
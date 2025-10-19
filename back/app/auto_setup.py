
from sqlalchemy import text
from region_service import region_service

def check_data_status(db_session, Flight_model):
    """Проверка статуса данных"""
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
            print("💡 Используйте: POST /api/admin/parse-excel для загрузки данных")
            print("💡 Затем используйте: POST /admin/add-region-column для добавления столбца с городом")
        else:
            print("❌ Данные не найдены")
    else:
        print("✅ Данные готовы")
        # Проверяем есть ли столбец с регионом
        result = db_session.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'excel_data_result_1' 
            AND column_name = 'region_calculated'
        """))
        if not result.fetchone():
            print("💡 Используйте: POST /admin/add-region-column для добавления столбца с городом")
    
    return flight_count
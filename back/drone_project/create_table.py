import psycopg2

try:
    # Подключение к вашей существующей базе данных postgres
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",  # ваша существующая база
        user="postgres",
        password="postgres",
        port="5432"
    )
    
    cursor = conn.cursor()
    
    # Создаем таблицу flights
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS flights (
            id SERIAL PRIMARY KEY,
            drone_id VARCHAR(100) NOT NULL,
            lat FLOAT NOT NULL,
            lon FLOAT NOT NULL,
            region_ru VARCHAR(200),
            region_en VARCHAR(200),
            admin_level INTEGER,
            timestamp TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    conn.commit()
    print("✅ Таблица flights успешно создана!")
    
    # Проверяем создание таблицы
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = 'flights';
    """)
    
    if cursor.fetchone():
        print("✅ Таблица flights существует в базе данных")
    else:
        print("❌ Таблица flights не создана")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Ошибка: {e}")

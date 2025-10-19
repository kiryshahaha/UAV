# create_test_tables.py
from database import SessionLocal, engine
from sqlalchemy import text

def create_test_tables():
    """Создание тестовых таблиц с данными за 2024 и 2023 годы"""
    db = SessionLocal()
    
    try:
        # Таблица за 2024 год
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS excel_data_2024 (
                tsentr_es_orvd TEXT,
                shr TEXT,
                dof TEXT,
                sts TEXT,
                dep_1 TEXT,
                dest TEXT,
                typ TEXT,
                reg TEXT,
                eet TEXT,
                opr TEXT,
                orgn TEXT,
                per TEXT,
                dle TEXT,
                rmk TEXT,
                departure_time TEXT,
                arrival_time TEXT,
                flight_level TEXT,
                flight_zone TEXT,
                flight_zone_radius TEXT,
                id SERIAL PRIMARY KEY,
                date DATE,
                region_calculated VARCHAR(200)
            )
        """))
        
        # Таблица за 2023 год  
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS excel_data_2023 (
                tsentr_es_orvd TEXT,
                shr TEXT,
                dof TEXT,
                sts TEXT,
                dep_1 TEXT,
                dest TEXT,
                typ TEXT,
                reg TEXT,
                eet TEXT,
                opr TEXT,
                orgn TEXT,
                per TEXT,
                dle TEXT,
                rmk TEXT,
                departure_time TEXT,
                arrival_time TEXT,
                flight_level TEXT,
                flight_zone TEXT,
                flight_zone_radius TEXT,
                id SERIAL PRIMARY KEY,
                date DATE,
                region_calculated VARCHAR(200)
            )
        """))
        
        # Очищаем таблицы если они уже существуют
        db.execute(text("DELETE FROM excel_data_2024"))
        db.execute(text("DELETE FROM excel_data_2023"))
        
        # Данные за 2024 год
        db.execute(text("""
            INSERT INTO excel_data_2024 VALUES
            ('Красноярский', 'SHR-DOF/240115 STS/ DEP/5957N02905E DEST/ TYP/SHAR REG/ EET/ OPR/МАЛИНОВСКИЙ НИКИТА АЛЕКСАНДРОВИЧ ORGN/ PER/ DLE/ RMK/ОБОЛОЧКА 300 ДЛЯ ЗОНДИРОВАНИЯ АТМОСФЕРЫ', '240115', NULL, '5957N02905E', NULL, 'SHAR', NULL, NULL, 'МАЛИНОВСКИЙ НИКИТА АЛЕКСАНДРОВИЧ', NULL, NULL, NULL, 'ОБОЛОЧКА 300 ДЛЯ ЗОНДИРОВАНИЯ АТМОСФЕРЫ', '07:05:00', NULL, NULL, NULL, NULL, 1, '2024-01-15', 'Ленинградская область'),
            ('Ставропольский', 'SHR-DOF/240120 STS/SAR DEP/4408N04308E DEST/4408N04308E TYP/BLA REG/00724RE600725 EET/ OPR/ГУ МАС РОССИИ ПО ORGN/ PER/ DLE/ RMK/WR655 В ЗОНЕ ВИЗУАЛЬНОГО ПОЛЕТА', '240120', 'SAR', '4408N04308E', '4408N04308E', 'BLA', '00724RE600725', NULL, 'ГУ МАС РОССИИ ПО', NULL, NULL, NULL, 'WR655 В ЗОНЕ ВИЗУАЛЬНОГО ПОЛЕТА', '06:00:00', '12:50:00', 'M0000/M0000', '4408N04308E', 'R0.5', 2, '2024-01-20', 'Ставропольский край')
            -- ... остальные 8 записей
        """))
        
        # Данные за 2023 год
        db.execute(text("""
            INSERT INTO excel_data_2023 VALUES
            ('Московский', 'SHR-DOF/230415 STS/ DEP/5545N03735E DEST/5545N03735E TYP/BLA REG/RF-35001 EET/UUEE0001 OPR/АЭРОФОТОСЪЕМКА ORGN/ PER/ DLE/ RMK/СЪЕМКА ТЕРРИТОРИИ МОСКВЫ', '230415', NULL, '5545N03735E', '5545N03735E', 'BLA', 'RF-35001', 'UUEE0001', 'АЭРОФОТОСЪЕМКА', NULL, NULL, NULL, 'СЪЕМКА ТЕРРИТОРИИ МОСКВЫ', '09:00:00', '17:00:00', 'M0050/M0050', '5545N03735E', 'R005', 1, '2023-04-15', 'Москва'),
            ('Санкт-Петербургский', 'SHR-DOF/230420 STS/ DEP/5950N03020E DEST/5950N03020E TYP/AER REG/RA-08888 EET/ OPR/ЛЕНАЭРО ОРГН/ PER/ DLE/ RMK/МОНИТОРИНГ ЛЭП', '230420', NULL, '5950N03020E', '5950N03020E', 'AER', 'RA-08888', NULL, 'ЛЕНАЭРО', NULL, NULL, NULL, 'МОНИТОРИНГ ЛЭП', '08:30:00', '15:45:00', 'M0000/M0000', '5950N03020E', 'R003', 2, '2023-04-20', 'Ленинградская область')
            -- ... остальные 8 записей
        """))
        
        db.commit()
        print("✅ Тестовые таблицы созданы успешно!")
        
    except Exception as e:
        db.rollback()
        print(f"❌ Ошибка создания таблиц: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    create_test_tables()
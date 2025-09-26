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

if __name__ == "__main__":
    upgrade_database()
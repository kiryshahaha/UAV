# database.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
import os
from dotenv import load_dotenv

load_dotenv()

# Настройка базы данных Supabase
DB_HOST = os.getenv('DB_HOST', 'aws-1-eu-north-1.pooler.supabase.com')
DB_PORT = os.getenv('DB_PORT', '6543')
DB_NAME = os.getenv('DB_NAME', 'postgres')
DB_USER = os.getenv('DB_USER', 'postgres.adrxmxwncvtbvrmqiihb')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'Trening0811!')
DB_SCHEMA = os.getenv('DB_SCHEMA', 'public')

# Строка подключения для Supabase с pooler
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

# Создание engine и сессии
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=300,
    echo=False  # Поставьте True для дебага SQL запросов
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependency для БД
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
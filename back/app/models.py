# models.py
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

# Модель базы данных для полетов
class Flight(Base):
    __tablename__ = "flights"
    
    id = Column(Integer, primary_key=True, index=True)
    message_type = Column(String(10), default="FPL")
    aircraft_id = Column(String(50))
    aircraft_type = Column(String(50))
    departure_aerodrome = Column(String(10))
    destination_aerodrome = Column(String(10))
    departure_time = Column(String(10))
    route = Column(Text)
    region = Column(String(50))
    source_table = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)

# Модели для системы выбора таблиц
class UserTable(Base):
    __tablename__ = "user_tables"
    
    id = Column(Integer, primary_key=True, index=True)
    table_name = Column(String(100), unique=True, nullable=False)
    original_filename = Column(String(255), nullable=False)
    upload_date = Column(DateTime, default=datetime.utcnow)
    records_count = Column(Integer, default=0)
    description = Column(String(500))
    is_active = Column(Boolean, default=False)
    user_id = Column(String(100))

class UserSession(Base):
    __tablename__ = "user_sessions"
    
    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(String(100), unique=True, nullable=False)
    current_table = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

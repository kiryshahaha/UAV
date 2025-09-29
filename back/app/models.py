from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

Base = declarative_base()

# Модель базы данных
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
    created_at = Column(DateTime, default=datetime.utcnow)

# Pydantic модели (схемы)
class FlightBase(BaseModel):
    message_type: Optional[str] = "FPL"
    aircraft_id: Optional[str] = None
    aircraft_type: Optional[str] = None
    departure_aerodrome: Optional[str] = None
    destination_aerodrome: Optional[str] = None
    departure_time: Optional[str] = None
    route: Optional[str] = None
    region: str
    source_table: Optional[str]

class FlightResponse(FlightBase):
    id: int
    created_at: Optional[datetime]=None
    
    class Config:
        from_attributes = True
        arbitrary_types_allowed = True

class RegionStats(BaseModel):
    region: str
    flights_count: int
    drones_count: int

class AnalyticsResponse(BaseModel):
    total_flights: int
    total_regions: int
    total_drones: int
    period: str
    last_updated: datetime
    top_regions: List[RegionStats]

# Дополнительные модели для запросов
class FlightCreate(FlightBase):
    pass

class FlightUpdate(BaseModel):
    aircraft_id: Optional[str] = None
    aircraft_type: Optional[str] = None
    region: Optional[str] = None
# schemas.py
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime

# Модели для работы с таблицами
class TableSelectRequest(BaseModel):
    table_name: str

class TableInfoResponse(BaseModel):
    table_name: str
    original_filename: str
    upload_date: Optional[str] = None
    records_count: int
    description: Optional[str] = None
    is_active: bool

class UploadResponse(BaseModel):
    message: str
    table_name: str
    sheets_processed: int
    records_added: int

class RegionInfoResponse(BaseModel):
    region_ru: str
    region_en: str
    admin_level: Optional[int] = None

class FlightZoneResponse(BaseModel):
    flight_id: int
    flight_zone: Optional[str] = None
    flight_zone_radius: Optional[str] = None
    takeoff_point: Dict[str, Any]
    landing_point: Optional[Dict[str, Any]] = None
    flight_time: Dict[str, Any]
    registration_number: Optional[str] = None
    date_of_flight: Optional[str] = None
    operator: Optional[str] = None
    additional_info: Dict[str, Any]

class StatisticsResponse(BaseModel):
    data: List[Dict[str, Any]]
    count: int
    current_table: Optional[str] = None
    columns: List[str]

class PaginationResponse(BaseModel):
    limit: Optional[int] = None
    offset: int
    total: int
    has_more: bool

class CityDataResponse(BaseModel):
    center: str
    data: List[Dict[str, Any]]
    count: int
    column_used: str

class HealthResponse(BaseModel):
    status: str
    timestamp: str

class MonthlyStatsResponse(BaseModel):
    regions: List[Dict[str, Any]]
    total_regions: int
    columns_used: Dict[str, str]

# Модели для регионов
class RegionCreate(BaseModel):
    name: str
    description: Optional[str] = None

# Модели для статистики
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


# schemas.py - добавляем новые модели

class TableCreateRequest(BaseModel):
    custom_name: str
    description: Optional[str] = None

class UploadWithNameRequest(BaseModel):
    custom_name: str
    description: Optional[str] = None

class ExportRequest(BaseModel):
    report_type: str
    date_from: str = None
    date_to: str = None
    regions: list = None
    operator_name: str = None
    operator_names: list = None
    
    
    class Config:
        json_schema_extra = {
            "example": {
                "report_type": "general",
                "date_from": "2025-01-01",
                "date_to": "2025-12-31",
                "regions": ["Москва", "Санкт-Петербург"]
            }
        }
class ExportPreviewResponse(BaseModel):
    report_type: str
    filters: dict
    record_count: int
    available_data: dict
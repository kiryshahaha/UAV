from pydantic import BaseModel 
from typing import List, Dict, Optional, Any
from datetime import datetime 

class RegionStatistics(BaseModel):
    region:str
    flights_count:int 
    drones_count:int 
    total_flight_time:float 

class ChartData(BaseModel):
    labels: List[str]
    datasets: List[Dict[str, Any]] 

class MapPoint(BaseModel):
    region:str 
    lat:float
    lon:float
    flights_count:int
    drones_count:int  
    
class AnalyticsResponse(BaseModel):
    total_flights: int
    total_regions: int
    total_drones: int
    period: str
    last_updated: datetime
    top_regions: List[RegionStatistics]
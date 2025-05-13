# models.py

from sqlmodel import SQLModel, Field
from typing import Optional

class MasterCard(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: int
    card_name: str
    set_name: str
    card_number: Optional[str] = None
    card_id: str
    query: str
    tier: Optional[str] = None
    status: Optional[str] = None
    high_demand_boost: Optional[str] = None

class DailyPriceLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: int
    sold_date: str
    median_price: Optional[float]
    average_price: Optional[float]
    sale_count: Optional[int]
    query_used: Optional[str]
    card_number: Optional[str]

class ActiveDailyPriceLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: int
    active_date: str
    median_price: Optional[float]
    average_price: Optional[float]
    sale_count: Optional[int]
    query_used: Optional[str]
    card_number: Optional[str]


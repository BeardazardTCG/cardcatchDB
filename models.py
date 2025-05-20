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

from sqlalchemy import Column, Integer, String, Numeric, DateTime
from datetime import datetime
from db import Base  # or wherever your Base comes from

class TrendTracker(Base):
    __tablename__ = "trendtracker"

    id = Column(Integer, primary_key=True)
    unique_id = Column(String)
    card_name = Column(String)
    set_name = Column(String)
    last_price = Column(Numeric)
    second_last = Column(Numeric)
    third_last = Column(Numeric)
    average_30d = Column(Numeric)
    sample_size = Column(Integer)
    pct_change = Column(Numeric)
    trend = Column(String)
    updated_at = Column(DateTime, default=datetime.utcnow)

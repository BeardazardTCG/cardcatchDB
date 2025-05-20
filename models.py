from typing import Optional
from sqlmodel import SQLModel, Field

# SQLModel-backed tables
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
    clean_avg_price: Optional[float] = None        # ✅ Add this
    net_resale_value: Optional[float] = None       # ✅ Add this

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

# SQLAlchemy-backed tables
from sqlalchemy import Column, Integer, String, Numeric, TIMESTAMP
from sqlalchemy.sql import func
from db import Base

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
    pct_change_stable = Column(Numeric)
    pct_change_spike = Column(Numeric)
    trend_stable = Column(String)
    trend_spike = Column(String)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

class SmartSuggestion(Base):
    __tablename__ = "smartsuggestions"

    id = Column(Integer, primary_key=True)
    unique_id = Column(String)
    card_name = Column(String)
    set_name = Column(String)
    card_number = Column(String)
    card_status = Column(String)
    clean_price = Column(Numeric)
    target_sell = Column(Numeric)
    target_buy = Column(Numeric)
    suggested_action = Column(String)
    trend = Column(String)
    resale_value = Column(Numeric)
    created_at = Column(TIMESTAMP, server_default=func.now())

class Inventory(Base):
    __tablename__ = "inventory"

    id = Column(Integer, primary_key=True)
    unique_id = Column(String)

class Wishlist(Base):
    __tablename__ = "wishlist"

    id = Column(Integer, primary_key=True)
    unique_id = Column(String)

from typing import Optional
from sqlmodel import SQLModel, Field
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime

class MasterCardV2(SQLModel, table=True):
    __tablename__ = "mastercard_v2"

    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: str
    card_name: str
    set_name: str
    card_number: Optional[str] = None
    card_number_raw: Optional[str] = None
    card_id: Optional[str] = None
    query: Optional[str] = None
    set_code: Optional[str] = None
    set_id: Optional[str] = None
    supertype: Optional[str] = None
    subtypes: Optional[str] = Field(default=None, sa_type=JSONB)
    rarity: Optional[str] = None
    artist: Optional[str] = None
    types: Optional[str] = Field(default=None, sa_type=JSONB)
    type: Optional[str] = None
    release_date: Optional[str] = None
    language: Optional[str] = None
    hot_character: Optional[bool] = None
    card_image_url: Optional[str] = None
    set_logo_url: Optional[str] = None
    set_symbol_url: Optional[str] = None

# === Master card record used throughout CardCatch ===
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
    clean_avg_price: Optional[float] = None
    net_resale_value: Optional[float] = None

# === Daily sold-price logging ===
class DailyPriceLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: int
    sold_date: str
    median_price: Optional[float]
    average_price: Optional[float]
    sale_count: Optional[int]
    query_used: Optional[str]
    card_number: Optional[str]

# === Daily active listings snapshot ===
class ActiveDailyPriceLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: int
    active_date: str
    median_price: Optional[float]
    average_price: Optional[float]
    sale_count: Optional[int]
    query_used: Optional[str]
    card_number: Optional[str]

# === Historical trend data for pricing movement ===
class TrendTracker(SQLModel, table=True):
    __tablename__ = "trendtracker"

    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: str
    card_name: str
    set_name: str
    last_price: float
    second_last: float
    third_last: float
    average_30d: float
    sample_size: int
    pct_change_stable: float
    pct_change_spike: float
    trend_stable: str
    trend_spike: str
    updated_at: Optional[datetime] = None

    # 🄞 Graded price tracking
    psa_10_price: Optional[float] = None
    psa_10_count: Optional[int] = None
    psa_9_price: Optional[float] = None
    psa_9_count: Optional[int] = None
    ace_10_price: Optional[float] = None
    ace_10_count: Optional[int] = None
    ace_9_price: Optional[float] = None
    ace_9_count: Optional[int] = None

# === Smart recommendations from algorithm ===
class SmartSuggestion(SQLModel, table=True):
    __tablename__ = "smartsuggestions"

    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: str
    card_name: str
    set_name: str
    card_number: str
    card_status: str
    clean_price: float
    target_sell: float
    target_buy: float
    suggested_action: str
    trend: str
    resale_value: float
    affiliate_buy_link: Optional[str] = None
    created_at: Optional[datetime] = None

# === Inventory tracking ===
class Inventory(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: str

# === Wishlist tracking ===
class Wishlist(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: str




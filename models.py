from datetime import datetime
from sqlmodel import SQLModel, Field
from typing import Optional

class MasterCard(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)  # Auto-increment ID for database
    unique_id: int
    card_id: Optional[str]
    card_name: str
    set_name: str
    card_number: Optional[str]
    image_url: Optional[str]
    tcg_market_price: Optional[float]
    daily_ebay_price: Optional[float]
    tcg_low_price: Optional[float]
    full_query: Optional[str]
    tier: Optional[str]
    status: Optional[str]
    high_demand_boost: Optional[bool]
    sold_ebay_median: Optional[float]
    active_ebay_median: Optional[float]
    clean_avg_value: Optional[float]
    net_resale_value: Optional[float]
    review_flag: Optional[str]
    sort_key: Optional[str]

class DailyPriceLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: int
    sold_date: datetime
    median_price: Optional[float]
    average_price: Optional[float]
    lowest_price: Optional[float]
    highest_price: Optional[float]
    sold_count: Optional[int]
    query_used: Optional[str]
    card_number: Optional[str]

class ActiveDailyPriceLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: int
    scrape_date: datetime
    median_price: Optional[float]
    average_price: Optional[float]
    lowest_price: Optional[float]
    highest_price: Optional[float]
    active_count: Optional[int]
    query_used: Optional[str]
    card_number: Optional[str]

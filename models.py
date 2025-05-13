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

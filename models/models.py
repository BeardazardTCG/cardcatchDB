# models/models.py
from typing import Optional
from sqlmodel import SQLModel, Field
from sqlalchemy.dialects.postgresql import JSONB

class MasterCardV2(SQLModel, table=True):
    __tablename__ = "mastercard_v2"

    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: str
    card_name: str
    set_name: str
    card_number: Optional[str] = None
    card_number_raw: Optional[str] = None
    query: Optional[str] = None
    set_code: Optional[str] = None
    set_id: Optional[str] = None
    supertype: Optional[str] = None
    subtypes: Optional[str] = Field(default=None, sa_column_kwargs={"type_": JSONB})
    rarity: Optional[str] = None
    artist: Optional[str] = None
    types: Optional[str] = Field(default=None, sa_column_kwargs={"type_": JSONB})
    type: Optional[str] = None
    release_date: Optional[str] = None
    language: Optional[str] = None
    hot_character: Optional[bool] = None
    card_image_url: Optional[str] = None
    set_logo_url: Optional[str] = None
    set_symbol_url: Optional[str] = None



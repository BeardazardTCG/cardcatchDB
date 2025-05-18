from fastapi import FastAPI, Query, HTTPException, status, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlmodel import SQLModel, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from pydantic import BaseModel, Field
from typing import List, Optional, Any, Dict
from datetime import datetime, timedelta
import os
import requests
import time  # for rate limiting in tcg-prices-batch

from models import MasterCard
from batch_manager import BatchManager
from scraper_launcher import ScraperLauncher
from scraper import parse_ebay_sold_page, parse_ebay_active_page

app = FastAPI(
    title="CardCatch Pricing API",
    description="Fetch sold-item stats from eBay with rich search filters",
    version="2.0.1"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set.")

engine = create_async_engine(DATABASE_URL, echo=True)
async_session = async_sessionmaker(engine, expire_on_commit=False)

async def get_db_session() -> AsyncSession:
    async with async_session() as session:
        yield session

@app.on_event("startup")
async def on_startup():
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    print("✅ Connected to the database successfully.")

token_cache: Dict[str, Any] = {"access_token": None, "expires_at": datetime.min}
CACHE_BUFFER_SEC = 60

class OAuthToken(BaseModel):
    access_token: str
    expires_at: datetime

class CardQuery(BaseModel):
    card: str
    number: Optional[str] = None
    set_name: Optional[str] = Field(None, alias="set")
    lang: str = "en"
    rarity: Optional[str] = None
    condition: Optional[str] = None
    buying_options: Optional[str] = "FIXED_PRICE"
    graded: Optional[bool] = None
    grade_agency: Optional[str] = None

class MasterCardUpsert(BaseModel):
    unique_id: int
    card_name: str
    set_name: str
    card_number: Optional[str]
    card_id: str
    query: str
    tier: Optional[str]
    status: Optional[str]
    high_demand_boost: Optional[str]

def fetch_oauth_token(sandbox: bool) -> Optional[str]:
    global token_cache
    if sandbox:
        return None
    if token_cache["access_token"] and token_cache["expires_at"] > datetime.utcnow():
        return token_cache["access_token"]
    client_id = os.getenv("EBAY_CLIENT_ID")
    client_secret = os.getenv("EBAY_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Missing OAuth credentials")
    token_url = "https://api.ebay.com/identity/v1/oauth2/token"
    resp = requests.post(
        token_url,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials", "scope": "https://api.ebay.com/oauth/api_scope"},
        auth=(client_id, client_secret),
        timeout=10
    )
    if resp.status_code != 200:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OAuth token fetch failed")
    info = resp.json()
    token = info.get("access_token")
    expires_in = info.get("expires_in", 3600)
    expires_at = datetime.utcnow() + timedelta(seconds=expires_in - CACHE_BUFFER_SEC)
    token_cache.update({"access_token": token, "expires_at": expires_at})
    return token

@app.get("/")
def health() -> Any:
    return {"message": "CardCatch is live — production mode active."}

@app.post("/bulk-upsert-master-cards")
async def bulk_upsert_master_cards(cards: List[MasterCardUpsert], db_session: AsyncSession = Depends(get_db_session)):
    upserted = 0
    for card in cards:
        result = await db_session.execute(select(MasterCard).where(MasterCard.unique_id == card.unique_id))
        matches = result.scalars().all()
        existing_card = matches[0] if matches else None

        if existing_card:
            existing_card.card_name = card.card_name
            existing_card.set_name = card.set_name
            existing_card.card_number = card.card_number
            existing_card.card_id = card.card_id
            existing_card.query = card.query
            existing_card.tier = card.tier
            existing_card.status = card.status
            existing_card.high_demand_boost = card.high_demand_boost
        else:
            new_card = MasterCard(**card.dict())
            db_session.add(new_card)

        upserted += 1

    await db_session.commit()
    return {"status": "Upsert complete", "count": upserted}

@app.get("/scraped-price")
def scraped_price(query: str, max_items: int = 20) -> Any:
    try:
        return parse_ebay_sold_page(query, max_items=max_items)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/scraped-active-price")
def get_active_price(query: str, max_items: int = 30) -> Any:
    try:
        return parse_ebay_active_page(query, max_items=max_items)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tcg-prices-batch")
async def tcg_prices_batch(request: Request):
    try:
        body = await request.json()
        card_ids = body.get("card_ids", [])
        results = []

        for card_id in card_ids:
            url = f"https://api.pokemontcg.io/v2/cards/{card_id}"
            try:
                resp = requests.get(
                    url,
                    headers={"X-Api-Key": os.getenv("POKEMONTCG_API_KEY")},
                    timeout=30
                )
                time.sleep(0.1)

                if resp.status_code != 200:
                    results.append({"id": card_id, "market": None, "low": None})
                    continue

                data = resp.json().get("data", {})
                prices = data.get("tcgplayer", {}).get("prices", {})

                market = (
                    prices.get("holofoil", {}).get("market") or
                    prices.get("reverseHolofoil", {}).get("market") or
                    prices.get("normal", {}).get("market") or
                    prices.get("1stEditionHolofoil", {}).get("market")
                )
                low = (
                    prices.get("holofoil", {}).get("low") or
                    prices.get("reverseHolofoil", {}).get("low") or
                    prices.get("normal", {}).get("low") or
                    prices.get("1stEditionHolofoil", {}).get("low")
                )

                results.append({
                    "id": card_id,
                    "market": round(market, 2) if market else None,
                    "low": round(low, 2) if low else None
                })
            except Exception as e:
                results.append({"id": card_id, "market": None, "low": None})

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/start-scrape")
async def start_full_scrape(db_session: AsyncSession = Depends(get_db_session)):
    launcher = ScraperLauncher(db_session)
    await launcher.run_all_scrapers()
    return {"status": "Scraping started!"}


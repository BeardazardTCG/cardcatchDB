from fastapi import FastAPI, Query, HTTPException, status, Request, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlmodel import SQLModel, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from pydantic import BaseModel, Field
from typing import List, Optional, Any, Dict
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import httpx
import asyncio

# Internal imports
from models import MasterCard
from batch_manager import BatchManager
from scraper_launcher import ScraperLauncher
from scraper import parse_ebay_sold_page, parse_ebay_active_page

# Load .env environment variables
load_dotenv()

# === App Setup ===
app = FastAPI(
    title="CardCatch Pricing API",
    description="Fetch sold-item stats from eBay with rich search filters",
    version="2.0.1"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 🔒 TODO: Restrict for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === Database Setup ===
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set in environment.")

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

async def get_db_session() -> AsyncSession:
    async with async_session() as session:
        yield session

@app.on_event("startup")
async def on_startup():
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    print("✅ Database initialized.")

# === API Key Middleware ===
API_KEY = os.getenv("API_KEY")

async def validate_api_key(x_api_key: Optional[str] = Header(None)):
    if not API_KEY or x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")

# === Input Schemas ===
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

# === API Routes ===
@app.get("/")
def health() -> Any:
    return {"message": "CardCatch is live — production mode active."}

@app.post("/bulk-upsert-master-cards", dependencies=[Depends(validate_api_key)])
async def bulk_upsert_master_cards(cards: List[MasterCardUpsert], db_session: AsyncSession = Depends(get_db_session)):
    upserted = 0
    for card in cards:
        result = await db_session.execute(select(MasterCard).where(MasterCard.unique_id == card.unique_id))
        existing_card = result.scalars().first()
        if existing_card:
            for key, value in card.dict().items():
                setattr(existing_card, key, value)
        else:
            db_session.add(MasterCard(**card.dict()))
        upserted += 1

    await db_session.commit()
    return {"status": "Upsert complete", "count": upserted}

@app.get("/scraped-price", dependencies=[Depends(validate_api_key)])
def scraped_price(query: str, max_items: int = 20) -> Any:
    try:
        return parse_ebay_sold_page(query, max_items=max_items)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/scraped-active-price", dependencies=[Depends(validate_api_key)])
def get_active_price(query: str, max_items: int = 30) -> Any:
    try:
        return parse_ebay_active_page(query, max_items=max_items)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tcg-prices-batch-async", dependencies=[Depends(validate_api_key)])
async def tcg_prices_batch_async(request: Request):
    try:
        body = await request.json()
        card_ids = body.get("card_ids", [])
        sem = asyncio.Semaphore(10)

        async def fetch_card(card_id: str):
            url = f"https://api.pokemontcg.io/v2/cards/{card_id.strip().lower()}"
            headers = {"X-Api-Key": os.getenv("POKEMONTCG_API_KEY")}
            try:
                async with sem:
                    async with httpx.AsyncClient(timeout=30) as client:
                        resp = await client.get(url, headers=headers)
                        await asyncio.sleep(0.1)
                if resp.status_code != 200:
                    return {"card_id": card_id, "market": None, "low": None}
                json_data = resp.json()
                prices = json_data.get("data", {}).get("tcgplayer", {}).get("prices", {})
                market = prices.get("holofoil", {}).get("market") or prices.get("normal", {}).get("market")
                low = prices.get("holofoil", {}).get("low") or prices.get("normal", {}).get("low")
                return {
                    "card_id": card_id,
                    "market": round(market, 2) if market else None,
                    "low": round(low, 2) if low else None
                }
            except Exception as e:
                return {"card_id": card_id, "market": None, "low": None, "error": str(e)}

        tasks = [fetch_card(cid) for cid in card_ids]
        return await asyncio.gather(*tasks)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/start-scrape", dependencies=[Depends(validate_api_key)])
async def start_full_scrape(db_session: AsyncSession = Depends(get_db_session)):
    launcher = ScraperLauncher(db_session)
    await launcher.run_all_scrapers()
    return {"status": "Scraping started"}


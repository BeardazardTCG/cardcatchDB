# scrape_ebay_active_to_db.py
# ✅ Scrapes eBay active listings and logs stats to ActiveDailyPriceLog

import os
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from sqlmodel import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from models.models import MasterCard, ActiveDailyPriceLog
from utils import calculate_median, calculate_average
from scraper import parse_ebay_active_page

# === Load .env config
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set in .env")

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

BATCH_SIZE = 100

async def run_ebay_active_scraper():
    async with async_session() as session:
        # Get unique_ids already logged in active table
        logged_result = await session.execute(
            select(ActiveDailyPriceLog.unique_id).distinct()
        )
        logged_ids = set(row[0] for row in logged_result.fetchall())

        # Get all cards with a query, and not already logged
        result = await session.execute(
            select(MasterCard).where(
                MasterCard.query.is_not(None),
                MasterCard.unique_id.notin_(logged_ids)
            )
        )
        cards = result.scalars().all()
        print(f"📦 Total unlogged cards with queries: {len(cards)}")

        scraped = 0
        skipped = 0

        for i in range(0, len(cards), BATCH_SIZE):
            batch = cards[i:i + BATCH_SIZE]
            print(f"🚀 Processing batch {i // BATCH_SIZE + 1} of {len(cards) // BATCH_SIZE + 1}")

            for card in batch:
                print(f"🔍 Scraping active listings for {card.unique_id} → {card.query}")
                try:
                    results = parse_ebay_active_page(card.query)
                except Exception as e:
                    print(f"❌ Error fetching for {card.unique_id}: {e}")
                    skipped += 1
                    continue

                prices = [item.get("price") for item in results if item.get("price") is not None]
                prices = [float(p) for p in prices if isinstance(p, (int, float, str)) and str(p).replace('.', '', 1).isdigit()]
                listing_count = len(prices)

                if listing_count == 0:
                    skipped += 1
                    continue

                median = round(calculate_median(prices), 2)
                average = round(calculate_average(prices), 2)

                log = ActiveDailyPriceLog(
                    unique_id=card.unique_id,
                    active_date=str(datetime.today().date()),
                    median_price=median,
                    average_price=average,
                    sale_count=listing_count,
                    query_used=card.query,
                    card_number=card.card_number
                )
                session.add(log)
                scraped += 1

        await session.commit()
        print(f"✅ Scrape complete. Cards scraped: {scraped} | Skipped: {skipped}")

if __name__ == "__main__":
    asyncio.run(run_ebay_active_scraper())

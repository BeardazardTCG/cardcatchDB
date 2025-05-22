# scrape_ebay_sold_to_db.py
# ✅ One-time backfill: Scrapes sold listings for unlogged cards, fills DailyPriceLog

import os
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from sqlmodel import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from models.models import MasterCard, DailyPriceLog
from utils import filter_outliers, calculate_median, calculate_average
from scraper import parse_ebay_sold_page

# === Load .env config
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

BATCH_SIZE = 60

async def backfill_ebay_sold():
    async with async_session() as session:
        # Find all cards not yet in DailyPriceLog
        result = await session.execute(
            select(MasterCard).where(
                MasterCard.unique_id.notin_(
                    select(DailyPriceLog.unique_id)
                )
            )
        )
        cards = result.scalars().all()
        print(f"🟡 Cards needing backfill: {len(cards)}")

        scraped, skipped = 0, 0

        for i in range(0, len(cards), BATCH_SIZE):
            batch = cards[i:i+BATCH_SIZE]
            print(f"🚀 Batch {i // BATCH_SIZE + 1} / {(len(cards) // BATCH_SIZE) + 1}")

            for card in batch:
                try:
                    results = parse_ebay_sold_page(card.query, max_items=30)
                except Exception as e:
                    print(f"❌ Scrape error: {e}")
                    skipped += 1
                    continue

                grouped = {}
                for item in results:
                    sold_date = item.get("sold_date")
                    price = item.get("price")
                    if not sold_date or price is None:
                        continue
                    grouped.setdefault(sold_date, []).append(price)

                if not grouped:
                    session.add(DailyPriceLog(
                        unique_id=card.unique_id,
                        sold_date=str(datetime.today().date()),
                        median_price=None,
                        average_price=None,
                        sale_count=0,
                        query_used=card.query,
                        card_number=card.card_number
                    ))
                    continue

                for sold_date, prices in grouped.items():
                    filtered = filter_outliers(prices)
                    if not filtered:
                        continue
                    session.add(DailyPriceLog(
                        unique_id=card.unique_id,
                        sold_date=sold_date,
                        median_price=calculate_median(filtered),
                        average_price=calculate_average(filtered),
                        sale_count=len(filtered),
                        query_used=card.query,
                        card_number=card.card_number
                    ))
                scraped += 1

        await session.commit()
        print(f"✅ Backfill done. Scraped: {scraped} | Skipped: {skipped}")

if __name__ == "__main__":
    asyncio.run(backfill_ebay_sold())

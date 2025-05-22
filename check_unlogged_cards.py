# check_unlogged_cards.py
# ✅ Finds and scrapes cards with no sales data using existing UID list

import os
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from sqlmodel import SQLModel, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from models import MasterCard, DailyPriceLog
from scraper import parse_ebay_sold_page
from utils import filter_outliers, calculate_median, calculate_average

# === Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

# === Async database setup
engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

async def get_session() -> AsyncSession:
    async with async_session() as session:
        yield session

# === Constants
BATCH_SIZE = 60
UNLOGGED_UID_FILE = "./data/unlogged_cards.txt"  # ✅ Update path if needed

# === Main scraper logic
async def run_scraper_for_unlogged():
    # Load unlogged unique_ids
    try:
        with open(UNLOGGED_UID_FILE, "r") as f:
            unlogged_uids = set(line.strip() for line in f if line.strip().isdigit())
    except FileNotFoundError:
        print("❌ UID file not found.")
        return

    async with async_session() as session:
        result = await session.execute(
            select(MasterCard).where(MasterCard.unique_id.in_(unlogged_uids))
        )
        cards = result.scalars().all()
        print(f"📦 Cards to scrape: {len(cards)}")

        scraped = 0
        skipped = 0

        for i in range(0, len(cards), BATCH_SIZE):
            batch = cards[i:i + BATCH_SIZE]
            print(f"🚀 Batch {i // BATCH_SIZE + 1} of {(len(cards) // BATCH_SIZE) + 1}")

            for card in batch:
                print(f"🔍 {card.unique_id} → {card.query}")
                try:
                    results = parse_ebay_sold_page(card.query, max_items=30)
                except Exception as e:
                    print(f"❌ Error while scraping: {e}")
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
        print(f"✅ Scraped: {scraped} | ❌ Skipped: {skipped}")

# === Entry point
if __name__ == "__main__":
    asyncio.run(run_scraper_for_unlogged())

import asyncio
from datetime import datetime
from db import get_session
from models import MasterCard, ActiveDailyPriceLog
from scraper import parse_ebay_active_page
from sqlalchemy import select
from utils import calculate_median, calculate_average

BATCH_SIZE = 100

async def run_ebay_active_scraper():
    async with get_session() as session:
        # Step 1: Get all unique_ids already logged in active table
        logged_result = await session.execute(
            select(ActiveDailyPriceLog.unique_id).distinct()
        )
        logged_ids = set(row[0] for row in logged_result.fetchall())

        # Step 2: Get all cards from master with a query, and not already logged
        result = await session.execute(
            select(MasterCard).where(
                MasterCard.query.isnot(None),
                MasterCard.unique_id.notin_(logged_ids)
            )
        )
        cards = result.scalars().all()

        print(f"📦 Total unlogged cards with queries: {len(cards)}")

        for i in range(0, len(cards), BATCH_SIZE):
            batch = cards[i:i + BATCH_SIZE]
            print(f"🚀 Processing batch {i // BATCH_SIZE + 1} of {len(cards) // BATCH_SIZE + 1}")

            for card in batch:
                print(f"🔍 Scraping active listings for {card.unique_id} → {card.query}")
                try:
                    results = parse_ebay_active_page(card.query)
                except Exception as e:
                    print(f"❌ Error fetching for {card.unique_id}: {e}")
                    continue

                prices = [item.get("price") for item in results if item.get("price") is not None]
                prices = [float(p) for p in prices if isinstance(p, (int, float, str)) and str(p).replace('.', '', 1).isdigit()]
                listing_count = len(prices)

                if listing_count == 0:
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

                await asyncio.sleep(1)  # Respect Render limits

            await session.commit()

if __name__ == "__main__":
    asyncio.run(run_ebay_active_scraper())

import asyncio
from datetime import datetime
from db import get_session
from models import MasterCard, DailyPriceLog
from scraper import parse_ebay_sold_page
from sqlalchemy import select
from utils import filter_outliers, calculate_median, calculate_average

BATCH_SIZE = 60

async def run_ebay_sold_scraper():
    async with get_session() as session:
        # ✅ STEP 1: Get all unique_ids already logged
        logged_result = await session.execute(select(DailyPriceLog.unique_id).distinct())
        logged_ids = set(row[0] for row in logged_result.fetchall())

        # ✅ STEP 2: Get only cards from MasterCard that aren't logged
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
                print(f"🔍 Scraping {card.unique_id} → {card.query}")
                try:
                    results = parse_ebay_sold_page(card.query, max_items=30)
                except Exception as e:
                    print(f"❌ Error fetching for {card.unique_id}: {e}")
                    continue

                grouped = {}
                for item in results:
                    date = item.get("sold_date")
                    price = item.get("price")
                    if not date or price is None:
                        continue
                    grouped.setdefault(date, []).append(price)

                if not grouped:
                    log = DailyPriceLog(
                        unique_id=card.unique_id,
                        sold_date=str(datetime.today().date()),
                        median_price=None,
                        average_price=None,
                        sale_count=0,
                        query_used=card.query,
                        card_number=card.card_number
                    )
                    session.add(log)
                    continue

                for sold_date, prices in grouped.items():
                    filtered = filter_outliers(prices)
                    if not filtered:
                        continue

                    median = calculate_median(filtered)
                    avg = calculate_average(filtered)

                    log = DailyPriceLog(
                        unique_id=card.unique_id,
                        sold_date=str(sold_date),
                        median_price=round(median, 2),
                        average_price=round(avg, 2),
                        sale_count=len(filtered),
                        query_used=card.query,
                        card_number=card.card_number
                    )
                    session.add(log)

            await session.commit()
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(run_ebay_sold_scraper())

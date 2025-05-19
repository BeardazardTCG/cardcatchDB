# scrape_unlogged_only.py

import asyncio
from datetime import datetime
from db import get_session
from models import MasterCard, DailyPriceLog
from scraper import parse_ebay_sold_page
from utils import filter_outliers, calculate_median, calculate_average

BATCH_SIZE = 60
UID_FILE = "./data/unlogged_cards.txt"

async def run_scraper_for_unlogged():
    with open(UID_FILE, "r") as f:
        unlogged_uids = set(line.strip() for line in f if line.strip().isdigit())

    async with get_session() as session:
        result = await session.execute(
            MasterCard.__table__.select().where(MasterCard.unique_id.in_(unlogged_uids))
        )
        cards = result.fetchall()
        print(f"📦 Cards to scrape: {len(cards)}")

        for i in range(0, len(cards), BATCH_SIZE):
            batch = cards[i:i + BATCH_SIZE]
            print(f"🚀 Batch {i // BATCH_SIZE + 1} of {(len(cards) // BATCH_SIZE) + 1}")

            for row in batch:
                card = row._mapping
                print(f"🔍 {card['unique_id']} → {card['query']}")
                try:
                    results = parse_ebay_sold_page(card['query'], max_items=30)
                except Exception as e:
                    print(f"❌ Error: {e}")
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
                        unique_id=card['unique_id'],
                        sold_date=str(datetime.today().date()),
                        median_price=None,
                        average_price=None,
                        sale_count=0,
                        query_used=card['query'],
                        card_number=card['card_number']
                    ))
                    continue

                for sold_date, prices in grouped.items():
                    filtered = filter_outliers(prices)
                    if not filtered:
                        continue
                    session.add(DailyPriceLog(
                        unique_id=card['unique_id'],
                        sold_date=sold_date,
                        median_price=round(calculate_median(filtered), 2),
                        average_price=round(calculate_average(filtered), 2),
                        sale_count=len(filtered),
                        query_used=card['query'],
                        card_number=card['card_number']
                    ))
            await session.commit()
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(run_scraper_for_unlogged())

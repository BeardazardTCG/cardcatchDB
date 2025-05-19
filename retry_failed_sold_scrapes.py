import csv
import asyncio
from datetime import datetime
from sqlalchemy import select
from db import get_session
from models import DailyPriceLog
from scraper import parse_ebay_sold_page
from utils import filter_outliers, calculate_median, calculate_average

INPUT_CSV = "query_6-2025-05-19_94543.csv"
BATCH_SLEEP_SEC = 2


def load_unscraped_cards():
    with open(INPUT_CSV, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        return [
            {
                "unique_id": int(row["unique_id"]),
                "query": row["query"],
                "card_number": row.get("card_number", "")
            }
            for row in reader if row.get("query")
        ]


async def run_recovery_scraper():
    cards = load_unscraped_cards()
    print(f"📦 Total cards to retry: {len(cards)}")

    async with get_session() as session:
        for idx, card in enumerate(cards):
            print(f"🔁 Retrying {idx+1}/{len(cards)} → {card['unique_id']} | {card['query']}")

            try:
                results = parse_ebay_sold_page(card['query'], max_items=30)
            except Exception as e:
                print(f"❌ Error fetching {card['unique_id']}: {e}")
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
                    unique_id=card['unique_id'],
                    sold_date=datetime.today().date().isoformat(),
                    median_price=None,
                    average_price=None,
                    sale_count=0,
                    query_used=card['query'],
                    card_number=card['card_number']
                )
                session.add(log)
                await session.commit()
                await asyncio.sleep(BATCH_SLEEP_SEC)
                continue

            for sold_date, prices in grouped.items():
                filtered = filter_outliers(prices)
                if not filtered:
                    continue

                median = calculate_median(filtered)
                avg = calculate_average(filtered)

                log = DailyPriceLog(
                    unique_id=card['unique_id'],
                    sold_date=sold_date,  # already str from page
                    median_price=round(median, 2),
                    average_price=round(avg, 2),
                    sale_count=len(filtered),
                    query_used=card['query'],
                    card_number=card['card_number']
                )
                session.add(log)

            await session.commit()
            await asyncio.sleep(BATCH_SLEEP_SEC)


if __name__ == "__main__":
    asyncio.run(run_recovery_scraper())

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import os
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from sqlmodel import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text
from models.models import MasterCard
from utils import filter_outliers, calculate_median, calculate_average
from archive.scraper import parse_ebay_sold_page
import json

# === Load .env config
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

BATCH_SIZE = 25  # Still in deeper test mode

async def test_scrape_ebay_sold():
    async with async_session() as session:
        result = await session.execute(
            select(MasterCard).limit(BATCH_SIZE)
        )
        cards = result.scalars().all()
        print(f"🧪 Testing {len(cards)} cards...")

        for card in cards:
            print(f"\n🔍 {card.query}")
            try:
                results = parse_ebay_sold_page(card.query, max_items=30)
            except Exception as e:
                print(f"❌ Scrape error: {e}")
                continue

            if not results:
                print(f"⚠️ No results found for: {card.query}")

            grouped = {}
            exclusion_log = []

            for item in results:
                sold_date = item.get("sold_date")
                price = item.get("price")
                title = item.get("title", "")
                url = item.get("url", "N/A")

                if not sold_date:
                    exclusion_log.append({"reason": "no sold date", "title": title, "url": url})
                    continue
                if price is None:
                    exclusion_log.append({"reason": "no price", "title": title, "url": url})
                    continue

                grouped.setdefault(sold_date, []).append(price)

            if exclusion_log:
                print("🧹 Excluded Listings:")
                for ex in exclusion_log:
                    print(f"   - {ex['reason']}: {ex['title']} ({ex['url']})")

            for sold_date, prices in grouped.items():
                filtered = filter_outliers(prices)
                summary = {
                    "query": card.query,
                    "date": sold_date,
                    "raw_count": len(prices),
                    "filtered_count": len(filtered),
                    "median": calculate_median(filtered),
                    "average": calculate_average(filtered),
                    "raw_prices": prices,
                    "filtered": filtered,
                    "exclusions": exclusion_log
                }

                try:
                    await session.execute(
                        text("""
                        INSERT INTO scraper_test_results (source, query, included_count, excluded_count, avg_price, raw_json)
                        VALUES (:source, :query, :included_count, :excluded_count, :avg_price, :raw_json)
                        """),
                        {
                            "source": "ebay_sold",
                            "query": card.query,
                            "included_count": len(filtered),
                            "excluded_count": len(prices) - len(filtered),
                            "avg_price": calculate_average(filtered),
                            "raw_json": json.dumps(summary)
                        }
                    )
                    await session.commit()
                    print(f"✅ Logged: {card.query} ({len(filtered)} used)")
                except Exception as e:
                    print(f"❌ DB error: {e}")

if __name__ == "__main__":
    asyncio.run(test_scrape_ebay_sold())

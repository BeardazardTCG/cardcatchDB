import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import json
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv
from sqlmodel import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text
from utils import filter_outliers, calculate_median, calculate_average
from archive.scraper import parse_ebay_sold_page

# === Load config
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

EXCLUSION_KEYWORDS = [
    "psa", "psa ", "psa-", "psa:", "cgc", "bgs", "ace", "graded", "gem mint",
    "bulk", "lot", "bundle", "set of", "collection",
    "spanish", "german", "french", "japanese", "italian", "chinese", "portuguese",
    "coin", "pin", "promo tin", "jumbo"
]

BATCH_SIZE = 120

async def run_ebay_sold_scraper():
    async with async_session() as session:
        result = await session.execute(text("SELECT unique_id, query FROM mastercard_v2"))
        cards = result.fetchall()

        for i in range(0, len(cards), BATCH_SIZE):
            batch = cards[i:i + BATCH_SIZE]

            for unique_id, query in batch:
                print(f"\n🔍 Scraping eBay sold for: {query} ({unique_id})")

                try:
                    results = parse_ebay_sold_page(query, max_items=120)
                except Exception as e:
                    print(f"❌ Scrape error for {unique_id}: {e}")
                    await session.execute(text("""
                        INSERT INTO scrape_failures (unique_id, scraper_source, error_message)
                        VALUES (:unique_id, :scraper_source, :error_message)
                    """), {
                        "unique_id": unique_id,
                        "scraper_source": "ebay_sold",
                        "error_message": str(e)
                    })
                    await session.commit()
                    continue

                grouped = defaultdict(list)
                for item in results:
                    title = item.get("title", "").lower()
                    price = item.get("price")
                    sold_date = item.get("sold_date")

                    if any(kw in title for kw in EXCLUSION_KEYWORDS):
                        continue
                    if price is None or not sold_date:
                        continue

                    try:
                        dt = datetime.strptime(sold_date, "%Y-%m-%d")
                        grouped[dt.date()].append(price)
                    except Exception:
                        continue

                if not grouped:
                    print(f"⚠️ No valid prices for {unique_id}, logging null result.")
                    await session.execute(text("""
                        INSERT INTO ebay_sold_nulls (unique_id, query_used, logged_at)
                        VALUES (:unique_id, :query_used, :logged_at)
                    """), {
                        "unique_id": unique_id,
                        "query_used": query,
                        "logged_at": datetime.utcnow()
                    })
                    await session.commit()
                    continue

                for sold_date, prices in grouped.items():
                    filtered_step1 = filter_outliers(prices)
                    median_val = calculate_median(filtered_step1)
                    band = 0.5 if median_val > 10 else 0.4
                    final_filtered = [p for p in filtered_step1 if abs(p - median_val) / median_val <= band]

                    median_price = calculate_median(final_filtered)
                    average_price = calculate_average(final_filtered)
                    sale_count = len(final_filtered)

                    try:
                        await session.execute(text("""
                            INSERT INTO dailypricelog (
                                unique_id, sold_date, median_price, average_price,
                                sale_count, query_used
                            )
                            VALUES (:unique_id, :sold_date, :median_price, :average_price,
                                    :sale_count, :query_used)
                        """), {
                            "unique_id": unique_id,
                            "sold_date": sold_date,
                            "median_price": median_price,
                            "average_price": average_price,
                            "sale_count": sale_count,
                            "query_used": query
                        })
                        await session.commit()
                        print(f"✅ Logged {sale_count} sales for {unique_id} on {sold_date}")
                    except Exception as e:
                        print(f"❌ DB insert error for {unique_id} on {sold_date}: {e}")
                        await session.rollback()

if __name__ == "__main__":
    asyncio.run(run_ebay_sold_scraper())

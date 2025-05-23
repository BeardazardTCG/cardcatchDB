import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import json
from datetime import datetime
from dotenv import load_dotenv
from sqlmodel import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text
from models.models import MasterCard
from utils import filter_outliers, calculate_median, calculate_average
from archive.scraper import parse_ebay_sold_page as original_parse

# === Load .env config
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

BATCH_SIZE = 25

# === Normalise bad queries
def clean_query(raw_query):
    return raw_query.lower().replace("-", " ").replace("/", " ")

# === Patched parsing with relaxed fuzzy matching
def patched_parse(query, max_items=30):
    results = original_parse(query, max_items=max_items)
    character, card_number, *_ = query.lower().split()
    card_number_digits = ''.join(filter(str.isdigit, card_number))

    filtered = []
    for r in results:
        title = r.get("title", "").lower()
        title_digits = ''.join(filter(str.isdigit, title))

        if not any(c in title for c in [character, character.replace('-', ''), character.replace('-', ' ')]):
            continue
        if not any(part in title_digits for part in [
            card_number_digits,
            card_number_digits[-3:],
            card_number_digits[-2:],
            card_number_digits.replace("0", "")
        ]):
            continue
        filtered.append(r)
    return filtered

async def test_scrape_ebay_sold():
    async with async_session() as session:
        result = await session.execute(select(MasterCard).limit(BATCH_SIZE))
        cards = result.scalars().all()
        print(f"🧪 Testing {len(cards)} cards...")

        for card in cards:
            cleaned_query = clean_query(card.query)
            print(f"\n🔍 {cleaned_query}")
            try:
                results = patched_parse(cleaned_query, max_items=30)
            except Exception as e:
                print(f"❌ Scrape error: {e}")
                continue

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

            if not grouped:
                summary = {
                    "query": cleaned_query,
                    "raw_count": 0,
                    "filtered_count": 0,
                    "median": None,
                    "average": None,
                    "raw_prices": [],
                    "filtered": [],
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
                            "query": cleaned_query,
                            "included_count": 0,
                            "excluded_count": 0,
                            "avg_price": None,
                            "raw_json": json.dumps(summary)
                        }
                    )
                    await session.commit()
                    print(f"📄 Logged exclusions only for: {cleaned_query}")
                except Exception as e:
                    print(f"❌ DB error (exclusion only): {e}")
                continue

            for sold_date, prices in grouped.items():
                filtered = filter_outliers(prices)
                summary = {
                    "query": cleaned_query,
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
                            "query": cleaned_query,
                            "included_count": len(filtered),
                            "excluded_count": len(prices) - len(filtered),
                            "avg_price": calculate_average(filtered),
                            "raw_json": json.dumps(summary)
                        }
                    )
                    await session.commit()
                    print(f"✅ Logged: {cleaned_query} ({len(filtered)} used)")
                except Exception as e:
                    print(f"❌ DB error: {e}")

if __name__ == "__main__":
    asyncio.run(test_scrape_ebay_sold())

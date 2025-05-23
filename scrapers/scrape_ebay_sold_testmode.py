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

# === Normalise bad queries
def clean_query(raw_query):
    return raw_query.lower().replace("-", " ").replace("/", " ")

# === Custom logic-matching Charizard test
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

async def test_scrape_batch():
    async with async_session() as session:
        result = await session.execute(select(MasterCard).limit(149))
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

            raw_prices = []
            exclusions = []

            exclusion_keywords = [
                "psa", "cgc", "bgs", "ace", "graded", "gem mint",
                "bulk", "lot", "bundle", "set of", "collection",
                "spanish", "german", "french", "japanese", "italian", "chinese", "portuguese",
                "coin", "pin", "promo tin", "jumbo"
            ]

            for item in results:
                title = item.get("title", "").lower()
                price = item.get("price")
                sold_date = item.get("sold_date")
                url = item.get("url", "N/A")

                if any(kw in title for kw in exclusion_keywords):
                    exclusions.append({"reason": "excluded keyword", "title": title, "url": url})
                    continue
                if price is None or not sold_date:
                    exclusions.append({"reason": "missing data", "title": title, "url": url})
                    continue

                raw_prices.append(price)

            if not raw_prices:
                summary = {
                    "query": cleaned_query,
                    "raw_count": 0,
                    "filtered_count": 0,
                    "median": None,
                    "average": None,
                    "raw_prices": [],
                    "filtered": [],
                    "exclusions": exclusions
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
                            "excluded_count": len(exclusions),
                            "avg_price": None,
                            "raw_json": json.dumps(summary)
                        }
                    )
                    await session.commit()
                    print(f"📄 Logged exclusions only for: {cleaned_query}")
                except Exception as e:
                    print(f"❌ DB error (exclusion only): {e}")
                continue

            # === Charizard filtering logic
            filtered_step1 = filter_outliers(raw_prices)
            median_val = calculate_median(filtered_step1)
            final_filtered = [p for p in filtered_step1 if abs(p - median_val) / median_val <= 0.4]

            summary = {
                "query": cleaned_query,
                "raw_count": len(raw_prices),
                "filtered_count": len(final_filtered),
                "median": calculate_median(final_filtered),
                "average": calculate_average(final_filtered),
                "raw_prices": raw_prices,
                "filtered": final_filtered,
                "exclusions": exclusions
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
                        "included_count": len(final_filtered),
                        "excluded_count": len(raw_prices) - len(final_filtered),
                        "avg_price": calculate_average(final_filtered),
                        "raw_json": json.dumps(summary)
                    }
                )
                await session.commit()
                print(f"✅ Logged: {cleaned_query} ({len(final_filtered)} used)")
            except Exception as e:
                print(f"❌ DB error: {e}")

if __name__ == "__main__":
    asyncio.run(test_scrape_batch())

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import json
from datetime import datetime
from dotenv import load_dotenv
from sqlmodel import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text
from utils import filter_outliers, calculate_median, calculate_average
from archive.scraper import parse_ebay_sold_page

# === Load .env config
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

# === Load test queries from file
QUERY_FILE_PATH = os.path.join(os.path.dirname(__file__), "charizard_test_queries.txt")
with open(QUERY_FILE_PATH, "r") as f:
    TEST_QUERIES = [line.strip() for line in f if line.strip()]

# === Keyword exclusions (expanded for graded detection)
EXCLUSION_KEYWORDS = [
    "psa", "psa ", "psa-", "psa:",
    "cgc", "bgs", "ace", "graded", "gem mint",
    "bulk", "lot", "bundle", "set of", "collection",
    "spanish", "german", "french", "japanese", "italian", "chinese", "portuguese",
    "coin", "pin", "promo tin", "jumbo"
]

async def run_scrape_tests():
    async with async_session() as session:
        for query in TEST_QUERIES:
            print(f"\n🔍 Running filtered scrape for: {query}")
            try:
                results = parse_ebay_sold_page(query, max_items=30)
            except Exception as e:
                print(f"❌ Scrape error for '{query}': {e}")
                continue

            raw_prices = []
            exclusions = []
            used_urls = []

            for item in results:
                title = item.get("title", "").lower()
                price = item.get("price")
                sold_date = item.get("sold_date")
                url = item.get("url")
                used_urls.append(url)

                if any(kw in title for kw in EXCLUSION_KEYWORDS):
                    exclusions.append({"reason": "excluded keyword", "title": title, "url": url})
                    continue

                if price is None or not sold_date:
                    exclusions.append({"reason": "missing data", "title": title, "url": url})
                    continue

                raw_prices.append(price)

            if not raw_prices:
                summary = {
                    "query": query,
                    "raw_count": 0,
                    "filtered_count": 0,
                    "median": None,
                    "average": None,
                    "raw_prices": [],
                    "filtered": [],
                    "urls": used_urls,
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
                            "query": query,
                            "included_count": 0,
                            "excluded_count": len(exclusions),
                            "avg_price": None,
                            "raw_json": json.dumps(summary)
                        }
                    )
                    await session.commit()
                    print(f"📄 Logged exclusions only for: {query}")
                except Exception as e:
                    print(f"❌ DB error (exclusion only): {e}")
                continue

            # === Charizard filtering logic ===
            filtered_step1 = filter_outliers(raw_prices)
            median_val = calculate_median(filtered_step1)
            band = 0.5 if median_val > 10 else 0.4
            final_filtered = [p for p in filtered_step1 if abs(p - median_val) / median_val <= band]

            summary = {
                "query": query,
                "raw_count": len(raw_prices),
                "filtered_count": len(final_filtered),
                "median": calculate_median(final_filtered),
                "average": calculate_average(final_filtered),
                "raw_prices": raw_prices,
                "filtered": final_filtered,
                "urls": used_urls,
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
                        "query": query,
                        "included_count": len(final_filtered),
                        "excluded_count": len(raw_prices) - len(final_filtered),
                        "avg_price": calculate_average(final_filtered),
                        "raw_json": json.dumps(summary)
                    }
                )
                await session.commit()
                print(f"✅ Logged: {query} ({len(final_filtered)} used)")
            except Exception as e:
                print(f"❌ DB error: {e}")

if __name__ == "__main__":
    asyncio.run(run_scrape_tests())

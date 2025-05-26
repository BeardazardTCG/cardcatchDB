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
import re

# === Load config
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

# === Updated exclusion logic
EXCLUSION_KEYWORDS = [
    "psa", "psa ", "psa-", "psa:", "cgc", "bgs", "ace", "graded", "gem mint",
    "bulk", "lot", "bundle", "set of", "collection", "coin", "pin", "promo tin", "jumbo",
    "x", "x1", "x2", "x3", "x5", "x10", "x15",
    "choose", "select", "playset", "save", "mix", "match",
    "multi", "offer", "your pick", "non holo base set lot", "energy lot",
    "singles", "menu", "all cards", "selection"
]

# === Inclusion logic
PREMIUM_EXCLUDE_KEYWORDS = ["psa", "bgs", "graded", "gem mint"]


def should_include_listing(title: str, price_text: str, card_number_digits: str, character: str) -> bool:
    title_lower = title.lower()
    title_digits = re.sub(r"[^\d]", "", title)

    if any(kw in title_lower for kw in EXCLUSION_KEYWORDS):
        return False
    if " to " in price_text.lower() or "0.00" in price_text:
        return False
    if card_number_digits and card_number_digits not in title_digits:
        return False

    # Normalize character matching
    norm_character = character.replace("-", "").lower()
    norm_title = title_lower.replace("-", "")
    if norm_character not in norm_title:
        return False

    return True


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
                listings_by_date = defaultdict(list)
                urls_used_tracker = defaultdict(set)

                for item in results:
                    title = item.get("title", "").strip()
                    price = item.get("price")
                    sold_date = item.get("sold_date")
                    url = item.get("url")

                    if not title or price is None or not sold_date:
                        continue

                    price_text = str(price)
                    character, _, card_number = query.partition(" ")
                    card_number_digits = re.sub(r"[^\d]", "", card_number)

                    if not should_include_listing(title, price_text, card_number_digits, character):
                        continue

                    try:
                        dt = datetime.strptime(sold_date, "%Y-%m-%d")
                        grouped[dt.date()].append(price)
                        listings_by_date[dt.date()].append({
                            "title": title,
                            "price": price,
                            "url": url
                        })
                        urls_used_tracker[dt.date()].add(url)
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

                    if median_val == 0 or median_val is None:
                        final_filtered = []
                    else:
                        final_filtered = [p for p in filtered_step1 if abs(p - median_val) / median_val <= (0.5 if median_val > 10 else 0.4)]

                    median_price = calculate_median(final_filtered)
                    average_price = calculate_average(final_filtered)
                    sale_count = len(final_filtered)
                    url_list = list(urls_used_tracker.get(sold_date, []))

                    try:
                        await session.execute(text("""
                            INSERT INTO dailypricelog (
                                unique_id, sold_date, median_price, average_price,
                                sale_count, query_used, urls_used
                            )
                            VALUES (:unique_id, :sold_date, :median_price, :average_price,
                                    :sale_count, :query_used, :urls_used)
                        """), {
                            "unique_id": unique_id,
                            "sold_date": sold_date,
                            "median_price": median_price,
                            "average_price": average_price,
                            "sale_count": sale_count,
                            "query_used": query,
                            "urls_used": json.dumps(url_list)
                        })
                        await session.commit()
                        print(f"✅ Logged {sale_count} sales for {unique_id} on {sold_date}")
                    except Exception as e:
                        print(f"❌ DB insert error for {unique_id} on {sold_date}: {e}")
                        await session.rollback()

if __name__ == "__main__":
    asyncio.run(run_ebay_sold_scraper())

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
import urllib.parse

# === Load config
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

# === Config
BATCH_SIZE = 50
CONCURRENT_SCRAPE_LIMIT = 5
CARD_SCRAPE_DELAY = 0.7
MAX_RESULTS = 240

EXCLUSION_KEYWORDS = [
    "psa", "bgs", "cgc", "graded", "gem mint", "slab", "bulk", "lot", "bundle",
    "set of", "collection", "coin", "pin", "promo tin", "jumbo", "x", "x1", "x2",
    "x3", "x5", "x10", "x15", "choose", "select", "playset", "save", "mix", "match",
    "multi", "offer", "your pick", "singles",
    "menu", "all cards", "selection", "1st edition", "1st ed", "first edition", "shadowless"
]
EXCLUSION_STRING = " ".join(EXCLUSION_KEYWORDS)

def build_search_url(query: str) -> str:
    base_url = "https://www.ebay.co.uk/sch/i.html"
    params = {
        "_nkw": query,
        "LH_Sold": "1",
        "LH_Complete": "1",
        "LH_PrefLoc": "1",
        "_dmd": "2",
        "_ipg": str(MAX_RESULTS),
        "_sop": "13",
        "_dcat": "183454",
        "Graded": "No",
        "_in_kw": "4",
        "_ex_kw": EXCLUSION_STRING
    }
    return f"{base_url}?{urllib.parse.urlencode(params)}"

def should_include_listing(title: str, price_text: str, card_number_digits: str, character: str) -> bool:
    title_lower = title.lower()
    title_digits = re.sub(r"[^\d]", "", title)
    if any(kw in title_lower for kw in EXCLUSION_KEYWORDS):
        return False
    if " to " in price_text.lower() or "0.00" in price_text:
        return False
    if card_number_digits and card_number_digits not in title_digits:
        return False
    norm_character = character.replace("-", "").lower()
    norm_title = title_lower.replace("-", "")
    if norm_character not in norm_title:
        return False
    return True

async def scrape_card(unique_id, query, semaphore, completed_set):
    if unique_id in completed_set:
        print(f"Skipping {unique_id} - already scraped")
        return

    async with semaphore:
        async with async_session() as session:
            print(f"\nScraping eBay sold for: {query} ({unique_id})")
            urls_used_tracker = defaultdict(set)
            search_url = build_search_url(query)
            print(f"Search URL: {search_url}")

            try:
                results = parse_ebay_sold_page(query, max_items=MAX_RESULTS)
            except Exception as e:
                print(f"Scrape error for {unique_id}: {e}")
                try:
                    await session.execute(text("""
                        INSERT INTO scrape_failures (unique_id, scraper_source, error_message, urls_used)
                        VALUES (:unique_id, :scraper_source, :error_message, :urls_used)
                    """), {
                        "unique_id": unique_id,
                        "scraper_source": "ebay_sold",
                        "error_message": str(e),
                        "urls_used": json.dumps([search_url])
                    })
                    await session.commit()
                except Exception as inner:
                    print(f"Failed to log scrape error for {unique_id}: {inner}")
                await asyncio.sleep(CARD_SCRAPE_DELAY)
                return

            grouped = defaultdict(list)
            listings_by_date = defaultdict(list)

            for item in results:
                title = item.get("title", "").strip()
                price = item.get("price")
                sold_date = item.get("sold_date")
                url = item.get("url")
                if not title or price is None or not sold_date or not url:
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
                print(f"No valid prices for {unique_id}, logging null result.")
                try:
                    await session.execute(text("""
                        INSERT INTO ebay_sold_nulls (unique_id, query_used, logged_at, urls_used)
                        VALUES (:unique_id, :query_used, :logged_at, :urls_used)
                    """), {
                        "unique_id": unique_id,
                        "query_used": query,
                        "logged_at": datetime.utcnow(),
                        "urls_used": json.dumps([search_url])
                    })
                    await session.commit()
                except Exception as e:
                    print(f"Failed to log null for {unique_id}: {e}")
                await asyncio.sleep(CARD_SCRAPE_DELAY)
                return

            for sold_date, prices in grouped.items():
                filtered_step1 = filter_outliers(prices)
                median_val = calculate_median(filtered_step1)
                if median_val == 0 or median_val is None:
                    final_filtered = []
                else:
                    threshold = 0.5 if median_val > 10 else 0.4
                    final_filtered = [p for p in filtered_step1 if abs(p - median_val) / median_val <= threshold]

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
                    print(f"Logged {sale_count} sales for {unique_id} on {sold_date}")
                except Exception as e:
                    print(f"DB insert error for {unique_id} on {sold_date}: {e}")
                    await session.rollback()

            await asyncio.sleep(CARD_SCRAPE_DELAY)

async def run_ebay_sold_scraper():
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT DISTINCT unique_id FROM dailypricelog
            UNION
            SELECT DISTINCT unique_id FROM ebay_sold_nulls
        """))
        completed_set = set(row[0] for row in result.fetchall())

        result = await session.execute(text("SELECT unique_id, query FROM mastercard_v2"))
        cards = result.fetchall()

    semaphore = asyncio.Semaphore(CONCURRENT_SCRAPE_LIMIT)
    tasks = [
        scrape_card(unique_id, query, semaphore, completed_set)
        for unique_id, query in cards
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(run_ebay_sold_scraper())

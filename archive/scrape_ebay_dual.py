# ===========================================
# CardCatch: scrape_ebay_dual.py (JSON MODE)
# Location: /archive/
# Purpose: Tier-based eBay Sold + Active scraper (uses cards_due.json)
# ===========================================

import os
import sys
import json
import asyncio
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import text

# === Load .env and force correct async driver ===
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not set in .env")
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

# === Import CardCatch core ===
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import filter_outliers, calculate_median, calculate_average
from scraper import parse_ebay_sold_page, parse_ebay_active_page

# === DB Setup ===
engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

# === Config ===
MAX_SOLD_RESULTS = 240
MAX_ACTIVE_RESULTS = 240
CONCURRENT_LIMIT = 5
CARD_DELAY = 0.75

print("\n🟢 scrape_ebay_dual.py started (cards_due.json mode)")

# === Scrape & Log ===
async def scrape_card(unique_id, query, tier):
    async with async_session() as session:
        print(f"\n🃏 {unique_id} | {query} | Tier {tier}")
        sold_success, active_success = False, False

        # Sold
        try:
            sold_results = parse_ebay_sold_page(query, max_items=MAX_SOLD_RESULTS)
            grouped_by_date = defaultdict(list)
            url_tracker = defaultdict(set)

            for item in sold_results:
                sold_date = item.get("sold_date")
                price = item.get("price")
                url = item.get("url")
                title = item.get("title")
                condition = item.get("condition", "Unknown")
                if not sold_date or price is None:
                    continue
                dt = datetime.strptime(sold_date, "%Y-%m-%d").date()
                grouped_by_date[dt].append(price)
                url_tracker[dt].add(url)

                # Insert raw listing
                await session.execute(text("""
                    INSERT INTO raw_ebay_sold (unique_id, query, title, price, quantity, date, url, condition)
                    VALUES (:uid, :query, :title, :price, 1, :date, :url, :condition)
                """), {
                    "uid": unique_id,
                    "query": query,
                    "title": title,
                    "price": price,
                    "date": dt,
                    "url": url,
                    "condition": condition
                })

            for sold_date, prices in grouped_by_date.items():
                filtered = filter_outliers(prices)
                median = calculate_median(filtered)
                average = calculate_average(filtered)
                sale_count = len(filtered)
                urls = json.dumps(list(url_tracker[sold_date]))

                await session.execute(text("""
                    INSERT INTO dailypricelog (unique_id, sold_date, median_price, average_price, sale_count, query_used, urls_used)
                    VALUES (:uid, :dt, :median, :avg, :count, :query, :urls)
                """), {
                    "uid": unique_id,
                    "dt": sold_date,
                    "median": median,
                    "avg": average,
                    "count": sale_count,
                    "query": query,
                    "urls": urls
                })
            sold_success = True

        except Exception as e:
            print(f"❌ Sold error for {unique_id}: {e}")

        # Active
        try:
            active_results = parse_ebay_active_page(query, max_items=MAX_ACTIVE_RESULTS)
            prices = [item["price"] for item in active_results if "price" in item]
            filtered = filter_outliers(prices)
            best = min(filtered) if filtered else None
            median = calculate_median(filtered)
            average = calculate_average(filtered)
            count = len(filtered)
            active_url = f"https://www.ebay.co.uk/sch/i.html?_nkw={query.replace(' ', '+')}&LH_BIN=1&LH_PrefLoc=1"

            if count > 0:
                await session.execute(text("""
                    INSERT INTO activedailypricelog (unique_id, active_date, median_price, average_price, sale_count, query_used, card_number, url_used, lowest_price)
                    VALUES (:uid, :dt, :median, :avg, :count, :query, :card, :url, :low)
                """), {
                    "uid": unique_id,
                    "dt": datetime.utcnow().date(),
                    "median": median,
                    "avg": average,
                    "count": count,
                    "query": query,
                    "card": query.split()[-1],
                    "url": active_url,
                    "low": best
                })
            active_success = True

        except Exception as e:
            print(f"❌ Active error for {unique_id}: {e}")

        await session.commit()
        print(f"✅ Done: {unique_id} | Sold: {'✔️' if sold_success else '❌'} | Active: {'✔️' if active_success else '❌'}")
        await asyncio.sleep(CARD_DELAY)

# === Run from JSON file ===
async def run_dual_scraper():
    try:
        with open("cards_due.json", "r") as f:
            cards = json.load(f)
    except Exception as e:
        print(f"❌ Failed to load cards_due.json: {e}")
        return

    print(f"🔁 Starting run on {len(cards)} cards from file")
    sem = asyncio.Semaphore(CONCURRENT_LIMIT)
    tasks = [run_card_with_semaphore(c["unique_id"], c["query"], c["tier"], sem) for c in cards]
    await asyncio.gather(*tasks)
    print("✅ scrape_ebay_dual.py finished")

async def run_card_with_semaphore(uid, q, t, sem):
    async with sem:
        await scrape_card(uid, q, t)

if __name__ == "__main__":
    asyncio.run(run_dual_scraper())

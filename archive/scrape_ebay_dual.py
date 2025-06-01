# ===========================================
# CardCatch: # Location: /archive/
# Purpose: Tier-based eBay Sold + Active scraper
# Priority: Critical / high accuracy / traceable
# ===========================================

import os
import sys
import json
import asyncio

print("🟢 Starting scrape_ebay_dual.py")

from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text
from utils import filter_outliers, calculate_median, calculate_average
from scraper import parse_ebay_sold_page, parse_ebay_active_page  # Confirmed naming

# === Load .env ===
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not set in .env")

# === DB Setup ===
engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

# === Config ===
MAX_SOLD_RESULTS = 240
MAX_ACTIVE_RESULTS = 240
CONCURRENT_LIMIT = 5
CARD_DELAY = 0.75

print("✅ Scraper config loaded.")

# === Scrape & Log Logic Per Card ===
async def scrape_card(unique_id, query, tier):
    async with async_session() as session:
        print(f"\n🃏 Scraping: {query} (UID: {unique_id}, Tier: {tier})")

        sold_success, active_success = False, False

        # --- Sold Listings ---
        try:
            sold_results = parse_ebay_sold_page(query, max_items=MAX_SOLD_RESULTS)
            grouped_by_date = defaultdict(list)
            url_tracker = defaultdict(set)

            for item in sold_results:
                sold_date = item.get("sold_date")
                price = item.get("price")
                url = item.get("url")
                if not sold_date or price is None:
                    continue
                dt = datetime.strptime(sold_date, "%Y-%m-%d").date()
                grouped_by_date[dt].append(price)
                url_tracker[dt].add(url)

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
            print(f"❌ Sold scrape error for {unique_id}: {e}")

        # --- Active Listings ---
        try:
            active_results = parse_ebay_active_page(query, max_items=MAX_ACTIVE_RESULTS)
            active_prices = [item["price"] for item in active_results if "price" in item]
            filtered_prices = filter_outliers(active_prices)
            best_price = min(filtered_prices) if filtered_prices else None
            median = calculate_median(filtered_prices)
            average = calculate_average(filtered_prices)
            count = len(filtered_prices)
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
                    "low": best_price
                })
            active_success = True

        except Exception as e:
            print(f"❌ Active scrape error for {unique_id}: {e}")

        await session.commit()
        print(f"✅ Finished: {unique_id} — Sold: {'✔️' if sold_success else '❌'}, Active: {'✔️' if active_success else '❌'}")
        await asyncio.sleep(CARD_DELAY)

# === Tier-Based Runner ===
async def run_dual_scraper():
    async with async_session() as session:
        result = await session.execute(text("""
            SELECT unique_id, query, tier
            FROM mastercard_v2
            WHERE tier IS NOT NULL
            ORDER BY tier ASC
        """))
        cards = result.fetchall()

    print(f"🔁 Running dual scraper on {len(cards)} cards...")
    semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)
    tasks = [run_card_with_semaphore(uid, q, t, semaphore) for uid, q, t in cards]
    await asyncio.gather(*tasks)
    print("✅ Dual scraper run complete.")

async def run_card_with_semaphore(unique_id, query, tier, semaphore):
    async with semaphore:
        await scrape_card(unique_id, query, tier)

if __name__ == "__main__":
    asyncio.run(run_dual_scraper())

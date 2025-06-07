import os
import sys
import json
import asyncio
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import text

# === Load .env and force async driver ===
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

# === Import shared logic ===
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import filter_outliers, calculate_median, calculate_average, parse_card_meta
from scraper import parse_ebay_sold_page, parse_ebay_active_page

# === DB setup ===
engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

# === Config ===
MAX_SOLD_RESULTS = 120
MAX_ACTIVE_RESULTS = 120
CONCURRENT_LIMIT = 5
CARD_DELAY = 0.75

print("\n🟢 scrape_ebay_dual.py started (cards_due.json mode)")

# === Main scrape function per card ===
async def scrape_card(unique_id, query, tier):
    async with async_session() as session:
        print(f"\n🃏 {unique_id} | {query} | Tier {tier}")
        sold_success, active_success = False, False

        # === SOLD listings ===
        try:
            sold_result = parse_ebay_sold_page(query, max_items=MAX_SOLD_RESULTS)
            sold_raw = sold_result.get("raw", [])
            sold_filtered = sold_result.get("filtered", [])
            search_url = sold_result.get("url", "")

            print(f"🔍 Sold raw: {len(sold_raw)} | Filtered: {len(sold_filtered)}")
            if not sold_raw and not sold_filtered:
                print(f"⚠️ No sold listings returned at all — possible eBay block or scrape fail for {unique_id}")

            for item in sold_raw:
                await session.execute(text("""
                    INSERT INTO raw_ebay_sold (unique_id, query, title, price, quantity, date, url, condition, holo_type)
                    VALUES (:uid, :query, :title, :price, 1, :date, :url, :condition, :holo)
                """), {
                    "uid": unique_id,
                    "query": query,
                    "title": item.get("title"),
                    "price": item.get("price"),
                    "date": item.get("sold_date"),
                    "url": item.get("url"),
                    "condition": item.get("condition"),
                    "holo": item.get("holo_type")
                })

            if not sold_filtered:
                await session.execute(text("""
                    INSERT INTO ebay_sold_nulls (unique_id, query_used, search_url, reason)
                    VALUES (:uid, :query, :url, :reason)
                """), {
                    "uid": unique_id,
                    "query": query,
                    "url": search_url,
                    "reason": "No filtered results"
                })
            else:
                grouped_by_date = defaultdict(list)
                url_tracker = defaultdict(set)

                for item in sold_filtered:
                    dt = datetime.strptime(item["sold_date"], "%Y-%m-%d").date()
                    grouped_by_date[dt].append(item["price"])
                    url_tracker[dt].add(item["url"])

                for sold_date, prices in grouped_by_date.items():
                    filtered = filter_outliers(prices)
                    if not filtered:
                        print(f"⚠️ Sold prices filtered out completely for {unique_id} on {sold_date}")
                        continue
                    median_val = calculate_median(filtered)
                    average = calculate_average(filtered)
                    sale_count = len(filtered)
                    urls = json.dumps(list(url_tracker[sold_date]))
                    trusted = True

                    await session.execute(text("""
                        INSERT INTO dailypricelog (
                            unique_id, sold_date, median_price, average_price,
                            sale_count, query_used, urls_used, trusted
                        )
                        VALUES (
                            :uid, :dt, :median, :avg,
                            :count, :query, :urls, :trusted
                        )
                    """), {
                        "uid": unique_id,
                        "dt": sold_date,
                        "median": median_val,
                        "avg": average,
                        "count": sale_count,
                        "query": query,
                        "urls": urls,
                        "trusted": trusted
                    })

            sold_success = True

        except Exception as e:
            print(f"❌ Sold error for {unique_id}: {e}")

        # === ACTIVE listings ===
        try:
            active_result = parse_ebay_active_page(query, max_items=MAX_ACTIVE_RESULTS)
            active_raw = active_result.get("raw", [])
            active_filtered = active_result.get("filtered", [])
            search_url = active_result.get("url", "")

            print(f"🔍 Active raw: {len(active_raw)} | Filtered: {len(active_filtered)}")
            if not active_raw and not active_filtered:
                print(f"⚠️ No active listings returned at all — possible eBay block or scrape fail for {unique_id}")

            prices = []
            for item in active_raw:
                await session.execute(text("""
                    INSERT INTO raw_ebay_active (unique_id, query, title, price, quantity, date, url, condition, holo_type)
                    VALUES (:uid, :query, :title, :price, 1, :date, :url, :condition, :holo)
                """), {
                    "uid": unique_id,
                    "query": query,
                    "title": item.get("title"),
                    "price": item.get("price"),
                    "date": datetime.utcnow().date(),
                    "url": item.get("url"),
                    "condition": item.get("condition"),
                    "holo": item.get("holo_type")
                })
                prices.append(item.get("price"))

            filtered = filter_outliers(prices)
            best = min(filtered) if filtered else None
            median_val = calculate_median(filtered)
            average = calculate_average(filtered)
            count = len(filtered)
            trusted = True

            if count > 0:
                _, digits = parse_card_meta(query)
                await session.execute(text("""
                    INSERT INTO activedailypricelog (
                        unique_id, active_date, median_price, average_price,
                        sale_count, query_used, card_number, url_used,
                        lowest_price, trusted
                    )
                    VALUES (
                        :uid, :dt, :median, :avg,
                        :count, :query, :card, :url,
                        :low, :trusted
                    )
                """), {
                    "uid": unique_id,
                    "dt": datetime.utcnow().date(),
                    "median": median_val,
                    "avg": average,
                    "count": count,
                    "query": query,
                    "card": digits,
                    "url": search_url,
                    "low": best,
                    "trusted": trusted
                })

            active_success = True

        except Exception as e:
            print(f"❌ Active error for {unique_id}: {e}")

        await session.commit()
        print(f"✅ Done: {unique_id} | Sold: {'✔️' if sold_success else '❌'} | Active: {'✔️' if active_success else '❌'}")
        await asyncio.sleep(CARD_DELAY)

# === Run full batch from cards_due.json ===
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

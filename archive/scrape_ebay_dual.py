import os
import sys
import json
import asyncio
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import text
import traceback

# === Load .env and force async driver ===
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

# === Import shared logic ===
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import filter_outliers, calculate_median, calculate_average, parse_card_meta, is_valid_price, is_valid_title
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
                if not item.get("price") or not item.get("sold_date"):
                    continue

                # Determine inclusion and reason(s)
                reasons = []
                if not is_valid_price(item["price"]):
                    reasons.append("price")
                if not is_valid_title(item["title"], item["character"], item["card_number"]):
                    reasons.append("title")
                if datetime.strptime(item["sold_date"], "%Y-%m-%d").date() < (datetime.utcnow().date() - timedelta(days=90)):
                    reasons.append("date")

                included = not reasons
                reason_excluded = None if included else ",".join(reasons)

                # Insert into raw_ebay_sold_debug
                await session.execute(text("""
                    INSERT INTO raw_ebay_sold_debug (
                        unique_id, query_used, title, price, sold_date,
                        url, condition, holo_type, included, reason_excluded
                    )
                    VALUES (
                        :uid, :query, :title, :price, :sold_date,
                        :url, :condition, :holo, :included, :reason
                    )
                """), {
                    "uid": unique_id,
                    "query": query,
                    "title": item["title"],
                    "price": item["price"],
                    "sold_date": datetime.strptime(item["sold_date"], "%Y-%m-%d").date(),
                    "url": item["url"],
                    "condition": item["condition"],
                    "holo": item["holo_type"],
                    "included": included,
                    "reason": reason_excluded
                })

            await session.commit()

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
                await session.commit()
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

                    await session.execute(text("""
                        INSERT INTO dailypricelog (
                            unique_id, sold_date, median_price, average_price,
                            sale_count, query_used, urls_used, trusted
                        )
                        VALUES (
                            :uid, :dt, :median, :avg,
                            :count, :query, :urls, TRUE
                        )
                    """), {
                        "uid": unique_id,
                        "dt": sold_date,
                        "median": median_val,
                        "avg": average,
                        "count": sale_count,
                        "query": query,
                        "urls": urls
                    })
                await session.commit()
            sold_success = True

        except Exception as e:
            print(f"❌ Sold error for {unique_id}: {e}")
            traceback.print_exc()

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
                if not item.get("price"):
                    continue
                await session.execute(text("""
                    INSERT INTO raw_ebay_active (unique_id, query, title, price, quantity, date, url, condition, holo_type)
                    VALUES (:uid, :query, :title, :price, 1, :date, :url, :condition, :holo)
                """), {
                    "uid": unique_id,
                    "query": query,
                    "title": item["title"],
                    "price": item["price"],
                    "date": datetime.utcnow().date(),
                    "url": item["url"],
                    "condition": item["condition"],
                    "holo": item["holo_type"]
                })
                prices.append(item.get("price"))
            await session.commit()

            filtered = filter_outliers(prices)
            if filtered:
                median_val = calculate_median(filtered)
                average = calculate_average(filtered)
                best = min(filtered)
                count = len(filtered)
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
                        :low, TRUE
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
                    "low": best
                })
                await session.commit()

            active_success = True

        except Exception as e:
            print(f"❌ Active error for {unique_id}: {e}")
            traceback.print_exc()

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

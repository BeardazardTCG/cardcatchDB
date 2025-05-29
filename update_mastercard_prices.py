import os
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy import text
from collections import defaultdict

# === Config ===
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

engine = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

# === Outlier Filtering ===
def filter_outliers(prices):
    if not prices:
        return []
    sorted_prices = sorted(prices)
    q1 = sorted_prices[len(sorted_prices) // 4]
    q3 = sorted_prices[(len(sorted_prices) * 3) // 4]
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return [p for p in prices if lower <= p <= upper]

# === Median ===
def calculate_median(prices):
    if not prices:
        return None
    sorted_prices = sorted(prices)
    mid = len(prices) // 2
    if len(prices) % 2 == 0:
        return (sorted_prices[mid - 1] + sorted_prices[mid]) / 2
    return sorted_prices[mid]

# === Batch SQL Generator ===
def generate_update_sql(batch):
    value_str = ", ".join([f"('{uid}', {price})" for uid, price in batch])
    return f"""
        UPDATE mastercard_v2 AS m
        SET sold_ebay_median = v.median
        FROM (VALUES {value_str}) AS v(unique_id, median)
        WHERE m.unique_id = v.unique_id;
    """

# === Main Async Runner ===
async def main():
    print("🔌 Connecting to database...")
    async with SessionLocal() as session:
        # === SOLD MEDIANS ===
        print("📦 Fetching sold prices...")
        result = await session.execute(text("""
            SELECT unique_id, median_price FROM dailypricelog
            WHERE median_price IS NOT NULL
        """))
        rows = result.fetchall()

        sold_map = defaultdict(list)
        for uid, price in rows:
            sold_map[uid.strip()].append(float(price))

        print(f"💾 Updating {len(sold_map)} sold medians in VALUE-batches...")
        batch = []
        BATCH_SIZE = 500

        batches = []
        for uid, prices in sold_map.items():
            filtered = filter_outliers(prices)
            median = calculate_median(filtered)
            if median is not None:
                batch.append((uid, round(median, 2)))
                if len(batch) >= BATCH_SIZE:
                    batches.append(batch)
                    batch = []
        if batch:
            batches.append(batch)

        for i, batch in enumerate(batches, 1):
            sql = generate_update_sql(batch)
            try:
                await session.execute(text(sql))
                await session.commit()
                print(f"🔄 Committed batch {i * BATCH_SIZE - BATCH_SIZE + 1}–{i * BATCH_SIZE}")
                await asyncio.sleep(0.3)  # Allow DB to release locks
            except Exception as e:
                print(f"❌ Failed batch {i}: {e}")
                await session.rollback()

        print("✅ All sold medians updated!")

if __name__ == "__main__":
    asyncio.run(main())

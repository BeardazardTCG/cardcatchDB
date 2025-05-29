import os
import asyncio
from collections import defaultdict
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import text

# === DATABASE SETUP ===
# Hardcoded fallback URL for production use
DATABASE_URL = os.getenv("DATABASE_URL") or "postgresql+asyncpg://postgres:ckQFRJkrJluWsJnHsDhlhvbtSridadDF@metro.proxy.rlwy.net:52025/railway"

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

# === OUTLIER FILTERING ===
def filter_outliers(prices):
    if not prices:
        return []
    sorted_prices = sorted(prices)
    q1 = sorted_prices[len(sorted_prices) // 4]
    q3 = sorted_prices[(len(sorted_prices) * 3) // 4]
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    return [p for p in prices if lower_bound <= p <= upper_bound]

def calculate_median(prices):
    n = len(prices)
    if n == 0:
        return None
    sorted_prices = sorted(prices)
    mid = n // 2
    return (sorted_prices[mid - 1] + sorted_prices[mid]) / 2 if n % 2 == 0 else sorted_prices[mid]

# === MAIN EXECUTION ===
async def main():
    print("🔌 Connecting to database...")
    async with async_session() as session:

        # === SOLD PRICES ===
        print("📦 Fetching sold prices...")
        sold_result = await session.execute(text("""
            SELECT unique_id, median_price
            FROM dailypricelog
            WHERE median_price IS NOT NULL
        """))
        sold_map = defaultdict(list)
        for uid, price in sold_result.fetchall():
            sold_map[uid.strip()].append(float(price))

        updates = []
        for uid, prices in sold_map.items():
            filtered = filter_outliers(prices)
            median = calculate_median(filtered)
            if median is not None:
                updates.append({"median": round(median, 2), "uid": uid})

        print(f"💾 Updating {len(updates)} sold medians...")
        await session.execute_many(
            text("UPDATE mastercard_v2 SET sold_ebay_median = :median WHERE unique_id = :uid"),
            updates
        )

        # === TCGPLAYER PRICES ===
        print("📦 Fetching latest TCGPlayer prices...")
        tcg_result = await session.execute(text("""
            SELECT DISTINCT ON (unique_id) unique_id, market_price
            FROM tcg_pricing_log
            WHERE market_price IS NOT NULL
            ORDER BY unique_id, date DESC
        """))
        tcg_updates = [
            {"uid": uid.strip(), "price": round(float(price), 2)}
            for uid, price in tcg_result.fetchall()
        ]

        print(f"💾 Updating {len(tcg_updates)} TCG prices...")
        await session.execute_many(
            text("UPDATE mastercard_v2 SET tcgplayer_market_price = :price WHERE unique_id = :uid"),
            tcg_updates
        )

        # === ACTIVE PRICES ===
        print("📦 Fetching active BIN prices...")
        try:
            active_result = await session.execute(text("""
                SELECT unique_id, lowest_price
                FROM activedailypricelog
                WHERE lowest_price IS NOT NULL
                  AND date = CURRENT_DATE
            """))
            active_map = defaultdict(list)
            for uid, price in active_result.fetchall():
                active_map[uid.strip()].append(float(price))

            active_updates = []
            for uid, prices in active_map.items():
                filtered = filter_outliers(prices)
                if filtered:
                    active_updates.append({"price": round(min(filtered), 2), "uid": uid})

            print(f"💾 Updating {len(active_updates)} active BIN prices...")
            await session.execute_many(
                text("UPDATE mastercard_v2 SET active_ebay_lowest = :price WHERE unique_id = :uid"),
                active_updates
            )
        except Exception as e:
            print(f"⚠️ Skipping active BIN update: {e}")

        await session.commit()
        print("🏁 All updates completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())

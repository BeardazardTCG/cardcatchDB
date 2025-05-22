# generate_trend_tracker.py
# ✅ Rebuilds the TrendTracker table with filtered 30-day sale history

import asyncio
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlmodel import SQLModel, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text
from models import TrendTracker

import os

# === Load environment
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set in .env")

# === DB setup
engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

# === Outlier filter using IQR
def filter_outliers_iqr(prices):
    if len(prices) < 4:
        return prices
    sorted_prices = sorted(prices)
    q1 = sorted_prices[len(prices) // 4]
    q3 = sorted_prices[(len(prices) * 3) // 4]
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return [p for p in prices if lower <= p <= upper]

# === Main trend generation function
async def generate_trend_tracker():
    async with async_session() as session:
        cutoff_date = datetime.today().date() - timedelta(days=30)

        result = await session.execute(text("""
            SELECT d.unique_id, d.median_price, d.sold_date, 
                   m.card_name, m.set_name, m.tier
            FROM dailypricelog d
            JOIN mastercard m ON d.unique_id = m.unique_id
            WHERE d.sold_date::date >= :cutoff 
              AND m.tier::text != '4'
            ORDER BY d.unique_id, d.sold_date DESC
        """), {"cutoff": cutoff_date})

        rows = result.fetchall()
        print(f"🔍 Total joined rows fetched: {len(rows)}")

        # Group by UID
        grouped = {}
        for uid, price, sold_date, name, set_name, tier in rows:
            if uid not in grouped:
                grouped[uid] = {
                    "card_name": name,
                    "set_name": set_name,
                    "prices": []
                }
            if price is not None:
                try:
                    grouped[uid]["prices"].append((sold_date, float(price)))
                except (TypeError, ValueError):
                    continue

        inserts = []

        for uid, data in grouped.items():
            sorted_prices = sorted(data["prices"], key=lambda x: x[0], reverse=True)
            prices_only = [p[1] for p in sorted_prices]

            prices_clean = filter_outliers_iqr(prices_only)
            if len(prices_clean) < 3:
                continue

            last, second, third = prices_clean[:3]
            avg = round(sum(prices_clean) / len(prices_clean), 2)

            # Calculate percentage changes
            try:
                pct_stable = round((last - second) / second * 100, 2)
                pct_spike = round((last - third) / third * 100, 2)
            except ZeroDivisionError:
                pct_stable = pct_spike = 0.0

            def tag_trend(pct):
                if pct >= 20:
                    return "📈"
                elif pct <= -15:
                    return "📉"
                else:
                    return "↔️"

            inserts.append(TrendTracker(
                unique_id=str(uid),
                card_name=data["card_name"],
                set_name=data["set_name"],
                last_price=last,
                second_last=second,
                third_last=third,
                average_30d=avg,
                sample_size=len(prices_clean),
                pct_change_stable=pct_stable,
                pct_change_spike=pct_spike,
                trend_stable=tag_trend(pct_stable),
                trend_spike=tag_trend(pct_spike),
                updated_at=datetime.utcnow()
            ))

        await session.execute(text("DELETE FROM trendtracker"))
        session.add_all(inserts)
        await session.commit()
        print(f"✅ TrendTracker rebuilt: {len(inserts)} cards processed.")

# === Run
if __name__ == "__main__":
    asyncio.run(generate_trend_tracker())

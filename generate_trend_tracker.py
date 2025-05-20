import asyncio
from datetime import datetime, timedelta
from db import get_session
from models import TrendTracker
from sqlalchemy import text

async def generate_trend_tracker():
    async with get_session() as session:
        cutoff_date = datetime.today().date() - timedelta(days=30)

        # Raw SQL for simplicity and speed
        result = await session.execute(text(f"""
            SELECT d.unique_id, d.median_price, d.sold_date, 
                   m.card_name, m.set_name, m.tier
            FROM dailypricelog d
            JOIN mastercard m ON d.unique_id = m.unique_id
            WHERE d.sold_date::date >= :cutoff 
              AND m.tier::text != '4'
            ORDER BY d.unique_id, d.sold_date DESC
        """), {"cutoff": cutoff_date})
        
        rows = result.fetchall()

        grouped = {}
        for uid, price, sold_date, name, set_name, tier in rows:
            if uid not in grouped:
                grouped[uid] = {
                    "card_name": name,
                    "set_name": set_name,
                    "prices": []
                }
            grouped[uid]["prices"].append(float(price))

        inserts = []

        for uid, data in grouped.items():
            prices = data["prices"]
            if len(prices) < 3:
                continue

            last = prices[0]
            second = prices[1] if len(prices) > 1 else None
            third = prices[2] if len(prices) > 2 else None
            avg = round(sum(prices) / len(prices), 2)

            if third and third != 0:
                pct_change = round(((last - third) / third) * 100, 2)
                trend = "📈" if pct_change > 5 else "📉" if pct_change < -5 else "➡️"
            else:
                pct_change = None
                trend = "⚠️"

            inserts.append(TrendTracker(
                unique_id=uid,
                card_name=data["card_name"],
                set_name=data["set_name"],
                last_price=last,
                second_last=second,
                third_last=third,
                average_30d=avg,
                sample_size=len(prices),
                pct_change=pct_change,
                trend=trend
            ))

        await session.execute(text("DELETE FROM trendtracker"))
        session.add_all(inserts)
        await session.commit()
        print(f"✅ Trend tracker generated for {len(inserts)} cards.")

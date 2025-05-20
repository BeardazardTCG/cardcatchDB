import asyncio
from datetime import datetime, timedelta
from db import get_session
from models import DailyPriceLog, MasterCard, TrendTracker
from utils import filter_outliers, calculate_median, calculate_average

async def generate_trend_tracker():
    async with get_session() as session:
        cutoff_date = datetime.today().date() - timedelta(days=30)

        result = await session.execute(
            """
            SELECT d.unique_id, d.median_price, d.sold_date, 
                   m.card_name, m.set_name, m.tier
            FROM dailypricelog d
            JOIN mastercard m ON d.unique_id = m.unique_id
            WHERE d.sold_date::date >= :cutoff AND m.tier::text != '4'
            ORDER BY d.unique_id, d.sold_date DESC
            """,
            {"cutoff": cutoff_date}
        )
        rows = result.fetchall()

        grouped = {}
        for uid, price, sold_date, name, set_name, tier in rows:
            if uid not in grouped:
                grouped[uid] = {
                    "card_name": name,
                    "set_name": set_name,
                    "entries": []
                }
            grouped[uid]["entries"].append((sold_date, price))

        inserts = []

        for uid, data in grouped.items():
            entries = sorted(data["entries"], key=lambda x: x[0], reverse=True)
            medians = [float(p) for _, p in entries]

            if len(medians) < 3:
                continue  # require at least 3 total entries

            filtered = filter_outliers(medians)
            if len(filtered) < 2:
                continue  # skip if nothing remains usable

            last_price = filtered[0]
            second_last = filtered[1] if len(filtered) > 1 else None
            third_last = filtered[2] if len(filtered) > 2 else None
            avg_30d = round(sum(filtered) / len(filtered), 2)

            if third_last and third_last != 0:
                pct_change = round(((last_price - third_last) / third_last) * 100, 2)
                if pct_change > 5:
                    trend = "📈"
                elif pct_change < -5:
                    trend = "📉"
                else:
                    trend = "➡️"
            else:
                pct_change = None
                trend = "⚠️"

            inserts.append(TrendTracker(
                unique_id=uid,
                card_name=data["card_name"],
                set_name=data["set_name"],
                last_price=last_price,
                second_last=second_last,
                third_last=third_last,
                average_30d=avg_30d,
                sample_size=len(filtered),
                pct_change=pct_change,
                trend=trend
            ))

        await session.execute("DELETE FROM trendtracker")
        session.add_all(inserts)
        await session.commit()
        print(f"✅ Trend tracker generated for {len(inserts)} cards.")

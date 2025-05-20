import asyncio
from datetime import datetime, timedelta
from db import get_session
from models import TrendTracker
from sqlalchemy import text

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

async def generate_trend_tracker():
    async with get_session() as session:
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
            # Sort prices by date
            sorted_prices = sorted(data["prices"], key=lambda x: x[0], reverse=True)
            prices_only = [p[1] for p in sorted_prices]

            # Filter outliers
            prices_clean = filter_outliers_iqr(prices_only)

            if len(prices_clean) < 3:
                continue

            last = prices_clean[0]
            second = prices_clean[1]
            third = prices_clean[2]
            avg = round(sum(prices_clean) / len(prices_clean), 2)

            # 📊 Stable: last vs average
            try:
                stable_change = round(((last - avg) / avg) * 100, 2)
            except ZeroDivisionError:
                stable_change = None
            if stable_change is not None:
                if stable_change > 5:
                    trend_stable = "📈"
                elif stable_change < -5:
                    trend_stable = "📉"
                else:
                    trend_stable = "➡️"
            else:
                trend_stable = "⚠️"

            # ⚡ Spike: last vs third
            try:
                spike_change = round(((last - third) / third) * 100, 2)
            except ZeroDivisionError:
                spike_change = None
            if spike_change is not None:
                if spike_change > 5:
                    trend_spike = "📈"
                elif spike_change < -5:
                    trend_spike = "📉"
                else:
                    trend_spike = "➡️"
            else:
                trend_spike = "⚠️"

            print(f"🧠 UID {uid} → 📊 {trend_stable} ({stable_change}%) | ⚡ {trend_spike} ({spike_change}%)")

            inserts.append(TrendTracker(
                unique_id=str(uid),
                card_name=data["card_name"],
                set_name=data["set_name"],
                last_price=last,
                second_last=second,
                third_last=third,
                average_30d=avg,
                sample_size=len(prices_clean),
                pct_change_stable=stable_change,
                pct_change_spike=spike_change,
                trend_stable=trend_stable,
                trend_spike=trend_spike
            ))

        print(f"📦 Cards with ≥3 entries after outlier filter: {len(inserts)}")

        await session.execute(text("DELETE FROM trendtracker"))
        session.add_all(inserts)
        await session.commit()
        print(f"✅ Trend tracker generated for {len(inserts)} cards.")

if __name__ == "__main__":
    asyncio.run(generate_trend_tracker())

import asyncio
from db import get_session
from models import MasterCard, TrendTracker, SmartSuggestion
from sqlalchemy import select, delete

async def generate_smart_suggestions():
    async with get_session() as session:
        # Get trend data and price lookups
        trend_result = await session.execute(select(TrendTracker))
        trend_cards = trend_result.scalars().all()

        master_result = await session.execute(select(MasterCard))
        master_map = {str(c.unique_id): c for c in master_result.scalars().all()}

        suggestions = []

        for trend in trend_cards:
            uid = str(trend.unique_id)
            card = master_map.get(uid)
            if not card or card.clean_avg_price is None or card.net_resale_value is None:
                continue

            clean_price = round(card.clean_avg_price, 2)
            resale = round(card.net_resale_value, 2)
            trend_symbol = trend.trend_stable or "⚠️"
            status = "Unlisted"  # Always "Unlisted" for general market mode

            # Price targets
            target_sell = round(clean_price * 0.85, 2)
            target_buy = round(clean_price * 0.75 * (0.9 if trend_symbol == "📉" else 1), 2)

            action = None

            # Suggestion logic (customise to taste)
            if clean_price < 0.80:
                continue
            elif resale < 2:
                action = "Job Lot"
            elif resale < 5:
                action = "Bundle"
            elif clean_price <= target_buy * 1.05:
                action = "Buy Now"
            elif clean_price <= target_buy * 1.25:
                action = "Monitor"
            elif clean_price >= 9.80:
                action = "List Now"
            else:
                action = "Monitor"

            suggestions.append(SmartSuggestion(
                unique_id=uid,
                card_name=card.card_name,
                set_name=card.set_name,
                card_number=card.card_number,
                card_status=status,
                clean_price=clean_price,
                target_sell=target_sell,
                target_buy=target_buy,
                suggested_action=action,
                trend=trend_symbol,
                resale_value=resale
            ))
            print(f"✅ UID {uid} → {action} | {status} | resale={resale}, avg={clean_price}, trend={trend_symbol}")

        # Overwrite suggestions table
        await session.execute(delete(SmartSuggestion))
        session.add_all(suggestions)
        await session.commit()
        print(f"✅ Smart Suggestions generated for {len(suggestions)} cards.")

if __name__ == "__main__":
    asyncio.run(generate_smart_suggestions())

import asyncio
from db import get_session
from models import MasterCard, TrendTracker, SmartSuggestion
from sqlalchemy import select, delete

async def generate_smart_suggestions():
    async with get_session() as session:
        # Load TrendTracker data
        trend_result = await session.execute(select(TrendTracker))
        trend_cards = trend_result.scalars().all()

        # Load MasterCard reference data
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
            status = "Unlisted"  # General market mode

            # Price targets (used for display/reference)
            target_sell = round(clean_price * 0.85, 2)
            target_buy = round(clean_price * 0.75 * (0.9 if trend_symbol == "📉" else 1), 2)

            action = None

            # ✅ SMART SUGGESTIONS v3 — Value-Based Buyer Logic
            if clean_price < 0.80:
                continue
            elif clean_price <= 3 and trend_symbol == "📉":
                action = "Buy Now"
            elif clean_price <= 5 and resale >= 8:
                action = "Buy Now"
            elif resale >= 10 and clean_price <= 7:
                action = "Buy Now"
            elif resale >= 6 and clean_price <= resale * 0.9:
                action = "Monitor"
            else:
                continue  # No action worth suggesting

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
            print(f"✅ UID {uid} → {action} | resale={resale}, avg={clean_price}, trend={trend_symbol}")

        # Final commit
        await session.execute(delete(SmartSuggestion))
        session.add_all(suggestions)
        await session.commit()
        print(f"✅ Smart Suggestions generated for {len(suggestions)} cards.")

if __name__ == "__main__":
    asyncio.run(generate_smart_suggestions())

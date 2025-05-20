import asyncio
from db import get_session
from models import MasterCard, TrendTracker, SmartSuggestion
from sqlalchemy import select, delete

HOT_CHARACTERS = {
    "pikachu", "charizard", "eevee", "mewtwo", "mew", "charmander", "snorlax",
    "blastoise", "squirtle", "bulbasaur", "gengar", "ace", "raichu", "venusaur",
    "clefairy", "jolteon", "vaporeon", "articuno", "charmeleon", "zapdos"
}

def is_hot_character(name: str) -> bool:
    return any(char in name.lower() for char in HOT_CHARACTERS)

async def generate_smart_suggestions():
    async with get_session() as session:
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
            name = card.card_name or ""
            is_hot = is_hot_character(name)
            status = "Unlisted"

            if clean_price < 0.80 or resale < 0.80:
                continue

            action = None
            target_sell = round(clean_price * 0.85, 2)
            target_buy = round(clean_price * 0.75 * (0.9 if trend_symbol == "📉" else 1), 2)

            # ✅ Expanded buyer logic only
            if resale >= 20 and clean_price <= resale * 0.90:
                action = "Buy Now"
            elif resale >= 10 and clean_price <= resale * 0.95:
                action = "Buy Now"
            elif resale >= 7 and clean_price <= resale:
                action = "Safe Buy"
            elif resale >= 5 and clean_price <= resale * 0.90:
                action = "Safe Buy"
            elif resale < 5 and clean_price < 2.50 and is_hot:
                action = "Buy for Bundle"
            elif 2 <= resale < 5 and is_hot and clean_price < resale:
                action = "Buy for Bundle"
            elif resale >= 1.25 and clean_price <= 1.25:
                action = "Buy for Bundle"
            elif 4 <= resale < 7 and is_hot:
                action = "Collector Pick"
            elif resale < 4 and is_hot:
                action = "Collector Pick"
            elif trend_symbol == "➡️" and is_hot and resale > 2 and clean_price < resale:
                action = "Collector Pick"

            if action:
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

        await session.execute(delete(SmartSuggestion))
        session.add_all(suggestions)
        await session.commit()
        print(f"✅ Smart Suggestions v3.4 (buyers only) generated for {len(suggestions)} cards.")

if __name__ == "__main__":
    asyncio.run(generate_smart_suggestions())

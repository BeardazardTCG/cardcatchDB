# generate_smart_suggestions.py
# ✅ Generates suggested card purchases based on price gap and trend score

import asyncio
import os
from dotenv import load_dotenv
from sqlmodel import SQLModel, select, delete
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from models import MasterCard, TrendTracker, SmartSuggestion

# === Load .env config ===
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

async def get_session() -> AsyncSession:
    async with async_session() as session:
        yield session

# === Define "hot" characters — extra weight for demand
HOT_CHARACTERS = {
    "pikachu", "charizard", "eevee", "mewtwo", "mew", "charmander", "snorlax",
    "blastoise", "squirtle", "bulbasaur", "gengar", "ace", "raichu", "venusaur",
    "clefairy", "jolteon", "vaporeon", "articuno", "charmeleon", "zapdos"
}

def is_hot_character(name: str) -> bool:
    return any(char in name.lower() for char in HOT_CHARACTERS)

# === Core logic
async def generate_smart_suggestions():
    async with async_session() as session:
        # Clean previous suggestions
        await session.execute(delete(SmartSuggestion))

        # Load data
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

            if clean_price < 0.80 or resale < 0.80:
                continue

            action = None
            target_sell = round(clean_price * 0.85, 2)
            target_buy = round(clean_price * 0.75 * (0.9 if trend_symbol == "📉" else 1), 2)

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
                action = "Watch"

            if not action:
                continue

            suggestion = SmartSuggestion(
                unique_id=uid,
                card_name=card.card_name,
                set_name=card.set_name,
                card_number=card.card_number or "N/A",
                card_status=card.status or "Unknown",
                clean_price=clean_price,
                target_sell=target_sell,
                target_buy=target_buy,
                suggested_action=action,
                trend=trend_symbol,
                resale_value=resale
            )
            suggestions.append(suggestion)

        session.add_all(suggestions)
        await session.commit()
        print(f"✅ {len(suggestions)} smart suggestions generated.")

# === Run
if __name__ == "__main__":
    asyncio.run(generate_smart_suggestions())

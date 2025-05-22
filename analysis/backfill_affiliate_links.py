# backfill_affiliate_links.py
# ✅ Updates SmartSuggestion with affiliate eBay buy links

import os
import asyncio
from dotenv import load_dotenv
from sqlmodel import select, update
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from models.models import SmartSuggestion, MasterCard

# === Load .env
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set in .env")

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

def build_affiliate_link(query: str) -> str:
    # This is a basic eBay affiliate template—customize as needed
    base_url = "https://www.ebay.co.uk/sch/i.html"
    affiliate_tag = "&_trkparms=aff_source=cardcatch"
    return f"{base_url}?_nkw={query.replace(' ', '+')}{affiliate_tag}"

async def backfill_affiliate_links():
    async with async_session() as session:
        # Get all SmartSuggestions
        result = await session.execute(select(SmartSuggestion))
        suggestions = result.scalars().all()
        print(f"🔍 Loaded {len(suggestions)} suggestions.")

        updates = 0

        for s in suggestions:
            # Get the card's query from MasterCard
            card = None
            if s.unique_id:
                card_result = await session.execute(
                    select(MasterCard).where(MasterCard.unique_id == s.unique_id)
                )
                card = card_result.scalars().first()

            if not card or not card.query:
                continue

            link = build_affiliate_link(card.query)
            # Only update if needed
            if s.affiliate_buy_link != link:
                s.affiliate_buy_link = link
                updates += 1

        await session.commit()
        print(f"✅ Updated {updates} affiliate links.")

if __name__ == "__main__":
    asyncio.run(backfill_affiliate_links())

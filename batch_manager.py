# batch_manager.py

from models.models import MasterCard
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

class BatchManager:
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def get_batches(self, scraper_type: str):
        """
        scraper_type: "tcg", "sold", or "active"
        """
        # Set batch size based on scraper type
        batch_size = {
            "tcg": 1500,
            "sold": 100,
            "active": 130
        }.get(scraper_type, 100)  # Default to 100 if unknown

        tier_order = ["1", "2", "3", "4", None]  # Priority order

        cards_to_scrape = []

        for tier_value in tier_order:
            if tier_value is None:
                query = select(MasterCard).where((MasterCard.tier == None) | (MasterCard.tier == ""))
            else:
                query = select(MasterCard).where(MasterCard.tier == tier_value)

            result = await self.db_session.execute(query)
            cards = result.scalars().all()

            cards_to_scrape.extend(cards)

        # Slice into batches
        batches = []
        for i in range(0, len(cards_to_scrape), batch_size):
            batch = cards_to_scrape[i:i + batch_size]
            batches.append(batch)

        return batches

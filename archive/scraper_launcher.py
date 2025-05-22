# scraper_launcher.py

import asyncio
from batch_manager import BatchManager
from models.models import MasterCard
from sqlalchemy.ext.asyncio import AsyncSession

class ScraperLauncher:
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session
        self.batch_manager = BatchManager(db_session)

    async def run_all_scrapers(self):
        """
        Launch TCG, Sold, and Active scrapers in parallel.
        """
        await asyncio.gather(
            self.run_scraper(scraper_type="tcg"),
            self.run_scraper(scraper_type="sold"),
            self.run_scraper(scraper_type="active")
        )

    async def run_scraper(self, scraper_type: str):
        """
        Handles scraping for one type ("tcg", "sold", "active").
        """
        print(f"Starting scraper for {scraper_type.upper()}...")

        batches = await self.batch_manager.get_batches(scraper_type)

        for idx, batch in enumerate(batches):
            try:
                print(f"Scraping batch {idx+1}/{len(batches)} for {scraper_type.upper()}...")

                if scraper_type == "tcg":
                    await self.scrape_tcg_batch(batch)
                elif scraper_type == "sold":
                    await self.scrape_ebay_sold_batch(batch)
                elif scraper_type == "active":
                    await self.scrape_ebay_active_batch(batch)

                # TODO: After scrape, save checkpoint here if needed

            except Exception as e:
                print(f"Error scraping {scraper_type.upper()} batch {idx+1}: {str(e)}")
                # Optionally: Retry or log failed batches

        print(f"Finished scraper for {scraper_type.upper()}!")

    async def scrape_tcg_batch(self, batch):
        """
        Handle a TCGPlayer batch scrape here.
        """
        # Example: Loop card_ids and call TCG API
        for card in batch:
            print(f"Scraping TCG market price for {card.name}")
            await asyncio.sleep(0.01)  # Simulate API delay

    async def scrape_ebay_sold_batch(self, batch):
        """
        Handle an eBay Sold batch scrape here.
        """
        for card in batch:
            print(f"Scraping eBay SOLD listings for {card.name}")
            await asyncio.sleep(0.05)  # Simulate web scrape delay

    async def scrape_ebay_active_batch(self, batch):
        """
        Handle an eBay Active batch scrape here.
        """
        for card in batch:
            print(f"Scraping eBay ACTIVE listings for {card.name}")
            await asyncio.sleep(0.05)  # Simulate web scrape delay

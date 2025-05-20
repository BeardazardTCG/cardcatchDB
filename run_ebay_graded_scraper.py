import asyncio
from datetime import datetime
from db import get_session
from models import MasterCard, TrendTracker
from scraper import parse_ebay_graded_page  # new parser
from sqlalchemy import select
from utils import filter_outliers, calculate_median

GRADES = ["PSA 10", "PSA 9", "ACE 10", "ACE 9"]

async def run_ebay_graded_scraper():
    async with get_session() as session:
        # Step 1: Get all cards from TrendTracker
        trend_result = await session.execute(select(TrendTracker))
        trend_cards = trend_result.scalars().all()

        # Step 2: Build UID to MasterCard lookup
        uids = [str(card.unique_id) for card in trend_cards]
        master_result = await session.execute(select(MasterCard))
        master_map = {str(c.unique_id): c for c in master_result.scalars().all() if str(c.unique_id) in uids}

        for trend_card in trend_cards:
            master = master_map.get(str(trend_card.unique_id))
            if not master or not master.query:
                continue

            print(f"\n📊 Scraping grades for UID {trend_card.unique_id} → {master.query}")
            for grade in GRADES:
                try:
                    graded_query = f"{master.query} {grade}"
                    print(f"🔍 Searching: {graded_query}")
                    results = parse_ebay_graded_page(graded_query)
                except Exception as e:
                    print(f"❌ Error on {grade}: {e}")
                    continue

                if not results:
                    continue

                prices = [r['price'] for r in results if r['price'] is not None]
                filtered = filter_outliers(prices)
                if not filtered:
                    continue

                median_price = round(calculate_median(filtered), 2)
                count = len(filtered)

                if grade == "PSA 10":
                    trend_card.psa_10_price = median_price
                    trend_card.psa_10_count = count
                elif grade == "PSA 9":
                    trend_card.psa_9_price = median_price
                    trend_card.psa_9_count = count
                elif grade == "ACE 10":
                    trend_card.ace_10_price = median_price
                    trend_card.ace_10_count = count
                elif grade == "ACE 9":
                    trend_card.ace_9_price = median_price
                    trend_card.ace_9_count = count

        await session.commit()
        print("✅ Graded scrape complete.")

if __name__ == "__main__":
    asyncio.run(run_ebay_graded_scraper())

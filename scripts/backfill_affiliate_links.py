import urllib.parse
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload
from db import get_session
from models import SmartSuggestion, MasterCard

async def backfill_affiliate_links():
    async with get_session() as session:
        results = await session.execute(
            select(SmartSuggestion).options(joinedload(SmartSuggestion.mastercard))
        )
        suggestions = results.scalars().all()

        updated = 0
        for row in suggestions:
            master = row.mastercard
            if not master or not master.full_query:
                continue

            encoded_query = urllib.parse.quote_plus(master.full_query)
            row.affiliate_buy_link = (
                "https://www.ebay.co.uk/sch/i.html"
                f"?_nkw={encoded_query}"
                "&_sop=15&LH_BIN=1&LH_ItemCondition=3000&_ipg=60"
                "&campid=5339108925"
            )
            updated += 1

        await session.commit()
        print(f"{updated} rows updated with affiliate links.")

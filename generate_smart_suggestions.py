import asyncio
from datetime import datetime
from db import get_session
from models import MasterCard, TrendTracker, SmartSuggestion, Inventory, Wishlist
from sqlalchemy import select, delete

async def generate_smart_suggestions():
    async with get_session() as session:
        # Load master cards
        master_result = await session.execute(select(MasterCard))
        master_cards = {str(c.unique_id): c for c in master_result.scalars().all()}

        # Load trends
        trend_result = await session.execute(select(TrendTracker))
        trend_data = {str(t.unique_id): t for t in trend_result.scalars().all()}

        # Load ownership
        inv_result = await session.execute(select(Inventory.unique_id))
        wishlist_result = await session.execute(select(Wishlist.unique_id))
        inventory_uids = set(str(row[0]) for row in inv_result.all())
        wishlist_uids = set(str(row[0]) for row in wishlist_result.all())

        suggestions = []

        for uid, card in master_cards.items():
            if uid not in trend_data:
                print(f"⛔ Skipped UID {uid} — no trend data")
                continue
            if card.clean_avg_price is None or card.net_resale_value is None:
                print(f"⛔ Skipped UID {uid} — missing price data (avg={card.clean_avg_price}, resale={card.net_resale_value})")
                continue

            clean_price = round(card.clean_avg_price, 2)
            resale = round(card.net_resale_value, 2)
            trend_symbol = trend_data[uid].trend_stable or "⚠️"

            # Determine ownership
            if uid in inventory_uids:
                status = "Inventory"
            elif uid in wishlist_uids:
                status = "Wishlist"
            else:
                status = "Unlisted"

            # Price targets
            target_sell = round(clean_price * 0.85, 2)
            target_buy = round(clean_price * 0.75 * (0.9 if trend_symbol == "📉" else 1), 2)

            # Suggestion logic
            action = None
            if status == "Inventory":
                if resale < 2:
                    action = "Job Lot"
                elif resale < 5:
                    action = "Bundle"
                elif resale >= 5 and clean_price >= target_sell:
                    action = "List Now"
            elif status in ["Wishlist", "Unlisted"]:
                if clean_price <= target_buy * 1.05:
                    action = "Buy Now"
                elif clean_price <= target_buy * 1.25:
                    action = "Monitor"

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
                print(f"✅ UID {uid} → {action} | {status}, resale={resale}, avg={clean_price}, trend={trend_symbol}")
            else:
                print(f"🧐 UID {uid} — no action triggered | {status}, resale={resale}, avg={clean_price}, trend={trend_symbol}")

        # Overwrite table
        await session.execute(delete(SmartSuggestion))
        session.add_all(suggestions)
        await session.commit()
        print(f"✅ Smart Suggestions generated for {len(suggestions)} cards.")

if __name__ == "__main__":
    asyncio.run(generate_smart_suggestions())

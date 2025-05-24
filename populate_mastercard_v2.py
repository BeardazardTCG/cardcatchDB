import os
import asyncio
import json
from datetime import datetime
from dotenv import load_dotenv
import aiohttp
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

HEADERS = {"User-Agent": "Mozilla/5.0"}

def convert_date(s):
    if not s:
        return None
    try:
        return datetime.strptime(s.replace("/", "-"), "%Y-%m-%d").date()
    except Exception:
        return None

async def fetch_json(session, url):
    try:
        async with session.get(url, headers=HEADERS) as resp:
            resp.raise_for_status()
            return await resp.json()
    except Exception as e:
        print(f"Failed to fetch URL {url}: {e}")
        return {}

async def fetch_all_sets(session):
    data = await fetch_json(session, "https://api.pokemontcg.io/v2/sets")
    return data.get("data", [])

async def fetch_cards(session, set_id):
    data = await fetch_json(session, f"https://api.pokemontcg.io/v2/cards?q=set.id:{set_id}&pageSize=250")
    return data.get("data", [])

def is_secret_card(card):
    number = card.get("number", "")
    rarity = card.get("rarity", "").lower() if card.get("rarity") else ""
    if rarity and ("secret" in rarity or "promo" in rarity):
        return True
    if "/" in number:
        num_part, total_part = number.split("/")
        try:
            if int(num_part) > int(total_part):
                return True
        except ValueError:
            return True
    return False

async def insert_card(db_session: AsyncSession, card, release_date):
    data = {
        "unique_id": card["id"],
        "card_name": card["name"],
        "card_number": card["number"],
        "card_number_raw": card["number"].split("/")[0],
        "rarity": card.get("rarity"),
        "type": card.get("types", [None])[0] if card.get("types") else None,
        "artist": card.get("artist"),
        "language": card.get("language", "en"),
        "set_name": card["set"]["name"],
        "set_code": card["set"].get("ptcgoCode") or card["set"]["id"],
        "release_date": release_date,
        "set_logo_url": card["set"]["images"]["logo"],
        "set_symbol_url": card["set"]["images"]["symbol"],
        "query": f"{card['name']} {card['set']['name']} {card['number'].replace('/', ' ')}",
        "set_id": card["set"]["id"],
        "types": json.dumps(card.get("types")) if card.get("types") else None,
        "hot_character": False,
        "card_image_url": card["images"]["small"],
        "subtypes": json.dumps(card.get("subtypes")) if card.get("subtypes") else None,
        "supertype": card.get("supertype")
    }
    try:
        await db_session.execute(text("""
            INSERT INTO mastercard_v2 (
                unique_id, card_name, card_number, card_number_raw, rarity,
                type, artist, language, set_name, set_code,
                release_date, set_logo_url, set_symbol_url, query, set_id,
                types, hot_character, card_image_url, subtypes, supertype
            ) VALUES (
                :unique_id, :card_name, :card_number, :card_number_raw, :rarity,
                :type, :artist, :language, :set_name, :set_code,
                :release_date, :set_logo_url, :set_symbol_url, :query, :set_id,
                :types, :hot_character, :card_image_url, :subtypes, :supertype
            )
            ON CONFLICT (unique_id) DO NOTHING
        """), data)
    except Exception as e:
        print(f"Failed to insert card {card['id']}: {e}")

async def main():
    async with async_session() as db_session, aiohttp.ClientSession() as http_session:
        sets = await fetch_all_sets(http_session)
        print(f"Total sets to process: {len(sets)}")

        processed = 0
        for s in sets:
            set_id = s["id"]
            set_name = s["name"]
            release_date = convert_date(s.get("releaseDate"))
            print(f"Processing set {set_name} ({set_id}), release date: {release_date}")

            cards = await fetch_cards(http_session, set_id)
            cards = [c for c in cards if not is_secret_card(c)]
            print(f"  Found {len(cards)} cards (excluding secret cards)")

            for card in cards:
                await insert_card(db_session, card, release_date)

            processed += 1

            if processed % 10 == 0:
                await db_session.commit()
                print(f"Committed after processing {processed} sets.")
                await asyncio.sleep(1)  # brief pause to avoid rate limits

        # Commit any remaining inserts
        await db_session.commit()
        print(f"Finished processing {processed} sets.")

if __name__ == "__main__":
    asyncio.run(main())

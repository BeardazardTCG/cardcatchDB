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
    async with session.get(url, headers=HEADERS) as resp:
        resp.raise_for_status()
        return await resp.json()

async def fetch_all_sets(session):
    url = "https://api.pokemontcg.io/v2/sets"
    data = await fetch_json(session, url)
    return data.get("data", [])

async def fetch_cards(session, set_id):
    url = f"https://api.pokemontcg.io/v2/cards?q=set.id:{set_id}&pageSize=250"
    data = await fetch_json(session, url)
    return data.get("data", [])

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
        "query": f"{card['name']} {card['set']['name']} {card['number'].split('/')[0]}",
        "set_id": card["set"]["id"],
        "types": json.dumps(card.get("types")) if card.get("types") else None,
        "hot_character": False,
        "card_image_url": card["images"]["small"],
        "subtypes": json.dumps(card.get("subtypes")) if card.get("subtypes") else None,
        "supertype": card.get("supertype")
    }

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

async def main():
    async with async_session() as db_session, aiohttp.ClientSession() as http_session:
        sets = await fetch_all_sets(http_session)
        print(f"Total sets to process: {len(sets)}")

        for s in sets:
            set_id = s["id"]
            set_name = s["name"]
            release_date = convert_date(s.get("releaseDate"))
            print(f"Processing set {set_name} ({set_id}), release date: {release_date}")

            cards = await fetch_cards(http_session, set_id)
            print(f"  Found {len(cards)} cards")

            for card in cards:
                try:
                    await insert_card(db_session, card, release_date)
                except Exception as e:
                    print(f"Failed to insert card {card['id']}: {e}")

            await db_session.commit()

if __name__ == "__main__":
    asyncio.run(main())

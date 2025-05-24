# async_populate_mastercard_v2.py
import os
import json
import asyncio
import requests
from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy import text
from dotenv import load_dotenv

# === Load environment variables ===
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = 'a4a5ed18-fbf7-4960-b0ac-2ac71e01eee7'
HEADERS = {'X-Api-Key': API_KEY}

# === Setup async DB session ===
engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

# === Fetch sets ===
def fetch_sets():
    print("🔍 Fetching sets...")
    res = requests.get("https://api.pokemontcg.io/v2/sets", headers=HEADERS)
    res.raise_for_status()
    sets = res.json()['data']
    filtered = [s for s in sets if s.get('printedTotal')]
    print(f"✅ {len(filtered)} valid sets found.")
    return filtered

# === Fetch cards ===
def fetch_cards(set_id):
    url = f"https://api.pokemontcg.io/v2/cards?q=set.id:{set_id}"
    res = requests.get(url, headers=HEADERS)
    res.raise_for_status()
    return res.json()['data']

# === Insert logic ===
async def populate():
    sets = fetch_sets()
    total_inserted = 0

    async with async_session() as session:
        for s in sets:
            set_id = s['id']
            set_code = s.get('ptcgoCode') or s['id']
            print(f"📦 {s['name']} ({set_id})")

            try:
                cards = fetch_cards(set_id)
            except Exception as e:
                print(f"❌ Skipping {set_id}: {e}")
                continue

            for c in cards:
                try:
                    stmt = text("""
                        INSERT INTO mastercard_v2 (
                            unique_id, card_name, set_name, card_number, card_number_raw,
                            query, set_code, set_id, supertype, subtypes,
                            rarity, artist, types, type, release_date,
                            language, hot_character, card_image_url,
                            set_logo_url, set_symbol_url
                        ) VALUES (
                            :unique_id, :card_name, :set_name, :card_number, :card_number_raw,
                            :query, :set_code, :set_id, :supertype, :subtypes,
                            :rarity, :artist, :types, :type, :release_date,
                            :language, :hot_character, :card_image_url,
                            :set_logo_url, :set_symbol_url
                        ) ON CONFLICT (unique_id) DO NOTHING;
                    """)

                    await session.execute(stmt, {
                        "unique_id": c['id'],
                        "card_name": c['name'],
                        "set_name": s['name'],
                        "card_number": f"{c['number']}/{s['printedTotal']}",
                        "card_number_raw": c['number'],
                        "query": f"{c['name']} {s['name']} {c['number']}",
                        "set_code": set_code,
                        "set_id": set_id,
                        "supertype": c.get('supertype'),
                        "subtypes": json.dumps(c.get('subtypes')) if c.get('subtypes') else None,
                        "rarity": c.get('rarity'),
                        "artist": c.get('artist'),
                        "types": json.dumps(c.get('types')) if c.get('types') else None,
                        "type": c.get('types', [None])[0] if c.get('types') else None,
                        "release_date": s.get('releaseDate'),
                        "language": "en",
                        "hot_character": False,
                        "card_image_url": c.get('images', {}).get('small'),
                        "set_logo_url": s['images'].get('logo'),
                        "set_symbol_url": s['images'].get('symbol'),
                    })
                    total_inserted += 1
                except Exception as e:
                    print(f"⚠️ Insert error for {c['id']}: {e}")
                    continue

        await session.commit()
        print(f"\n✅ Inserted {total_inserted} cards into mastercard_v2")

# === RUN ===
if __name__ == "__main__":
    print("🚀 Script started")
    asyncio.run(populate())

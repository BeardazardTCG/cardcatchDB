import os
import re
import asyncio
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import (
    Column, String, Integer, Date, Text, MetaData, Table
)
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.dialects.postgresql import insert

# === Load environment ===
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set in environment")

engine = create_async_engine(DATABASE_URL, echo=False)
metadata = MetaData()

# === Define DB Table ===
mastercard_v2 = Table(
    "mastercard_v2", metadata,
    Column("id", Integer, primary_key=True),
    Column("unique_id", String, unique=True),
    Column("card_name", String),
    Column("card_number", String),
    Column("card_number_raw", String),
    Column("rarity", String),
    Column("type", String),
    Column("artist", String),
    Column("language", String),
    Column("set_name", String),
    Column("set_code", String),
    Column("release_date", Date),
    Column("series", String),
    Column("set_logo_url", Text),
    Column("set_symbol_url", Text),
    Column("query", Text),
)

# === Local file parse logic ===
def parse_local_expansions():
    with open("expansions.html", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    tables = soup.find_all("table", {"style": re.compile("margin:auto; text-align: center;")})
    print(f"🔍 Found {len(tables)} expansion tables")

    cards = []
    for table in tables:
        rows = table.find_all("tr")[1:]  # skip header
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 6:
                continue

            set_name = cols[2].text.strip()
            release_text = cols[5].text.strip()
            release_date = None
            try:
                release_date = datetime.strptime(release_text, "%B %d, %Y").date()
            except:
                pass

            set_code = cols[6].text.strip() if len(cols) > 6 else None
            unique_id = f"SET_{set_code or set_name.replace(' ', '_')}"

            cards.append({
                "unique_id": unique_id,
                "card_name": None,
                "card_number": None,
                "card_number_raw": None,
                "rarity": None,
                "type": None,
                "artist": None,
                "language": "en",
                "set_name": set_name,
                "set_code": set_code,
                "release_date": release_date,
                "series": None,
                "set_logo_url": None,
                "set_symbol_url": None,
                "query": set_name,
            })

    print(f"📦 Parsed {len(cards)} set entries")
    return cards

# === Async insert logic ===
async def main():
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)

    expansions = parse_local_expansions()

    async_session = async_sessionmaker(engine, expire_on_commit=False)
    async with async_session() as session:
        for card in expansions:
            stmt = insert(mastercard_v2).values(card).on_conflict_do_nothing()
            await session.execute(stmt)
        await session.commit()

    print(f"✅ Inserted {len(expansions)} sets into mastercard_v2")

if __name__ == "__main__":
    print("🟢 Script starting...")
    asyncio.run(main())
    print("🏁 Script finished cleanly.")

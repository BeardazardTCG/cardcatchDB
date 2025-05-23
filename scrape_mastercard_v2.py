import os
import re
import asyncio
from datetime import datetime

import aiohttp
from bs4 import BeautifulSoup
from sqlalchemy import (
    Column, String, Integer, Date, Text, MetaData, Table
)
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker
from dotenv import load_dotenv

# === Load environment ===
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set in environment")

engine = create_async_engine(DATABASE_URL, echo=False)
metadata = MetaData()

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

def clean_card_number(raw):
    match = re.match(r"(\d+)[^\d]?(\d+)", raw)
    if match:
        return f"{int(match.group(1))}/{int(match.group(2))}"
    return raw

async def scrape():
    BASE_URL = "https://bulbapedia.bulbagarden.net"
    EXPANSIONS_URL = f"{BASE_URL}/wiki/List_of_Pok%C3%A9mon_Trading_Card_Game_expansions"

    async with aiohttp.ClientSession() as session:
        print("🌐 Fetching set list page...")
        async with session.get(EXPANSIONS_URL) as response:
            if response.status != 200:
                raise Exception(f"Failed to load set list page: {response.status}")
            text = await response.text()
            soup = BeautifulSoup(text, "html.parser")

        # DEBUG OUTPUT – first 3,000 chars of the parsed HTML
        print("🧪 Page content preview:\n")
        print(soup.prettify()[:3000])
        print("\n🧪 End of preview\n")

        seen = set()
        set_links = []

        for a in soup.select("table.wikitable a[href^='/wiki/']"):
            href = a.get("href")
            if href and "TCG" in href and href not in seen:
                full_url = BASE_URL + href
                seen.add(href)
                set_links.append(full_url)

        print(f"🔍 Found {len(set_links)} set links")

        all_cards = []

        for link in set_links:
            print(f"📥 Scraping set page: {link}")
            async with session.get(link) as page:
                if page.status != 200:
                    print(f"❌ Failed to load set page: {link}")
                    continue
                html = await page.text()
                set_soup = BeautifulSoup(html, "html.parser")

            title_tag = set_soup.find("h1")
            if not title_tag:
                print("❌ Missing <h1> title")
                continue

            title = title_tag.text.replace("(TCG)", "").strip()
            table = set_soup.find("table", {"class": "wikitable"})
            if not table:
                print(f"⚠️ No card table found in set: {title}")
                continue

            rows = table.find_all("tr")[1:]
            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 2:
                    continue

                card_number_raw = cols[0].text.strip()
                card_name = cols[1].text.strip()
                card_number = clean_card_number(card_number_raw)
                query = f"{card_name} {title} {card_number.replace('/', ' ')}"
                unique_id = f"{title}_{card_number}"

                print(f"➕ Card: {card_name} ({card_number}) in {title}")

                all_cards.append({
                    "unique_id": unique_id,
                    "card_name": card_name,
                    "card_number": card_number,
                    "card_number_raw": card_number_raw,
                    "rarity": None,
                    "type": None,
                    "artist": None,
                    "language": "en",
                    "set_name": title,
                    "set_code": None,
                    "release_date": None,
                    "series": None,
                    "set_logo_url": None,
                    "set_symbol_url": None,
                    "query": query,
                })

        print(f"📦 Total cards scraped: {len(all_cards)}")
        return all_cards

async def main():
    print("⚙️ Creating table if needed...")
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)

    print("🚀 Starting full scrape process...")
    cards = await scrape()

    print("📤 Inserting into DB...")
    async_session = async_sessionmaker(engine, expire_on_commit=False)
    async with async_session() as session:
        for card in cards:
            stmt = insert(mastercard_v2).values(card).on_conflict_do_update(
                index_elements=["unique_id"],
                set_={"card_name": insert.excluded.card_name}
            )
            await session.execute(stmt)
        await session.commit()

    print(f"✅ Done! {len(cards)} cards written to `mastercard_v2`")

if __name__ == "__main__":
    print("🟢 Script starting...")
    asyncio.run(main())
    print("🏁 Script finished cleanly.")

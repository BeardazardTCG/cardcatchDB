import os
import re
import asyncio
from datetime import datetime

import aiohttp
from bs4 import BeautifulSoup
from sqlalchemy import (
    Column, String, Integer, Date, Text, MetaData, Table
)
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv

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

# === Scraper Logic ===
BASE_URL = "https://www.tcdb.com"

async def fetch_page(session, url):
    async with session.get(url) as resp:
        return await resp.text()

async def parse_sets(session, page_num):
    url = f"{BASE_URL}/ViewAll.cfm/sp/Gaming/brand/Pokemon/page/{page_num}"
    html = await fetch_page(session, url)
    soup = BeautifulSoup(html, "html.parser")

    table = soup.find("table", class_="layout")
    if not table:
        return [], False

    sets = []
    for row in table.select("tr")[1:]:
        cols = row.find_all("td")
        if len(cols) < 2:
            continue

        link = cols[0].find("a")
        if not link:
            continue

        set_name = link.text.strip()
        set_url = BASE_URL + link['href']
        set_code = re.search(r"sid/(\d+)", link['href'])
        set_id = set_code.group(1) if set_code else None
        year_text = cols[1].text.strip()

        try:
            release_date = datetime.strptime(year_text, "%Y").date()
        except:
            release_date = None

        sets.append({
            "unique_id": f"TCDB_{set_id or set_name.replace(' ', '_')}",
            "card_name": None,
            "card_number": None,
            "card_number_raw": None,
            "rarity": None,
            "type": None,
            "artist": None,
            "language": "en",
            "set_name": set_name,
            "set_code": set_id,
            "release_date": release_date,
            "series": None,
            "set_logo_url": None,
            "set_symbol_url": None,
            "query": set_name,
        })

    # Stop if there's no "Next" link
    next_link = soup.find("a", string="Next >")
    has_next = bool(next_link)

    return sets, has_next

async def main():
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)

    async_session = async_sessionmaker(engine, expire_on_commit=False)
    all_sets = []
    page = 1
    has_more = True

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        while has_more:
            print(f"🔄 Fetching page {page}...")
            sets, has_more = await parse_sets(session, page)
            all_sets.extend(sets)
            page += 1

    print(f"📦 Parsed {len(all_sets)} sets total")

    async with async_session() as session:
        for s in all_sets:
            stmt = insert(mastercard_v2).values(s).on_conflict_do_nothing()
            await session.execute(stmt)
        await session.commit()

    print(f"✅ Inserted {len(all_sets)} sets into mastercard_v2")

if __name__ == "__main__":
    print("🟢 Starting TCDB set scrape...")
    asyncio.run(main())
    print("🏁 Finished.")

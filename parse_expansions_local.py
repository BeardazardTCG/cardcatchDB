import os
import re
from bs4 import BeautifulSoup
from sqlalchemy import (
    create_engine, Column, String, Integer, Date, Text, MetaData, Table
)
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
from dotenv import load_dotenv

# === Load .env for DATABASE_URL ===
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set in environment")

engine = create_engine(DATABASE_URL)
metadata = MetaData()

# === Define table ===
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

metadata.create_all(engine)

# === Load the saved file ===
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

        # These are just set-level placeholders
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

with engine.begin() as conn:
    for card in cards:
        stmt = insert(mastercard_v2).values(card).on_conflict_do_nothing()
        conn.execute(stmt)

print(f"✅ Inserted {len(cards)} sets into mastercard_v2")

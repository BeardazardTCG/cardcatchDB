import os
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, Date, Text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
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

# === Create the table if it doesn't exist ===
metadata.create_all(engine)

# === Scrape Bulbapedia expansions page ===
BASE_URL = "https://bulbapedia.bulbagarden.net"
EXPANSIONS_URL = f"{BASE_URL}/wiki/List_of_Pok%C3%A9mon_Trading_Card_Game_expansions"
response = requests.get(EXPANSIONS_URL)
soup = BeautifulSoup(response.text, "html.parser")

# === Extract all English set links ===
set_links = []
for a in soup.select("table.wikitable a[href^='/wiki/']"):
    href = a.get("href")
    if href and ")_(TCG)" in href:
        set_links.append(BASE_URL + href)

# === Helper to clean card number ===
def clean_card_number(raw):
    match = re.match(r"(\d+)[^\d]?(\d+)", raw)
    if match:
        return f"{int(match.group(1))}/{int(match.group(2))}"
    return raw

# === Scrape each set page for cards ===
cards = []

for link in set_links:
    set_page = requests.get(link)
    set_soup = BeautifulSoup(set_page.text, "html.parser")

    title = set_soup.find("h1").text.replace("(TCG)", "").strip()
    table = set_soup.find("table", {"class": "wikitable"})
    if not table:
        continue

    rows = table.find_all("tr")[1:]  # Skip header

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 2:
            continue

        card_number_raw = cols[0].text.strip()
        card_name = cols[1].text.strip()

        card_number = clean_card_number(card_number_raw)
        query = f"{card_name} {title} {card_number.replace('/', ' ')}"
        unique_id = f"{title}_{card_number}"

        cards.append({
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

# === Insert into DB ===
with engine.begin() as conn:
    for card in cards:
        stmt = insert(mastercard_v2).values(card).on_conflict_do_nothing()
        conn.execute(stmt)

print(f"✅ Inserted {len(cards)} cards into mastercard_v2")

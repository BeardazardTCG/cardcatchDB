# populate_mastercard_v2.py
import requests
import json
from sqlalchemy import create_engine, Table, Column, String, Integer, Boolean, MetaData
from sqlalchemy.dialects.postgresql import insert

# --- CONFIG ---
API_KEY = 'a4a5ed18-fbf7-4960-b0ac-2ac71e01eee7'
DB_URL = "postgresql://postgres:ckQFRJkrJluWsJnHsDhlhvbtSridadDF@metro.proxy.rlwy.net:52025/railway"
HEADERS = {'X-Api-Key': API_KEY}

# --- DB SETUP ---
engine = create_engine(DB_URL)
metadata = MetaData()

table = Table(
    "mastercard_v2",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("unique_id", String),
    Column("card_name", String),
    Column("set_name", String),
    Column("card_number", String),
    Column("card_number_raw", String),
    Column("query", String),
    Column("set_code", String),
    Column("set_id", String),
    Column("supertype", String),
    Column("subtypes", String),
    Column("rarity", String),
    Column("artist", String),
    Column("types", String),
    Column("type", String),
    Column("release_date", String),
    Column("language", String),
    Column("hot_character", Boolean),
    Column("card_image_url", String),
    Column("set_logo_url", String),
    Column("set_symbol_url", String),
)

# --- FETCH SETS ---
def fetch_sets():
    print("🔍 Fetching sets...")
    res = requests.get("https://api.pokemontcg.io/v2/sets", headers=HEADERS)
    res.raise_for_status()
    sets = res.json()['data']
    filtered = [s for s in sets if s.get('printedTotal')]
    print(f"✅ {len(filtered)} valid sets found.")
    return filtered

# --- FETCH CARDS FOR A SET ---
def fetch_cards(set_id):
    url = f"https://api.pokemontcg.io/v2/cards?q=set.id:{set_id}"
    res = requests.get(url, headers=HEADERS)
    res.raise_for_status()
    return res.json()['data']

# --- MAIN INSERT FUNCTION ---
def populate():
    print("✅ Starting card population...")
    sets = fetch_sets()
    total_inserted = 0

    with engine.begin() as conn:
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
                    row = {
                        "unique_id": c['id'],
                        "card_name": c['name'],
                        "card_number": f"{c['number']}/{s['printedTotal']}",
                        "card_number_raw": c['number'],
                        "rarity": c.get('rarity'),
                        "type": c.get('types', [None])[0] if c.get('types') else None,
                        "artist": c.get('artist'),
                        "language": "en",
                        "set_name": s['name'],
                        "set_code": set_code,
                        "release_date": s.get('releaseDate'),
                        # "series": s.get('series'),  # Removed to match table structure
                        "set_logo_url": s['images'].get('logo'),
                        "set_symbol_url": s['images'].get('symbol'),
                        "query": f"{c['name']} {s['name']} {c['number']}",
                        "set_id": set_id,
                        "types": json.dumps(c.get('types')) if c.get('types') else None,
                        "hot_character": False,
                        "card_image_url": c.get('images', {}).get('small'),
                        "subtypes": json.dumps(c.get('subtypes')) if c.get('subtypes') else None,
                        "supertype": c.get('supertype')
                    }
                    stmt = insert(table).values(**row).on_conflict_do_nothing()
                    conn.execute(stmt)
                    total_inserted += 1
                except Exception as e:
                    print(f"⚠️ Insert error for card {c.get('id', 'unknown')}: {e}")
                    continue

    print(f"\n✅ Inserted {total_inserted} cards into mastercard_v2")

# --- RUN ---
if __name__ == "__main__":
    print("🚀 Script started")
    populate()

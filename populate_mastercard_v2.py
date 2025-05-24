import requests
import json
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, insert
from sqlalchemy.exc import SQLAlchemyError

# CONFIG
API_KEY = 'a4a5ed18-fbf7-4960-b0ac-2ac71e01eee7'
DB_URL = "postgresql://postgres:ckQFRJkrJluWsJnHsDhlhvbtSridadDF@metro.proxy.rlwy.net:52025/railway"
HEADERS = {'X-Api-Key': API_KEY}

# Setup
engine = create_engine(DB_URL)
metadata = MetaData()
metadata.reflect(bind=engine)
table = metadata.tables.get("mastercard_v2")

if table is None:
    print("❌ ERROR: 'mastercard_v2' table not found.")
    exit(1)

def fetch_sets():
    print("🔍 Fetching sets...")
    res = requests.get("https://api.pokemontcg.io/v2/sets", headers=HEADERS)
    res.raise_for_status()
    sets = res.json()['data']
    return [s for s in sets if s.get('printedTotal')]

def fetch_cards(set_id):
    url = f"https://api.pokemontcg.io/v2/cards?q=set.id:{set_id}"
    res = requests.get(url, headers=HEADERS)
    res.raise_for_status()
    return res.json()['data']

def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y/%m/%d").date()
    except Exception:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except:
            return None

def populate():
    print("✅ Starting card population...")
    sets = fetch_sets()
    print(f"✅ {len(sets)} valid sets found.")
    conn = engine.connect()
    inserted_total = 0

    for s in sets:
        set_id = s['id']
        set_code = s.get('ptcgoCode') or s['id']
        print(f"📦 {s['name']} ({set_id})")

        try:
            cards = fetch_cards(set_id)
        except Exception as e:
            print(f"❌ Failed to fetch cards for {set_id}: {e}")
            continue

        for c in cards:
            try:
                row = {
                    "unique_id": c['id'],
                    "card_name": c['name'],
                    "set_name": s['name'],
                    "card_number": f"{c['number']}/{s['printedTotal']}",
                    "card_number_raw": c['number'],
                    "query": f"{c['name']} {s['name']} {c['number']}",
                    "set_code": set_code,
                    "set_id": set_id,
                    "supertype": c.get('supertype'),
                    "subtypes": ", ".join(c.get('subtypes', [])) if c.get('subtypes') else None,
                    "rarity": c.get('rarity'),
                    "artist": c.get('artist'),
                    "types": ", ".join(c.get('types', [])) if c.get('types') else None,
                    "type": c.get('types', [None])[0] if c.get('types') else None,
                    "release_date": parse_date(s.get('releaseDate')) if s.get('releaseDate') else None,
                    "language": "en",
                    "hot_character": False,
                    "card_image_url": c.get('images', {}).get('small'),
                    "set_logo_url": s['images'].get('logo'),
                    "set_symbol_url": s['images'].get('symbol'),
                }

                stmt = insert(table).values(**row).prefix_with("ON CONFLICT (unique_id) DO NOTHING")
                conn.execute(stmt)
                inserted_total += 1

            except SQLAlchemyError as db_err:
                print(f"⚠️ Insert error for {c.get('id', 'unknown')}: {db_err}")
                continue

    conn.close()
    print(f"\n✅ Finished. Inserted {inserted_total} cards into mastercard_v2.")

if __name__ == "__main__":
    print("🚀 Script started")
    populate()


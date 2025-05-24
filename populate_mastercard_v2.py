import requests
import json
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert

# --- CONFIG ---
API_KEY = 'a4a5ed18-fbf7-4960-b0ac-2ac71e01eee7'
DB_URL = "postgresql://postgres:ckQFRJkrJluWsJnHsDhlhvbtSridadDF@metro.proxy.rlwy.net:52025/railway"
HEADERS = {'X-Api-Key': API_KEY}

# --- FETCH SETS ---
def fetch_sets():
    res = requests.get("https://api.pokemontcg.io/v2/sets", headers=HEADERS)
    res.raise_for_status()
    data = res.json()['data']
    return [s for s in data if s.get('printedTotal')]

# --- FETCH CARDS FOR SET ---
def fetch_cards(set_id):
    url = f"https://api.pokemontcg.io/v2/cards?q=set.id:{set_id}"
    res = requests.get(url, headers=HEADERS)
    res.raise_for_status()
    return res.json()['data']

# --- FORMAT + INSERT ---
def normalize_and_insert(all_sets):
    engine = create_engine(DB_URL)
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = metadata.tables['mastercard_v2']
    conn = engine.connect()
    inserted = 0

    for s in all_sets:
        set_id = s['id']
        set_code = s['ptcgoCode'] if s.get('ptcgoCode') else s['id']
        print(f"📦 {s['name']} ({set_id})")

        try:
            cards = fetch_cards(set_id)
        except:
            print(f"❌ Skipping {set_id} (error or no data)")
            continue

        for c in cards:
            card_number = f"{c['number']}/{s['printedTotal']}" if s.get('printedTotal') else c['number']
            record = {
                "unique_id": c['id'],
                "card_name": c['name'],
                "card_number": card_number,
                "card_number_raw": c['number'],
                "rarity": c.get('rarity'),
                "type": c.get('types', [None])[0],
                "artist": c.get('artist'),
                "language": "en",
                "set_name": s['name'],
                "set_code": set_code,
                "release_date": s.get('releaseDate'),
                "series": s.get('series'),
                "set_logo_url": s['images'].get('logo'),
                "set_symbol_url": s['images'].get('symbol'),
                "query": f"{c['name']} {s['name']} {card_number}",
                "set_id": set_id,
                "types": c.get('types'),
                "hot_character": False,
                "card_image_url": c.get('images', {}).get('small'),
                "subtypes": c.get('subtypes'),
                "supertype": c.get('supertype')
            }

            stmt = insert(table).values(**record).on_conflict_do_nothing()
            conn.execute(stmt)
            inserted += 1

    conn.close()
    print(f"\n✅ Inserted {inserted} cards into mastercard_v2")

# --- MAIN ---
if __name__ == "__main__":
    all_sets = fetch_sets()
    normalize_and_insert(all_sets)

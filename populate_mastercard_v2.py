import requests
import json
from sqlalchemy import create_engine, MetaData, Table, insert

# --- CONFIG ---
API_KEY = 'a4a5ed18-fbf7-4960-b0ac-2ac71e01eee7'
DB_URL = "postgresql://postgres:ckQFRJkrJluWsJnHsDhlhvbtSridadDF@metro.proxy.rlwy.net:52025/railway"
HEADERS = {'X-Api-Key': API_KEY}

engine = create_engine(DB_URL)
metadata = MetaData()
metadata.reflect(bind=engine)
table = metadata.tables["mastercard_v2"]

# --- FETCH SETS ---
def fetch_sets():
    print("🔍 Fetching sets...")
    res = requests.get("https://api.pokemontcg.io/v2/sets", headers=HEADERS)
    res.raise_for_status()
    sets = res.json()['data']
    sets = [s for s in sets if s.get('printedTotal')]
    print(f"✅ {len(sets)} sets found.")
    return sets

# --- FETCH CARDS ---
def fetch_cards(set_id):
    url = f"https://api.pokemontcg.io/v2/cards?q=set.id:{set_id}"
    res = requests.get(url, headers=HEADERS)
    res.raise_for_status()
    return res.json()['data']

# --- RUNNER ---
def populate():
    sets = fetch_sets()
    total = 0
    conn = engine.connect()

    for s in sets:
        set_id = s['id']
        set_code = s.get('ptcgoCode') or s['id']
        print(f"📦 {s['name']} ({set_id})")

        try:
            cards = fetch_cards(set_id)
        except Exception as e:
            print(f"❌ {set_id} failed: {e}")
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
                    "series": s.get('series'),
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
                conn.execute(insert(table).values(**row).on_conflict_do_nothing())
                total += 1
            except Exception as e:
                print(f"⚠️ Insert error: {e}")
                continue

    conn.close()
    print(f"✅ Inserted {total} cards into mastercard_v2")

# --- RUN ---
if __name__ == "__main__":
    print("🚀 Running populate script...")
    populate()

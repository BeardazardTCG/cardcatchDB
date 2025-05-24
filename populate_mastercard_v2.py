# populate_mastercard_v2.py
import requests
import json
from sqlmodel import SQLModel, Session, create_engine
from sqlalchemy.exc import SQLAlchemyError
from models.models import MasterCardV2

API_KEY = 'a4a5ed18-fbf7-4960-b0ac-2ac71e01eee7'
DB_URL = "postgresql://postgres:ckQFRJkrJluWsJnHsDhlhvbtSridadDF@metro.proxy.rlwy.net:52025/railway"
HEADERS = {'X-Api-Key': API_KEY}

engine = create_engine(DB_URL)

# --- FETCH SETS ---
def fetch_sets():
    print("🔍 Fetching sets...")
    try:
        res = requests.get("https://api.pokemontcg.io/v2/sets", headers=HEADERS)
        res.raise_for_status()
        data = res.json()['data']
        sets = [s for s in data if s.get('printedTotal')]
        print(f"✅ {len(sets)} sets fetched.")
        return sets
    except Exception as e:
        print(f"❌ Failed to fetch sets: {e}")
        return []

# --- FETCH CARDS FOR A SET ---
def fetch_cards(set_id):
    try:
        url = f"https://api.pokemontcg.io/v2/cards?q=set.id:{set_id}"
        res = requests.get(url, headers=HEADERS)
        res.raise_for_status()
        cards = res.json()['data']
        print(f"   ↳ {len(cards)} cards fetched")
        return cards
    except Exception as e:
        print(f"   ❌ Error fetching cards for {set_id}: {e}")
        return []

# --- POPULATE CARDS INTO DB ---
def populate_cards():
    all_sets = fetch_sets()
    total_inserted = 0

    with Session(engine) as session:
        for s in all_sets:
            set_id = s['id']
            set_code = s.get('ptcgoCode') or s['id']
            print(f"📦 {s['name']} ({set_id})")

            cards = fetch_cards(set_id)

            for c in cards:
                try:
                    card = MasterCardV2(
                        unique_id=c['id'],
                        card_name=c['name'],
                        card_number=f"{c['number']}/{s['printedTotal']}" if s.get('printedTotal') else c['number'],
                        card_number_raw=c['number'],
                        rarity=c.get('rarity'),
                        type=c.get('types', [None])[0] if c.get('types') else None,
                        artist=c.get('artist'),
                        language="en",
                        set_name=s['name'],
                        set_code=set_code,
                        release_date=s.get('releaseDate'),
                        series=s.get('series'),
                        set_logo_url=s['images'].get('logo'),
                        set_symbol_url=s['images'].get('symbol'),
                        query=f"{c['name']} {s['name']} {c['number']}",
                        set_id=set_id,
                        types=json.dumps(c.get('types')) if c.get('types') else None,
                        hot_character=False,
                        card_image_url=c.get('images', {}).get('small'),
                        subtypes=json.dumps(c.get('subtypes')) if c.get('subtypes') else None,
                        supertype=c.get('supertype')
                    )
                    session.add(card)
                    total_inserted += 1

                except SQLAlchemyError as db_err:
                    print(f"⚠️ Insert error for card {c.get('id', 'unknown')}: {db_err}")
                    continue

            session.commit()

    print(f"\n✅ Total inserted into mastercard_v2: {total_inserted}")

# --- RUN ---
if __name__ == "__main__":
    populate_cards()



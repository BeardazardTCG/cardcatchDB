import requests
from sqlmodel import SQLModel, Session, create_engine
from models import MasterCardV2  # make sure this matches your model filename
from sqlalchemy.exc import SQLAlchemyError

# --- CONFIG ---
API_KEY = 'a4a5ed18-fbf7-4960-b0ac-2ac71e01eee7'
DB_URL = "postgresql://postgres:ckQFRJkrJluWsJnHsDhlhvbtSridadDF@metro.proxy.rlwy.net:52025/railway"
HEADERS = {'X-Api-Key': API_KEY}

engine = create_engine(DB_URL)

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

# --- MAIN INSERT FUNCTION ---
def populate_cards():
    all_sets = fetch_sets()
    total_inserted = 0

    with Session(engine) as session:
        for s in all_sets:
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
                        types=c.get('types'),
                        hot_character=False,
                        card_image_url=c.get('images', {}).get('small'),
                        subtypes=c.get('subtypes'),
                        supertype=c.get('supertype')
                    )

                    session.add(card)
                    total_inserted += 1

                except SQLAlchemyError as db_err:
                    print(f"⚠️ Insert error for card {c.get('id', 'unknown')}: {db_err}")
                    continue

            session.commit()

    print(f"\n✅ Total inserted into mastercard_v2: {total_inserted}")

# ---


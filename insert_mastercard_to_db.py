import json
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert

# Replace with your DB URL
DATABASE_URL = "postgresql://postgres:ckQFRJkrJluWsJnHsDhlhvbtSridadDF@metro.proxy.rlwy.net:52025/railway"

def load_cards():
    with open("normalized_mastercard.json", "r") as f:
        return json.load(f)

def insert_cards():
    engine = create_engine(DATABASE_URL)
    metadata = MetaData()
    metadata.reflect(bind=engine)

    table = metadata.tables['mastercard_v2']
    conn = engine.connect()

    cards = load_cards()
    inserted = 0

    for card in cards:
        stmt = insert(table).values(**card).on_conflict_do_nothing()
        conn.execute(stmt)
        inserted += 1

    conn.close()
    print(f"✅ Inserted {inserted} cards into mastercard_v2")

if __name__ == "__main__":
    insert_cards()

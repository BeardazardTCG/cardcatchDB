import requests
import psycopg2
from psycopg2.extras import execute_batch

# --- CONFIG ---
API_URL = "https://cardcatchdb.onrender.com/tcg-prices-batch"
DB_CONFIG = {
    "dbname": "your_db_name",
    "user": "your_user",
    "password": "your_password",
    "host": "your_host",
    "port": "your_port",
}
BATCH_SIZE = 1000

# --- GET CARD IDS ---
def get_card_ids():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM mastercard")
            return [row[0] for row in cur.fetchall()]

# --- UPDATE PRICES IN DB ---
def update_prices(data):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            execute_batch(
                cur,
                """
                UPDATE mastercard
                SET tcg_market_price = %s,
                    tcg_low_price = %s
                WHERE id = %s
                """,
                [(d["market"], d["low"], d["id"]) for d in data],
            )
            conn.commit()

# --- MAIN LOOP ---
def run():
    card_ids = get_card_ids()
    for i in range(0, len(card_ids), BATCH_SIZE):
        batch = card_ids[i:i + BATCH_SIZE]
        response = requests.post(API_URL, json={"card_ids": batch})
        if response.status_code == 200:
            update_prices(response.json())
            print(f"✅ Updated batch {i}–{i + len(batch)}")
        else:
            print(f"❌ Failed batch {i}: {response.status_code} - {response.text}")

if __name__ == "__main__":
    run()

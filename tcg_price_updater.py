import os
import re
import json
import requests
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from datetime import date

# Load env variables
load_dotenv()

# Fix DATABASE_URL for psycopg2
DATABASE_URL = os.getenv("DATABASE_URL").replace("postgresql+asyncpg", "postgres")
API_URL = os.getenv("TCG_API_URL", "https://cardcatchdb.onrender.com/tcg-prices-batch-async")
API_KEY = os.getenv("API_KEY")  # Now used as x-api-key
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "333"))

def normalize_card_id(card_id: str) -> str:
    match = re.match(r"([a-zA-Z0-9]+)-(\d+)", card_id)
    if match:
        set_code, number = match.groups()
        return f"{set_code.upper()}-{number}"
    return card_id.upper()

def get_card_ids():
    print("📡 Connecting to DB and pulling card IDs...")

    # Check for scoped scrape list from controller
    try:
        with open("cards_due.json") as f:
            due_cards = json.load(f)
            limited_ids = [normalize_card_id(card["unique_id"]) for card in due_cards]
            print(f"🔒 Scoped to {len(limited_ids)} cards from cards_due.json")
            return limited_ids
    except Exception as e:
        print(f"⚠️ Could not use cards_due.json (fallback to all): {e}")

    # Default full run fallback
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT unique_id FROM mastercard_v2 WHERE tcg_market_price IS NULL")
            rows = cur.fetchall()
            print(f"🔢 Found {len(rows)} unpriced card IDs.")
            return [normalize_card_id(row[0]) for row in rows]

def insert_pricing_logs(data):
    print(f"💾 Inserting pricing logs...")
    records = []
    today = date.today()
    for d in data:
        if d.get("market") is None and d.get("low") is None:
            continue
        records.append((d.get("card_id"), d.get("market"), d.get("low"), today))
    if not records:
        print("⚠️ No valid pricing data to insert.")
        return

    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            execute_batch(cur,
                """
                INSERT INTO tcg_pricing_log (unique_id, market_price, low_price, date_logged)
                VALUES (%s, %s, %s, %s)
                """, records
            )
            conn.commit()
    print(f"✅ Inserted {len(records)} pricing log records.")

def log_failure(card_id, source, error_msg):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scrape_failures (unique_id, scraper_source, error_message)
                VALUES (%s, %s, %s)
                """, (card_id, source, error_msg)
            )
            conn.commit()

def run():
    card_ids = get_card_ids()
    headers = {"x-api-key": API_KEY}

    for i in range(0, len(card_ids), BATCH_SIZE):
        batch = card_ids[i:i + BATCH_SIZE]
        print(f"\n🚀 Sending batch {i}–{i + len(batch)} to TCG endpoint...")

        try:
            response = requests.post(API_URL, json={"card_ids": batch}, headers=headers)
            print(f"📡 Response status: {response.status_code}")

            if response.status_code != 200:
                print(f"❌ Failed batch {i}: {response.status_code} - {response.text}")
                for card_id in batch:
                    log_failure(card_id, "tcg", f"Batch failed with status {response.status_code}")
                continue

            results = response.json()
            print(f"📦 Received {len(results)} results")
            if results:
                print(f"🧪 Sample: {results[:3]}")
            else:
                print("⚠️ No results returned from endpoint.")

            insert_pricing_logs(results)

        except Exception as e:
            print(f"❌ Exception in batch {i}: {e}")
            for card_id in batch:
                log_failure(card_id, "tcg", str(e))

if __name__ == "__main__":
    print("🟢 TCG Price Update Script Starting...")
    run()

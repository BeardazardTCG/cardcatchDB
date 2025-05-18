import requests
import psycopg2
import re
from psycopg2.extras import execute_batch

# --- CONFIG ---
API_URL = "https://cardcatchdb.onrender.com/tcg-prices-batch-async"
DB_CONFIG = {
    "dbname": "railway",
    "user": "postgres",
    "password": "ckQFRJkrJluWsJnHsDhlhvbtSridadDF",
    "host": "metro.proxy.rlwy.net",
    "port": "52025",
}
BATCH_SIZE = 333

# --- CARD ID NORMALIZER ---
def normalize_card_id(card_id: str) -> str:
    match = re.match(r"([a-zA-Z0-9]+)-(\d+)", card_id)
    if match:
        set_code, number = match.groups()
        return f"{set_code.upper()}-{number}"  # ✅ no zero-padding
    return card_id.upper()

# --- GET CARD IDs ---
def get_card_ids():
    print("📡 Connecting to DB and pulling card IDs...")
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT card_id FROM mastercard WHERE tcg_market_price IS NULL AND tier IN ('1','2','3')")
            rows = cur.fetchall()
            print(f"🔢 Found {len(rows)} unpriced card IDs in Tier 1–3.")
            return [normalize_card_id(row[0]) for row in rows]

# --- UPDATE PRICES IN DB ---
def update_prices(data):
    print(f"💾 Writing prices to DB...")
    updated_rows = 0
    skipped = 0
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for d in data:
                if d["market"] is None and d["low"] is None:
                    skipped += 1
                    continue
                cur.execute(
                    """
                    UPDATE mastercard
                    SET tcg_market_price = %s,
                        tcg_low_price = %s
                    WHERE card_id = %s
                    """,
                    (d["market"], d["low"], d["card_id"])
                )
                updated_rows += cur.rowcount
            conn.commit()
    print(f"✅ DB updated rows: {updated_rows} (Skipped: {skipped})")

# --- MAIN LOOP ---
def run():
    card_ids = get_card_ids()
    for i in range(0, len(card_ids), BATCH_SIZE):
        batch = card_ids[i:i + BATCH_SIZE]
        print(f"\n🚀 Sending batch {i}–{i + len(batch)} to TCG endpoint...")

        try:
            response = requests.post(API_URL, json={"card_ids": batch})
            print(f"📡 Response status: {response.status_code}")

            if response.status_code != 200:
                print(f"❌ Failed batch {i}: {response.status_code} - {response.text}")
                continue

            results = response.json()
            print(f"📦 Received {len(results)} results")
            if results:
                print(f"🧪 Sample: {results[:3]}")
            else:
                print("⚠️ No results returned from endpoint.")

            update_prices(results)

        except Exception as e:
            print(f"❌ Exception in batch {i}: {e}")
            try:
                print(f"🔴 Raw response: {response.text[:500]}")
            except:
                pass

if __name__ == "__main__":
    print("🟢 TCG Price Update Script Starting...")
    run()

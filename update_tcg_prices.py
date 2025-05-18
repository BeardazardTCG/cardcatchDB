import requests
import psycopg2
from psycopg2.extras import execute_batch

# --- CONFIG ---
API_URL = "https://cardcatchdb.onrender.com/tcg-prices-batch"
DB_CONFIG = {
    "dbname": "railway",
    "user": "postgres",
    "password": "ckQFRJkrJluWsJnHsDhlhvbtSridadDF",
    "host": "metro.proxy.rlwy.net",
    "port": "52025",
}
BATCH_SIZE = 1000

# --- GET CARD IDs ---
def get_card_ids():
    print("📡 Connecting to DB and pulling card IDs...")
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT card_id FROM mastercard")
            rows = cur.fetchall()
            print(f"🔢 Found {len(rows)} card IDs.")
            return [row[0] for row in rows]

# --- UPDATE PRICES IN DB ---
def update_prices(data):
    print(f"💾 Writing {len(data)} prices to DB...")
    updated_rows = 0
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for d in data:
                cur.execute(
                    """
                    UPDATE mastercard
                    SET tcg_market_price = %s,
                        tcg_low_price = %s
                    WHERE card_id = %s
                    """,
                    (d["market"], d["low"], d["id"])
                )
                updated_rows += cur.rowcount
            conn.commit()
    print(f"✅ DB updated rows: {updated_rows}")

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
            print(f"🧪 Sample: {results[:3]}")

            if len(results) == 0:
                print("⚠️  No data returned for this batch. Skipping DB update.")
            else:
                update_prices(results)

        except Exception as e:
            print(f"❌ Exception in batch {i}: {e}")
            print(f"🔴 Raw response: {response.text[:500]}")

if __name__ == "__main__":
    print("🟢 TCG Price Update Script Starting...")
    run()

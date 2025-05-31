# scraper_controller.py
# Purpose: Decide which cards are due for scraping based on tier and freshness,
# and safely call the existing TCG + eBay dual scraper scripts. Logs all activity.

import os
import subprocess
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# === Load env ===
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
DATABASE_URL = DATABASE_URL.replace("postgresql+asyncpg", "postgres")  # Fix for psycopg2

# === Tier frequency rules ===
TIER_INTERVALS = {
    1: timedelta(days=1),
    2: timedelta(days=2),
    3: timedelta(days=3),
    4: timedelta(days=7),
}

# === Logging helpers ===
def log_scrape_event(source, status, count, notes=""):
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO scrape_log (source, status, card_count, notes)
                    VALUES (%s, %s, %s, %s)
                """, (source, status, count, notes))
                conn.commit()
    except Exception as e:
        print(f"❌ Failed to log scrape event: {e}")

def log_failure(source, message):
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO scrape_failures (unique_id, scraper_source, error_message)
                    VALUES (%s, %s, %s)
                """, ("controller", source, message))
                conn.commit()
    except Exception as e:
        print(f"❌ Failed to log scrape failure: {e}")

# === Pull card list from DB ===
def get_cards_due():
    print("📡 Connecting to DB and checking for due cards...")
    today = datetime.utcnow().date()
    due_cards = []

    try:
        with psycopg2.connect(DATABASE_URL, connect_timeout=10) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT unique_id, query, tier
                    FROM mastercard_v2
                    WHERE tier IS NOT NULL
                    
                """)
                cards = cur.fetchall()

                for card in cards:
                    tier = card["tier"]
                    threshold = TIER_INTERVALS.get(tier)
                    if not threshold:
                        continue

                    cur.execute("""
                        SELECT MAX(sold_date) AS last_date FROM dailypricelog
                        WHERE unique_id = %s
                    """, (card["unique_id"],))
                    last_entry = cur.fetchone()["last_date"]

                    if not last_entry or (today - last_entry) >= threshold:
                        due_cards.append(card)

    except Exception as e:
        print(f"❌ Error during DB check: {e}")
        log_failure("controller", str(e))

    print(f"✅ {len(due_cards)} cards are due for scraping.")
    return due_cards

# === Call scraper script ===
def call_dual_scraper():
    try:
        print("🚀 Running dual eBay scraper...")
        subprocess.run(["python", "archive/scrape_ebay_dual.py"], check=True)
        log_scrape_event("ebay_dual", "success", -1)
    except subprocess.CalledProcessError as e:
        print("❌ eBay scraper failed:", e)
        log_scrape_event("ebay_dual", "fail", 0, str(e))
        log_failure("ebay_dual", str(e))


def call_tcg_scraper():
    try:
        print("🚀 Running TCG scraper...")
        subprocess.run(["python", "tcg_price_updater.py"], check=True)
        log_scrape_event("tcg", "success", -1)
    except subprocess.CalledProcessError as e:
        print("❌ TCG scraper failed:", e)
        log_scrape_event("tcg", "fail", 0, str(e))
        log_failure("tcg", str(e))

# === Main ===
if __name__ == "__main__":
    print("🟢 Starting CardCatch Scraper Controller...")
    due_cards = get_cards_due()

    if due_cards:
        call_dual_scraper()
        call_tcg_scraper()

        # ✅ Run post-scrape logic
        try:
            print("🧮 Running post-scrape update (clean values + tier recalculation)...")
            subprocess.run(["python", "post_scrape_update.py"], check=True)
            log_scrape_event("post_scrape_update", "success", -1)
        except subprocess.CalledProcessError as e:
            print("❌ Post-scrape update failed:", e)
            log_scrape_event("post_scrape_update", "fail", 0, str(e))
            log_failure("post_scrape_update", str(e))

    else:
        print("🛌 No cards due today.")
        log_scrape_event("controller", "no_due_cards", 0, "No cards met scrape conditions.")

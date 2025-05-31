# scraper_controller.py
# Purpose: Decides which cards are due for scraping (based on tier and last update),
# and calls the appropriate scraper without modifying them. Logs all run outcomes.

import os
import subprocess
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# === Load env ===
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# === Tier frequency rules ===
TIER_INTERVALS = {
    1: timedelta(days=1),
    2: timedelta(days=2),
    3: timedelta(days=3),
    4: timedelta(days=7),
}

# === Logging helpers ===
def log_scrape_event(source, status, count, notes=""):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO scrape_log (source, status, card_count, notes)
                VALUES (%s, %s, %s, %s)
            """, (source, status, count, notes))
            conn.commit()

def log_failure(source, message):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO scrape_failures (unique_id, scraper_source, error_message)
                VALUES (%s, %s, %s)
            """, ("controller", source, message))
            conn.commit()

# === Pull card list from DB ===
def get_cards_due():
    print("📡 Fetching cards due for scrape...")
    today = datetime.utcnow().date()
    due_cards = []

    with psycopg2.connect(DATABASE_URL) as conn:
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

    print(f"✅ {len(due_cards)} cards are due for scraping.")
    return due_cards

# === Call scraper script ===
def call_dual_scraper():
    try:
        print("🚀 Running dual eBay scraper...")
        subprocess.run(["python", "archive/run_dual_scraper.py"], check=True)
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
    due_cards = get_cards_due()

    if due_cards:
        call_dual_scraper()
        call_tcg_scraper()
    else:
        print("🛌 No cards due today.")
        log_scrape_event("controller", "no_due_cards", 0, "No cards met scrape threshold")

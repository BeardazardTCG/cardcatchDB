import os
import sys
import json
import subprocess
import argparse
from datetime import datetime, timedelta, date
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# === Boot log ===
try:
    with open("controller_boot_log.txt", "a") as f:
        f.write(f"🟢 Started at {datetime.utcnow()}\n")
except:
    pass

# === Load and fix env ===
load_dotenv()
raw_url = os.getenv("DATABASE_URL")
if not raw_url:
    raise RuntimeError("❌ DATABASE_URL not found in environment.")
DATABASE_URL = raw_url.replace("postgresql+asyncpg", "postgresql")

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

# === Pull cards from DB ===
def get_cards_due():
    print("📡 Connecting to DB and checking for due cards...")
    today = date.today()
    due_cards = []

    try:
        with psycopg2.connect(DATABASE_URL, connect_timeout=15) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                print("✅ DB connected. Pulling cards and scrape timestamps...")

                cur.execute("""SELECT unique_id, query, tier FROM mastercard_v2 WHERE tier IS NOT NULL""")
                cards = cur.fetchall()

                cur.execute("""SELECT unique_id, MAX(created_at) AS last_scrape FROM dailypricelog GROUP BY unique_id""")
                sold_seen = {row["unique_id"]: row["last_scrape"] for row in cur.fetchall()}

                cur.execute("""SELECT unique_id, MAX(created_at) AS last_scrape FROM activedailypricelog GROUP BY unique_id""")
                active_seen = {row["unique_id"]: row["last_scrape"] for row in cur.fetchall()}

                for card in cards:
                    tier = card["tier"]
                    threshold = TIER_INTERVALS.get(tier)
                    if not threshold:
                        continue

                    uid = card["unique_id"]
                    latest_scrape_date = max(
                        (sold_seen.get(uid) or datetime(2000, 1, 1)).date(),
                        (active_seen.get(uid) or datetime(2000, 1, 1)).date()
                    )

                    if (today - latest_scrape_date) >= threshold:
                        due_cards.append(card)

    except Exception as e:
        print(f"❌ Error during DB check: {e}")
        log_failure("controller", str(e))
        with open("controller_errors.txt", "a") as f:
            f.write(f"{datetime.utcnow()} DB error: {str(e)}\n")

    print(f"✅ {len(due_cards)} cards are due for scraping.")
    return due_cards

def get_cards_by_tiers(tiers):
    print(f"📡 Fetching all cards in Tier(s) {tiers}...")
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                placeholders = ','.join(['%s'] * len(tiers))
                cur.execute(f"""
                    SELECT unique_id, query, tier FROM mastercard_v2
                    WHERE tier IN ({placeholders})
                """, tuple(tiers))
                cards = cur.fetchall()
                print(f"✅ Found {len(cards)} cards across tiers {tiers}")
                return cards
    except Exception as e:
        print(f"❌ Failed to fetch tier(s): {e}")
        return []

# === Script wrappers ===
def call_dual_scraper():
    try:
        print("🚀 Running dual eBay scraper...")
        result = subprocess.run(
            [sys.executable, "archive/scrape_ebay_dual.py"],
            check=True,
            stdout=sys.stdout,
            stderr=sys.stderr
        )
        print(f"🔁 Scraper finished with return code {result.returncode}")
        log_scrape_event("ebay_dual", "success", -1)
    except subprocess.CalledProcessError as e:
        print("❌ eBay scraper failed:", e)
        log_scrape_event("ebay_dual", "fail", 0, str(e))
        log_failure("ebay_dual", str(e))

def call_tcg_scraper():
    try:
        print("🚀 Running TCG scraper...")
        result = subprocess.run(
            [sys.executable, "tcg_price_updater.py"],
            check=True,
            stdout=sys.stdout,
            stderr=sys.stderr
        )
        print(f"🔁 TCG scraper finished with return code {result.returncode}")
        log_scrape_event("tcg", "success", -1)
    except subprocess.CalledProcessError as e:
        print("❌ TCG scraper failed:", e)
        log_scrape_event("tcg", "fail", 0, str(e))
        log_failure("tcg", str(e))

# === MAIN ===
if __name__ == "__main__":
    try:
        print("🟢 Starting CardCatch Scraper Controller...")

        parser = argparse.ArgumentParser()
        parser.add_argument("--tier", type=str, help="Comma-separated tier(s) to manually scrape, e.g. 4 or 2,5,6")
        args = parser.parse_args()

        if args.tier:
            tier_values = [int(t.strip()) for t in args.tier.split(",")]
            print(f"⚙️ Manual override: Scraping Tier(s): {tier_values}")
            due_cards = get_cards_by_tiers(tier_values)
        else:
            due_cards = get_cards_due()

        if due_cards:
            try:
                with open("cards_due.json", "w", encoding="utf-8") as f:
                    json.dump(due_cards, f, indent=2)
                print(f"📄 Wrote {len(due_cards)} cards to cards_due.json")
            except Exception as e:
                print(f"❌ Failed to write cards_due.json: {e}")
                log_failure("controller", f"JSON write failed: {e}")

            call_dual_scraper()
            call_tcg_scraper()

            try:
                print("🧮 Running post-scrape update (clean values + tier recalculation)...")
                result = subprocess.run(
                    [sys.executable, "update_clean_and_tiers.py"],
                    check=True,
                    stdout=sys.stdout,
                    stderr=sys.stderr
                )
                print(f"🔁 Post-scrape update finished with return code {result.returncode}")
                log_scrape_event("post_scrape_update", "success", -1)
            except subprocess.CalledProcessError as e:
                print("❌ Post-scrape update failed:", e)
                log_scrape_event("post_scrape_update", "fail", 0, str(e))
                log_failure("post_scrape_update", str(e))
        else:
            print("🛌 No cards to scrape.")
            log_scrape_event("controller", "no_due_cards", 0, "Nothing to run.")

    except Exception as e:
        print(f"🔥 Fatal controller crash: {e}")
        with open("controller_errors.txt", "a") as f:
            f.write(f"{datetime.utcnow()} FATAL: {str(e)}\n")
        log_failure("controller", str(e))

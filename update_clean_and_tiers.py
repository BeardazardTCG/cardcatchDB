import psycopg2
from collections import defaultdict
import datetime
import json
import traceback

DATABASE_URL = "postgresql://postgres:ckQFRJkrJluWsJnHsDhlhvbtSridadDF@metro.proxy.rlwy.net:52025/railway"

def filter_outliers(prices):
    if not prices:
        return []
    sorted_prices = sorted(prices)
    q1 = sorted_prices[len(prices) // 4]
    q3 = sorted_prices[(len(prices) * 3) // 4]
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return [p for p in prices if lower <= p <= upper]

def calculate_median(prices):
    n = len(prices)
    if n == 0:
        return None
    sorted_prices = sorted(prices)
    mid = n // 2
    return (sorted_prices[mid - 1] + sorted_prices[mid]) / 2 if n % 2 == 0 else sorted_prices[mid]

def log_update(cur, uid, changes):
    def serialize(obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        return str(obj)

    cur.execute("""
        INSERT INTO post_scrape_log (unique_id, log_time, changes)
        VALUES (%s, %s, %s)
    """, (uid, datetime.datetime.utcnow(), json.dumps(changes, default=serialize)))

def main():
    try:
        print("Connecting to database...")
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        print("Fetching price logs...")
        ninety_days_ago = datetime.datetime.utcnow().date() - datetime.timedelta(days=90)

        cur.execute("""
            SELECT unique_id, median_price, sold_date 
            FROM dailypricelog 
            WHERE median_price IS NOT NULL AND trusted = TRUE
        """)
        sold_data = defaultdict(list)
        for uid, price, sold_date in cur.fetchall():
            if sold_date >= ninety_days_ago:
                sold_data[uid.strip()].append(float(price))

        cur.execute("SELECT unique_id, median_price FROM activedailypricelog WHERE median_price IS NOT NULL")
        active_data = defaultdict(list)
        for uid, price in cur.fetchall():
            active_data[uid.strip()].append(float(price))

        cur.execute("SELECT unique_id, market_price, low_price FROM tcg_pricing_log")
        tcg_data = {}
        for uid, market, low in cur.fetchall():
            tcg_data[uid.strip()] = {
                "market": float(market) if market else None,
                "low": float(low) if low else None,
            }

        cur.execute("""
            SELECT m.unique_id,
                   CASE WHEN w.unique_id IS NOT NULL THEN TRUE ELSE FALSE END AS wishlist,
                   CASE WHEN i.unique_id IS NOT NULL THEN TRUE ELSE FALSE END AS inventory,
                   m.hot_character
            FROM mastercard_v2 m
            LEFT JOIN wishlist w ON m.unique_id = w.unique_id
            LEFT JOIN inventory i ON m.unique_id = i.unique_id
        """)
        flag_data = {}
        for uid, wishlist, inventory, hot_character in cur.fetchall():
            flag_data[uid.strip()] = {
                "wishlist": wishlist,
                "inventory": inventory,
                "hot_character": hot_character
            }

        try:
            with open("cards_due.json") as f:
                scoped_ids = set(card["unique_id"].strip() for card in json.load(f))
                print(f"🔒 Limiting post-scrape updates to {len(scoped_ids)} cards from cards_due.json")
        except Exception as e:
            scoped_ids = None
            print(f"⚠️ No cards_due.json found or invalid. Running full update. ({e})")

        cur.execute("SELECT unique_id FROM mastercard_v2")
        all_cards = [row[0].strip() for row in cur.fetchall()]
        if scoped_ids:
            all_cards = [uid for uid in all_cards if uid in scoped_ids]
        conn.commit()

        for i, uid in enumerate(all_cards, 1):
            try:
                updates = {}
                sold_prices = sold_data.get(uid, [])
                active_prices = active_data.get(uid, [])
                tcg_prices = tcg_data.get(uid, {})

                filtered_sold = []
                median_sold = None
                if sold_prices:
                    filtered_sold = filter_outliers(sold_prices)
                    if filtered_sold:
                        median_sold = calculate_median(filtered_sold)

                filtered_active = []
                median_active = None
                if median_sold is None and active_prices:
                    filtered_active = filter_outliers(active_prices)
                    if len(filtered_active) >= 2:
                        median_active = calculate_median(filtered_active)

                tcg_price = None
                if median_sold is None and median_active is None:
                    if tcg_prices.get("market") is not None:
                        tcg_price = tcg_prices["market"]
                    elif tcg_prices.get("low") is not None:
                        tcg_price = tcg_prices["low"]

                clean = median_sold or median_active or tcg_price
                if clean is not None:
                    updates["clean_avg_value"] = round(clean, 2)

                if filtered_sold:
                    updates["verified_sales_logged"] = len(filtered_sold)
                    updates["price_range_seen_min"] = round(min(filtered_sold), 2)
                    updates["price_range_seen_max"] = round(max(filtered_sold), 2)

                flags = flag_data.get(uid, {"wishlist": False, "inventory": False, "hot_character": False})
                wishlist = flags["wishlist"]
                inventory = flags["inventory"]
                hot = flags["hot_character"]

                tier = None
                if wishlist or inventory:
                    tier = 1
                elif clean is not None:
                    if 7 <= clean <= 11:
                        tier = 2 if hot else 3
                    elif clean > 11:
                        tier = 4 if hot else 5
                    elif 3 <= clean < 7:
                        tier = 6 if hot else 7
                    elif clean < 3:
                        tier = 8 if hot else 9
                updates["tier"] = tier

                if updates:
                    set_clause = ', '.join([f"{key} = %s" for key in updates.keys()])
                    values = list(updates.values()) + [uid]
                    cur.execute(f"""
                        UPDATE mastercard_v2
                        SET {set_clause}
                        WHERE unique_id = %s
                    """, values)
                    log_update(cur, uid, updates)
                    with open("clean_log.txt", "a") as f:
                        f.write(f"✅ Updated {uid} at {datetime.datetime.utcnow().isoformat()} | {updates}\n")
                    conn.commit()
                    print(f"✅ {uid} | sold={median_sold} active={median_active} tcg={tcg_price} → tier {tier}")
                else:
                    print(f"⚠️ Skipped {uid} — no usable price data")
            except Exception as e:
                print(f"❌ Error updating {uid}: {e}")
                traceback.print_exc()
                conn.rollback()

        print("✅ All updates complete.")
        cur.close()
        conn.close()

        import os
        if os.path.exists("cards_due.json"):
            try:
                os.remove("cards_due.json")
                print("🧹 Removed cards_due.json after clean update.")
            except Exception as e:
                print(f"⚠️ Could not remove cards_due.json: {e}")

    except Exception as e:
        print(f"💥 Fatal error during execution: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()

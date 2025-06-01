import psycopg2
from collections import defaultdict
import datetime

DATABASE_URL = "postgresql://postgres:ckQFRJkrJluWsJnHsDhlhvbtSridadDF@metro.proxy.rlwy.net:52025/railway"

def filter_outliers(prices):
    if not prices:
        return []
    sorted_prices = sorted(prices)
    q1 = sorted_prices[len(sorted_prices) // 4]
    q3 = sorted_prices[(len(sorted_prices) * 3) // 4]
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
    cur.execute("""
        INSERT INTO post_scrape_log (unique_id, log_time, changes)
        VALUES (%s, %s, %s)
    """, (uid, datetime.datetime.utcnow(), changes))

def main():
    print("🔌 Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # ------------------- FETCH RAW DATA -------------------
    print("📥 Fetching price logs...")

    cur.execute("SELECT unique_id, median_price FROM dailypricelog WHERE median_price IS NOT NULL")
    sold_data = defaultdict(list)
    for uid, price in cur.fetchall():
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

    cur.execute("SELECT unique_id, wishlist, inventory, hot_character FROM mastercard_v2")
    flag_data = {}
    for uid, wishlist, inventory, hot_character in cur.fetchall():
        flag_data[uid.strip()] = {
            "wishlist": wishlist,
            "inventory": inventory,
            "hot_character": hot_character
        }

    cur.execute("SELECT unique_id FROM mastercard_v2")
    all_cards = [row[0].strip() for row in cur.fetchall()]

    # ------------------- PROCESS + UPDATE -------------------
    for i, uid in enumerate(all_cards, 1):
        updates = {}

        # Sold median
        if uid in sold_data:
            filtered = filter_outliers(sold_data[uid])
            median_sold = calculate_median(filtered)
            if median_sold:
                updates['sold_ebay_median'] = round(median_sold, 2)

        # Active median
        if uid in active_data:
            filtered = filter_outliers(active_data[uid])
            median_active = calculate_median(filtered)
            if median_active:
                updates['active_ebay_median'] = round(median_active, 2)

        # TCG
        if uid in tcg_data:
            if tcg_data[uid]['market'] is not None:
                updates['tcg_market_price'] = round(tcg_data[uid]['market'], 2)
            if tcg_data[uid]['low'] is not None:
                updates['tcg_low_price'] = round(tcg_data[uid]['low'], 2)

        # Clean Avg
        sold = updates.get('sold_ebay_median')
        active = updates.get('active_ebay_median')
        tcg = updates.get('tcg_market_price')

        clean = None
        if sold is not None:
            clean = sold
        elif active is not None and tcg is not None:
            clean = (active + tcg) / 2
        elif active is not None:
            clean = active
        elif tcg is not None:
            clean = tcg

        if clean is not None:
            updates['clean_avg_value'] = round(clean, 2)

        # Tier Logic (Locked)
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

        if i % 500 == 0:
            conn.commit()
            print(f"🔁 Committed batch: {i - 499}–{i}")

    conn.commit()
    print("✅ All updates complete.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()

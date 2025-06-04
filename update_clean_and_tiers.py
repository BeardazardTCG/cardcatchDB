import psycopg2
from collections import defaultdict
import datetime
import json

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
    """, (uid, datetime.datetime.utcnow(), json.dumps(changes)))

def main():
    print("🔌 Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # --- Fetch sold price logs with date ---
    print("📥 Fetching price logs...")
    ninety_days_ago = datetime.datetime.utcnow().date() - datetime.timedelta(days=90)
    cur.execute("""
        SELECT unique_id, median_price, sold_date 
        FROM dailypricelog 
        WHERE median_price IS NOT NULL AND sold_date >= %s
    """, (ninety_days_ago,))
    raw_sold = defaultdict(list)
    sold_dates = defaultdict(list)
    for uid, price, sold_date in cur.fetchall():
        uid = uid.strip()
        raw_sold[uid].append(float(price))
        sold_dates[uid].append(sold_date)

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

    cur.execute("SELECT unique_id FROM mastercard_v2")
    all_cards = [row[0].strip() for row in cur.fetchall()]

    for i, uid in enumerate(all_cards, 1):
        updates = {}

        # --- Clean Value Logic ---
        sold_prices = raw_sold.get(uid, [])
        sold_price_dates = sold_dates.get(uid, [])
        filtered_sold = filter_outliers(sold_prices)

        median_sold = None
        if len(filtered_sold) >= 2:
            median_sold = calculate_median(filtered_sold)

        median_active = None
        filtered_active = filter_outliers(active_data.get(uid, []))
        if median_sold is None and len(filtered_active) >= 2:
            median_active = calculate_median(filtered_active)

        tcg_price = None
        if median_sold is None and median_active is None:
            tcg_entry = tcg_data.get(uid, {})
            if tcg_entry.get("market") is not None:
                tcg_price = tcg_entry["market"]
            elif tcg_entry.get("low") is not None:
                tcg_price = tcg_entry["low"]

        clean = median_sold or median_active or tcg_price
        if clean is not None:
            updates["clean_avg_value"] = round(clean, 2)

        # --- Add filtered supporting fields if valid ---
        if filtered_sold:
            updates["verified_sales_logged"] = len(filtered_sold)
            updates["price_range_seen_min"] = round(min(filtered_sold), 2)
            updates["price_range_seen_max"] = round(max(filtered_sold), 2)

            # Get dates of matching filtered values
            matched_dates = [
                date for price, date in zip(sold_prices, sold_price_dates)
                if price in filtered_sold
            ]
            if matched_dates:
                updates["last_verified_sale"] = max(matched_dates)

        # --- Tier Logic ---
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

        # --- Apply DB Update ---
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

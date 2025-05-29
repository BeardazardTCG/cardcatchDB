import os
import psycopg2
from collections import defaultdict
import time

# === Hardcoded DB connection ===
DATABASE_URL = "postgresql://postgres:ckQFRJkrJluWsJnHsDhlhvbtSridadDF@metro.proxy.rlwy.net:52025/railway"

# === Outlier filtering ===
def filter_outliers(prices):
    if not prices:
        return []
    sorted_prices = sorted(prices)
    q1 = sorted_prices[len(sorted_prices) // 4]
    q3 = sorted_prices[(len(sorted_prices) * 3) // 4]
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    return [p for p in prices if lower_bound <= p <= upper_bound]

# === Median calculator ===
def calculate_median(prices):
    n = len(prices)
    if n == 0:
        return None
    sorted_prices = sorted(prices)
    mid = n // 2
    if n % 2 == 0:
        return (sorted_prices[mid - 1] + sorted_prices[mid]) / 2
    return sorted_prices[mid]

def batch_update(cur, data, query):
    args_str = ','.join(cur.mogrify("(%s, %s)", x).decode('utf-8') for x in data)
    sql = f"""
        UPDATE mastercard_v2 AS m
        SET sold_ebay_median = v.median
        FROM (VALUES {args_str}) AS v(unique_id, median)
        WHERE m.unique_id = v.unique_id;
    """
    cur.execute(sql)

def main():
    print("🔌 Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # === 1. Sold eBay median ===
    print("📦 Fetching sold prices...")
    cur.execute("""
        SELECT unique_id, median_price
        FROM dailypricelog
        WHERE median_price IS NOT NULL
    """)
    sold_map = defaultdict(list)
    for uid, price in cur.fetchall():
        sold_map[uid.strip()].append(float(price))

    print(f"💾 Updating {len(sold_map)} sold medians in VALUE-batches...")
    batch = []
    for i, (uid, prices) in enumerate(sold_map.items(), 1):
        filtered = filter_outliers(prices)
        median = calculate_median(filtered)
        if median is not None:
            batch.append((uid, round(median, 2)))
        if i % 500 == 0:
            batch_update(cur, batch, None)
            conn.commit()
            print(f"🔄 Committed batch {i-499}–{i}")
            batch.clear()
            time.sleep(0.3)

    if batch:
        batch_update(cur, batch, None)
        conn.commit()
        print(f"🔄 Committed final batch")

    # === 2. TCGPlayer market price ===
    print("📦 Fetching TCGPlayer prices...")
    cur.execute("""
        SELECT DISTINCT ON (unique_id) unique_id, market_price
        FROM tcg_pricing_log
        WHERE market_price IS NOT NULL
        ORDER BY unique_id, date DESC
    """)
    for uid, price in cur.fetchall():
        cur.execute("""
            UPDATE mastercard_v2
            SET tcgplayer_market_price = %s
            WHERE unique_id = %s
        """, (round(float(price), 2), uid.strip()))
    conn.commit()
    print("✅ TCGPlayer prices updated")

    # === 3. Active BIN prices (filtered) ===
    print("📦 Fetching active BIN prices...")
    try:
        cur.execute("""
            SELECT unique_id, lowest_price
            FROM activedailypricelog
            WHERE lowest_price IS NOT NULL
              AND date = CURRENT_DATE
        """)
        active_map = defaultdict(list)
        for uid, price in cur.fetchall():
            active_map[uid.strip()].append(float(price))

        for uid, prices in active_map.items():
            filtered = filter_outliers(prices)
            if filtered:
                lowest = min(filtered)
                cur.execute("""
                    UPDATE mastercard_v2
                    SET active_ebay_lowest = %s
                    WHERE unique_id = %s
                """, (round(lowest, 2), uid))
        conn.commit()
        print("✅ Active BIN prices updated")

    except psycopg2.errors.UndefinedColumn:
        print("⚠️ Skipping active BIN update — column not found")

    cur.close()
    conn.close()
    print("🏁 All updates completed.")

if __name__ == "__main__":
    main()

import os
import time
import psycopg2
from collections import defaultdict

# === Hardcoded database URL ===
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

def batch_commit(cur, conn, batch, query, label):
    if batch:
        cur.executemany(query, batch)
        conn.commit()
        print(f"🔄 Committed batch {label}")
        batch.clear()
        time.sleep(0.1)  # Reduced throttle for better performance

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
    sold_query = """
        UPDATE mastercard_v2
        SET sold_ebay_median = %s
        WHERE unique_id = %s
    """
    sold_batch = []
    for i, (uid, prices) in enumerate(sold_map.items(), 1):
        filtered = filter_outliers(prices)
        median = calculate_median(filtered)
        if median is not None:
            sold_batch.append((round(median, 2), uid))
        if i % 500 == 0:
            label = f"{i - 499}–{i}"
            batch_commit(cur, conn, sold_batch, sold_query, label)

    if sold_batch:
        batch_commit(cur, conn, sold_batch, sold_query, f"{i - (i % 500) + 1}–{i}")
    print(f"✅ Sold updates complete: {i} processed")

    # === 2. TCGPlayer prices ===
    print("📦 Fetching TCGPlayer prices...")
    cur.execute("""
        SELECT DISTINCT ON (unique_id) unique_id, market_price
        FROM tcg_pricing_log
        WHERE market_price IS NOT NULL
        ORDER BY unique_id, date_logged DESC
    """)
    tcg_query = """
        UPDATE mastercard_v2
        SET tcgplayer_market_price = %s
        WHERE unique_id = %s
    """
    tcg_batch = [(round(float(price), 2), uid.strip()) for uid, price in cur.fetchall()]
    if tcg_batch:
        cur.executemany(tcg_query, tcg_batch)
        conn.commit()
    print(f"✅ TCG updates complete: {len(tcg_batch)} cards updated")

    # === 3. Active BIN prices ===
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

        active_query = """
            UPDATE mastercard_v2
            SET active_ebay_lowest = %s
            WHERE unique_id = %s
        """
        active_batch = []
        for i, (uid, prices) in enumerate(active_map.items(), 1):
            filtered = filter_outliers(prices)
            if filtered:
                active_batch.append((round(min(filtered), 2), uid))
            if i % 500 == 0:
                label = f"{i - 499}–{i}"
                batch_commit(cur, conn, active_batch, active_query, label)

        if active_batch:
            batch_commit(cur, conn, active_batch, active_query, f"{i - (i % 500) + 1}–{i}")
        print(f"✅ Active BIN updates complete: {i} processed")

    except psycopg2.errors.UndefinedColumn:
        print("⚠️ Skipping active BIN update — column not found")

    # === Done ===
    cur.close()
    conn.close()
    print("🏁 All updates completed.")

if __name__ == "__main__":
    main()

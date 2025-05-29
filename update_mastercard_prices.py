import os
import time
import psycopg2
from collections import defaultdict

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

def batch_commit(cur, conn, batch, query, label):
    if batch:
        cur.executemany(query, batch)
        conn.commit()
        print(f"🔄 Committed batch {label}")
        batch.clear()
        time.sleep(0.1)

def main():
    print("🔌 Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("📦 Fetching all active eBay listing medians...")
    cur.execute("""
        SELECT unique_id, median_price
        FROM activedailypricelog
        WHERE median_price IS NOT NULL
    """)

    price_map = defaultdict(list)
    for uid, price in cur.fetchall():
        price_map[uid.strip()].append(float(price))

    update_query = """
        UPDATE mastercard_v2
        SET active_ebay_median = %s
        WHERE unique_id = %s
    """

    batch = []
    for i, (uid, prices) in enumerate(price_map.items(), 1):
        filtered = filter_outliers(prices)
        median = calculate_median(filtered)
        if median is not None:
            batch.append((round(median, 2), uid))
        if i % 500 == 0:
            label = f"{i - 499}–{i}"
            batch_commit(cur, conn, batch, update_query, label)

    batch_commit(cur, conn, batch, update_query, f"{i - (i % 500) + 1}–{i}")
    print(f"✅ Active eBay median updates complete: {i} processed")

    cur.close()
    conn.close()
    print("🏁 Active-only update finished.")

if __name__ == "__main__":
    main()

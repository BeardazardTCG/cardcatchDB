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

    # === Active BIN prices only ===
    print("📦 Fetching active BIN prices...")
    try:
        cur.execute("""
            SELECT unique_id, lowest_price
            FROM activedailypricelog
            WHERE lowest_price IS NOT NULL
              AND active_date = CURRENT_DATE
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

        batch_commit(cur, conn, active_batch, active_query, f"{i - (i % 500) + 1}–{i}")
        print(f"✅ Active BIN updates complete: {i} processed")

    except psycopg2.errors.UndefinedColumn:
        print("⚠️ Skipping active BIN update — column not found")

    cur.close()
    conn.close()
    print("🏁 Active-only update finished.")

if __name__ == "__main__":
    main()

import os
import psycopg2
from collections import defaultdict

# Use original asyncpg-style URL from .env
raw_url = os.getenv("DATABASE_URL")

# Convert to psycopg2-compatible format
DATABASE_URL = raw_url.replace("postgresql+asyncpg://", "postgresql://")

conn = psycopg2.connect(DATABASE_URL)

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

# === Main execution ===
def main():
    print("🔌 Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("📦 Fetching price data from dailypricelog...")
    cur.execute("""
        SELECT unique_id, median_price
        FROM dailypricelog
        WHERE median_price IS NOT NULL
    """)
    rows = cur.fetchall()

    price_map = defaultdict(list)
    for uid, price in rows:
        if price is not None:
            price_map[uid.strip()] += [float(price)]

    print(f"🧮 Calculating medians for {len(price_map)} unique cards...")
    updates = 0
    for uid, prices in price_map.items():
        filtered = filter_outliers(prices)
        median = calculate_median(filtered)
        if median is not None:
            cur.execute("""
                UPDATE mastercard_v2
                SET sold_ebay_median = %s
                WHERE unique_id = %s
            """, (round(median, 2), uid))
            updates += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ Update complete: {updates} rows updated.")

# Entry point
if __name__ == "__main__":
    main()

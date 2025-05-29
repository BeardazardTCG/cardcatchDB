import os
import psycopg2
from collections import defaultdict

# === Database connection ===
DATABASE_URL = os.getenv("DATABASE_URL")  # NO replace()

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

# === Main ===
def main():
    print("🔌 Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # === 1. Sold eBay median ===
    print("📦 Fetching sold prices from dailypricelog...")
    cur.execute("""
        SELECT unique_id, median_price
        FROM dailypricelog
        WHERE median_price IS NOT NULL
    """)
    sold_map = defaultdict(list)
    for uid, price in cur.fetchall():
        sold_map[uid.strip()].append(float(price))

    print(f"🧮 Calculating filtered medians for {len(sold_map)} cards...")
    sold_updates = 0
    for uid, prices in sold_map.items():
        filtered = filter_outliers(prices)
        median = calculate_median(filtered)
        if median is not None:
            cur.execute("""
                UPDATE mastercard_v2
                SET sold_ebay_median = %s
                WHERE unique_id = %s
            """, (round(median, 2), uid))
            sold_updates += 1

    # === 2. TCGPlayer market price ===
    print("📦 Fetching latest TCGPlayer prices...")
    cur.execute("""
        SELECT DISTINCT ON (unique_id) unique_id, market_price
        FROM tcg_pricing_log
        WHERE market_price IS NOT NULL
        ORDER BY unique_id, date DESC
    """)
    tcg_updates = 0
    for uid, price in cur.fetchall():
        cur.execute("""
            UPDATE mastercard_v2
            SET tcgplayer_market_price = %s
            WHERE unique_id = %s
        """, (round(float(price), 2), uid))
        tcg_updates += 1

    # === 3. Active eBay lowest price (filtered) ===
    print("📦 Fetching and filtering active BIN prices...")
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

        active_updates = 0
        for uid, prices in active_map.items():
            filtered = filter_outliers(prices)
            if filtered:
                lowest = min(filtered)
                cur.execute("""
                    UPDATE mastercard_v2
                    SET active_ebay_lowest = %s
                    WHERE unique_id = %s
                """, (round(lowest, 2), uid))
                active_updates += 1

    except psycopg2.errors.UndefinedColumn:
        print("⚠️ Skipping active price update — 'active_ebay_lowest' column not found.")

    # === Finalize ===
    conn.commit()
    cur.close()
    conn.close()

    print("✅ Update complete:")
    print(f"   • {sold_updates} sold prices updated")
    print(f"   • {tcg_updates} TCG prices updated")
    print(f"   • {active_updates if 'active_updates' in locals() else 0} active prices updated")

# Entry point
if __name__ == "__main__":
    main()

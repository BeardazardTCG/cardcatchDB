# historical_pricelog_cleanse.py
# Purpose: Audit and clean existing dailypricelog data for extreme or low-trust entries

import psycopg2
from psycopg2.extras import RealDictCursor
import datetime
import json

# === Config ===
DATABASE_URL = "postgresql://postgres:ckQFRJkrJluWsJnHsDhlhvbtSridadDF@metro.proxy.rlwy.net:52025/railway"
TRUSTED_FLAGGING_ENABLED = True  # Set to False to run in dry mode
MIN_SALES_REQUIRED = 3
MAX_MEDIAN_MULTIPLIER = 10  # If median > 10x min price in cluster → flag
MAX_IQR_MULTIPLIER = 3.0    # If IQR span > 3x Q1 → flag
BATCH_SIZE = 1000

# === Helper: Filter outliers using IQR ===
def filter_outliers(prices):
    if not prices or len(prices) < 2:
        return prices
    sorted_prices = sorted(prices)
    q1 = sorted_prices[len(prices) // 4]
    q3 = sorted_prices[(len(sorted_prices) * 3) // 4]
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return [p for p in prices if lower <= p <= upper]

# === Main ===
def main():
    print("🔍 Connecting to DB...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Check if 'trusted' column exists
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name='dailypricelog' AND column_name='trusted'
    """)
    if not cur.fetchone():
        print("🔧 Adding 'trusted' column to dailypricelog...")
        cur.execute("ALTER TABLE dailypricelog ADD COLUMN trusted BOOLEAN DEFAULT TRUE;")
        conn.commit()

    print("📦 Streaming dailypricelog in batches...")
    cur.execute("SELECT COUNT(*) FROM dailypricelog WHERE median_price IS NOT NULL")
    total_rows = cur.fetchone()["count"]
    print(f"🔎 Total rows to check: {total_rows}")

    flagged = 0
    updated = 0
    skipped = 0
    offset = 0

    while offset < total_rows:
        cur.execute("""
            SELECT id, unique_id, sold_date, median_price, average_price, sale_count, query_used, urls_used
            FROM dailypricelog
            WHERE median_price IS NOT NULL
            ORDER BY id ASC
            LIMIT %s OFFSET %s
        """, (BATCH_SIZE, offset))

        rows = cur.fetchall()
        if not rows:
            break

        for row in rows:
            prices = []
            try:
                prices = [float(p) for p in json.loads(row['urls_used']) if isinstance(p, (int, float, str))]
            except:
                continue

            median = float(row['median_price'])
            count = row['sale_count'] or len(prices)

            if count < MIN_SALES_REQUIRED:
                reason = f"Low sample ({count})"
                flagged += 1
            else:
                clean = filter_outliers(prices)
                if not clean or len(clean) < 2:
                    reason = "All filtered"
                    flagged += 1
                else:
                    span = max(clean) - min(clean)
                    if median > MAX_MEDIAN_MULTIPLIER * min(clean):
                        reason = f"Median {median} too high vs min {min(clean)}"
                        flagged += 1
                    elif span > MAX_IQR_MULTIPLIER * min(clean):
                        reason = f"IQR span too wide: {span}"
                        flagged += 1
                    else:
                        skipped += 1
                        continue  # No issue

            print(f"🚩 Flagged ID {row['id']} ({row['unique_id']}) | Reason: {reason}")

            if TRUSTED_FLAGGING_ENABLED:
                cur.execute("""
                    UPDATE dailypricelog
                    SET trusted = FALSE
                    WHERE id = %s
                """, (row['id'],))
                updated += 1

        conn.commit()
        offset += BATCH_SIZE

    cur.close()
    conn.close()
    print(f"\n✅ Done. Flagged: {flagged}, Updated: {updated}, Skipped: {skipped}")

if __name__ == "__main__":
    main()

# historical_pricelog_cleanse.py
# Purpose: Cross-audit dailypricelog against mastercard_v2 to flag suspicious spread/mismatch

import psycopg2
from psycopg2.extras import RealDictCursor
import datetime

# === Config ===
DATABASE_URL = "postgresql://postgres:ckQFRJkrJluWsJnHsDhlhvbtSridadDF@metro.proxy.rlwy.net:52025/railway"
TRUSTED_FLAGGING_ENABLED = True  # Set to False for dry run
SPREAD_THRESHOLD = 20.0           # e.g. if range > £20
MEDIAN_MULTIPLIER = 3.0           # e.g. median > 3x min price = flag
HIGH_PRICE_THRESHOLD = 100.0
LOW_SAMPLE_LIMIT = 2
BATCH_SIZE = 1000

# === Main ===
def main():
    print("🔍 Connecting to DB...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Ensure trusted column exists
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name='dailypricelog' AND column_name='trusted'
    """)
    if not cur.fetchone():
        print("🔧 Adding 'trusted' column to dailypricelog...")
        cur.execute("ALTER TABLE dailypricelog ADD COLUMN trusted BOOLEAN DEFAULT TRUE;")
        conn.commit()

    print("📦 Fetching row count...")
    cur.execute("SELECT COUNT(*) FROM dailypricelog WHERE median_price IS NOT NULL")
    total_rows = cur.fetchone()["count"]
    print(f"🔎 Scanning {total_rows} daily price rows...")

    flagged = 0
    updated = 0
    skipped = 0
    offset = 0

    while offset < total_rows:
        cur.execute("""
            SELECT d.id, d.unique_id, d.median_price, d.sale_count,
                   m.price_range_seen_min, m.price_range_seen_max
            FROM dailypricelog d
            LEFT JOIN mastercard_v2 m ON d.unique_id = m.unique_id
            WHERE d.median_price IS NOT NULL
            ORDER BY d.id ASC
            LIMIT %s OFFSET %s
        """, (BATCH_SIZE, offset))

        rows = cur.fetchall()
        if not rows:
            break

        for row in rows:
            try:
                median = float(row['median_price'])
                count = row['sale_count'] or 0
                min_price = float(row['price_range_seen_min']) if row['price_range_seen_min'] else None
                max_price = float(row['price_range_seen_max']) if row['price_range_seen_max'] else None

                reason = None

                if count < LOW_SAMPLE_LIMIT:
                    reason = f"Low sample count ({count})"

                elif min_price is not None and max_price is not None:
                    spread = max_price - min_price
                    if spread > SPREAD_THRESHOLD and median > MEDIAN_MULTIPLIER * min_price:
                        reason = f"Spread too wide ({min_price}–{max_price}) and inflated median ({median})"

                if not reason and median > HIGH_PRICE_THRESHOLD and count < 3:
                    reason = f"High median ({median}) with low count ({count})"

                if reason:
                    print(f"🚩 Flagged ID {row['id']} ({row['unique_id']}) | Reason: {reason}")
                    flagged += 1
                    if TRUSTED_FLAGGING_ENABLED:
                        cur.execute("""
                            UPDATE dailypricelog
                            SET trusted = FALSE
                            WHERE id = %s
                        """, (row['id'],))
                        updated += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"⚠️ Error processing row ID {row['id']}: {e}")

        conn.commit()
        offset += BATCH_SIZE

    cur.close()
    conn.close()
    print(f"\n✅ Done. Flagged: {flagged}, Updated: {updated}, Skipped: {skipped}")

if __name__ == "__main__":
    main()

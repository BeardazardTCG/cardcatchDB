import requests
from bs4 import BeautifulSoup
import time
import json
import pandas as pd
import psycopg2
import os

# === CONFIG ===
DRY_RUN = True
AUDIT_MODE = True
HEADERS = {'User-Agent': 'Mozilla/5.0'}
INPUT_CSV = "Data/Scraper_Batch_Input.csv"

# === DB CONNECTION ===
DB_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DB_URL)
cursor = conn.cursor()

# === SCRAPE FUNCTION ===
def scrape_ebay_sold(query):
    base_url = "https://www.ebay.com/sch/i.html"
    params = {
        '_nkw': query,
        '_sop': '13',          # Best Match
        'LH_Sold': '1',
        'LH_Complete': '1',
        '_ipg': '200',         # Max items per page
        '_dmd': '1',           # Gallery view
    }

    response = requests.get(base_url, headers=HEADERS, params=params)
    soup = BeautifulSoup(response.text, 'html.parser')

    listings = soup.select("li.s-item")
    results = []
    audit_log = {
        'query': query,
        'url': response.url,
        'total_found': len(listings),
        'included': [],
        'excluded': []
    }

    for item in listings:
        title_el = item.select_one(".s-item__title")
        price_el = item.select_one(".s-item__price")
        link_el = item.select_one(".s-item__link")

        if not title_el or not price_el:
            audit_log['excluded'].append({
                'reason': 'Missing title or price',
                'raw_html': str(item)
            })
            continue

        title = title_el.text.strip()
        price_raw = price_el.text.strip().replace("$", "").replace(",", "")
        link = link_el['href'] if link_el else 'N/A'

        try:
            price = float(price_raw)
        except:
            audit_log['excluded'].append({
                'reason': 'Price parse failed',
                'title': title,
                'raw_price': price_raw,
                'link': link
            })
            continue

        if "lot" in title.lower() or "bundle" in title.lower():
            audit_log['excluded'].append({
                'reason': 'Likely bundle',
                'title': title,
                'price': price,
                'link': link
            })
            continue

        result = {
            'title': title,
            'price': price,
            'link': link
        }
        audit_log['included'].append(result)
        results.append(price)

    if results:
        audit_log['summary'] = {
            'count': len(results),
            'low': min(results),
            'high': max(results),
            'avg': round(sum(results)/len(results), 2)
        }
        avg_price = audit_log['summary']['avg']
    else:
        audit_log['summary'] = None
        avg_price = None

    if DRY_RUN and AUDIT_MODE:
        try:
            cursor.execute("""
                INSERT INTO scraper_test_results (source, query, included_count, excluded_count, avg_price, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                'ebay_sold',
                query,
                len(audit_log['included']),
                len(audit_log['excluded']),
                avg_price,
                json.dumps(audit_log)
            ))
            conn.commit()
            print(f"[DB LOG SAVED] {query}")
        except Exception as db_err:
            print(f"❌ DB INSERT FAILED for {query}: {db_err}")

    return audit_log
# === BATCH MODE WITH ERROR LOGGING ===
if __name__ == "__main__":
    df = pd.read_csv(INPUT_CSV)
    test_df = df.head(5)
    for i, row in test_df.iterrows():
        q = row['query']
        print(f"\n[{i+1}/{len(test_df)}] Scraping: {q}")
        try:
            scrape_ebay_sold(q)
        except Exception as e:
            print(f"❌ FAILED: {q} — {e}")


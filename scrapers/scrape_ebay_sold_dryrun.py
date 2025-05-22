import requests
from bs4 import BeautifulSoup
import time
import json

# === CONFIG ===
DRY_RUN = True
AUDIT_MODE = True
HEADERS = {'User-Agent': 'Mozilla/5.0'}

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

        # === FILTER LOGIC ===
        if "lot" in title.lower() or "bundle" in title.lower():
            audit_log['excluded'].append({
                'reason': 'Likely bundle',
                'title': title,
                'price': price,
                'link': link
            })
            continue

        # === PASSED ===
        result = {
            'title': title,
            'price': price,
            'link': link
        }
        audit_log['included'].append(result)
        results.append(price)

    # === AGGREGATE ===
    if results:
        audit_log['summary'] = {
            'count': len(results),
            'low': min(results),
            'high': max(results),
            'avg': round(sum(results)/len(results), 2)
        }
    else:
        audit_log['summary'] = None

    # === DRY RUN LOG ===
    if DRY_RUN and AUDIT_MODE:
        timestamp = int(time.time())
        filename = f"audit_ebay_sold_{query.replace(' ', '_')}_{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(audit_log, f, indent=2)
        print(f"[AUDIT LOG SAVED] {filename}")

    return audit_log

# === EXAMPLE ===
if __name__ == "__main__":
    test_query = "Gengar Skyridge 10/144"
    result = scrape_ebay_sold(test_query)
    print(json.dumps(result, indent=2))


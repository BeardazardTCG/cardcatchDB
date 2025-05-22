import requests
import time
import json
import pandas as pd

# === CONFIG ===
DRY_RUN = True
AUDIT_MODE = True
HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'CardCatch-Test-Agent'
}
TCG_API_KEY = "your_api_key_here"
INPUT_CSV = "data/scraper_batch_input.csv"

# === SCRAPE FUNCTION ===
def fetch_tcg_prices(query):
    base_url = f"https://api.tcgplayer.com/catalog/products"
    headers = HEADERS.copy()
    headers['Authorization'] = f"Bearer {TCG_API_KEY}"

    search_params = {
        'productName': query,
        'limit': 1
    }

    response = requests.get(base_url, headers=headers, params=search_params)

    if response.status_code != 200:
        return {
            'query': query,
            'error': f"API call failed with status {response.status_code}"
        }

    data = response.json()
    if not data.get('results'):
        return {
            'query': query,
            'error': "No products found"
        }

    product_id = data['results'][0]['productId']
    pricing_url = f"https://api.tcgplayer.com/pricing/product/{product_id}"
    pricing_response = requests.get(pricing_url, headers=headers)

    if pricing_response.status_code != 200:
        return {
            'query': query,
            'error': f"Pricing call failed with status {pricing_response.status_code}"
        }

    pricing_data = pricing_response.json()
    prices = pricing_data.get('results', [])

    market, low, mid = None, None, None
    for price in prices:
        if price.get('subTypeName') == 'Normal':
            market = price.get('marketPrice')
            low = price.get('lowPrice')
            mid = price.get('midPrice')
            break

    result = {
        'query': query,
        'tcg_market': market,
        'tcg_low': low,
        'tcg_mid': mid,
        'product_id': product_id,
        'url': f"https://www.tcgplayer.com/product/{product_id}"
    }

    if DRY_RUN and AUDIT_MODE:
        timestamp = int(time.time())
        filename = f"audit_tcg_{query.replace(' ', '_')}_{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"[AUDIT LOG SAVED] {filename}")

    return result

# === BATCH MODE ===
if __name__ == "__main__":
    df = pd.read_csv(INPUT_CSV)
    for _, row in df.iterrows():
        q = row['query']
        print(f"\n\n=== Running TCG scrape for: {q} ===")
        fetch_tcg_prices(q)

import requests
import time
import json

# === CONFIG ===
DRY_RUN = True
AUDIT_MODE = True
HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'CardCatch-Test-Agent'
}

# Simulated API key placeholder (replace with env variable in real usage)
TCG_API_KEY = "your_api_key_here"

# === TCGPlayer Scraper Function ===
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

    # Filter market/low/mid pricing
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

    # === DRY RUN LOG ===
    if DRY_RUN and AUDIT_MODE:
        timestamp = int(time.time())
        filename = f"audit_tcg_{query.replace(' ', '_')}_{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"[AUDIT LOG SAVED] {filename}")

    return result

# === EXAMPLE ===
if __name__ == "__main__":
    test_query = "Gengar Skyridge 10/144"
    result = fetch_tcg_prices(test_query)
    print(json.dumps(result, indent=2))

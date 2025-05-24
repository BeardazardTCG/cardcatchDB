import requests
import json
import time

API_KEY = 'YOUR_API_KEY'  # Replace with your actual key
HEADERS = {'X-Api-Key': API_KEY}

def fetch_all_sets():
    url = "https://api.pokemontcg.io/v2/sets"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    sets = response.json()['data']

    english_sets = []
    for s in sets:
        if s.get('ptcgoCode') and 'EN' in s.get('id', '').upper():  # loose English filter fallback
            english_sets.append({
                'id': s['id'],
                'name': s['name'],
                'releaseDate': s.get('releaseDate'),
                'printedTotal': s.get('printedTotal'),
                'total': s.get('total'),
                'logo_url': s['images']['logo'],
                'symbol_url': s['images']['symbol']
            })
    return english_sets

if __name__ == "__main__":
    sets = fetch_all_sets()
    with open('english_sets.json', 'w') as f:
        json.dump(sets, f, indent=2)
    print(f"✅ Saved {len(sets)} English sets to english_sets.json")

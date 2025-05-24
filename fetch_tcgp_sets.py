import requests
import json

API_KEY = 'a4a5ed18-fbf7-4960-b0ac-2ac71e01eee7'
HEADERS = {'X-Api-Key': API_KEY}

def fetch_all_sets():
    url = "https://api.pokemontcg.io/v2/sets"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    sets = response.json()['data']

    english_sets = []
    for s in sets:
        # Skip sets without valid card count
        if not s.get('printedTotal') or s['printedTotal'] == 0:
            continue

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
    print(f"✅ Saved {len(sets)} valid English sets to english_sets.json")

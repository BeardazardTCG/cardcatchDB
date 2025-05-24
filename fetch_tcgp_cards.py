import requests
import json

API_KEY = 'a4a5ed18-fbf7-4960-b0ac-2ac71e01eee7'
HEADERS = {'X-Api-Key': API_KEY}

def fetch_cards_for_set(set_id):
    url = f"https://api.pokemontcg.io/v2/cards?q=set.id:{set_id}"
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 404:
            print(f"❌ 404 Not Found for {set_id}")
            return []
        response.raise_for_status()
        cards = response.json()['data']
        print(f"✅ {len(cards)} cards fetched for {set_id}")
        return cards
    except Exception as e:
        print(f"❌ Error on {set_id}: {e}")
        return []

if __name__ == "__main__":
    with open("english_sets.json", "r") as f:
        sets = json.load(f)

    card_dump = {}
    for s in sets:
        sid = s['id']
        print(f"📦 Fetching cards for set: {s['name']} ({sid})")
        cards = fetch_cards_for_set(sid)
        if cards:
            card_dump[sid] = cards

    with open("all_cards_by_set.json", "w") as f:
        json.dump(card_dump, f, indent=2)

    print(f"✅ Fetched cards for {len(card_dump)} sets.")

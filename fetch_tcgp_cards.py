import requests
import json
import time

API_KEY = 'a4a5ed18-fbf7-4960-b0ac-2ac71e01eee7'
HEADERS = {'X-Api-Key': API_KEY}

def fetch_cards_for_set(set_id):
    all_cards = []
    page = 1
    page_size = 250

    while True:
       url = f"https://api.pokemontcg.io/v2/cards?q=set:{set_id}&page={page}&pageSize=250"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        data = response.json()['data']
        all_cards.extend(data)

        if len(data) < page_size:
            break
        page += 1
        time.sleep(0.1)

    return all_cards

if __name__ == "__main__":
    with open("english_sets.json", "r") as f:
        sets = json.load(f)

    card_dump = {}
    for s in sets:
        sid = s['id']
        print(f"📦 Fetching cards for set: {s['name']} ({sid})")
        try:
            cards = fetch_cards_for_set(sid)
            card_dump[sid] = cards
        except Exception as e:
            print(f"❌ Failed on {sid}: {e}")

    with open("all_cards_by_set.json", "w") as f:
        json.dump(card_dump, f, indent=2)

    print(f"✅ Fetched cards for {len(card_dump)} sets.")

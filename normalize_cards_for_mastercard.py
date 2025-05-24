import json

def load_json(filename):
    with open(filename, 'r') as f:
        return json.load(f)

def normalize_card_data():
    sets = load_json('english_sets.json')
    cards_by_set = load_json('all_cards_by_set.json')

    set_lookup = {s['id']: s for s in sets}
    normalized_cards = []

    for set_id, cards in cards_by_set.items():
        set_info = set_lookup.get(set_id)
        if not set_info:
            continue  # skip unmatched sets

        for card in cards:
            card_name = card.get('name', '').strip()
            number = card.get('number', '').strip()
            total = set_info.get('printedTotal')

            # Skip if anything critical is missing
            if not card_name or not number or not total:
                continue

            try:
                card_number = f"{number}/{total}"
            except:
                card_number = number  # fallback

            normalized_cards.append({
                "card_name": card_name,
                "card_number": card_number,
                "set_id": set_id,
                "set_name": set_info.get('name'),
                "release_date": set_info.get('releaseDate'),
                "rarity": card.get('rarity'),
                "supertype": card.get('supertype'),
                "subtypes": card.get('subtypes', []),
                "types": card.get('types', []),
                "card_image_url": card.get('images', {}).get('small'),
                "set_logo_url": set_info.get('logo_url'),
                "set_symbol_url": set_info.get('symbol_url'),
                "hot_character": False  # default, update later
            })

    with open('normalized_mastercard.json', 'w') as f:
        json.dump(normalized_cards, f, indent=2)

    print(f"✅ Normalized {len(normalized_cards)} cards into normalized_mastercard.json")

if __name__ == "__main__":
    normalize_card_data()

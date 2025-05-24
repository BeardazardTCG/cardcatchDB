import os
import requests
import psycopg2
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

def fetch_all_sets():
    url = "https://api.pokemontcg.io/v2/sets"
    res = requests.get(url, headers=HEADERS)
    if res.ok:
        return res.json().get("data", [])
    else:
        print("Failed to fetch sets:", res.status_code)
        return []

def fetch_cards(set_id):
    url = f"https://api.pokemontcg.io/v2/cards?q=set.id:{set_id}&pageSize=250"
    res = requests.get(url, headers=HEADERS)
    if res.ok:
        return res.json().get("data", [])
    else:
        print(f"Failed to fetch cards for set {set_id}: {res.status_code}")
        return []

def convert_date(s):
    if not s:
        return None
    try:
        return datetime.strptime(s.replace("/", "-"), "%Y-%m-%d").date()
    except Exception:
        return None

def run():
    sets = fetch_all_sets()
    print(f"Total sets to process: {len(sets)}")
    for s in sets:
        set_id = s["id"]
        set_name = s["name"]
        release_date = convert_date(s.get("releaseDate"))
        print(f"Processing set {set_name} ({set_id}), release date: {release_date}")

        cards = fetch_cards(set_id)
        print(f"  Found {len(cards)} cards")

        for card in cards:
            try:
                data = {
                    "unique_id": card["id"],
                    "card_name": card["name"],
                    "card_number": card["number"],
                    "card_number_raw": card["number"].split("/")[0],
                    "rarity": card.get("rarity"),
                    "type": card.get("types", [None])[0] if card.get("types") else None,
                    "artist": card.get("artist"),
                    "language": card.get("language", "en"),
                    "set_name": card["set"]["name"],
                    "set_code": card["set"].get("ptcgoCode") or card["set"]["id"],
                    "release_date": release_date,
                    "set_logo_url": card["set"]["images"]["logo"],
                    "set_symbol_url": card["set"]["images"]["symbol"],
                    "query": f"{card['name']} {card['set']['name']} {card['number'].split('/')[0]}",
                    "set_id": card["set"]["id"],
                    "types": json.dumps(card.get("types")) if card.get("types") else None,
                    "hot_character": False,
                    "card_image_url": card["images"]["small"],
                    "subtypes": json.dumps(card.get("subtypes")) if card.get("subtypes") else None,
                    "supertype": card.get("supertype")
                }

                cur.execute("""
                    INSERT INTO mastercard_v2 (
                        unique_id, card_name, card_number, card_number_raw, rarity,
                        type, artist, language, set_name, set_code,
                        release_date, set_logo_url, set_symbol_url, query, set_id,
                        types, hot_character, card_image_url, subtypes, supertype
                    ) VALUES (
                        %(unique_id)s, %(card_name)s, %(card_number)s, %(card_number_raw)s, %(rarity)s,
                        %(type)s, %(artist)s, %(language)s, %(set_name)s, %(set_code)s,
                        %(release_date)s, %(set_logo_url)s, %(set_symbol_url)s, %(query)s, %(set_id)s,
                        %(types)s, %(hot_character)s, %(card_image_url)s, %(subtypes)s, %(supertype)s
                    ) ON CONFLICT (unique_id) DO NOTHING
                """, data)
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"Failed to insert card {card['id']}: {e}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    run()

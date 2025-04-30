from bs4 import BeautifulSoup
import requests
import re
from datetime import datetime

def extract_sold_date(item):
    spans = item.find_all("span")
    for span in spans:
        text = span.get_text(strip=True)
        if text.lower().startswith("sold"):
            match = re.search(r"Sold\s+(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", text)
            if match:
                try:
                    return datetime.strptime(match.group(0)[5:], "%d %b %Y").date()
                except ValueError:
                    return None
    return None

def parse_character_set_and_number(query):
    parts = query.split()
    character = parts[0] if parts else ""
    set_name = ""

    known_rarities = ["ex", "v", "vmax", "vstar", "gx", "illustration", "rare", "promo", "ultra"]
    filtered = [p for p in parts if not re.match(r"\d+/\d+", p) and p.lower() not in known_rarities]
    if len(filtered) > 1:
        set_name = " ".join(filtered[1:])  # everything after character

    match = re.search(r"\d+/\d+", query)
    card_number = match.group(0) if match else ""

    return character.lower(), set_name, card_number

def parse_ebay_sold_page(query, max_items=100):
    character, set_name, card_number = parse_character_set_and_number(query)

    url = "https://www.ebay.co.uk/sch/i.html"
    params = {
        "_nkw": query,
        "LH_Sold": "1",
        "LH_Complete": "1",
        "LH_PrefLoc": "1",
        "_dmd": "2",         # gallery view
        "_ipg": "120",       # max per page
        "_sop": "13",        # most recent
        "_dcat": "183454",   # TCG individual cards
        "Graded": "No",
        "LH_BIN": "1"
    }

    try:
        resp = requests.get(url, params=params, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        print("Scraper error:", e)
        return []

    results = []
    count = 0

    for item in soup.select(".s-item"):
        if count >= max_items:
            break

        title_tag = item.select_one(".s-item__title")
        price_tag = item.select_one(".s-item__price")
        sold_date = extract_sold_date(item)

        if not title_tag or not price_tag or not sold_date:
            continue

        title = title_tag.text.strip()
        title_lower = title.lower()
        if card_number not in title and card_number.replace("/", "") not in title:
            continue
        if character not in title_lower:
            continue

        price_clean = re.sub(r"[^\d.]", "", price_tag.text)
        try:
            price_float = float(price_clean)
        except ValueError:
            continue

        results.append({
            "character": character.title(),
            "set": set_name,
            "title": title,
            "price": price_float,
            "sold_date": str(sold_date)
        })
        count += 1

    return results

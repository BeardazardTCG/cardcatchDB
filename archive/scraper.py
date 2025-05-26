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
                    # Parse date in format "DD MMM YYYY"
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
        set_name = " ".join(filtered[1:])

    match = re.search(r"\d+/\d+", query)
    card_number = match.group(0) if match else ""

    return character.lower(), set_name, card_number

def parse_ebay_sold_page(query, max_items=100):
    character, set_name, card_number = parse_character_set_and_number(query)
    card_number_digits = re.sub(r"[^\d]", "", card_number)

    url = "https://www.ebay.co.uk/sch/i.html"
    params = {
        "_nkw": query,
        "LH_Sold": "1",
        "LH_Complete": "1",
        "LH_PrefLoc": "1",
        "_dmd": "2",
        "_ipg": str(max_items),
        "_sop": "13",
        "_dcat": "183454",
        "Graded": "No"
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-GB,en;q=0.9"
    }

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        print("✅ Fetched from:", resp.url)
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
        link_tag = item.select_one(".s-item__link")

        if not title_tag or not price_tag or not sold_date or not link_tag:
            continue

        title = title_tag.text.strip()
        title_lower = title.lower()
        title_digits = re.sub(r"[^\d]", "", title)
        url = link_tag['href']

        # Card number and character matching for filtering
        if card_number_digits and card_number_digits not in title_digits:
            continue
        if character and character not in title_lower:
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
            "sold_date": str(sold_date),
            "url": url
        })
        count += 1

    return results

def parse_ebay_active_page(query, max_items=30):
    character, set_name, card_number = parse_character_set_and_number(query)
    card_number_digits = re.sub(r"[^\d]", "", card_number)

    url = "https://www.ebay.co.uk/sch/i.html"
    params = {
        "_nkw": query,
        "LH_BIN": "1",           # Buy It Now only
        "LH_PrefLoc": "1",       # UK only
        "_ipg": str(max_items),
        "_sop": "12",            # Best Match sort
        "_dcat": "183454",
        "Graded": "No"
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-GB,en;q=0.9"
    }

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        print("Active scrape error:", e)
        return []

    results = []
    count = 0

    for item in soup.select(".s-item"):
        if count >= max_items:
            break

        title_tag = item.select_one(".s-item__title")
        price_tag = item.select_one(".s-item__price")
        link_tag = item.select_one(".s-item__link")

        if not title_tag or not price_tag or not link_tag:
            continue

        title = title_tag.text.strip()
        title_lower = title.lower()
        title_digits = re.sub(r"[^\d]", "", title)
        url = link_tag['href']

        # Exclude common junk terms for active listings
        if any(term in title_lower for term in ["proxy", "lot", "damaged", "jumbo", "binder", "custom"]):
            continue
        if character and character not in title_lower:
            continue
        if card_number_digits and card_number_digits not in title_digits:
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
            "url": url,
            "listing_date": str(datetime.today().date())
        })
        count += 1

    return results

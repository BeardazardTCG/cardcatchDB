import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from statistics import median

# === CONSTANTS ===
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-GB,en;q=0.9"
}

EXCLUDED_TERMS = [
    "psa", "graded", "bgs", "cgc", "beckett", "sgc",
    "lot", "joblot", "bundle", "proxy", "custom", "fake",
    "damaged", "played", "heavy", "poor", "excellent",
    "japanese", "german", "french", "italian", "spanish", "korean", "chinese"
]

# === HELPERS ===
def extract_sold_date(item):
    for span in item.find_all("span"):
        text = span.get_text(strip=True).lower()
        if text.startswith("sold"):
            match = re.search(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", text)
            if match:
                try:
                    return datetime.strptime(match.group(0), "%d %b %Y").date()
                except ValueError:
                    return None
    return None

def clean_price(text):
    try:
        return float(re.sub(r"[^\d.]", "", text))
    except:
        return None

def parse_card_meta(query):
    parts = query.split()
    character = parts[0].lower() if parts else ""
    number_match = re.search(r"\d+/\d+", query)
    card_number = number_match.group(0) if number_match else ""
    digits_only = re.sub(r"[^\d]", "", card_number)
    return character, digits_only

def is_valid_title(title, character, digits):
    lower = title.lower()
    if any(term in lower for term in EXCLUDED_TERMS):
        return False
    if character and character not in lower:
        return False
    if digits and digits not in re.sub(r"[^\d]", "", lower):
        return False
    if re.search(r"\b(x|\u00d7)?\d+\b", lower):
        return False
    return True

def apply_iqr_filter(prices):
    if len(prices) < 4:
        return prices
    sorted_prices = sorted(prices)
    q1 = sorted_prices[len(prices) // 4]
    q3 = sorted_prices[(len(prices) * 3) // 4]
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return [p for p in prices if lower <= p <= upper]

# === SOLD SCRAPER ===
def parse_ebay_sold_page(query, max_items=120):
    character, digits = parse_card_meta(query)
    results = []

    url = "https://www.ebay.co.uk/sch/i.html"
    params = {
        "_nkw": query,
        "_sacat": "183454",
        "LH_Complete": "1",
        "LH_Sold": "1",
        "LH_ItemCondition": "4000",
        "LH_PrefLoc": "1",
        "_ipg": "200",
        "_ex_kw": "+".join(EXCLUDED_TERMS)
    }

    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        print("Sold scrape error:", e)
        return []

    for item in soup.select(".s-item"):
        title_tag = item.select_one(".s-item__title")
        price_tag = item.select_one(".s-item__price")
        link_tag = item.select_one(".s-item__link")
        sold_date = extract_sold_date(item)

        if not all([title_tag, price_tag, link_tag, sold_date]):
            continue

        title = title_tag.text.strip()
        url = link_tag['href']
        price = clean_price(price_tag.text)

        if not price or price < 1:
            continue
        if not is_valid_title(title, character, digits):
            continue

        results.append({
            "character": character,
            "card_number": digits,
            "title": title,
            "price": price,
            "sold_date": sold_date.strftime("%Y-%m-%d"),
            "url": url,
            "condition": "Unknown"
        })

    return results

# === ACTIVE SCRAPER ===
def parse_ebay_active_page(query, max_items=120):
    character, digits = parse_card_meta(query)
    results = []

    url = "https://www.ebay.co.uk/sch/i.html"
    params = {
        "_nkw": query,
        "_sacat": "183454",
        "LH_BIN": "1",
        "LH_ItemCondition": "4000",
        "LH_PrefLoc": "1",
        "_ipg": "200",
        "_ex_kw": "+".join(EXCLUDED_TERMS)
    }

    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        print("Active scrape error:", e)
        return []

    for item in soup.select(".s-item"):
        title_tag = item.select_one(".s-item__title")
        price_tag = item.select_one(".s-item__price")
        link_tag = item.select_one(".s-item__link")

        if not all([title_tag, price_tag, link_tag]):
            continue

        title = title_tag.text.strip()
        url = link_tag['href']
        price = clean_price(price_tag.text)

        if not price or price < 1:
            continue
        if not is_valid_title(title, character, digits):
            continue

        results.append({
            "character": character,
            "card_number": digits,
            "title": title,
            "price": price,
            "url": url,
            "condition": "Unknown"
        })

    return results

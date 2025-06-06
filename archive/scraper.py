import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
from urllib.parse import urlencode

from utils import (
    EXCLUDED_TERMS,
    is_valid_price,
    is_valid_condition,
    is_valid_title,
    detect_holo_type,
    parse_card_meta  # optional, if externalized
)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-GB,en;q=0.9"
}

# === Helpers ===

def clean_price(text):
    try:
        return float(re.sub(r"[^\d.]", "", text))
    except:
        return None

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

def parse_card_meta(query):
    parts = query.split()
    character = parts[0].lower() if parts else ""
    number_match = re.search(r"\d+/\d+", query)
    card_number = number_match.group(0) if number_match else ""
    digits_only = re.sub(r"[^\d]", "", card_number)
    return character, digits_only

# === eBay URL builder ===

def build_ebay_url(query, sold=False):
    base_url = "https://www.ebay.co.uk/sch/i.html"
    params = {
        "_nkw": query,
        "_sacat": "183454",
        "_ipg": "240",
        "_in_kw": "4",
        "LH_PrefLoc": "1",
        "LH_ViewType": "Gallery",
        "_ex_kw": "+".join(EXCLUDED_TERMS)
    }
    if sold:
        params["LH_Complete"] = "1"
        params["LH_Sold"] = "1"
    else:
        params["LH_BIN"] = "1"
    return f"{base_url}?{urlencode(params)}"

# === Main Parsers ===

def parse_ebay_sold_page(query, max_items=240):
    character, digits = parse_card_meta(query)
    results_raw = []
    results_filtered = []
    url = build_ebay_url(query, sold=True)

    try:
        resp = requests.get(url, headers=HEADERS, timeout=12)
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        print("Sold scrape error:", e)
        return {"url": url, "raw": [], "filtered": []}

    ninety_days_ago = datetime.utcnow().date() - timedelta(days=90)

    for item in soup.select(".s-item"):
        if len(results_raw) >= max_items:
            break

        title_tag = item.select_one(".s-item__title")
        price_tag = item.select_one(".s-item__price")
        link_tag = item.select_one(".s-item__link")
        sold_date = extract_sold_date(item)

        if not all([title_tag, price_tag, link_tag, sold_date]):
            continue

        title = title_tag.text.strip()
        url_item = link_tag['href']
        price = clean_price(price_tag.text)
        holo_type = detect_holo_type(title)
        condition = "Unknown"

        result = {
            "character": character,
            "card_number": digits,
            "title": title,
            "price": price,
            "sold_date": sold_date.strftime("%Y-%m-%d"),
            "url": url_item,
            "condition": condition,
            "holo_type": holo_type
        }

        results_raw.append(result)

        if (
            is_valid_price(price) and
            is_valid_title(title, character, digits) and
            sold_date >= ninety_days_ago
        ):
            results_filtered.append(result)

    return {
        "url": url,
        "raw": results_raw,
        "filtered": results_filtered
    }

def parse_ebay_active_page(query, max_items=240):
    character, digits = parse_card_meta(query)
    results_raw = []
    results_filtered = []
    url = build_ebay_url(query, sold=False)

    try:
        resp = requests.get(url, headers=HEADERS, timeout=12)
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        print("Active scrape error:", e)
        return {"url": url, "raw": [], "filtered": []}

    for item in soup.select(".s-item"):
        if len(results_raw) >= max_items:
            break

        title_tag = item.select_one(".s-item__title")
        price_tag = item.select_one(".s-item__price")
        link_tag = item.select_one(".s-item__link")

        if not all([title_tag, price_tag, link_tag]):
            continue

        title = title_tag.text.strip()
        url_item = link_tag['href']
        price = clean_price(price_tag.text)
        holo_type = detect_holo_type(title)
        condition = "Unknown"

        result = {
            "character": character,
            "card_number": digits,
            "title": title,
            "price": price,
            "url": url_item,
            "condition": condition,
            "holo_type": holo_type
        }

        results_raw.append(result)

        if (
            is_valid_price(price) and
            is_valid_title(title, character, digits)
        ):
            results_filtered.append(result)

    return {
        "url": url,
        "raw": results_raw,
        "filtered": results_filtered
    }

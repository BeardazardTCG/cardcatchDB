# ⚠️ WARNING: This script can trigger eBay bans/503s if run as-is.
# Parked for future refactor with proper scraping safety.

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re

def parse_ebay_active_page(query, max_items=30):
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    base_url = "https://www.ebay.co.uk/sch/i.html"
    params = {
        "_nkw": query,
        "_sop": "15",               # Lowest price
        "_ipg": str(max_items),     # Items per page
        "_dmd": "1",               # Full item view
        "LH_BIN": "1",             # Buy It Now only
        "LH_PrefLoc": "1"          # UK only
    }

    response = requests.get(base_url, headers=headers, params=params)
    if not response.ok:
        raise Exception(f"Active request failed with status {response.status_code}")

    soup = BeautifulSoup(response.text, "html.parser")
    items = soup.select("li.s-item")
    results = []

    for item in items:
        title_elem = item.select_one("h3.s-item__title")
        price_elem = item.select_one("span.s-item__price")
        if not title_elem or not price_elem:
            continue

        title = title_elem.get_text().strip()
        price_text = price_elem.get_text().strip()

        try:
            price = float(re.sub(r"[^\d.]", "", price_text))
        except:
            continue

        results.append({"title": title, "price": price})

        if len(results) >= max_items:
            break

    return results

def parse_ebay_sold_page(query, max_items=30):
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    base_url = "https://www.ebay.co.uk/sch/i.html"
    params = {
        "_nkw": query,
        "LH_Sold": "1",
        "LH_Complete": "1",
        "rt": "nc",
        "LH_PrefLoc": "1",         # UK only
        "_ipg": str(max_items)      # Items per page
    }

    response = requests.get(base_url, headers=headers, params=params)
    if not response.ok:
        raise Exception(f"Sold request failed with status {response.status_code}")

    soup = BeautifulSoup(response.text, "html.parser")
    items = soup.select("li.s-item")
    results = []

    for item in items:
        title_elem = item.select_one("h3.s-item__title")
        price_elem = item.select_one("span.s-item__price")
        shipping_elem = item.select_one("span.s-item__shipping")

        if not title_elem or not price_elem:
            continue

        title = title_elem.get_text().strip()
        price_text = price_elem.get_text()
        shipping_text = shipping_elem.get_text() if shipping_elem else ""

        try:
            price = float(re.sub(r"[^\d.]", "", price_text))
        except:
            continue

        if "Best offer accepted" in price_text:
            price *= 0.9
        elif "auction" in title.lower():
            price *= 0.92

        if "Free" not in shipping_text and "free" not in shipping_text:
            try:
                ship_price = float(re.sub(r"[^\d.]", "", shipping_text))
                price += ship_price
            except:
                pass

        sold_date = datetime.today().date()

        results.append({
            "title": title,
            "price": price,
            "sold_date": str(sold_date)
        })

        if len(results) >= max_items:
            break

    return results

def parse_ebay_graded_page(query, max_items=30):
    # This is the same logic as sold page but might be customized later
    return parse_ebay_sold_page(query, max_items)

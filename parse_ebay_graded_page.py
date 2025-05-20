import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re
import time
import random

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0)"
]

def parse_ebay_graded_page(query, max_items=30):
    headers = {
        "User-Agent": random.choice(USER_AGENTS)
    }

    base_url = "https://www.ebay.co.uk/sch/i.html"
    params = {
        "_nkw": query,
        "LH_Sold": "1",
        "LH_Complete": "1",
        "rt": "nc",
        "LH_ItemCondition": "3000",
        "LH_PrefLoc": "1",
        "_ipg": max_items
    }

    # Retry logic
    for attempt in range(3):
        response = requests.get(base_url, headers=headers, params=params)
        if response.status_code == 200:
            break
        time.sleep(2)
    else:
        raise Exception(f"Request failed with status {response.status_code}")

    soup = BeautifulSoup(response.text, "html.parser")
    items = soup.select("li.s-item")

    results = []

    for item in items:
        title_elem = item.select_one("h3.s-item__title")
        price_elem = item.select_one("span.s-item__price")
        date_elem = item.select_one("div.s-item__title--tagblock span.POSITIVE")
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

    time.sleep(2)
    return results

def parse_ebay_sold_page(query, max_items=30):
    return []

def parse_ebay_active_page(query, max_items=30):
    return []

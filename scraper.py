from bs4 import BeautifulSoup
import requests
import re
from datetime import datetime

def extract_sold_date(item):
    # Method 1: Tagblock (common in gallery view)
    tagblock = item.select_one(".s-item__title--tagblock")
    if tagblock:
        match = re.search(r"Sold\s+(\d{1,2}\s+\w+,\s+\d{4})", tagblock.text)
        if match:
            try:
                return datetime.strptime(match.group(1), "%d %b, %Y").date()
            except:
                pass

    # Method 2: Subtitle or alternate blocks
    subtitle = item.select_one(".s-item__subtitle")
    if subtitle:
        match = re.search(r"Sold\s+(\d{1,2}\s+\w+\s+\d{4})", subtitle.text)
        if match:
            try:
                return datetime.strptime(match.group(1), "%d %b %Y").date()
            except:
                pass

    return None

def parse_ebay_sold_page(query, max_items=100):
    url = "https://www.ebay.co.uk/sch/i.html"
    params = {
        "_nkw": query,
        "LH_Sold": "1",
        "LH_Complete": "1",
        "_sop": "13",     # Sort by most recent
        "_ipg": "100"     # Items per page
    }

    resp = requests.get(url, params=params, timeout=10)
    soup = BeautifulSoup(resp.text, "html.parser")

    results = []
    count = 0

    for item in soup.select(".s-item"):
        if count >= max_items:
            break

        title = item.select_one(".s-item__title")
        price = item.select_one(".s-item__price")
        sold_date = extract_sold_date(item)

        if title and price:
            price_clean = re.sub(r"[^\d.]", "", price.text)
            try:
                price_float = float(price_clean)
            except ValueError:
                continue

            results.append({
                "title": title.text.strip(),
                "price": price_float,
                "sold_date": str(sold_date) if sold_date else None
            })
            count += 1

    return results

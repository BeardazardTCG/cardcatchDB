from bs4 import BeautifulSoup
import requests
import re
from datetime import datetime

def extract_sold_date(item):
    spans = item.find_all("span")
    for span in spans:
        text = span.get_text(strip=True)
        if text.startswith("Sold"):
            match = re.search(r"Sold\s+(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", text)
            if match:
                day, month, year = match.groups()
                try:
                    return datetime.strptime(f"{day} {month} {year}", "%d %b %Y").date()
                except ValueError:
                    pass
    return None

def getCardPrice(query: str, includes: list = [], excludes: list = [], max_items: int = 100):
    url = "https://www.ebay.co.uk/sch/i.html"
    params = {
        "_nkw": query,
        "LH_Sold": "1",
        "LH_Complete": "1",
        "_sop": "13",
        "_ipg": "100"
    }

    resp = requests.get(url, params=params, timeout=10)
    soup = BeautifulSoup(resp.text, "html.parser")
    items = soup.select(".s-item")

    prices = []
    sold_dates = []

    for item in items[:max_items]:
        title_elem = item.select_one(".s-item__title")
        price_elem = item.select_one(".s-item__price")
        if not title_elem or not price_elem:
            continue

        title = title_elem.text.strip()
        if not all(term.lower() in title.lower() for term in includes):
            continue
        if any(term.lower() in title.lower() for term in excludes):
            continue

        price_clean = re.sub(r"[^\d.]", "", price_elem.text)
        try:
            price = float(price_clean)
        except ValueError:
            continue

        prices.append(price)
        sold_date = extract_sold_date(item)
        if sold_date:
            sold_dates.append(sold_date)

    if not prices:
        return {
            "query": query,
            "medianPrice": None,
            "soldCount": 0,
            "lowestPrice": None,
            "highestPrice": None,
            "lastSoldDate": None,
            "latestSoldDate": None
        }

    sorted_prices = sorted(prices)
    mid = len(sorted_prices) // 2
    if len(sorted_prices) % 2 == 0:
        median = round((sorted_prices[mid - 1] + sorted_prices[mid]) / 2, 2)
    else:
        median = round(sorted_prices[mid], 2)

    last_sold_date = sold_dates[0] if sold_dates else None
    latest_sold_date = max(sold_dates) if sold_dates else None

    return {
        "query": query,
        "medianPrice": median,
        "soldCount": len(prices),
        "lowestPrice": round(min(prices), 2),
        "highestPrice": round(max(prices), 2),
        "lastSoldDate": str(last_sold_date) if last_sold_date else None,
        "latestSoldDate": str(latest_sold_date) if latest_sold_date else None
    }

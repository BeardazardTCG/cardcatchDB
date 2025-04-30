from bs4 import BeautifulSoup
import requests
import re
from datetime import datetime

def extract_sold_date(item):
    sold_date = None
    # Look for span containing "Sold <date>" inside common containers
    spans = item.select("div.s-item__title--tagblock span, .s-item__title span")

    for span in spans:
        text = span.text.strip()
        if text.lower().startswith("sold"):
            match = re.search(r"Sold (\d{1,2} \w+ \d{4})", text)
            if match:
                try:
                    sold_date = datetime.strptime(match.group(1), "%d %b %Y").date()
                except:
                    sold_date = None
            break

    return sold_date

def parse_ebay_sold_page(query, max_items=20):
    url = "https://www.ebay.co.uk/sch/i.html"
    params = {
        "_nkw": query,
        "LH_Sold": "1",
        "LH_Complete": "1",
        "_sop": "13"  # Sort by most recent
    }

    resp = requests.get(url, params=params, timeout=10)
    soup = BeautifulSoup(resp.text, "html.parser")

    results = []
    for item in soup.select(".s-item")[:max_items]:
        title = item.select_one(".s-item__title")
        price = item.select_one(".s-item__price")
        sold_date = extract_sold_date(item)

        if title and price:
            price_clean = re.sub(r"[^\d.]", "", price.text)

            results.append({
                "title": title.text.strip(),
                "price": float(price_clean) if price_clean else None,
                "sold_date": str(sold_date) if sold_date else None
            })

    return results

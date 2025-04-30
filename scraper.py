from bs4 import BeautifulSoup
import requests
import re
from datetime import datetime

def parse_ebay_sold_page(query, max_items=20):
    url = "https://www.ebay.co.uk/sch/i.html"
    params = {
        "_nkw": query,
        "LH_Sold": "1",
        "LH_Complete": "1",
        "_sop": "13"
    }

    resp = requests.get(url, params=params, timeout=10)
    soup = BeautifulSoup(resp.text, "html.parser")

    results = []
    for item in soup.select(".s-item")[:max_items]:
        title = item.select_one(".s-item__title")
        price = item.select_one(".s-item__price")
        sold = item.select_one(".s-item__title--tagblock")

        if title and price:
            price_clean = re.sub(r"[^\d.]", "", price.text)
            try:
                date_match = re.search(r"Sold (\d{1,2} \w+)", sold.text) if sold else None
                sold_date = datetime.strptime(date_match.group(1) + f" {datetime.now().year}", "%d %b %Y").date() if date_match else None
            except:
                sold_date = None

            results.append({
                "title": title.text.strip(),
                "price": float(price_clean) if price_clean else None,
                "sold_date": str(sold_date) if sold_date else None
            })

    return results

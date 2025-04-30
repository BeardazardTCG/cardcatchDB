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
    soup = BeautifulSoup(resp.text

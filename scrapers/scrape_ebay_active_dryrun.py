import requests
from bs4 import BeautifulSoup
import time
import json
import pandas as pd

# === CONFIG ===
DRY_RUN = True
AUDIT_MODE = True
HEADERS = {'User-Agent': 'Mozilla/5.0'}
INPUT_CSV = "data/scraper_batch_input.csv"

# === SCRAPE FUNCTION ===
def scrape_ebay_active(query):
    base_url = "https://www.ebay.com/sch/i.html"
    params = {
        '_nkw': query,
        'LH_BIN': '1',          # Buy It Now only
        '_sop': '12',           # Newly listed
        '_ipg': '200',          # Max items per page
        '_dmd': '1',            # Gallery view
    }

    response = requests.get(base_url, headers=HEADERS, params=params)
    soup = BeautifulSoup(response.text, 'html.parser')

    listings = soup.select("li.s-item")
    results = []
    audit_log = {
        'query': query,
        'url': response.url,
        'total_found': len(listings),
        'included': [],
        'excluded': []
    }

    for item in listings:

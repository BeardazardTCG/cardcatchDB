import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from archive.scraper import parse_ebay_sold_page
from utils import filter_outliers, calculate_median, calculate_average

query = "charizard ex 11 83 generations"

print(f"\n🔍 Running one-off scrape for: {query}")
results = parse_ebay_sold_page(query, max_items=30)

raw_prices = []
exclusions = []

for item in results:
    title = item.get("title", "")
    price = item.get("price")
    sold_date = item.get("sold_date")
    url = item.get("url")

    if price is None or not sold_date:
        exclusions.append({"reason": "missing data", "title": title, "url": url})
        continue

    raw_prices.append(price)

print(f"\n✅ Found {len(raw_prices)} valid prices")
print("💵 Prices:", raw_prices)

filtered = filter_outliers(raw_prices)
print("\n📊 Filtered Stats:")
print(f"- Median: £{calculate_median(filtered)}")
print(f"- Average: £{calculate_average(filtered)}")
print(f"- Filtered count: {len(filtered)}")

if exclusions:
    print("\n🧹 Exclusions:")
    for e in exclusions:
        print(f"- {e['reason']}: {e['title']} ({e['url']})")

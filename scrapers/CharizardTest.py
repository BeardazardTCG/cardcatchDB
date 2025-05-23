
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from archive.scraper import parse_ebay_sold_page
from utils import filter_outliers, calculate_median, calculate_average

def run_charizard_test():
    query = "charizard ex 11 83 generations"

    print(f"\n🔍 Running filtered scrape for: {query}")
    results = parse_ebay_sold_page(query, max_items=30)

    raw_prices = []
    exclusions = []
    titles = []

    exclusion_keywords = [
        "psa", "cgc", "bgs", "ace", "graded", "gem mint",
        "bulk", "lot", "bundle", "set of", "collection",
        "spanish", "german", "french", "japanese", "italian", "chinese", "portuguese",
        "coin", "pin", "promo tin", "jumbo"
    ]

    for item in results:
        title = item.get("title", "").lower()
        price = item.get("price")
        sold_date = item.get("sold_date")
        url = item.get("url")

        if any(kw in title for kw in exclusion_keywords):
            exclusions.append({"reason": "excluded keyword", "title": title, "url": url})
            continue

        if price is None or not sold_date:
            exclusions.append({"reason": "missing data", "title": title, "url": url})
            continue

        raw_prices.append(price)
        titles.append(title)

    print(f"\n✅ Found {len(raw_prices)} valid prices")
    print("💵 Raw Prices:", raw_prices)

    # Pass 1: filter obvious outliers
    filtered = filter_outliers(raw_prices)

    # Pass 2: remove anything >2x from recalculated median
    refined = []
    median_val = calculate_median(filtered)
    for price in filtered:
        if abs(price - median_val) / median_val <= 2:
            refined.append(price)

    print("\n📊 Final Filtered Stats:")
    print(f"- Median: £{calculate_median(refined)}")
    print(f"- Average: £{calculate_average(refined)}")
    print(f"- Filtered count: {len(refined)}")

    if exclusions:
        print("\n🧹 Exclusions:")
        for e in exclusions:
            print(f"- {e['reason']}: {e['title']} ({e['url']})")

if __name__ == "__main__":
    run_charizard_test()

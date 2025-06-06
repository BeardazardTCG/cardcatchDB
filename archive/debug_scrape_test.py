# debug_scrape_test.py

from scraper import parse_ebay_sold_page

query = "Charizard EX Generations 11/83"
print(f"🔎 Testing query: {query}")

result = parse_ebay_sold_page(query, max_items=20)

print("\n🔗 eBay URL used:")
print(result["url"])

print(f"\n📊 Total raw listings found: {len(result['raw'])}")
print(f"✅ Filtered listings passed: {len(result['filtered'])}")

print("\n🧪 First 5 raw listings:")
for item in result["raw"][:5]:
    print(f"- {item['title']} | £{item['price']} | {item['sold_date']} | {item['holo_type']}")

if not result["raw"]:
    print("⚠️ No raw listings parsed. Likely issue with .s-item or title/price extraction.")

import json
from pathlib import Path
from statistics import mean
from typing import Any
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright

AUTH_STATE_PATH = Path(__file__).resolve().parents[1] / "auth" / "ebay-storage-state.json"
SEARCH_API_PATH = "/sh/research/api/search"

TEST_CASES = [
    {
        "label": "charizard_025_185_cond_4000_sort_date_30d",
        "url": "https://www.ebay.co.uk/sh/research?marketplace=EBAY-UK&keywords=charizard+025+185&dayRange=30&endDate=1779134797822&startDate=1776542797822&categoryId=183454&conditionId=4000&sellerCountry=SellerLocation%3A%3A%3AGB&offset=0&limit=50&sorting=-datelastsold&tabName=SOLD&tz=Europe%2FLondon",
    },
    {
        "label": "charizard_025_185_prize_pack_cond_4000_sort_date_90d",
        "url": "https://www.ebay.co.uk/sh/research?marketplace=EBAY-UK&keywords=charizard+025+185+vivid+voltage+prize+pack&dayRange=90&endDate=1779134853763&startDate=1771362453763&categoryId=183454&conditionId=4000&sellerCountry=SellerLocation%3A%3A%3AGB&offset=0&limit=50&sorting=-datelastsold&tabName=SOLD&tz=Europe%2FLondon",
    },
    {
        "label": "charizard_025_185_non_holo_cond_2750_sort_avg_90d",
        "url": "https://www.ebay.co.uk/sh/research?marketplace=EBAY-UK&keywords=charizard+025+185+vivid+voltage+non-holo&dayRange=90&endDate=1779135098393&startDate=1771362698393&categoryId=183454&conditionId=2750&sellerCountry=SellerLocation%3A%3A%3AGB&offset=0&limit=50&sorting=-avgsalesprice&tabName=SOLD&tz=Europe%2FLondon",
    },
]

EXCLUSION_TERMS = [
    "bundle", "lot", "job lot", "collection", "bulk", "playset", "2x", "3x", "4x", "5x", "6x",
    "proxy", "custom", "digital", "booster", "pack", "box", "tin", "elite trainer", "etb", "case",
    "psa", "bgs", "cgc", "ace", "sgc", "tag", "pristine", "slab", "graded",
]


def looks_like_search_api(url: str) -> bool:
    parsed = urlparse(url)
    return SEARCH_API_PATH in parsed.path


def collect_listings(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    queue = [payload]
    listings: list[dict[str, Any]] = []
    while queue:
        node = queue.pop(0)
        if isinstance(node, dict):
            for key, val in node.items():
                if isinstance(val, list) and key.lower() in {"results", "listings", "items", "itemsummaries"}:
                    listings.extend([x for x in val if isinstance(x, dict)])
                elif isinstance(val, (dict, list)):
                    queue.append(val)
        elif isinstance(node, list):
            queue.extend(node)
    return listings


def extract_title(item: dict[str, Any]) -> str:
    for key in ("title", "name", "itemTitle"):
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def extract_price(item: dict[str, Any]) -> Any:
    for key in ("price", "soldPrice", "averageSoldPrice", "value"):
        value = item.get(key)
        if isinstance(value, (int, float, str)):
            return value
        if isinstance(value, dict):
            for nested_key in ("value", "amount", "convertedValue"):
                nested = value.get(nested_key)
                if isinstance(nested, (int, float, str)):
                    return nested
    return None


def to_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = "".join(ch for ch in value if ch.isdigit() or ch in ".,")
        cleaned = cleaned.replace(",", "")
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def run_diagnostic() -> None:
    if not AUTH_STATE_PATH.exists():
        print(f"Missing auth storage state: {AUTH_STATE_PATH}")
        print("Run scripts/save_ebay_auth_state.py first.")
        return

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            channel="chrome",
            headless=False,
        )
        context = browser.new_context(storage_state=str(AUTH_STATE_PATH))
        page = context.new_page()

        for case in TEST_CASES:
            captured: dict[str, Any] = {}

            def on_response(response):
                if looks_like_search_api(response.url):
                    try:
                        payload = response.json()
                    except Exception:
                        payload = None
                    captured.update(
                        {
                            "url": response.url,
                            "status": response.status,
                            "payload": payload,
                        }
                    )

            context.on("response", on_response)
            page.goto(case["url"], wait_until="domcontentloaded")
            page.wait_for_timeout(5000)
            context.remove_listener("response", on_response)

            print("\n==============================")
            print(f"test label: {case['label']}")

            if not captured:
                print("final API URL captured: (none)")
                print("HTTP status: (none)")
                print("module names: []")
                print("aggregate metrics: {}")
                print("first 10 listing titles: []")
                print("first 10 prices: []")
                print("exclusion counts: {'excluded': 0, 'kept': 0}")
                print("trusted listing count: 0")
                print("rough trusted average: None")
                continue

            payload = captured.get("payload") or {}
            modules = list(payload.keys()) if isinstance(payload, dict) else []
            listings = collect_listings(payload)

            titles = [extract_title(item) for item in listings]
            prices_raw = [extract_price(item) for item in listings]

            trusted_prices: list[float] = []
            excluded = 0
            kept = 0

            for title, price_raw in zip(titles, prices_raw):
                title_l = (title or "").lower()
                if any(term in title_l for term in EXCLUSION_TERMS):
                    excluded += 1
                    continue
                kept += 1
                price_num = to_float(price_raw)
                if price_num is not None:
                    trusted_prices.append(price_num)

            aggregate = payload.get("aggregates") if isinstance(payload, dict) else {}
            if not aggregate and isinstance(payload, dict):
                aggregate = payload.get("summary") or payload.get("metrics") or {}

            print(f"final API URL captured: {captured.get('url')}")
            print(f"HTTP status: {captured.get('status')}")
            print(f"module names: {modules}")
            print(f"aggregate metrics: {aggregate}")
            print(f"first 10 listing titles: {titles[:10]}")
            print(f"first 10 prices: {prices_raw[:10]}")
            print(f"exclusion counts: {{'excluded': {excluded}, 'kept': {kept}}}")
            print(f"trusted listing count: {len(trusted_prices)}")
            print(f"rough trusted average: {round(mean(trusted_prices), 2) if trusted_prices else None}")

        browser.close()


if __name__ == "__main__":
    run_diagnostic()

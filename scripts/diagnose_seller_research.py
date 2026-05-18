import json
from pathlib import Path
from statistics import mean
from typing import Any
from urllib.parse import parse_qs, urlparse

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


def module_query_params(url: str) -> list[str]:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    modules: list[str] = []
    for value in query.get("modules", []):
        modules.extend(part.strip() for part in value.split(",") if part.strip())
    return modules


def payload_type_name(payload: Any) -> str:
    if payload is None:
        return "None"
    return type(payload).__name__


def top_level_keys(payload: Any) -> list[str]:
    if isinstance(payload, dict):
        return list(payload.keys())
    return []


def compact_preview(payload: Any, max_chars: int = 800) -> str:
    try:
        preview = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
    except TypeError:
        preview = repr(payload)
    if len(preview) > max_chars:
        return f"{preview[:max_chars]}..."
    return preview


def collect_listings(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    queue = [payload]
    listings: list[dict[str, Any]] = []
    listing_keys = {"results", "searchresults", "listings", "items", "itemsummaries"}
    while queue:
        node = queue.pop(0)
        if isinstance(node, dict):
            for key, val in node.items():
                if isinstance(val, list) and key.lower() in listing_keys:
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


def listing_score(payload: Any) -> int:
    score = 0
    for item in collect_listings(payload):
        if extract_title(item):
            score += 2
        if extract_price(item) is not None:
            score += 1
    return score


def find_named_dict(payload: Any, names: set[str]) -> dict[str, Any]:
    if not isinstance(payload, (dict, list)):
        return {}
    queue = [payload]
    while queue:
        node = queue.pop(0)
        if isinstance(node, dict):
            for key, val in node.items():
                if key.lower() in names and isinstance(val, dict):
                    return val
                if isinstance(val, (dict, list)):
                    queue.append(val)
        elif isinstance(node, list):
            queue.extend(x for x in node if isinstance(x, (dict, list)))
    return {}


def extract_aggregate_metrics(
    payload: Any, modules: list[str] | None = None
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}

    module_names = {module.lower() for module in modules or []}

    aggregate = find_named_dict(payload, {"aggregates", "aggregate", "summary", "metrics"})
    if aggregate:
        return aggregate

    payload_modules = payload.get("modules")
    if isinstance(payload_modules, dict):
        aggregate = find_named_dict(
            payload_modules, {"aggregates", "aggregate", "summary", "metrics"}
        )
        if aggregate:
            return aggregate

    if "aggregates" in module_names:
        return payload

    return {}


def aggregate_score(payload: Any, modules: list[str]) -> int:
    score = 0
    module_names = {module.lower() for module in modules}
    if "aggregates" in module_names:
        score += 5
    aggregate = extract_aggregate_metrics(payload, modules)
    if aggregate:
        score += 10 + len(aggregate)
    return score


def best_listing_capture(captures: list[dict[str, Any]]) -> dict[str, Any] | None:
    scored = [(listing_score(capture.get("payload")), capture) for capture in captures]
    scored = [item for item in scored if item[0] > 0]
    if not scored:
        return None
    return max(scored, key=lambda item: item[0])[1]


def best_aggregate_capture(captures: list[dict[str, Any]]) -> dict[str, Any] | None:
    scored = [
        (aggregate_score(capture.get("payload"), capture.get("modules", [])), capture)
        for capture in captures
    ]
    scored = [item for item in scored if item[0] > 0]
    if not scored:
        return None
    return max(scored, key=lambda item: item[0])[1]


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
            captures: list[dict[str, Any]] = []

            def on_response(response):
                if not looks_like_search_api(response.url):
                    return

                parse_error = None
                try:
                    payload = response.json()
                except Exception as exc:
                    payload = None
                    parse_error = str(exc)

                captures.append(
                    {
                        "url": response.url,
                        "status": response.status,
                        "modules": module_query_params(response.url),
                        "payload": payload,
                        "parse_error": parse_error,
                    }
                )

            context.on("response", on_response)
            page.goto(case["url"], wait_until="domcontentloaded")
            page.wait_for_timeout(5000)
            context.remove_listener("response", on_response)

            print("\n==============================")
            print(f"test label: {case['label']}")
            print(f"captured Seller Research API responses: {len(captures)}")

            if not captures:
                print("listing API URL: (none)")
                print("aggregate API URL: (none)")
                print("module names: []")
                print("aggregate metrics: {}")
                print("first 10 listing titles: []")
                print("first 10 prices: []")
                print("exclusion counts: {'excluded': 0, 'kept': 0}")
                print("trusted listing count: 0")
                print("rough trusted average: None")
                continue

            for index, capture in enumerate(captures, start=1):
                payload = capture.get("payload")
                listings_in_response = collect_listings(payload)
                print(f"\nresponse #{index}")
                print(f"API URL: {capture.get('url')}")
                print(f"HTTP status: {capture.get('status')}")
                print(f"module query params: {capture.get('modules', [])}")
                print(f"payload type: {payload_type_name(payload)}")
                if isinstance(payload, dict):
                    print(f"top-level keys: {top_level_keys(payload)}")
                if capture.get("parse_error"):
                    print(f"parse error: {capture.get('parse_error')}")
                if not listings_in_response:
                    print(f"compact preview: {compact_preview(payload)}")

            listing_capture = best_listing_capture(captures)
            aggregate_capture = best_aggregate_capture(captures)
            listing_payload = listing_capture.get("payload") if listing_capture else {}
            aggregate_payload = aggregate_capture.get("payload") if aggregate_capture else {}

            listings = collect_listings(listing_payload)
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

            aggregate = extract_aggregate_metrics(
                aggregate_payload,
                aggregate_capture.get("modules", []) if aggregate_capture else [],
            )

            print("\nselected diagnostic responses")
            print(f"listing API URL: {listing_capture.get('url') if listing_capture else '(none)'}")
            print(f"listing modules: {listing_capture.get('modules', []) if listing_capture else []}")
            print(f"aggregate API URL: {aggregate_capture.get('url') if aggregate_capture else '(none)'}")
            print(f"aggregate modules: {aggregate_capture.get('modules', []) if aggregate_capture else []}")
            print(f"aggregate metrics: {aggregate}")
            print(f"first 10 listing titles: {titles[:10]}")
            print(f"first 10 prices: {prices_raw[:10]}")
            print(f"exclusion counts: {{'excluded': {excluded}, 'kept': {kept}}}")
            print(f"trusted listing count: {len(trusted_prices)}")
            print(f"rough trusted average: {round(mean(trusted_prices), 2) if trusted_prices else None}")

        browser.close()


if __name__ == "__main__":
    run_diagnostic()

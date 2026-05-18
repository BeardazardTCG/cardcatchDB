import json
from pathlib import Path
from statistics import mean
from typing import Any
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import sync_playwright

AUTH_STATE_PATH = (
    Path(__file__).resolve().parents[1] / "auth" / "ebay-storage-state.json"
)
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
    "bundle",
    "lot",
    "job lot",
    "collection",
    "bulk",
    "playset",
    "2x",
    "3x",
    "4x",
    "5x",
    "6x",
    "proxy",
    "custom",
    "digital",
    "booster",
    "pack",
    "box",
    "tin",
    "elite trainer",
    "etb",
    "case",
    "psa",
    "bgs",
    "cgc",
    "ace",
    "sgc",
    "tag",
    "pristine",
    "slab",
    "graded",
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
        preview = json.dumps(
            payload, ensure_ascii=False, separators=(",", ":"), default=str
        )
    except TypeError:
        preview = repr(payload)
    if len(preview) > max_chars:
        return f"{preview[:max_chars]}..."
    return preview


def parse_concatenated_json_modules(body: str) -> tuple[list[Any], str | None]:
    decoder = json.JSONDecoder()
    modules: list[Any] = []
    index = 0
    length = len(body)

    while index < length:
        while index < length and body[index].isspace():
            index += 1
        if index >= length:
            break

        try:
            module, next_index = decoder.raw_decode(body, index)
        except json.JSONDecodeError as exc:
            return modules, f"{exc.msg}: line {exc.lineno} column {exc.colno}"

        modules.append(module)
        index = next_index

    return modules, None


def module_type(module: Any) -> str:
    if isinstance(module, dict):
        value = module.get("_type")
        if isinstance(value, str):
            return value
    return ""


def find_module_by_type(modules: list[Any], wanted_type: str) -> dict[str, Any]:
    for module in modules:
        if isinstance(module, dict) and module.get("_type") == wanted_type:
            return module
    return {}


def first_text_span_text(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    text_spans = value.get("textSpans")
    if not isinstance(text_spans, list) or not text_spans:
        return ""
    first_span = text_spans[0]
    if not isinstance(first_span, dict):
        return ""
    text = first_span.get("text")
    return text.strip() if isinstance(text, str) else ""


def nested_get(payload: Any, path: list[str]) -> Any:
    node = payload
    for key in path:
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    return node


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
    title = first_text_span_text(nested_get(item, ["listing", "title"]))
    if title:
        return title

    extended_title = nested_get(item, ["listing", "extendedTitle", "value"])
    if isinstance(extended_title, str) and extended_title.strip():
        return extended_title.strip()

    for key in ("title", "name", "itemTitle"):
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def extract_price(item: dict[str, Any]) -> Any:
    avg_sales_price = first_text_span_text(
        nested_get(item, ["avgsalesprice", "avgsalesprice"])
    )
    if avg_sales_price:
        return avg_sales_price

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


def extract_listing_details(item: dict[str, Any]) -> dict[str, Any]:
    item_id = nested_get(item, ["listing", "itemId", "value"])
    return {
        "title": extract_title(item),
        "price": extract_price(item),
        "items_sold": first_text_span_text(item.get("itemssold")),
        "total_sales": first_text_span_text(item.get("totalsales")),
        "date_last_sold": first_text_span_text(item.get("datelastsold")),
        "item_id": item_id if isinstance(item_id, str) else "",
    }


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


def extract_search_results(module: dict[str, Any]) -> list[dict[str, Any]]:
    results = module.get("results")
    if isinstance(results, list):
        return [result for result in results if isinstance(result, dict)]
    return []


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

    if payload.get("_type") == "ResearchAggregateModule":
        aggregate: dict[str, Any] = {}
        sections = payload.get("sections")
        if not isinstance(sections, list):
            return aggregate

        for section in sections:
            if not isinstance(section, dict):
                continue
            data_items = section.get("dataItems")
            if not isinstance(data_items, list):
                continue
            for data_item in data_items:
                if not isinstance(data_item, dict):
                    continue
                header = first_text_span_text(data_item.get("header"))
                value = first_text_span_text(data_item.get("value"))
                if header:
                    aggregate[header] = value
        return aggregate

    module_names = {module.lower() for module in modules or []}

    aggregate = find_named_dict(
        payload, {"aggregates", "aggregate", "summary", "metrics"}
    )
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

                body = ""
                parse_error = None
                try:
                    body = response.text()
                    parsed_modules, parse_error = parse_concatenated_json_modules(body)
                except Exception as exc:
                    parsed_modules = []
                    parse_error = str(exc)

                module_types = [
                    module_type(module)
                    for module in parsed_modules
                    if module_type(module)
                ]

                captures.append(
                    {
                        "url": response.url,
                        "status": response.status,
                        "modules": module_query_params(response.url),
                        "payload": parsed_modules,
                        "body_preview": body[:800],
                        "parsed_module_count": len(parsed_modules),
                        "module_types": module_types,
                        "research_aggregate_module": find_module_by_type(
                            parsed_modules, "ResearchAggregateModule"
                        ),
                        "search_results_module": find_module_by_type(
                            parsed_modules, "SearchResultsModule"
                        ),
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
                print("parsed module count: 0")
                print("module types found: []")
                print("aggregate metrics: {}")
                print("first 10 listing titles: []")
                print("first 10 prices: []")
                print("exclusion counts: {'excluded': 0, 'kept': 0}")
                print("trusted listing count: 0")
                print("rough trusted average: None")
                continue

            for index, capture in enumerate(captures, start=1):
                search_results_module = capture.get("search_results_module")
                listings_in_response = (
                    extract_search_results(search_results_module)
                    if isinstance(search_results_module, dict)
                    else []
                )
                print(f"\nresponse #{index}")
                print(f"API URL: {capture.get('url')}")
                print(f"HTTP status: {capture.get('status')}")
                print(f"module query params: {capture.get('modules', [])}")
                print(f"parsed module count: {capture.get('parsed_module_count', 0)}")
                print(f"module types found: {capture.get('module_types', [])}")
                if capture.get("parse_error"):
                    print(f"parse error: {capture.get('parse_error')}")
                if not listings_in_response:
                    print(f"body preview: {capture.get('body_preview', '')}")

            listing_capture = next(
                (
                    capture
                    for capture in captures
                    if capture.get("search_results_module")
                ),
                None,
            )
            aggregate_capture = next(
                (
                    capture
                    for capture in captures
                    if capture.get("research_aggregate_module")
                ),
                None,
            )
            search_results_module = (
                listing_capture.get("search_results_module") if listing_capture else {}
            )
            aggregate_module = (
                aggregate_capture.get("research_aggregate_module")
                if aggregate_capture
                else {}
            )

            listings = (
                extract_search_results(search_results_module)
                if isinstance(search_results_module, dict)
                else []
            )
            listing_details = [extract_listing_details(item) for item in listings]
            titles = [details["title"] for details in listing_details]
            prices_raw = [details["price"] for details in listing_details]

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
                aggregate_module,
                aggregate_capture.get("modules", []) if aggregate_capture else [],
            )

            selected_module_types = sorted(
                {
                    module_type
                    for capture in captures
                    for module_type in capture.get("module_types", [])
                }
            )
            parsed_module_count = sum(
                int(capture.get("parsed_module_count", 0)) for capture in captures
            )

            print("\nselected diagnostic responses")
            print(
                f"listing API URL: {listing_capture.get('url') if listing_capture else '(none)'}"
            )
            print(
                f"listing modules: {listing_capture.get('modules', []) if listing_capture else []}"
            )
            print(
                f"aggregate API URL: {aggregate_capture.get('url') if aggregate_capture else '(none)'}"
            )
            print(
                f"aggregate modules: {aggregate_capture.get('modules', []) if aggregate_capture else []}"
            )
            print(f"parsed module count: {parsed_module_count}")
            print(f"module types found: {selected_module_types}")
            print(f"aggregate metrics: {aggregate}")
            print(f"first 10 listing titles: {titles[:10]}")
            print(f"first 10 prices: {prices_raw[:10]}")
            print(f"exclusion counts: {{'excluded': {excluded}, 'kept': {kept}}}")
            print(f"trusted listing count: {len(trusted_prices)}")
            print(
                f"rough trusted average: {round(mean(trusted_prices), 2) if trusted_prices else None}"
            )

        browser.close()


if __name__ == "__main__":
    run_diagnostic()

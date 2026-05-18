import os
import sys
import requests
from bs4 import BeautifulSoup

# Allow importing from repo root and archive scraper module.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARCHIVE_DIR = os.path.join(ROOT, "archive")
if ROOT not in sys.path:
    sys.path.append(ROOT)
if ARCHIVE_DIR not in sys.path:
    sys.path.append(ARCHIVE_DIR)

from scraper import (
    HEADERS,
    build_ebay_url,
    parse_ebay_sold_page,
    parse_ebay_active_page,
)

TEST_CARDS = [
    "Pikachu ex Surging Sparks 238",
    "Zapdos ex 151 202",
    "Charizard ex 151 199",
    "Ethan's Pinsir Destined Rivals 1",
]

LISTING_SELECTORS = [".s-item", ".srp-results .s-item"]
PRICE_SELECTORS = [".s-item__price", "span.s-item__price"]


def detect_reason(status_code, final_url, title, html, listing_count, raw_count, filtered_count, parser_blocked=False):
    lower = html.lower()
    final_url_lower = (final_url or "").lower()
    title_lower = (title or "").lower()
    challenge_markers = [
        "/splashui/challenge",
        "pardon our interruption",
        "captcha",
        "automated access",
        "verify yourself",
        "robot check",
    ]
    is_challenge = parser_blocked or any(m in final_url_lower or m in title_lower or m in lower for m in challenge_markers)
    if is_challenge:
        return "blocked/captcha/challenge"
    if "request_error" in lower and ("403" in lower or "proxyerror" in lower or "forbidden" in lower):
        return "blocked/captcha/403"
    if status_code in (403, 429) or "captcha" in lower or "robot" in lower or "puzzle" in lower:
        return "blocked/captcha/403"
    if listing_count == 0 and len(html.strip()) > 0:
        return "selector mismatch"
    if raw_count > 0 and filtered_count == 0:
        return "parser filtering too strict"
    if raw_count == 0 and listing_count > 0:
        return "parser could not parse listing fields"
    if raw_count == 0 and listing_count == 0:
        return "genuinely no results"
    return "results found"


def diagnose_path(query, sold):
    url = build_ebay_url(query, sold=sold, max_items=30)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        status_code = resp.status_code
        final_url = resp.url
        html = resp.text or ""
    except requests.RequestException as exc:
        status_code = 0
        final_url = url
        html = f"REQUEST_ERROR: {type(exc).__name__}: {exc}"

    soup = BeautifulSoup(html, "html.parser")

    title = soup.title.get_text(strip=True) if soup.title else "(no title)"
    html_head = html[:300].replace("\n", " ").replace("\r", " ")
    listing_count = max(len(soup.select(sel)) for sel in LISTING_SELECTORS)
    price_count = max(len(soup.select(sel)) for sel in PRICE_SELECTORS)

    parsed = parse_ebay_sold_page(query, max_items=30) if sold else parse_ebay_active_page(query, max_items=30)
    raw_count = len(parsed.get("raw", []))
    filtered_count = len(parsed.get("filtered", []))
    parser_blocked = bool(parsed.get("blocked_challenge"))
    has_splashui_challenge = "/splashui/challenge" in (final_url or "").lower()

    reason = detect_reason(status_code, final_url, title, html, listing_count, raw_count, filtered_count, parser_blocked=parser_blocked)

    label = "SOLD" if sold else "ACTIVE"
    print(f"\n--- {label} ---")
    print(f"query used: {query}")
    print(f"final eBay URL: {final_url}")
    print(f"HTTP status code: {status_code}")
    print(f"response length: {len(html)}")
    print(f"page title: {title}")
    print(f"final_url contains /splashui/challenge: {has_splashui_challenge}")
    print(f"first 300 chars: {html_head}")
    print(f"listing container count: {listing_count}")
    print(f"price selector count: {price_count}")
    print(f"parsed raw results: {raw_count}")
    print(f"parsed filtered results: {filtered_count}")
    print(f"parser blocked/challenge signal: {parser_blocked}")
    print(f"diagnosis: {reason}")
    if raw_count == 0 or filtered_count == 0:
        print(f"no results reason: {reason}")

    return {
        "status": status_code,
        "listing_count": listing_count,
        "raw_count": raw_count,
        "filtered_count": filtered_count,
        "blocked_challenge": parser_blocked or has_splashui_challenge,
        "reason": reason,
    }


def likely_diagnosis(results):
    reasons = [r["reason"] for r in results]
    statuses = [r["status"] for r in results]
    if any(r.get("blocked_challenge") for r in results) or any(r == "blocked/captcha/challenge" for r in reasons):
        return "blocked/captcha/challenge"
    if any(code in (403, 429) for code in statuses) or any(r == "blocked/captcha/403" for r in reasons):
        return "blocked/captcha/403"
    if any(r == "selector mismatch" for r in reasons):
        return "selector mismatch"
    if any(r == "parser filtering too strict" for r in reasons):
        return "parser filtering too strict"
    if all(r in ("genuinely no results", "results found") for r in reasons) and any(r == "genuinely no results" for r in reasons):
        return "genuinely no results"
    return "unknown"


def main():
    print("eBay scraper diagnostic mode (read-only; no DB writes)")
    all_results = []
    sold_raw_total = 0
    active_raw_total = 0

    for query in TEST_CARDS:
        print("\n============================")
        print(f"Card: {query}")
        sold_result = diagnose_path(query, sold=True)
        active_result = diagnose_path(query, sold=False)

        sold_raw_total += sold_result["raw_count"]
        active_raw_total += active_result["raw_count"]
        all_results.extend([sold_result, active_result])

    print("\n===== FINAL SUMMARY =====")
    print(f"cards tested: {len(TEST_CARDS)}")
    print(f"sold raw total: {sold_raw_total}")
    print(f"active raw total: {active_raw_total}")
    print(f"likely diagnosis: {likely_diagnosis(all_results)}")


if __name__ == "__main__":
    main()

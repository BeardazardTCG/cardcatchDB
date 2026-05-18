import json
from pathlib import Path

from playwright.sync_api import sync_playwright

AUTH_STATE_PATH = Path(__file__).resolve().parents[1] / "auth" / "ebay-storage-state.json"
RESEARCH_URL = "https://www.ebay.co.uk/sh/research"


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def main() -> None:
    ensure_parent(AUTH_STATE_PATH)
    print("Opening headed browser for eBay Seller Research login...")
    print(f"Navigate target: {RESEARCH_URL}")
    print("Please complete login manually in the opened browser window.")
    print("After login is complete and Seller Research is visible, press Enter here to save auth state.")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        page.goto(RESEARCH_URL, wait_until="domcontentloaded")

        input("\nPress Enter to save storage state... ")

        state = context.storage_state()
        AUTH_STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")

        print(f"Saved Playwright storage state to: {AUTH_STATE_PATH}")
        browser.close()


if __name__ == "__main__":
    main()

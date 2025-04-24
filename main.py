from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import os
import requests

app = FastAPI()

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"message": "CardCatch is live — sandbox mode active."}

@app.get("/price")
def get_price(
    card: str = Query(..., description="Card name"),
    number: str = Query(None, description="Card number"),
    set: str = Query(None, description="Card set"),
    lang: str = Query("en", description="Language code"),
    test: bool = Query(False, description="Return dummy data for testing")
):
    # If in test mode, return static demo data immediately
    if test:
        return {
            "card": card,
            "sold_count": 5,
            "average_price": 12.34,
            "lowest_price": 8.50,
            "highest_price": 16.00,
            "suggested_resale": 13.57
        }

    # Build the search query
    query_parts = [card]
    if number:
        query_parts.append(number)
    if set:
        query_parts.append(set)
    query_parts.append(lang)
    query = " ".join(query_parts)

    # eBay Sandbox FindingService endpoint
    url = "https://svcs.sandbox.ebay.com/services/search/FindingService/v1"
    params = {
        "OPERATION-NAME": "findCompletedItems",
        "SERVICE-VERSION": "1.13.0",
        "SECURITY-APPNAME": os.getenv("EBAY_SANDBOX_APP_ID"),
        "RESPONSE-DATA-FORMAT": "JSON",
        "REST-PAYLOAD": "",
        "keywords": query,
        "siteid": "3",  # UK site
        "paginationInput.entriesPerPage": 10,
        "itemFilter(0).name": "SoldItemsOnly",
        "itemFilter(0).value": "true"
    }

    # Call the eBay Sandbox API
    response = requests.get(url, params=params)
    if response.status_code != 200:
        return {
            "error": "Failed to contact eBay Sandbox",
            "detail": response.text
        }

    try:
        data = response.json()
        items = data["findCompletedItemsResponse"][0]["searchResult"][0].get("item", [])
        prices = []
        for item in items:
            price_info = item["sellingStatus"][0]["currentPrice"][0]
            if price_info.get("@currencyId") == "GBP":
                prices.append(float(price_info.get("__value__", 0)))

        if not prices:
            return {"message": "No mock pricing data found in sandbox."}

        avg_price = round(sum(prices) / len(prices), 2)
        min_price = round(min(prices), 2)
        max_price = round(max(prices), 2)
        suggested_resale = round(avg_price * 1.1, 2)

        return {
            "card": card,
            "sold_count": len(prices),
            "average_price": avg_price,
            "lowest_price": min_price,
            "highest_price": max_price,
            "suggested_resale": suggested_resale
        }
    except Exception as e:
        return {
            "error": "Failed to parse sandbox response",
            "detail": str(e)
        }

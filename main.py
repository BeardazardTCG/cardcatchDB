from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
import requests
import time

app = FastAPI()

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Simple in-memory cache with TTL
cache = {}
CACHE_TTL = 300  # seconds

@app.get("/")
def root():
    return {"message": "CardCatch is live — production mode active."}

@app.get("/price")
def get_price(
    card: str = Query(..., description="Card name to search"),
    number: str = Query(default=None, description="Optional card number"),
    set: str = Query(default=None, description="Optional card set name"),
    lang: str = Query(default="en", description="Optional language code")
):
    # Build a cache key
    cache_key = f"{card}|{number}|{set}|{lang}"
    # Check cache
    entry = cache.get(cache_key)
    if entry and time.time() - entry["timestamp"] < CACHE_TTL:
        return entry["data"]

    # Build search query
    query_parts = [card]
    if number:
        query_parts.append(number)
    if set:
        query_parts.append(set)
    query_parts.append(lang)
    query = " ".join(query_parts)

    # eBay FindingService production endpoint
    url = "https://svcs.ebay.com/services/search/FindingService/v1"
    params = {
        "OPERATION-NAME": "findCompletedItems",
        "SERVICE-VERSION": "1.13.0",
        "SECURITY-APPNAME": os.getenv("EBAY_CLIENT_ID"),  # Production App ID
        "RESPONSE-DATA-FORMAT": "JSON",
        "REST-PAYLOAD": "",
        "keywords": query,
        "siteid": "3",  # eBay UK site
        "paginationInput.entriesPerPage": 20,
        "itemFilter(0).name": "SoldItemsOnly",
        "itemFilter(0).value": "true"
    }

    response = requests.get(url, params=params)
    # Handle rate limit specifically
    if response.status_code == 200:
        text = response.text
        if 'RateLimiter' in text or 'exceeded the number of times' in text:
            return JSONResponse(
                status_code=429,
                content={"error": "rate_limit_exceeded", "message": "eBay API rate limit reached. Please retry after a few minutes."}
            )
    if response.status_code != 200:
        return JSONResponse(
            status_code=502,
            content={"error": "Failed to contact eBay", "detail": response.text}
        )

    try:
        data = response.json()
        items = data.get("findCompletedItemsResponse", [])[0].get("searchResult", [])[0].get("item", [])
        # Extract GBP prices
        prices = [
            float(item["sellingStatus"][0]["currentPrice"][0]["__value__"])
            for item in items
            if item["sellingStatus"][0]["currentPrice"][0]["@currencyId"] == "GBP"
        ]

        if not prices:
            result = {"message": "No UK sold data found for this card."}
        else:
            avg_price = round(sum(prices) / len(prices), 2)
            min_price = round(min(prices), 2)
            max_price = round(max(prices), 2)
            suggested_resale = round(avg_price * 1.1, 2)
            result = {
                "card": card,
                "number": number,
                "set": set,
                "lang": lang,
                "sold_count": len(prices),
                "average_price": avg_price,
                "lowest_price": min_price,
                "highest_price": max_price,
                "suggested_resale": suggested_resale
            }

        # Cache the result
        cache[cache_key] = {"timestamp": time.time(), "data": result}
        return result

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": "Failed to parse eBay response", "detail": str(e)}
        )

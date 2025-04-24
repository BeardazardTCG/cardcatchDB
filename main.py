from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
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
    card: str = Query(...),
    number: str = Query(default=None),
    set: str = Query(default=None),
    lang: str = Query(default="en")
):
    query_parts = [card]
    if number:
        query_parts.append(number)
    if set:
        query_parts.append(set)
    query_parts.append(lang)
    query = " ".join(query_parts)

    url = "https://svcs.sandbox.ebay.com/services/search/FindingService/v1"
    params = {
        "OPERATION-NAME": "findCompletedItems",
        "SERVICE-VERSION": "1.13.0",
        "SECURITY-APPNAME": os.getenv("EBAY_SANDBOX_APP_ID"),
        "RESPONSE-DATA-FORMAT": "JSON",
        "REST-PAYLOAD": "",
        "keywords": query,
        "siteid": "3",
        "paginationInput.entriesPerPage": 10,
        "itemFilter(0).name": "SoldItemsOnly",
        "itemFilter(0).value": "true"
    }

    r = requests.get(url, params=params)
    if r.status_code != 200:
        return {"error": "Failed to contact eBay Sandbox", "detail": r.text}

    try:
        results = r.json()
        items = results["findCompletedItemsResponse"][0]["searchResult"][0].get("item", [])
        prices = [
            float(item["sellingStatus"][0]["currentPrice"][0]["__value__"])
            for item in items
            if item["sellingStatus"][0]["currentPrice"][0]["@currencyId"] == "GBP"
        ]

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
        return {"error": "Failed to parse sandbox response", "detail": str(e)}


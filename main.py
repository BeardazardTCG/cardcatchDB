from fastapi import FastAPI, Request, Query
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
    return {"message": "CardCatch API is live!"}

@app.get("/price")
def get_price(
    card: str = Query(..., description="Card name"),
    number: str = Query(default=None, description="Card number"),
    set: str = Query(default=None, description="Card set name"),
    lang: str = Query(default="en", description="Card language")
):
    query_parts = [card]
    if number:
        query_parts.append(number)
    if set:
        query_parts.append(set)
    query_parts.append(lang)
    query = " ".join(query_parts)

    url = "https://svcs.ebay.com/services/search/FindingService/v1"
    params = {
        "OPERATION-NAME": "findCompletedItems",
        "SERVICE-VERSION": "1.13.0",
        "SECURITY-APPNAME": os.getenv("EBAY_CLIENT_ID"),  # Must be set in Render
        "RESPONSE-DATA-FORMAT": "JSON",
        "REST-PAYLOAD": "",
        "keywords": query,
        "siteid": "3",  # UK eBay
        "paginationInput.entriesPerPage": 20,
        "itemFilter(0).name": "SoldItemsOnly",
        "itemFilter(0).value": "true"
    }

    response = requests.get(url, params=params)
    if response.status_code != 200:
        return JSONResponse(status_code=502, content={"error": "Failed to contact eBay"})

    data = response.json()
    try:
        items = data["findCompletedItemsResponse"][0]["searchResult"][0]["item"]
        prices = [
            float(item["sellingStatus"][0]["currentPrice"][0]["__value__"])
            for item in items
            if item["sellingStatus"][0]["currentPrice"][0]["@currencyId"] == "GBP"
        ]

        if not prices:
            return {"message": "No UK sold data found for this card."}

        avg_price = round(sum(prices) / len(prices), 2)
        min_price = round(min(prices), 2)
        max_price = round(max(prices), 2)
        suggested_resale = round(avg_price * 1.1, 2)

        return {
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
    except:
        return {"error": "Failed to parse eBay data"}

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
    card: str = Query(..., description="Card name"),
    number: str = Query(None, description="Card number"),
    set: str = Query(None, description="Card set"),
    lang: str = Query("en", description="Language code"),
    test: bool = Query(False, description="Return dummy data for testing")
):
    # If test mode, return static demo response
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

    # Sandbox FindingService endpoint
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

    # Call eBay Sandbox API
    r = requests.get(url, params=params)
    if r.status_code != 200:
        return {"error":

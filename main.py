import os
import requests
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

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
def read_root():
    return {"message": "eBay API is live!"}

@app.get("/search")
def search_ebay(q: str = Query(..., description="Search term")):
    EBAY_APP_ID = os.getenv("EBAY_CLIENT_ID")
    if not EBAY_APP_ID:
        return {"error": "Missing eBay credentials"}

    url = "https://svcs.ebay.com/services/search/FindingService/v1"
    params = {
        "OPERATION-NAME": "findItemsByKeywords",
        "SERVICE-VERSION": "1.0.0",
        "SECURITY-APPNAME": EBAY_APP_ID,
        "RESPONSE-DATA-FORMAT": "JSON",
        "REST-PAYLOAD": "",
        "keywords": q,
        "paginationInput.entriesPerPage": 5
    }

    response = requests.get(url, params=params)
    if response.status_code != 200:
        return {"error": "Failed to fetch from eBay"}

    data = response.json()
    try:
        items = data["findItemsByKeywordsResponse"][0]["searchResult"][0]["item"]
        results = [
            {
                "title": item["title"][0],
                "price": item["sellingStatus"][0]["currentPrice"][0]["__value__"],
                "currency": item["sellingStatus"][0]["currentPrice"][0]["@currencyId"]
            }
            for item in items
        ]
        return {"results": results}
    except:
        return {"error": "No results found or bad format"}

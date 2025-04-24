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
    if r.status_code_

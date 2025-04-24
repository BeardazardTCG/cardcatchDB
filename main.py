from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
import requests
from datetime import datetime, timedelta

app = FastAPI()

# Allow cross-origin calls
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory cache for the OAuth token
token_cache = {"access_token": None, "expires_at": datetime.utcnow()}

def get_sandbox_oauth_token():
    global token_cache
    # Return cached token if still valid
    if token_cache["access_token"] and token_cache["expires_at"] > datetime.utcnow():
        return token_cache["access_token"]

    client_id = os.getenv("EBAY_SANDBOX_APP_ID")
    client_secret = os.getenv("EBAY_SANDBOX_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="Missing sandbox OAuth credentials")

    # Request a new token
    url = "https://api.sandbox.ebay.com/identity/v1/oauth2/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope"
    }
    resp = requests.post(url, headers=headers, data=data, auth=(client_id, client_secret))
    if resp.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"Sandbox OAuth failed: {resp.status_code} {resp.text}"
        )

    info = resp.json()
    token_cache["access_token"] = info["access_token"]
    # Subtract 60s to avoid edge-of-expiry
    token_cache["expires_at"] = datetime.utcnow() + timedelta(seconds=info["expires_in"] - 60)
    return token_cache["access_token"]

@app.get("/")
def root():
    return {"message": "CardCatch sandbox + OAuth is live."}

@app.get("/price")
def get_price(
    card: str = Query(..., description="Card name"),
    number: str = Query(None, description="Card number"),
    set: str = Query(None, description="Card set"),
    lang: str = Query("en", description="Language code")
):
    # Build search query
    parts = [card]
    if number:
        parts.append(number)
    if set:
        parts.append(set)
    parts.append(lang)
    query = " ".join(parts)

    # Fetch OAuth token
    token = get_sandbox_oauth_token()

    # Call sandbox Browse API for completed (fixed-price) items in GBP
    url = "https://api.sandbox.ebay.com/buy/browse/v1/item_summary/search"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "q": query,
        "filter": "priceCurrency:GBP,conditions:{NEW|USED},buyingOptions:{FIXED_PRICE}",
        "limit": "20",
        "sort": "-price"
    }
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code != 200:
        return JSONResponse(
            status_code=502,
            content={"error": "Browse API failed", "detail": resp.text}
        )

    data = resp.json().get("itemSummaries", [])
    prices = [float(item["price"]["value"]) for item in data if "price" in item]

    if not prices:
        return {"message": "No sandbox sold data for this query."}

    avg_p = round(sum(prices) / len(prices), 2)
    low_p = round(min(prices), 2)
    high_p = round(max(prices), 2)
    suggested = round(avg_p * 1.1, 2)

    return {
        "card": card,
        "sold_count": len(prices),
        "average_price": avg_p,
        "lowest_price": low_p,
        "highest_price": high_p,
        "suggested_resale": suggested
    }

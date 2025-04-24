from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os, requests

app = FastAPI()

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def fetch_oauth_token(sandbox: bool):
    # Choose credentials & token URL
    if sandbox:
        client_id = os.getenv("EBAY_SANDBOX_APP_ID")
        client_secret = os.getenv("EBAY_SANDBOX_CLIENT_SECRET")
        token_url = "https://api.sandbox.ebay.com/identity/v1/oauth2/token"
    else:
        client_id = os.getenv("EBAY_CLIENT_ID")
        client_secret = os.getenv("EBAY_CLIENT_SECRET")
        token_url = "https://api.ebay.com/identity/v1/oauth2/token"

    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="Missing OAuth credentials")

    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope"
    }
    resp = requests.post(token_url, headers=headers, data=data, auth=(client_id, client_secret))
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"OAuth token fetch failed: {resp.text}")
    return resp.json()["access_token"]

@app.get("/token")
def token_endpoint(
    sandbox: bool = Query(True, description="true=Sandbox, false=Production")
):
    token = fetch_oauth_token(sandbox)
    return {"access_token": token}

@app.get("/price")
def price_lookup(
    card: str = Query(..., description="Card name"),
    number: str = Query(None, description="Card number"),
    set_name: str = Query(None, alias="set", description="Card set"),
    lang: str = Query("en", description="Language code"),
    sandbox: bool = Query(True, description="true=Sandbox, false=Production")
):
    # Build search query
    parts = [card]
    if number:
        parts.append(number)
    if set_name:
        parts.append(set_name)
    parts.append(lang)
    query = " ".join(parts)

    # Always fetch a fresh token
    token = fetch_oauth_token(sandbox)
    headers = {"Authorization": f"Bearer {token}"}

    # Browse API endpoint
    base = "api.sandbox.ebay.com" if sandbox else "api.ebay.com"
    url = f"https://{base}/buy/browse/v1/item_summary/search"
    params = {
        "q": query,
        "filter": "priceCurrency:GBP,conditions:{NEW|USED},buyingOptions:{FIXED_PRICE}",
        "limit": "20",
        "sort": "-price"
    }
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code != 200:
        return JSONResponse(status_code=502, content={"error": "Browse API failed", "detail": resp.text})

    items = resp.json().get("itemSummaries", [])
    prices = [float(item["price"]["value"]) for item in items if "price" in item]

    if not prices:
        return {"message": "No sold data found for this query."}

    avg_price = round(sum(prices) / len(prices), 2)
    min_price = round(min(prices), 2)
    max_price = round(max(prices), 2)
    suggested = round(avg_price * 1.1, 2)

    return {
        "card": card,
        "sold_count": len(prices),
        "average_price": avg_price,
        "lowest_price": min_price,
        "highest_price": max_price,
        "suggested_resale": suggested
    }

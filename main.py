from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta
import os
import requests

app = FastAPI()

# CORS setup\ napp.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Token cache
_token_cache = {"token": None, "expires": datetime.utcnow()}

def _get_app_token():
    global _token_cache
    if _token_cache["token"] and _token_cache["expires"] > datetime.utcnow():
        return _token_cache["token"]

    client_id = os.getenv("EBAY_CLIENT_ID")
    client_secret = os.getenv("EBAY_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="Missing eBay credentials")

    auth = (client_id + ":" + client_secret)
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope"
    }

    resp = requests.post(
        "https://api.ebay.com/identity/v1/oauth2/token",
        headers=headers,
        data=data,
        auth=(client_id, client_secret)
    )
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"OAuth fetch failed: {resp.text}")

    js = resp.json()
    token = js.get("access_token")
    expires = datetime.utcnow() + timedelta(seconds=js.get("expires_in", 3600) - 60)
    _token_cache = {"token": token, "expires": expires}
    return token

@app.get("/")
def root():
    return {"message": "CardCatch API live (Browse API + OAuth)"}

@app.get("/price")
def browse_price(
    card: str = Query(...),
    lang: str = Query(default="en")
):
    token = _get_app_token()
    query = f"{card} {lang}".strip()

    url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    params = {
        "q": query,
        "filter": "priceCurrency:GBP,conditions:{NEW|USED},buyingOptions:{FIXED_PRICE}",
        "limit": "20"
    }

    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code != 200:
        return JSONResponse(status_code=502, content={"error": "Browse API failed", "detail": resp.text})

    data = resp.json().get("itemSummaries", [])
    prices = [float(it["price"]["value"]) for it in data if it.get("price")]
    if not prices:
        return {"message": "No UK pricing data found"}

    avg = round(sum(prices) / len(prices), 2)
    lo = round(min(prices), 2)
    hi = round(max(prices), 2)
    sug = round(avg * 1.1, 2)

    return {
        "card": card,
        "count": len(prices),
        "average": avg,
        "lowest": lo,
        "highest": hi,
        "suggested_resale": sug
    }


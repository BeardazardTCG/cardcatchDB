from fastapi import FastAPI, Query, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Any, Dict
import os
import requests
from datetime import datetime, timedelta

# Initialize app
title = "CardCatch Pricing API"
description = "Fetch sold-item stats from eBay with rich search filters"
version = "2.0.0"
app = FastAPI(title=title, description=description, version=version)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# OAuth token cache
token_cache: Dict[str, Any] = {"access_token": None, "expires_at": datetime.min}
CACHE_BUFFER_SEC = 60

class OAuthToken(BaseModel):
    access_token: str
    expires_at: datetime

class CardQuery(BaseModel):
    card: str = Field(..., description="Name of the card to search")
    number: Optional[str] = Field(None, description="Optional card number")
    set_name: Optional[str] = Field(None, alias="set", description="Optional card set name")
    lang: Optional[str] = Field("en", description="Language code")
    rarity: Optional[str] = Field(None, description="Rarity filter, e.g. 'Rare'")
    condition: Optional[str] = Field(None, description="Condition filter, e.g. 'NEW,USED'")
    buying_options: Optional[str] = Field("FIXED_PRICE", description="Buying options filter, e.g. 'FIXED_PRICE,AUCTION'")
    graded: Optional[bool] = Field(None, description="Filter only graded cards if true")
    grade_agency: Optional[str] = Field(None, description="Grading agency filter, e.g. 'PSA'")

def fetch_oauth_token(sandbox: bool) -> Optional[str]:
    """
    Fetches and caches OAuth token for production; returns None for sandbox stub.
    """
    global token_cache
    if sandbox:
        return None
    if token_cache["access_token"] and token_cache["expires_at"] > datetime.utcnow():
        return token_cache["access_token"]
    client_id = os.getenv("EBAY_CLIENT_ID")
    client_secret = os.getenv("EBAY_CLIENT_SECRET")
    token_url = "https://api.ebay.com/identity/v1/oauth2/token"
    if not client_id or not client_secret:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Missing OAuth credentials")
    resp = requests.post(
        token_url,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type":"client_credentials","scope":"https://api.ebay.com/oauth/api_scope"},
        auth=(client_id, client_secret),
        timeout=10
    )
    if resp.status_code != 200:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Failed to fetch OAuth token")
    info = resp.json()
    token = info.get("access_token")
    expires_in = info.get("expires_in", 3600)
    expires_at = datetime.utcnow() + timedelta(seconds=expires_in - CACHE_BUFFER_SEC)
    token_cache = {"access_token": token, "expires_at": expires_at}
    return token

@app.get("/", summary="Health check")
def health() -> Any:
    return {"message": "CardCatch is live — production mode active."}

@app.get("/token", response_model=OAuthToken, summary="Get OAuth token (production)")
def token_endpoint(sandbox: bool = Query(False, description="true=Sandbox, false=Production")) -> OAuthToken:
    if sandbox:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Sandbox mode has no OAuth token")
    token = fetch_oauth_token(False)
    return OAuthToken(access_token=token, expires_at=token_cache["expires_at"])

@app.get("/price", summary="Get single-card pricing stats")
def price_lookup(
    card: str = Query(..., description="Card name to search"),
    number: Optional[str] = Query(None, description="Card number"),
    set_name: Optional[str] = Query(None, alias="set", description="Card set name"),
    lang: str = Query("en", description="Language code"),
    rarity: Optional[str] = Query(None, description="Rarity filter"),
    condition: Optional[str] = Query(None, description="Condition filter"),
    buying_options: Optional[str] = Query("FIXED_PRICE", description="Buying options filter"),
    graded: Optional[bool] = Query(None, description="Only graded?"),
    grade_agency: Optional[str] = Query(None, description="Grading agency filter"),
    sandbox: bool = Query(True, description="true=Stub sandbox, false=Production"),
    limit: int = Query(20, ge=1, le=100, description="Max items to fetch")
) -> Any:
    # Build search query string
    parts = [card]
    if number: parts.append(number)
    if set_name: parts.append(set_name)
    if rarity: parts.append(rarity)
    if graded: parts.append("Graded")
    parts.append(lang)
    query = " ".join(parts)
    # Sandbox stub response
    if sandbox:
        return {"card": card, "sold_count": 0, "average_price": 0.0, "lowest_price": 0.0, "highest_price": 0.0, "suggested_resale": 0.0}
    # Fetch OAuth token
    token = fetch_oauth_token(False)
    headers = {"Authorization": f"Bearer {token}"}
    url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
    # Build filter
    filters = ["priceCurrency:GBP"]
    if condition:
        conds = condition.replace(" ", "").split(",")
        filters.append(f"conditions:{{{'|'.join(conds)}}}")
    if buying_options:
        opts = buying_options.replace(" ", "").split(",")
        filters.append(f"buyingOptions:{{{'|'.join(opts)}}}")
    if grade_agency:
        filters.append(f"aspectFilter=GradingCompany:{{{grade_agency}}}")
    params = {"q": query, "filter": ",".join(filters), "limit": limit, "sort": "-price"}
    resp = requests.get(url, headers=headers, params=params, timeout=10)
    if resp.status_code != 200:
        return JSONResponse(status_code=status.HTTP_502_BAD_GATEWAY, content={"error": resp.text})
    summaries = resp.json().get("itemSummaries", [])
    prices = [float(it["price"]["value"]) for it in summaries if it.get("price")]
    if not prices:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No sold data found for this query.")
    avg_price = round(sum(prices) / len(prices), 2)
    min_price = round(min(prices), 2)
    max_price = round(max(prices), 2)
    suggested = round(avg_price * 1.1, 2)
    return {"card": card, "sold_count": len(prices), "average_price": avg_price, "lowest_price": min_price, "highest_price": max_price, "suggested_resale": suggested}

@app.post("/bulk-price", summary="Get bulk pricing stats")
def bulk_price(
    queries: List[CardQuery],
    sandbox: bool = Query(True, description="true=Stub sandbox, false=Production"),
    limit: int = Query(20, ge=1, le=100, description="Max items per card")
) -> List[Any]:
    results: List[Any] = []
    for q in queries:
        stats = price_lookup(
            card=q.card, number=q.number, set_name=q.set_name, lang=q.lang,
            rarity=None, condition=q.condition, buying_options=q.buying_options,
            graded=q.graded, grade_agency=q.grade_agency,
            sandbox=sandbox, limit=limit
        )
        results.append(stats)
    return results

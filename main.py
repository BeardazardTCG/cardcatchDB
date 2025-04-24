from fastapi import FastAPI, Query, HTTPException, status, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Any
from functools import lru_cache
import os
import requests
import logging
from datetime import datetime, timedelta

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cardcatch")

app = FastAPI(
    title="CardCatch Pricing API",
    description="Fetches live sold-item stats from eBay via OAuth-protected Browse API",
    version="1.0.0"
)

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class CardQuery(BaseModel):
    card: str = Field(..., description="Name of the card to search")
    number: Optional[str] = Field(None, description="Optional card number")
    set_name: Optional[str] = Field(None, alias="set", description="Optional card set name")
    lang: Optional[str] = Field("en", description="Language code, e.g. 'en'")

class OAuthToken(BaseModel):
    access_token: str
    expires_at: datetime

# Cache token with LRU for size 1
@lru_cache(maxsize=1)
def get_token_cache() -> OAuthToken:
    # Dummy initial
    return OAuthToken(access_token="", expires_at=datetime.utcnow())

def fetch_oauth_token(sandbox: bool) -> str:
    """
    Fetch or refresh OAuth token for sandbox or production.
    Caches token until expiration.
    """
    cache = get_token_cache()
    if cache.access_token and cache.expires_at > datetime.utcnow():
        return cache.access_token

    # Determine credentials
    if sandbox:
        client_id = os.getenv("EBAY_SANDBOX_APP_ID")
        client_secret = os.getenv("EBAY_SANDBOX_CLIENT_SECRET")
        token_url = "https://api.sandbox.ebay.com/identity/v1/oauth2/token"
    else:
        client_id = os.getenv("EBAY_CLIENT_ID")
        client_secret = os.getenv("EBAY_CLIENT_SECRET")
        token_url = "https://api.ebay.com/identity/v1/oauth2/token"

    if not client_id or not client_secret:
        logger.error("Missing OAuth credentials for %s environment", "sandbox" if sandbox else "production")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Missing OAuth credentials")

    payload = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope"
    }
    auth = (client_id, client_secret)
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    resp = requests.post(token_url, data=payload, auth=auth, headers=headers, timeout=10)
    if resp.status_code != 200:
        logger.error("OAuth token fetch failed: %s", resp.text)
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OAuth token fetch failed")

    info = resp.json()
    token = info.get("access_token")
    expires_in = info.get("expires_in", 3600)
    expires_at = datetime.utcnow() + timedelta(seconds=expires_in - 60)

    # Update cache via new instance
    get_token_cache.cache_clear()
    _ = OAuthToken(access_token=token, expires_at=expires_at)
    logger.info("Fetched new OAuth token, expires at %s", expires_at.isoformat())
    return token

@app.get("/", summary="Health check")
def root() -> Any:
    """
    Simple health check endpoint.
    """
    return {"message": "CardCatch is live — production mode active."}

@app.get("/token", response_model=OAuthToken, summary="Get OAuth token")
def token_endpoint(
    sandbox: bool = Query(True, description="Use sandbox environment if true, else production")
) -> OAuthToken:
    """
    Returns a fresh OAuth token with expiry.
    """
    token = fetch_oauth_token(sandbox)
    expires_at = get_token_cache().expires_at
    return OAuthToken(access_token=token, expires_at=expires_at)

@app.get("/price", summary="Get single-card pricing stats")
def price_lookup(
    card: str = Query(..., description="Card name to search"),
    number: Optional[str] = Query(None, description="Card number"),
    set_name: Optional[str] = Query(None, alias="set", description="Card set name"),
    lang: str = Query("en", description="Language code"),
    sandbox: bool = Query(True, description="Use sandbox environment if true, else production"),
    limit: int = Query(20, description="Max items to fetch", ge=1, le=100)
) -> Any:
    """
    Returns sold item count, average, min, max prices, and suggested resale.
    """
    # Build search query
    query_parts = [card]
    if number:
        query_parts.append(number)
    if set_name:
        query_parts.append(set_name)
    query_parts.append(lang)
    query = " ".join(query_parts)

    token = fetch_oauth_token(sandbox)
    headers = {"Authorization": f"Bearer {token}"}
    base = "api.sandbox.ebay.com" if sandbox else "api.ebay.com"
    url = f"https://{base}/buy/browse/v1/item_summary/search"
    params = {
        "q": query,
        "filter": "priceCurrency:GBP,conditions:{NEW|USED},buyingOptions:{FIXED_PRICE}",
        "limit": limit,
        "sort": "-price"
    }
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error("Browse API request failed: %s", str(e))
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Browse API request failed")

    summaries = resp.json().get("itemSummaries", [])
    prices = [float(item["price"]["value"]) for item in summaries if item.get("price")]

    if not prices:
        return JSONResponse(status_code=status.HTTP_204_NO_CONTENT, content={"message": "No sold data found for this query."})

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

@app.post("/bulk-price", summary="Get bulk pricing stats")
def bulk_price(
    queries: List[CardQuery],
    sandbox: bool = Query(True, description="Use sandbox environment if true, else production"),
    limit: int = Query(20, description="Max items per card", ge=1, le=100)
) -> List[Any]:
    """
    Accepts a list of card queries and returns pricing stats for each.
    """
    results: List[Any] = []
    for q in queries:
        stats = price_lookup(
            card=q.card,
            number=q.number,
            set_name=q.set_name,
            lang=q.lang,
            sandbox=sandbox,
            limit=limit
        )
        results.append(stats)
    return results

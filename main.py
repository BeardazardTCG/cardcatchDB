from fastapi import FastAPI, Query, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Any
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
    version="1.0.1"
)

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class OAuthToken(BaseModel):
    access_token: str
    expires_at: datetime

class CardQuery(BaseModel):
    card: str = Field(..., description="Name of the card to search")
    number: Optional[str] = Field(None, description="Optional card number")
    set_name: Optional[str] = Field(None, alias="set", description="Optional card set name")
    lang: Optional[str] = Field("en", description="Language code, e.g. 'en'")

# Global token cache
token_cache: dict = {"access_token": None, "expires_at": datetime.min}
CACHE_BUFFER_SEC = 60

def get_oauth_token(sandbox: bool) -> str:
    """
    Fetches and caches OAuth token for sandbox or production.
    """
    global token_cache
    # Return cached if valid
    if token_cache["access_token"] and token_cache["expires_at"] > datetime.utcnow():
        return token_cache["access_token"]

    # Select credentials
    if sandbox:
        client_id = os.getenv("EBAY_SANDBOX_APP_ID")
        client_secret = os.getenv("EBAY_SANDBOX_CLIENT_SECRET")
        token_url = "https://api.sandbox.ebay.com/identity/v1/oauth2/token"
    else:
        client_id = os.getenv("EBAY_CLIENT_ID")
        client_secret = os.getenv("EBAY_CLIENT_SECRET")
        token_url = "https://api.ebay.com/identity/v1/oauth2/token"

    if not client_id or not client_secret:
        logger.error("Missing OAuth credentials for %s", "sandbox" if sandbox else "production")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Missing OAuth credentials")

    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "client_credentials", "scope": "https://api.ebay.com/oauth/api_scope"}
    try:
        resp = requests.post(token_url, headers=headers, data=data, auth=(client_id, client_secret), timeout=10)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error("OAuth token fetch failed: %s", str(e))
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OAuth token fetch failed")

    info = resp.json()
    token = info.get("access_token")
    expires_in = info.get("expires_in", 3600)
    expires_at = datetime.utcnow() + timedelta(seconds=expires_in - CACHE_BUFFER_SEC)

    token_cache = {"access_token": token, "expires_at": expires_at}
    logger.info("Fetched new OAuth token, expires at %s", expires_at.isoformat())
    return token

@app.get("/", summary="Health check")
def health() -> Any:
    return {"message": "CardCatch is live — production mode active."}

@app.get("/token", response_model=OAuthToken, summary="Get OAuth token")
def token_endpoint(
    sandbox: bool = Query(True, description="true=Sandbox, false=Production")
) -> OAuthToken:
    token = get_oauth_token(sandbox)
    return OAuthToken(access_token=token, expires_at=token_cache["expires_at"])

@app.get("/price", summary="Get single-card pricing stats")
def price_lookup(
    card: str = Query(..., description="Card name to search"),
    number: Optional[str] = Query(None, description="Card number"),
    set_name: Optional[str] = Query(None, alias="set", description="Card set name"),
    lang: str = Query("en", description="Language code"),
    sandbox: bool = Query(True, description="true=Sandbox, false=Production"),
    limit: int = Query(20, ge=1, le=100, description="Max items to fetch")
) -> Any:
    # Build search query
    parts = [card]
    if number: parts.append(number)
    if set_name: parts.append(set_name)
    parts.append(lang)
    query = " ".join(parts)

    token = get_oauth_token(sandbox)
    headers = {"Authorization": f"Bearer {token}"}
    base = "api.sandbox.ebay.com" if sandbox else "api.ebay.com"
    url = f"https://{base}/buy/browse/v1/item_summary/search"
    params = {"q": query, "filter": "priceCurrency:GBP,conditions:{NEW|USED},buyingOptions:{FIXED_PRICE}", "limit": limit, "sort": "-price"}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error("Browse API request failed: %s", str(e))
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Browse API request failed")

    items = resp.json().get("itemSummaries", [])
    prices = [float(it["price"]["value"]) for it in items if it.get("price")]

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
    sandbox: bool = Query(True, description="true=Sandbox, false=Production"),
    limit: int = Query(20, ge=1, le=100, description="Max items per card")
) -> List[Any]:
    results: List[Any] = []
    for q in queries:
        results.append(price_lookup(card=q.card, number=q.number, set_name=q.set_name, lang=q.lang, sandbox=sandbox, limit=limit))
    return results

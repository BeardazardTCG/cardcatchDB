from fastapi import FastAPI, Query, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Any
import os
import requests
import logging
from datetime import datetime, timedelta

# Setup logging
tlogging = logging.getLogger("cardcatch")
logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="CardCatch Pricing API",
    description="Fetch live sold-item stats from eBay with rich search filters",
    version="2.0.0"
)

# Enable CORS
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
    rarity: Optional[str] = Field(None, description="Optional rarity filter, e.g. 'Rare'")
    condition: Optional[str] = Field(None, description="Condition filter, comma-separated: NEW,USED")
    buying_options: Optional[str] = Field("FIXED_PRICE", description="Buying options filter, comma-separated: FIXED_PRICE,AUCTION")
    graded: Optional[bool] = Field(None, description="Filter only graded cards if true")
    grade_agency: Optional[str] = Field(None, description="Grading agency filter, e.g. 'PSA', 'Beckett'")

# Token cache
token_cache: dict = {"access_token": None, "expires_at": datetime.min}
CACHE_BUFFER_SEC = 60

def get_oauth_token(sandbox: bool) -> Optional[str]:
    """
    Returns a cached OAuth token for production, stub for sandbox.
    """
    global token_cache
    if sandbox:
        return None  # no OAuth needed for stub sandbox

    if token_cache["access_token"] and token_cache["expires_at"] > datetime.utcnow():
        return token_cache["access_token"]

    # Production OAuth fetch
    client_id = os.getenv("EBAY_CLIENT_ID")
    client_secret = os.getenv("EBAY_CLIENT_SECRET")
    token_url = "https://api.ebay.com/identity/v1/oauth2/token"

    if not client_id or not client_secret:
        logging.error("Missing production OAuth credentials")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Missing OAuth credentials")

    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "client_credentials", "scope": "https://api.ebay.com/oauth/api_scope"}
    try:
        resp = requests.post(token_url, headers=headers, data=data, auth=(client_id, client_secret), timeout=10)
        resp.raise_for_status()
    except requests.RequestException as e:
        logging.error("OAuth token fetch failed: %s", e)
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OAuth token fetch failed")

    info = resp.json()
    token = info.get("access_token")
    expires_in = info.get("expires_in", 3600)
    expires_at = datetime.utcnow() + timedelta(seconds=expires_in - CACHE_BUFFER_SEC)

    token_cache = {"access_token": token, "expires_at": expires_at}
    logging.info("Fetched new OAuth token, expires at %s", expires_at)
    return token

@app.get("/", summary="Health check")
def health() -> Any:
    """
    Health check.
    """
    return {"message": "CardCatch is live — production mode active."}

@app.get("/token", response_model=OAuthToken, summary="Get OAuth token (production only)")
def token_endpoint(
    sandbox: bool = Query(False, description="false=Production only")
) -> OAuthToken:
    """
    Returns a cached or fresh OAuth token for production.
    """
    if sandbox:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Sandbox does not support OAuth tokens")
    token = get_oauth_token(False)
    return OAuthToken(access_token=token, expires_at=token_cache["expires_at"])

@app.get("/price", summary="Get single-card pricing stats")
def price_lookup(
    card: str = Query(..., description="Card name to search"),
    number: Optional[str] = Query(None, description="Card number"),
    set_name: Optional[str] = Query(None, alias="set", description="Card set name"),
    lang: str = Query("en", description="Language code"),
    rarity: Optional[str] = Query(None, description="Rarity filter"),
    condition: Optional[str] = Query(None, description="Condition filter, comma-separated: NEW,USED"),
    buying_options: Optional[str] = Query("FIXED_PRICE", description="Buying options filter, comma-separated"),
    graded: Optional[bool] = Query(None, description="Filter only graded cards if true"),
    grade_agency: Optional[str] = Query(None, description="Grading agency filter"),
    sandbox: bool = Query(True, description="true=Stub sandbox, false=Production live data"),
    limit: int = Query(20, ge=1, le=100, description="Max items to fetch")
) -> Any:
    """
    Returns UK GBP sold-item stats with advanced filters.
    """
    # Build query string
    qparts = [card]
    if number: qparts.append(number)
    if set_name: qparts.append(set_name)
    if rarity: qparts.append(rarity)
    if graded: qparts.append("Graded")
    qparts.append(lang)
    query = " ".join(qparts)

    if sandbox:
        # Stub sandbox response
        return {"card": card, "sold_count": 0, "average_price": 0.0, "lowest_price": 0.0, "highest_price": 0.0, "suggested_resale": 0.0}

    # Production flow
    token = get_oauth_token(False)
    headers = {"Authorization": f"Bearer {token}"}
    url = "https://api.ebay.com/buy/browse/v1/item_summary/search"

    # Build filter parameter
    filters = ["priceCurrency:GBP"]
    if condition:
        conds = [c.strip() for c in condition.split(",")]
        filters.append(f"conditions:{{{'|'.join(conds)}}}")
    else:
        filters.append("conditions:{NEW|USED}")
    if buying_options:
        opts = [o.strip() for o in buying_options.split(",")]
        filters.append(f"buyingOptions:{{{'|'.join(opts)}}}")
    if grade_agency:
        filters.append(f"aspectFilter=GradingCompany:{{{grade_agency}}}")

    filter_param = ",".join(filters)

    params = {"q": query, "filter": filter_param, "limit": limit, "sort": "-price"}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as e:
        logging.error("Browse API request failed: %s", e)
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Browse API request failed")

    items = resp.json().get("itemSummaries", [])
    prices = [float(it["price"]["value"]) for it in items if it.get("price")]

    if not prices:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No sold data found for this query.")

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

@app.post("/bulk-price", summary="Get bulk pricing stats")
def bulk_price(
    queries: List[CardQuery],
    rarity: Optional[str] = Query(None),
    condition: Optional[str] = Query(None),
    buying_options: Optional[str] = Query("FIXED_PRICE"),
    graded: Optional[bool] = Query(None),
    grade_agency: Optional[str] = Query(None),
    lang: str = Query("en"),
    sandbox: bool = Query(True),
    limit: int = Query(20, ge=1, le=100)
) -> List[Any]:
    """
    Bulk pricing with advanced filters.
    """
    results: List[Any] = []
    for q in queries:
        stats = price_lookup(
            card=q.card,
            number=q.number,
            set_name=q.set_name,
            lang=lang,
            rarity=rarity,
            condition=condition,
            buying_options=buying_options,
            graded=graded,
            grade_agency=grade_agency,
            sandbox=sandbox,
            limit=limit
        )
        results.append(stats)
    return results

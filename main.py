from fastapi import FastAPI, Query, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Any, Dict
import os
import requests
from datetime import datetime, timedelta

# Initialize FastAPI app
app = FastAPI(
    title="CardCatch Pricing API",
    description="Fetch sold-item stats from eBay with rich search filters",
    version="2.0.1"
)

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
    set_name: Optional[str] = Field(None, alias="set", description="Card set name")
    lang: Optional[str] = Field("en", description="Language code")
    rarity: Optional[str] = Field(None, description="Rarity filter")
    condition: Optional[str] = Field(None, description="Condition filter (NEW,USED)")
    buying_options: Optional[str] = Field("FIXED_PRICE", description="Buying options (FIXED_PRICE,AUCTION)")
    graded: Optional[bool] = Field(None, description="Only graded? True/False")
    grade_agency: Optional[str] = Field(None, description="Grading agency (PSA)")

def fetch_oauth_token(sandbox: bool) -> Optional[str]:
    global token_cache
    if sandbox:
        return None
    if token_cache["access_token"] and token_cache["expires_at"] > datetime.utcnow():
        return token_cache["access_token"]
    client_id = os.getenv("EBAY_CLIENT_ID")
    client_secret = os.getenv("EBAY_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Missing OAuth credentials")
    token_url = "https://api.ebay.com/identity/v1/oauth2/token"
    resp = requests.post(
        token_url,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials", "scope": "https://api.ebay.com/oauth/api_scope"},
        auth=(client_id, client_secret), timeout=10
    )
    if resp.status_code != 200:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OAuth token fetch failed")
    info = resp.json()
    token = info.get("access_token")
    expires_in = info.get("expires_in", 3600)
    expires_at = datetime.utcnow() + timedelta(seconds=expires_in - CACHE_BUFFER_SEC)
    token_cache.update({"access_token": token, "expires_at": expires_at})
    return token

@app.get("/", summary="Health check")
def health() -> Any:
    return {"message": "CardCatch is live — production mode active."}

@app.get("/token", response_model=OAuthToken, summary="Get OAuth token (production)")
def token_endpoint(sandbox: bool = Query(False)) -> OAuthToken:
    if sandbox:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Sandbox mode has no OAuth token")
    token = fetch_oauth_token(False)
    return OAuthToken(access_token=token, expires_at=token_cache["expires_at"])

@app.get("/price", summary="Get single-card pricing stats")
def price_lookup(
    card: str = Query(...),
    number: Optional[str] = Query(None),
    set_name: Optional[str] = Query(None, alias="set"),
    lang: str = Query("en"),
    rarity: Optional[str] = Query(None),
    condition: Optional[str] = Query(None),
    buying_options: Optional[str] = Query("FIXED_PRICE"),
    graded: Optional[bool] = Query(None),
    grade_agency: Optional[str] = Query(None),
    sandbox: bool = Query(True),
    limit: int = Query(20, ge=1, le=100)
) -> Any:
    parts = [card]
    if number: parts.append(number)
    if set_name: parts.append(set_name)
    if rarity: parts.append(rarity)
    if graded: parts.append("Graded")
    parts.append(lang)
    q = " ".join(parts)
    if sandbox:
        return {"card": card, "sold_count": 0, "average_price": 0.0, "lowest_price": 0.0, "highest_price": 0.0, "suggested_resale": 0.0}
    token = fetch_oauth_token(False)
    headers = {"Authorization": f"Bearer {token}"}
    url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
    filters = ["priceCurrency:GBP"]
    if condition:
        conds = condition.replace(" ", "").split(",")
        filters.append(f"conditions:{{{'|'.join(conds)}}}")
    if buying_options:
        opts = buying_options.replace(" ", "").split(",")
        filters.append(f"buyingOptions:{{{'|'.join(opts)}}}")
    if grade_agency:
        filters.append(f"aspectFilter=GradingCompany:{{{grade_agency}}}")
    params = {"q": q, "filter": ",".join(filters), "limit": limit, "sort": "-price"}
    resp = requests.get(url, headers=headers, params=params, timeout=10)
    if resp.status_code != 200:
        return JSONResponse(status_code=status.HTTP_502_BAD_GATEWAY, content={"error": resp.text})
    items = resp.json().get("itemSummaries", [])
    prices = [float(i["price"]["value"]) for i in items if i.get("price")]
    if not prices:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No sold data found for this query.")
    avg = round(sum(prices)/len(prices), 2)
    lo = round(min(prices), 2)
    hi = round(max(prices), 2)
    suggestion = round(avg * 1.1, 2)
    return {"card": card, "sold_count": len(prices), "average_price": avg, "lowest_price": lo, "highest_price": hi, "suggested_resale": suggestion}

@app.post("/bulk-price", summary="Get bulk pricing stats")
def bulk_price(
    queries: List[CardQuery],
    sandbox: bool = Query(True),
    limit: int = Query(20, ge=1, le=100)
) -> List[Any]:
    results: List[Any] = []
    for q in queries:
        try:
            stats = price_lookup(
                card=q.card, number=q.number, set_name=q.set_name,
                lang=q.lang, rarity=q.rarity, condition=q.condition,
                buying_options=q.buying_options, graded=q.graded,
                grade_agency=q.grade_agency, sandbox=sandbox, limit=limit
            )
        except HTTPException as e:
            results.append({"card": q.card, "error": e.detail})
            continue
        results.append(stats)
    return results

@app.get("/scraped-price", summary="Scrape sold listings from eBay UK")
def scraped_price(query: str, max_items: int = 20) -> Any:
    from scraper import parse_ebay_sold_page
    try:
        return parse_ebay_sold_page(query, max_items=max_items)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/getCardPrice", summary="Get filtered median sold price using scraper")
def get_card_price(query: str) -> Any:
    from scraper import getCardPrice
    try:
        return getCardPrice(
            query=query,
            includes=[],
            excludes=["lot", "bundle", "proxy"]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sold-history", summary="Get sold prices and dates for recent listings")
def sold_history(query: str) -> Any:
    from scraper import getSoldDataByDate
    try:
        return getSoldDataByDate(
            query=query,
            includes=[],
            excludes=["lot", "bundle", "proxy"]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


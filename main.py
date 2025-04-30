from fastapi import FastAPI, Query, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional, Any
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
token_cache = {"access_token": None, "expires_at": datetime.min}
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

# OAuth token retrieval function
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

# Health check endpoint
@app.get("/", summary="Health check")
def health() -> Any:
    return {"message": "CardCatch is live — production mode active."}

# Scraped data for sold prices
@app.get("/scraped-price", summary="Scrape sold listings from eBay UK")
def scraped_price(query: str, max_items: int = 20) -> Any:
    from scraper import getSoldDataByDate
    try:
        results = getSoldDataByDate(query, max_items=max_items)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Get sold history from eBay listings
@app.get("/api/sold-history", summary="Get sold prices and dates for recent listings")
def sold_history(query: str) -> Any:
    from scraper import getSoldDataByDate
    try:
        return getSoldDataByDate(
            query=query,
            includes=[],  # Optional filters
            excludes=["lot", "bundle", "proxy"]  # Excluding irrelevant results
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


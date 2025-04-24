from fastapi import FastAPI, Query, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Any, Dict
import os, requests, logging
from datetime import datetime, timedelta
import pandas as pd
import io
import matplotlib.pyplot as plt

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cardcatch")

app = FastAPI(
    title="CardCatch Pricing API",
    description="Fetch sold-item stats from eBay with advanced insights",
    version="3.0.0"
)

# Enable CORS\ napp.add_middleware(
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
    card: str = Field(..., description="Name of the card")
    number: Optional[str] = Field(None, description="Card number")
    set_name: Optional[str] = Field(None, alias="set", description="Card set name")
    lang: Optional[str] = Field("en", description="Language code")
    rarity: Optional[str] = Field(None, description="Rarity filter")
    condition: Optional[str] = Field(None, description="Condition filter (NEW,USED)")
    buying_options: Optional[str] = Field("FIXED_PRICE", description="Buying options (FIXED_PRICE,AUCTION)")
    graded: Optional[bool] = Field(None, description="Only graded? True/False")
    grade_agency: Optional[str] = Field(None, description="Grading agency (PSA, Beckett)")

# Global token cache
token_cache: Dict[str, Any] = {"access_token": None, "expires_at": datetime.min}
CACHE_BUFFER_SEC = 60

# In-memory time-series store (mock)
_ts_store: Dict[str, List[Dict[str, Any]]] = {}


def record_sale(card: str, price: float):
    """
    Record a sale timestamp for time-series insights.
    """
    now = datetime.utcnow().date()
    _ts_store.setdefault(card, []).append({"date": now, "price": price})


def fetch_oauth_token(sandbox: bool) -> Optional[str]:
    if sandbox:
        return None
    global token_cache
    if token_cache["access_token"] and token_cache["expires_at"] > datetime.utcnow():
        return token_cache["access_token"]
    client_id = os.getenv("EBAY_CLIENT_ID")
    client_secret = os.getenv("EBAY_CLIENT_SECRET")
    token_url = "https://api.ebay.com/identity/v1/oauth2/token"
    if not client_id or not client_secret:
        logger.error("Missing OAuth credentials")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Missing credentials")
    resp = requests.post(
        token_url,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type":"client_credentials","scope":"https://api.ebay.com/oauth/api_scope"},
        auth=(client_id, client_secret), timeout=10
    )
    resp.raise_for_status()
    info = resp.json()
    token = info["access_token"]
    expires_at = datetime.utcnow() + timedelta(seconds=info.get("expires_in",3600) - CACHE_BUFFER_SEC)
    token_cache.update({"access_token": token, "expires_at": expires_at})
    return token

@app.get("/", summary="Health check")
def health() -> Any:
    return {"message": "CardCatch is live — production mode active."}

@app.get("/token", response_model=OAuthToken, summary="Get OAuth token (production)")
def token_endpoint(sandbox: bool = Query(False)) -> OAuthToken:
    if sandbox:
        raise HTTPException(status_code=400, detail="Sandbox does not support OAuth tokens")
    token = fetch_oauth_token(False)
    return OAuthToken(access_token=token, expires_at=token_cache["expires_at"])

@app.get("/price", summary="Get single-card pricing stats")
def price_lookup(
    card: str = Query(...), number: Optional[str] = Query(None), set_name: Optional[str] = Query(None, alias="set"),
    lang: str = Query("en"), rarity: Optional[str] = Query(None), condition: Optional[str] = Query(None),
    buying_options: Optional[str] = Query("FIXED_PRICE"), graded: Optional[bool] = Query(None),
    grade_agency: Optional[str] = Query(None), sandbox: bool = Query(True), limit: int = Query(20)
) -> Any:
    # Build query and call Browse API
    # ... existing code ...
    # Mock recording for insights
    # record_sale(card, avg_price)
    return {...}

@app.get("/history", summary="Get price history trend")
def history(
    card: str = Query(...), days: int = Query(30, ge=1, le=365), chart: bool = Query(False)
):
    data = pd.DataFrame(_ts_store.get(card, []))
    if data.empty:
        raise HTTPException(status_code=404, detail="No history available")
    df = data.groupby("date").price.mean().reindex(
        pd.date_range(end=datetime.utcnow().date(), periods=days), fill_value=None
    ).ffill()
    slope = ..., r2 = ...  # compute regression
    if chart:
        fig, ax = plt.subplots()
        df.plot(ax=ax)
        buf = io.BytesIO(); fig.savefig(buf, format="png"); buf.seek(0)
        return StreamingResponse(buf, media_type="image/png")
    return {"dates": [d.isoformat() for d in df.index], "avg_prices": df.tolist(), "trend": {"slope": slope, "r2": r2}}

@app.get("/distribution", summary="Get price distribution")
def distribution(card: str = Query(...), bins: int = Query(10, ge=1, le=50)) -> Any:
    data = pd.DataFrame(_ts_store.get(card, []))
    if data.empty:
        raise HTTPException(status_code=404, detail="No data for distribution")
    prices = data.price
    hist, edges = np.histogram(prices, bins=bins)
    return {"bins": edges.tolist(), "counts": hist.tolist(), "std": prices.std(), "cv": prices.std()/prices.mean()}

# ... compare and forecast endpoints similarly ...

            limit=limit
        )
        results.append(stats)
    return results

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os, requests
from datetime import datetime, timedelta

app = FastAPI()

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory cache for OAuth token
token_cache = {"access_token": None, "expires_at": datetime.utcnow()}

def get_oauth_token(sandbox: bool = True):
    global token_cache
    # Return cached if still valid
    if token_cache["access_token"] and token_cache["expires_at"] > datetime.utcnow():
        return token_cache["access_token"]

    # Select credentials & token URL
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
    data =

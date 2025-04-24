from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
import requests
from datetime import datetime, timedelta

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

token_cache = {
    "access_token": None,
    "expires_at": None
}

@app.get("/")
def root():
    return {"message": "CardCatch is live — token test only."}

@app.get("/token")
def get_token():
    global token_cache
    if token_cache["access_token"] and token_cache["expires_at"] > datetime.utcnow():
        return {"token": token_cache["access_token"], "status": "cached"}

    client_id = os.getenv("EBAY_CLIENT_ID")
    client_secret = os.getenv("EBAY_CLIENT_SECRET")

    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope"
    }

    response = requests.post(
        "https://api.ebay.com/identity/v1/oauth2/token",
        headers=headers,
        data=data,
        auth=(client_id, client_secret)
    )

    if response.status_code != 200:
        return JSONResponse(status_code=502, content={"error": "OAuth token fetch failed", "detail": response.text})

    token_info = response.json()
    token_cache["access_token"] = token_info["access_token"]
    token_cache["expires_at"] = datetime.utcnow() + timedelta(seconds=token_info["expires_in"] - 30)

    return {
        "token": token_info["access_token"],
        "expires_in": token_info["expires_in"],
        "status": "fresh"
    }

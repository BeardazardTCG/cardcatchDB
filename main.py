from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from hashlib import sha256
import os

app = FastAPI()

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"message": "eBay API is live!"}

@app.get("/marketplace-deletion")
def verify_challenge(challenge_code: str = None):
    verification_token = os.getenv("EBAY_VERIFICATION_TOKEN")
    endpoint_url = "https://cardcatch-ebay-endpointnew.onrender.com/marketplace-deletion"

    if not challenge_code:
        raise HTTPException(status_code=400, detail="Missing challenge_code")

    combined = challenge_code + verification_token + endpoint_url
    hashed = sha256(combined.encode("utf-8")).hexdigest()
    
    return JSONResponse(content={"challengeResponse": hashed}, media_type="application/json")

@app.post("/marketplace-deletion")
async def handle_deletion(request: Request):
    data = await request.json()
    print("Received deletion notification:", data)
    return {"status": "received"}

from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
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

@app.post("/marketplace-deletion")
async def handle_deletion(
    request: Request,
    x_ebay_verification_token: str = Header(default=None)
):
    expected_token = os.getenv("EBAY_VERIFICATION_TOKEN")
    if x_ebay_verification_token != expected_token:
        raise HTTPException(status_code=401, detail="Invalid verification token")

    data = await request.json()

    # If it's a verification challenge
    if "challenge" in data:
        return {"challenge": data["challenge"]}

    # Otherwise handle deletion notice
    print("Received deletion notification:", data)
    return {"status": "received"}

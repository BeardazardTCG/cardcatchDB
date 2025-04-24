from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import os

app = FastAPI()

# CORS
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
async def handle_marketplace_deletion(request: Request):
    data = await request.json()
    verification_token = os.getenv("EBAY_VERIFICATION_TOKEN")

    if "challenge" in data:
        return {"challenge": data["challenge"]}

    print("Received deletion notification:", data)
    return {"status": "received"}

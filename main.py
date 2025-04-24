from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

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

    # Step 1: respond to eBay challenge
    if "challenge" in data:
        return {"challenge": data["challenge"]}

    # Step 2: Handle actual deletion notification
    print("Received deletion notification:", data)
    return {"status": "received"}

    return {"status": "received"}

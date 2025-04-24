from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
import requests

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
    return {"message": "CardCatch is live — sandbox mode active."}

@app.get("/price")
def get_price(
    card: str = Query(...),
    number: str = Query(default=None),
    set: str = Query(default=None),
    lang: str = Query(default="en")
):
    query_parts = [card]
    if number:

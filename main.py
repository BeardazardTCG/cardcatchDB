import requests
from fastapi import FastAPI, HTTPException
from datetime import datetime

app = FastAPI()

# Your eBay API credentials (make sure to replace this with your own API key)
EBAY_API_KEY = "YOUR_EBAY_API_KEY"  # Replace this with your actual eBay API key

# Define the eBay API URL to get item summaries
EBAY_API_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

# FastAPI route to scrape eBay listings for a specific Pokémon card
@app.get("/scrape")
async def scrape_card_sales(card_name: str, limit: int = 20):
    headers = {
        "Authorization": f"Bearer {EBAY_API_KEY}"  # Add your eBay API key here
    }
    params = {
        "q": card_name,  # The card name you're searching for (e.g., "Charizard")
        "filter": "sold:true",  # Only show sold listings
        "limit": limit  # Limit the number of results to fetch
    }

    # Make a request to eBay's API to fetch the data
    response = requests.get(EBAY_API_URL, headers=headers, params=params)
    
    # Check for API errors
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="Failed to fetch eBay data")
    
    # Parse the JSON response
    data = response.json()
    sold_items = data.get("itemSummaries", [])
    
    sales_data = []  # Prepare a list to store the card sales da_

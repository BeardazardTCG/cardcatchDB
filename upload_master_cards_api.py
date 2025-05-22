# upload_master_cards_api.py
# ✅ Clean upload script — sends master card data to your live API

import os
import pandas as pd
import requests
from dotenv import load_dotenv

# === Load .env values ===
load_dotenv()
API_URL = "https://cardcatchdb.onrender.com/bulk-upsert-master-cards"
API_KEY = os.getenv("API_KEY")

if not API_KEY:
    raise RuntimeError("❌ API_KEY not found in .env")

# === Excel source file
EXCEL_FILE = "CardBrain_Master.xlsx"
SHEET_NAME = "Master Card Library"

# === Load Excel data
df = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME)

# === Validate required columns
expected_columns = [
    "Unique ID", "Card Name", "Set Name", "Card Number",
    "Card ID", "Full Query", "Tier", "Status", "High Demand Boost"
]

for col in expected_columns:
    if col not in df.columns:
        raise ValueError(f"❌ Missing required column: {col}")

# === Prepare cards for upload
cards = []
for _, row in df.iterrows():
    try:
        card = {
            "unique_id": int(row["Unique ID"]),
            "card_name": str(row["Card Name"]).strip(),
            "set_name": str(row["Set Name"]).strip(),
            "card_number": str(row["Card Number"]).strip() if pd.notna(row["Card Number"]) else None,
            "card_id": str(row["Card ID"]).strip(),
            "query": str(row["Full Query"]).strip(),
            "tier": str(row["Tier"]).strip() if pd.notna(row["Tier"]) else None,
            "status": str(row["Status"]).strip() if pd.notna(row["Status"]) else None,
            "high_demand_boost": str(row["High Demand Boost"]).strip() if pd.notna(row["High Demand Boost"]) else None
        }
        cards.append(card)
    except Exception as e:
        print(f"❌ Skipping row due to error: {e}")

# === Upload in chunks
def upload_cards(cards):
    chunk_size = 75
    headers = {"x-api-key": API_KEY}
    for i in range(0, len(cards), chunk_size):
        chunk = cards[i:i+chunk_size]
        try:
            response = requests.post(API_URL, json=chunk, headers=headers)
            if response.status_code == 200:
                print(f"✅ Uploaded {len(chunk)} cards.")
            else:
                print(f"❌ Error on batch {i//chunk_size + 1}: {response.text}")
        except Exception as e:
            print(f"❌ Failed to upload batch {i//chunk_size + 1} due to error: {e}")

# === Run
if __name__ == "__main__":
    print(f"📦 Preparing to upload {len(cards)} cards to CardCatch API...")
    upload_cards(cards)

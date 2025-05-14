import pandas as pd
import requests

# Your live Render API endpoint
API_URL = "https://cardcatchdb.onrender.com/bulk-upsert-master-cards"

# Read the Excel file
df = pd.read_excel("CardBrain_Master.xlsx", sheet_name="Master Card Library")

# Expected columns
expected_columns = [
    "Unique ID", "Card Name", "Set Name", "Card Number",
    "Card ID", "Full Query", "Tier", "Status", "High Demand Boost"
]

# Validate columns
for col in expected_columns:
    if col not in df.columns:
        raise ValueError(f"Missing required column: {col}")

# Prepare cards
cards = []
for _, row in df.iterrows():
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

# Upload in chunks
def upload_cards(cards):
    chunk_size = 250
    for i in range(0, len(cards), chunk_size):
        chunk = cards[i:i+chunk_size]
        response = requests.post(API_URL, json=chunk)
        if response.status_code == 200:
            print(f"✅ Uploaded/Updated {len(chunk)} cards.")
        else:
            print(f"❌ Error: {response.text}")

if __name__ == "__main__":
    print(f"Preparing to upload {len(cards)} cards...")
    upload_cards(cards)

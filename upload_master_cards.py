# upload_master_cards.py
# ⚠️ ARCHIVAL SCRIPT — one-time use for manually uploading cards from Excel

import os
import pandas as pd
from sqlmodel import SQLModel, create_engine, Session
from models import MasterCard

# === Setup database connection (sync mode for script use)
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

# Convert async URL to sync format
DATABASE_URL_SYNC = DATABASE_URL.replace("postgresql+asyncpg", "postgresql")
engine = create_engine(DATABASE_URL_SYNC)

# === Excel source file
EXCEL_FILE = "CardBrain_Master.xlsx"
SHEET_NAME = "Master Card Library"

# === Load data
try:
    df = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME)
except Exception as e:
    raise RuntimeError(f"❌ Failed to load Excel file: {e}")

# === Validate expected columns
expected_columns = [
    "Unique ID", "Card Name", "Set Name", "Card Number",
    "Card ID", "Full Query", "Tier", "Status", "High Demand Boost"
]

for col in expected_columns:
    if col not in df.columns:
        raise ValueError(f"❌ Missing required column: {col}")

# === Map rows into MasterCard models
cards_to_insert = []
for _, row in df.iterrows():
    card = MasterCard(
        unique_id=row["Unique ID"],
        card_name=row["Card Name"],
        set_name=row["Set Name"],
        card_number=str(row["Card Number"]) if pd.notna(row["Card Number"]) else None,
        card_id=row["Card ID"],
        query=row["Full Query"],
        tier=str(row["Tier"]) if pd.notna(row["Tier"]) else None,
        status=row["Status"],
        high_demand_boost=str(row["High Demand Boost"]) if pd.notna(row["High Demand Boost"]) else None,
    )
    cards_to_insert.append(card)

# === Insert into the database
print(f"📥 Preparing to insert {len(cards_to_insert)} cards...")

SQLModel.metadata.create_all(engine)

try:
    with Session(engine) as session:
        session.add_all(cards_to_insert)
        session.commit()
    print("✅ All cards uploaded successfully.")
except Exception as e:
    print(f"❌ Upload failed: {e}")

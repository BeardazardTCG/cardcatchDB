# upload_master_cards.py

import pandas as pd
from sqlmodel import SQLModel, create_engine, Session
from models import MasterCard
import os

# Load environment variable
DATABASE_URL = os.getenv("DATABASE_URL")

# Connect to the database
engine = create_engine(DATABASE_URL)

# Read the Excel file
df = pd.read_excel("CardBrain_Master.xlsx", sheet_name="Master Card Library")

# Columns we expect in your spreadsheet:
expected_columns = [
    "Unique ID", "Card Name", "Set Name", "Card Number",
    "Card ID", "Full Query", "Tier", "Status", "High Demand Boost"
]

# Validate columns
for col in expected_columns:
    if col not in df.columns:
        raise ValueError(f"Missing required column: {col}")

# Map rows into MasterCard models
cards_to_insert = []
for _, row in df.iterrows():
    card = MasterCard(
        unique_id=row["Unique ID"],
        card_name=row["Card Name"],
        set_name=row["Set Name"],
        card_number=str(row["Card Number"]),
        card_id=row["Card ID"],
        query=row["Full Query"],
        tier=str(row["Tier"]) if pd.notna(row["Tier"]) else None,
        status=row["Status"],
        high_demand_boost=str(row["High Demand Boost"]) if pd.notna(row["High Demand Boost"]) else None
    )
    cards_to_insert.append(card)

# Insert into the database
print(f"Inserting {len(cards_to_insert)} cards...")
SQLModel.metadata.create_all(engine)
with Session(engine) as session:
    session.add_all(cards_to_insert)
    session.commit()

print("✅ All cards uploaded successfully!")

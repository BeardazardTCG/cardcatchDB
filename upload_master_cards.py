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

# Only load the required columns
df = df[
    ["Unique ID", "Card Name", "Set Name", "Card Number", "Card ID", "Full Query", "Tier", "Status", "High Demand Boost"]
]

# Map rows into MasterCard models
cards_to_insert = []
for _, row in df.iterrows():
    card = MasterCard(
        unique_id=int(row["Unique ID"]),
        card_name=str(row["Card Name"]),
        set_name=str(row["Set Name"]),
        card_number=str(row["Card Number"]) if pd.notna(row["Card Number"]) else None,
        card_id=str(row["Card ID"]),
        query=str(row["Full Query"]),
        tier=str(row["Tier"]) if pd.notna(row["Tier"]) else None,
        status=str(row["Status"]) if pd.notna(row["Status"]) else None,
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

import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import select
from models import MasterCard
import asyncio
import os

# ✅ Adjust this if your CSV is somewhere else in your repo
CSV_PATH = "./data/query_6-2025-05-19_94543.csv"

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

async def find_unlogged_ids():
    # Load logged UIDs from CSV
    logged_df = pd.read_csv(CSV_PATH)
    logged_ids = set(logged_df["unique_id"].dropna().astype(int))

    async with async_session() as session:
        result = await session.execute(select(MasterCard.unique_id))
        all_ids = set(row[0] for row in result.all())

    missing_ids = sorted(list(all_ids - logged_ids))

    print(f"🧮 Total in Master: {len(all_ids)}")
    print(f"🧾 Total Logged: {len(logged_ids)}")
    print(f"❌ Missing: {len(missing_ids)}")

    with open("unlogged_cards.txt", "w") as f:
        for uid in missing_ids:
            f.write(f"{uid}\n")

    print("📄 Saved to unlogged_cards.txt")

if __name__ == "__main__":
    asyncio.run(find_unlogged_ids())

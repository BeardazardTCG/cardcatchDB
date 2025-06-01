import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not set in .env")

engine = create_engine(DATABASE_URL)
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT NOW()"))
        print("✅ Connected! Time:", result.scalar())
except Exception as e:
    print("❌ DB connection failed:", e)

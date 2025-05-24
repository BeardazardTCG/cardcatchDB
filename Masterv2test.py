from sqlmodel import SQLModel, Session, create_engine
from models.models import MasterCardV2

engine = create_engine("postgresql://postgres:ckQFRJkrJluWsJnHsDhlhvbtSridadDF@metro.proxy.rlwy.net:52025/railway")

card = MasterCardV2(
    unique_id="test123",
    card_name="Test Card",
    card_number="001/100",
    card_number_raw="001",
    rarity="Rare",
    type="Fire",
    artist="AI GPT",
    language="en",
    set_name="Test Set",
    set_code="TST",
    release_date="2025-01-01",
    series="Test Series",
    set_logo_url=None,
    set_symbol_url=None,
    query="Test Card Test Set 001/100",
    set_id="tst1",
    types=["Fire"],
    hot_character=False,
    card_image_url=None,
    subtypes=["Basic"],
    supertype="Pokémon"
)

with Session(engine) as session:
    session.add(card)
    session.commit()
    print("✅ Inserted test card")

import os
from sqlalchemy import create_engine

user = os.getenv("DB__USER", "postgres")
password = os.getenv("DB__PASSWORD", "postgres")
host = os.getenv("DB_SERVICE_NAME", "localhost")
port = os.getenv("DB_INTERNAL_PORT", "5432")
name = os.getenv("DB__NAME", "fly_ai_db")

db_url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{name}"
print(f"Connecting to {db_url}")

try:
    engine = create_engine(db_url)
    with engine.connect() as conn:
        print("Success")
except Exception as e:
    print("Failed:", e)

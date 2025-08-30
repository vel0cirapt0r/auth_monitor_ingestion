import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
from typing import Optional

load_dotenv()

APP_HOST = os.getenv("APP_HOST", "127.0.0.1")
APP_PORT = int(os.getenv("APP_PORT", 8000))
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# DB vars are optional at load (API doesn't need them); worker will check at runtime
DB_NAME: Optional[str] = os.getenv("DATABASE_NAME")
DB_USER: Optional[str] = os.getenv("DATABASE_USER")
DB_PASSWORD: Optional[str] = os.getenv("DATABASE_PASSWORD")
DB_HOST: Optional[str] = os.getenv("DATABASE_HOST")
DB_PORT: Optional[str] = os.getenv("DATABASE_PORT", "5432")

# Construct DATABASE_URL only when needed (e.g., in worker)
def get_database_url() -> str:
    if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST]):
        raise ValueError("Missing required database environment variables")
    encoded_password = quote_plus(DB_PASSWORD)
    return f"postgres+asyncpg://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Constants
SCHEMA_VERSION = 1
APP_VERSION = "0.1.0"
STREAM_KEY = "ingest.v1"
CONSUMER_GROUP = "ingest_workers"
MAX_ITEMS = 100
MIN_ITEMS = 1
MAX_BODY_SIZE = 5 * 1024 * 1024  # 5 MB
ALLOWED_PROTOCOLS = {"rps", "pms", "css", "dss"}

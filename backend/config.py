import os
from dotenv import load_dotenv

load_dotenv()

# Gemini
GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")

# Groq (server-side silent fallback only)
GROQ_API_KEY: str = os.getenv("GROQ_API_KEY", "")

# SQLite
DATABASE_PATH: str = os.getenv("DATABASE_PATH", "data/database.db")

# Dataset table name (fixed — dropped and recreated on each upload)
DATASET_TABLE: str = "dataset"

# Upload limits
MAX_CSV_SIZE_MB: int = int(os.getenv("MAX_CSV_SIZE_MB", "50"))
MAX_CSV_SIZE_BYTES: int = MAX_CSV_SIZE_MB * 1024 * 1024

# SQL safety — only these statement types are permitted
ALLOWED_SQL_PREFIXES: tuple[str, ...] = ("select",)

# Blocked SQL keywords (case-insensitive check)
BLOCKED_SQL_KEYWORDS: list[str] = [
    "drop", "delete", "insert", "update", "alter", "create",
    "truncate", "replace", "attach", "detach", "pragma",
    "--", ";--", "/*", "*/",
]

"""
Configuration settings for the Instalily Case Study application.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Base project directory
BASE_DIR = Path(__file__).parent.parent

# Data paths
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DATA_PATH = os.path.join(DATA_DIR, "raw_contractors.json")
PROCESSED_DATA_PATH = os.path.join(DATA_DIR, "processed_contractors.json")
INSIGHTS_DATA_PATH = os.path.join(DATA_DIR, "insights.json")

# Create necessary directories
os.makedirs(DATA_DIR, exist_ok=True)

# Scraper settings
SCRAPER_DEFAULT_ZIP = "10013"
SCRAPER_DEFAULT_DISTANCE = 25
SCRAPER_DEFAULT_RATE_LIMIT = 10
SCRAPER_DEFAULT_TIMEOUT = 30000  # 30 seconds

# API Keys (loaded from environment variables)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    print("WARNING: OPENAI_API_KEY not found in environment variables.")

# OpenAI API settings
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4")
OPENAI_TEMPERATURE = float(os.getenv("OPENAI_TEMPERATURE", "0.1"))
OPENAI_MAX_TOKENS = int(os.getenv("OPENAI_MAX_TOKENS", "2048"))

# Proxies (if needed)
PROXY_FILE = os.getenv("PROXY_FILE", None)
PROXIES = []
if PROXY_FILE and os.path.exists(PROXY_FILE):
    with open(PROXY_FILE, 'r') as f:
        PROXIES = [line.strip() for line in f if line.strip()]

# Backend API settings
API_HOST = os.getenv("API_HOST", "127.0.0.1")
API_PORT = int(os.getenv("API_PORT", "8000"))
API_DEBUG = os.getenv("API_DEBUG", "True").lower() in ("true", "1", "t", "yes")

# Database settings (SQLite by default)
DB_PATH = os.path.join(BASE_DIR, "db", "contractors.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{DB_PATH}")

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.path.join(BASE_DIR, "logs", "app.log")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# Define which fields to extract from raw data
CONTRACTOR_FIELDS = [
    "name",
    "rating",
    "address",
    "phone",
    "certifications",
    "description",
    "website",
    "source",
    "zip_code",
]

# Define processors for specific fields (optional)
FIELD_PROCESSORS = {
    "rating": lambda x: float(x) if x and x != "N/A" else None,
    "certifications": lambda x: x if isinstance(x, list) else [],
}
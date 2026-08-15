import os
from dotenv import load_dotenv

load_dotenv()

API_KEY: str = os.getenv("PSEUDOGRAM_API_KEY", "")
BASE_URL: str = "https://pseudogram-api.onrender.com"
DB_PATH: str = os.getenv("DB_PATH", "linkplease.db")

# Rate limit: 10 requests per rolling 60 seconds
RATE_LIMIT_CALLS: int = 10
RATE_LIMIT_WINDOW: float = 60.0  # seconds

# Retry config for DM sending
MAX_ATTEMPTS: int = 6            # 1 initial + 5 retries
BACKOFF_BASE: float = 2.0        # seconds; delay = BACKOFF_BASE ^ attempt

# Reconciler: how often to check accepted-but-unconfirmed DMs
RECONCILER_INTERVAL: float = 30.0  # seconds

"""
Configuration module for the funded companies scraper.
Loads API keys from environment variables.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# API Keys (set via environment variables or .env file)
SEC_API_KEY = os.getenv("SEC_API_KEY", "")
PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY", "")
CLAY_WEBHOOK_URL = os.getenv("CLAY_WEBHOOK_URL", "")

# API Endpoints
SEC_FORM_D_ENDPOINT = "https://api.sec-api.io/form-d"
PERPLEXITY_API_ENDPOINT = "https://api.perplexity.ai/chat/completions"

# Rate Limiting (seconds between requests)
SEC_API_DELAY = 0.5  # 2 requests/second (conservative to avoid rate limiting)
PERPLEXITY_API_DELAY = 1.0  # 1 request/second

# Pagination
SEC_API_PAGE_SIZE = 50  # Max allowed by SEC API

# Date range for fetching (days)
LOOKBACK_DAYS = 30

# Clay webhook batch size (1 = send one company at a time)
CLAY_BATCH_SIZE = 1

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds, will be multiplied by attempt number


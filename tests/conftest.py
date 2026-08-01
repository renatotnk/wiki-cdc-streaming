"""Shared test setup: load .env once for the whole test session (convention
9.8) -- only test_ingestion_smoke.py needs real values; a no-op otherwise.
"""

from dotenv import load_dotenv

load_dotenv()

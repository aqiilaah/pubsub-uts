import pytest
import pytest_asyncio
import os
from httpx import AsyncClient
from httpx import ASGITransport
from typing import AsyncGenerator

from src.main import app  

DB_FILE = "dedup.db" 

@pytest_asyncio.fixture(scope="function")
async def api_client() -> AsyncGenerator[AsyncClient, None]:
    print(f"\n--- Menyiapkan tes ---")
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print(f"Removed old DB: {DB_FILE}")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        
        yield client

    print(f"Tes selesai")
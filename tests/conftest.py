"""Pytest configuration and fixtures."""

import asyncio
import os
import tempfile
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from httpx import AsyncClient

# Set test environment variables before imports
os.environ["DATABASE_PATH"] = ":memory:"
os.environ["ENCRYPTION_KEY_FILE"] = "/tmp/test_encryption.key"
os.environ["PUBLIC_URL"] = "http://localhost:3000"


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="function")
def test_encryption_key():
    """Create a temporary encryption key for tests."""
    from app.encryption import generate_encryption_key

    key = generate_encryption_key()

    # Write to temp file
    with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".key") as f:
        f.write(key)
        key_path = f.name

    os.environ["ENCRYPTION_KEY_FILE"] = key_path

    yield key

    # Cleanup
    if os.path.exists(key_path):
        os.remove(key_path)


@pytest_asyncio.fixture
async def test_db():
    """Create a test database."""
    from app.database import get_database, close_database, init_schema, _db_connection
    import app.database as db_module

    # Reset the global connection
    db_module._db_connection = None

    # Create in-memory database
    db = await get_database()
    await init_schema(db)

    yield db

    await close_database()
    db_module._db_connection = None


@pytest.fixture
def client():
    """Create a test client for the FastAPI app."""
    from app.main import app

    with TestClient(app) as c:
        yield c


@pytest_asyncio.fixture
async def async_client():
    """Create an async test client."""
    from app.main import app

    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac


@pytest.fixture
def mock_google_api(mocker):
    """Mock Google API calls."""
    mock_service = mocker.MagicMock()

    # Mock calendar list
    mock_service.calendarList().list().execute.return_value = {
        "items": [
            {
                "id": "primary",
                "summary": "Primary Calendar",
                "primary": True,
                "accessRole": "owner",
            },
            {
                "id": "work@example.com",
                "summary": "Work Calendar",
                "accessRole": "owner",
            },
        ]
    }

    # Mock events list
    mock_service.events().list().execute.return_value = {
        "items": [],
        "nextSyncToken": "test_sync_token",
    }

    mocker.patch(
        "googleapiclient.discovery.build",
        return_value=mock_service,
    )

    return mock_service

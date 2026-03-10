"""
Shared fixtures for all tests.

Strategy:
  - The FastAPI lifespan (DB create_all, OS index init) is replaced with a
    no-op so tests need no running infrastructure.
  - get_session is overridden via dependency_overrides to inject a mock session.
  - api.os_client is patched at the module level with an AsyncMock.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

import api
from api import app


# ---------------------------------------------------------------------------
# Lifespan — skip DB migrations and OS index creation in tests
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _noop_lifespan(application):
    yield


@pytest.fixture(autouse=True)
def patch_lifespan(monkeypatch):
    monkeypatch.setattr(app.router, "lifespan_context", _noop_lifespan)


# ---------------------------------------------------------------------------
# OpenSearch mock
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_os():
    """Patch api.os_client with an AsyncMock for the duration of the test."""
    mock = AsyncMock()
    with patch.object(api, "os_client", mock):
        yield mock


# ---------------------------------------------------------------------------
# Database session mock
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session():
    mock = AsyncMock()
    # add / add_all are synchronous on AsyncSession — override so no coroutine warning
    mock.add = MagicMock()
    mock.add_all = MagicMock()
    return mock


@pytest.fixture
def with_session(mock_session):
    """Override the get_session dependency with mock_session."""
    async def _override():
        yield mock_session

    app.dependency_overrides[api.get_session] = _override
    yield mock_session
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------

@pytest.fixture
async def client(with_session, mock_os):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# Result helpers
# ---------------------------------------------------------------------------

def scalars(*items):
    """Mimics session.execute(...).scalars().all() / .one()"""
    result = MagicMock()
    result.scalars.return_value.all.return_value = list(items)
    result.scalars.return_value.one.return_value = items[0] if items else None
    return result

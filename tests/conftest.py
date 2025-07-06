"""
Test configuration and fixtures.
"""

import asyncio
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock

import redis.asyncio as redis
from src.models import ProviderConfig, AuthConfig, ProviderType
from src.sync_engine import SyncEngine
from src.rate_limiter import RateLimiterManager
from src.schema_transformer import SchemaTransformer


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def redis_client():
    """Mock Redis client for testing."""
    mock_redis = AsyncMock(spec=redis.Redis)
    
    # Setup common Redis operations
    mock_redis.ping.return_value = True
    mock_redis.zadd.return_value = 1
    mock_redis.zrem.return_value = 1
    mock_redis.zcard.return_value = 0
    mock_redis.llen.return_value = 0
    mock_redis.lpush.return_value = 1
    mock_redis.get.return_value = "0"
    mock_redis.incrby.return_value = 1
    mock_redis.expire.return_value = True
    mock_redis.hmget.return_value = [None, None]
    mock_redis.hmset.return_value = True
    mock_redis.eval.return_value = 1
    mock_redis.bzpopmin.return_value = None
    mock_redis.zrange.return_value = []
    mock_redis.lrange.return_value = []
    
    return mock_redis


@pytest.fixture
def sample_provider_config():
    """Sample provider configuration for testing."""
    return ProviderConfig(
        name="test_provider",
        provider_type=ProviderType.SALESFORCE,
        base_url="https://api.test.com",
        rate_limit_per_minute=1000,
        burst_limit=100,
        timeout_seconds=30,
        max_retries=3,
        auth_config=AuthConfig(
            auth_type="oauth2",
            credentials={
                "client_id": "test_client_id",
                "client_secret": "test_client_secret"
            },
            token_url="https://api.test.com/oauth2/token"
        )
    )


@pytest_asyncio.fixture
async def sync_engine(redis_client):
    """Sync engine instance for testing."""
    engine = SyncEngine(redis_client)
    return engine


@pytest_asyncio.fixture
async def rate_limiter(redis_client):
    """Rate limiter instance for testing."""
    return RateLimiterManager(redis_client)


@pytest.fixture
def schema_transformer():
    """Schema transformer instance for testing."""
    return SchemaTransformer()
